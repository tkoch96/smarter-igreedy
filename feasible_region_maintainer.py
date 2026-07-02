import math
import numpy as np
from scipy.optimize import minimize
from typing import Optional

from utils import LatLon, get_distance
from probabilistic_helpers import (
    gaussian_nll,
    mean_absolute_residual,
    GLOBAL_SIGMA_MS,
    GAUSSIAN_NOISE,
    STUDENT_T_NOISE,
    ASYMMETRIC_NOISE,
    ProbConstraint,
)

TARGET_OF_INTEREST = '85.93.215.0'

EARTH_RADIUS_KM = 6371.0


def _normalize_latlon(lat: float, lon: float) -> tuple[float, float]:
    """
    Wrap coordinates back onto the globe (lat ∈ [-90, 90], lon ∈ [-180, 180)).

    Nelder-Mead optimises lat/lon as unconstrained reals; the trig-based
    haversine is periodic, so the optimiser happily converges to off-globe
    parameterisations (lat=125°, lat=223°, ...) that are wrap-equivalent for
    distance evaluation but garbage for every other consumer.  Crossing a
    pole flips to the antimeridian.
    """
    lat = (lat + 90.0) % 360.0 - 90.0     # into [-90, 270)
    if lat > 90.0:
        lat = 180.0 - lat                 # walked over a pole
        lon += 180.0
    lon = (lon + 180.0) % 360.0 - 180.0
    return lat, lon


def _destination_point(loc: 'LatLon', bearing_rad: float, dist_km: float) -> 'LatLon':
    """Great-circle destination: start at loc, travel dist_km along bearing."""
    lat1 = np.radians(loc[0])
    lon1 = np.radians(loc[1])
    delta = dist_km / EARTH_RADIUS_KM
    lat2 = np.arcsin(
        np.sin(lat1) * np.cos(delta)
        + np.cos(lat1) * np.sin(delta) * np.cos(bearing_rad)
    )
    lon2 = lon1 + np.arctan2(
        np.sin(bearing_rad) * np.sin(delta) * np.cos(lat1),
        np.cos(delta) - np.sin(lat1) * np.sin(lat2),
    )
    lon_deg = (float(np.degrees(lon2)) + 540.0) % 360.0 - 180.0
    return (float(np.degrees(lat2)), lon_deg)

# Hard-circle mode: (vp_location, max_radius_km)
HardConstraint = tuple[LatLon, float]
# Gaussian mode:   (vp_location, sigma_ms, rtt_ms)
# ProbConstraint imported from probabilistic_helpers

HARD_CIRCLE = 'hard_circle'
GAUSSIAN    = 'gaussian'
EM_GAUSSIAN = 'em_gaussian'

# EM (adaptive gaussian) settings
EM_ITERS = 4               # E/M alternations per measurement update
EM_MU_PRIOR_STRENGTH = 3.0     # pseudo-measurements anchoring μ to the prior
EM_SIGMA_PRIOR_STRENGTH = 1.0  # lighter anchor for σ (σ is inert for MAP)
EM_SIGMA_FLOOR_MS = 0.5    # don't let fitted σ collapse to zero
EM_MU_BOUNDS = (1.0, 2.0)  # physics: RTT can't beat SOL; >2× is implausible
RADIUS_MULTIPLIER = 1.05   # hard-circle safety slack on top of the slope model
KM_PER_MS = 100.0          # speed-of-light floor in fiber, 1ms RTT ≈ 100km
# Predictive RTT model shared by both modes: expected rtt = slope × d / 100.
# slope = 1.0 would mean pure SOL, which real fiber never achieves; routing
# overhead makes ~1.1–1.3× realistic.  Both the synthetic ground truth
# (tests) and the estimators default to this value.
DEFAULT_SLOPE = 1.3


class FeasibleRegion:
    """
    Tracks the estimated geographic region for a target based on RTT constraints.

    Both modes share one predictive RTT model, expected rtt = slope × d/100
    (slope defaults to DEFAULT_SLOPE = 1.3 — realistic fiber overhead over
    SOL).  They differ in how they treat deviations from it:

    mode='hard_circle'  — each RTT becomes a maximum-radius circle at the
                          model-implied distance × radius_multiplier;
                          Nelder-Mead minimises a penalty that fires when the
                          point falls outside any circle.  A measurement that
                          beats the slope makes the truth INFEASIBLE.

    mode='gaussian'     — each RTT contributes a Gaussian log-likelihood term
                          around the model prediction; Nelder-Mead finds the
                          MAP estimate.  Slope-beating measurements and
                          outliers are unlikely, not impossible.

    mode='em_gaussian'  — gaussian, but the per-target slope μ (and noise σ)
                          are UNKNOWN and fitted online: each measurement
                          update alternates MAP location with a
                          prior-anchored least-squares refit of (μ, σ) from
                          the residuals.  `slope` holds the current μ.

    The public API (add_measurement, add_measurements_batch, get_region_size,
    get_location, clone, distance_to) is identical in both modes.
    add_measurement accepts an optional sigma_ms argument that is used only
    in gaussian mode.
    """

    def __init__(
        self,
        target_id: str,
        prior_guess: LatLon = (0.0, 0.0),
        mode: str = HARD_CIRCLE,
        radius_multiplier: float = RADIUS_MULTIPLIER,
        slope: float = DEFAULT_SLOPE,
        noise_model: str = GAUSSIAN_NOISE,
    ) -> None:
        self.target_id = target_id
        self.mode = mode
        # Predictive RTT model: expected rtt = slope × d / KM_PER_MS.
        # hard_circle inverts it (radius = implied distance × multiplier);
        # gaussian computes residuals against it.  In em_gaussian mode,
        # `slope` is the CURRENT per-target μ estimate, re-fitted after
        # every measurement; `prior_slope` anchors the fit at low counts.
        self.slope = slope
        self.prior_slope = slope
        self.fitted_sigma_ms = GLOBAL_SIGMA_MS
        # Per-residual likelihood shape for the soft modes (gaussian /
        # em_gaussian): GAUSSIAN_NOISE, STUDENT_T_NOISE (heavy-tailed,
        # outlier-robust) or ASYMMETRIC_NOISE (steep below the model,
        # forgiving above — matches one-sided RTT overhead dynamics).
        # Ignored by hard_circle mode.
        self.noise_model = noise_model
        self.radius_multiplier = radius_multiplier
        self.best_guess: np.ndarray = np.array(prior_guess)
        # hard_circle: list[HardConstraint]   (vp_loc, max_radius_km)
        # gaussian:    list[ProbConstraint]   (vp_loc, sigma_ms, rtt_ms)
        self.constraints: list = []
        self._cached_region_size: Optional[float] = None

    # ------------------------------------------------------------------
    # Predictive RTT model (shared by both modes)
    # ------------------------------------------------------------------

    def expected_rtt_ms(self, dist_km: float) -> float:
        """Model-predicted RTT for a target at distance dist_km."""
        return self.slope * dist_km / KM_PER_MS

    def implied_distance_km(self, rtt_ms: float) -> float:
        """Model-implied distance for an observed RTT (inverse of the above)."""
        return rtt_ms * KM_PER_MS / self.slope

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_measurement(
        self,
        vp_loc: LatLon,
        min_rtt: float,
        sigma_ms: float = GLOBAL_SIGMA_MS,
    ) -> None:
        """Add a single RTT measurement and update the location estimate."""
        if self.target_id == TARGET_OF_INTEREST:
            print(f"[{self.target_id}] add_measurement vp={vp_loc} rtt={min_rtt:.2f}ms")
        self._append_constraint(vp_loc, min_rtt, sigma_ms)
        self._update_estimate()

    def add_measurements_batch(
        self,
        measurements: list[tuple[LatLon, float]],
        sigma_ms: float = GLOBAL_SIGMA_MS,
    ) -> None:
        """Batch-add measurements, re-optimising only once at the end."""
        for vp_loc, min_rtt in measurements:
            self._append_constraint(vp_loc, min_rtt, sigma_ms)
        self._update_estimate()

    def get_region_size(self) -> float:
        """
        Uncertainty proxy for the current estimate, in km for BOTH modes, so
        callers can threshold it (e.g. BASICALLY_GEOLOCATED in the greedy)
        without knowing which mode they hold.

        hard_circle: largest feasible displacement from the estimate (km).
                     Lower = tighter.
        gaussian:    mean absolute RTT residual at the current estimate,
                     converted to km-equivalent via the slope model
                     (× KM_PER_MS / slope).  Lower = more confident.  A
                     single constraint yields the model-implied distance
                     (rtt × 100 / slope), directly comparable to
                     hard-circle's implied-distance × multiplier radius.

        Returns 20037.0 (half Earth's circumference) when there are no
        constraints.
        """
        if self._cached_region_size is not None:
            return self._cached_region_size

        if not self.constraints:
            return 20037.0

        if self.mode == HARD_CIRCLE:
            result = self._hard_circle_region_size()
        else:
            result = (mean_absolute_residual(self.get_location(), self.constraints,
                                             slope=self.slope)
                      * KM_PER_MS / self.slope)

        if len(self.constraints) < 3:
            # Trilateration bound: with fewer than 3 VPs the position is
            # geometrically ambiguous no matter how well the RTTs fit
            # (1 ping -> a ring of candidates, 2 pings -> two mirror
            # intersection points).  Without this floor, two consistent
            # pings give a near-zero gaussian residual and the region
            # falsely reports "geolocated".  Floor the reported
            # uncertainty at the best ping's model-implied distance.
            result = max(result, self._min_implied_distance_km())

        self._cached_region_size = result
        return result

    def get_location(self) -> LatLon:
        """Returns the current estimated (lat, lon) tuple."""
        return (float(self.best_guess[0]), float(self.best_guess[1]))

    def clone(self) -> 'FeasibleRegion':
        """
        Fast isolated copy for parallel utility evaluation.
        Avoids the overhead of copy.deepcopy().
        """
        new_region = FeasibleRegion(self.target_id, mode=self.mode,
                                    radius_multiplier=self.radius_multiplier,
                                    slope=self.slope,
                                    noise_model=self.noise_model)
        new_region.prior_slope = self.prior_slope
        new_region.fitted_sigma_ms = self.fitted_sigma_ms
        new_region.best_guess = self.best_guess.copy()
        new_region.constraints = self.constraints.copy()
        new_region._cached_region_size = self._cached_region_size
        return new_region

    def distance_to(self, vp_loc: LatLon) -> float:
        """Great-circle distance from the current estimate to vp_loc (km)."""
        return get_distance(vp_loc, self.get_location())

    def _min_implied_distance_km(self) -> float:
        """Model-implied distance of the best (lowest-RTT) constraint.
        Hard constraints store the radius = implied distance × multiplier,
        so the implied distance is recovered by undoing the multiplier."""
        if self.mode == HARD_CIRCLE:
            return min(radius for _, radius in self.constraints) / self.radius_multiplier
        return min(self.implied_distance_km(rtt) for _, _, rtt in self.constraints)

    # ------------------------------------------------------------------
    # Internal — hard_circle mode
    # ------------------------------------------------------------------

    def _append_constraint_hard(self, vp_loc: LatLon, min_rtt: float) -> None:
        max_radius_km = self.implied_distance_km(min_rtt) * self.radius_multiplier
        self.constraints.append((vp_loc, max_radius_km))
        if self.target_id == TARGET_OF_INTEREST:
            print(f"[{self.target_id}] hard constraint added, total={len(self.constraints)}")
        self._cached_region_size = None

    def _update_estimate_hard(self) -> None:
        if len(self.constraints) == 1:
            self.best_guess = np.array(self.constraints[0][0])
            return
        if self.target_id == TARGET_OF_INTEREST:
            print(self.constraints)

        def error_function(point: np.ndarray) -> float:
            lat, lon = point
            penalty = 0.0
            for (src_lat, src_lon), max_dist in self.constraints:
                dist = get_distance((lat, lon), (src_lat, src_lon))
                if dist > max_dist:
                    penalty += (dist - max_dist) ** 2
                else:
                    penalty += 0.001 * dist
            return penalty

        result = minimize(
            error_function,
            self.best_guess,
            method='Nelder-Mead',
            tol=1.0,
            options={'maxiter': 200},
        )
        self.best_guess = np.array(_normalize_latlon(result.x[0], result.x[1]))

    def _hard_circle_region_size(self) -> float:
        """
        Largest displacement from the current estimate that still satisfies
        every constraint — i.e. how far away the target could really be.

        The previous proxy (slack of the tightest constraint) measured the
        distance to ONE circle's boundary; with far-away VPs the estimate
        sits near the edge of a huge feasible lens, slack reads near-zero,
        and impossible-to-geolocate targets were declared done.  Probing 8
        bearings (geometric ladder + bisection) bounds the actual feasible
        extent instead.
        """
        if len(self.constraints) == 1:
            # Exact: the feasible set is the full circle around the VP.
            return self.constraints[0][1]

        estimate = self.get_location()

        def feasible(point: LatLon) -> bool:
            return all(
                get_distance(point, vp_loc) <= max_radius
                for vp_loc, max_radius in self.constraints
            )

        max_displacement = 0.0
        for bearing in np.linspace(0.0, 2 * np.pi, 8, endpoint=False):
            lo, hi = 0.0, None
            d = 25.0
            while d <= 20037.0:
                if feasible(_destination_point(estimate, bearing, d)):
                    lo = d
                    d *= 2
                else:
                    hi = d
                    break
            if hi is None:
                # Feasible at every probed distance: unbounded for our
                # purposes; over-estimating is the safe direction.
                lo = 20037.0
            else:
                for _ in range(4):
                    mid = (lo + hi) / 2
                    if feasible(_destination_point(estimate, bearing, mid)):
                        lo = mid
                    else:
                        hi = mid
            max_displacement = max(max_displacement, lo)
        return min(max_displacement, 20037.0)

    # ------------------------------------------------------------------
    # Internal — gaussian mode
    # ------------------------------------------------------------------

    def _append_constraint_gaussian(
        self, vp_loc: LatLon, min_rtt: float, sigma_ms: float
    ) -> None:
        self.constraints.append((vp_loc, sigma_ms, min_rtt))
        self._cached_region_size = None

    def _update_estimate_gaussian(self) -> None:
        if len(self.constraints) == 1:
            self.best_guess = np.array(self.constraints[0][0])
            return

        def loss(point: np.ndarray) -> float:
            return gaussian_nll((point[0], point[1]), self.constraints,
                                slope=self.slope, noise_model=self.noise_model)

        result = minimize(
            loss,
            self.best_guess,
            method='Nelder-Mead',
            tol=1e-4,
            options={'maxiter': 500},
        )
        self.best_guess = np.array(_normalize_latlon(result.x[0], result.x[1]))

    # ------------------------------------------------------------------
    # Internal — em_gaussian mode (online per-target μ/σ calibration)
    # ------------------------------------------------------------------

    def _update_estimate_em(self) -> None:
        """
        Alternate MAP location (E-step) with a closed-form refit of the
        per-target slope μ and noise σ from the residuals (M-step).

        The M-step is a prior-anchored least-squares fit: with pseudo-count
        priors (μ ← prior_slope, σ ← GLOBAL_SIGMA_MS) the cold start is
        well-posed even though 1–2 pings cannot identify μ jointly with
        the location; as measurements accumulate the data take over.  A
        full refit each update is preferred over an EWMA here because a
        target sees at most tens of pings — no forgetting is needed and
        the refit is cheap.
        """
        for _ in range(EM_ITERS):
            self._update_estimate_gaussian()   # E-step under current μ
            new_mu, new_sigma = self._m_step()
            converged = abs(new_mu - self.slope) < 1e-4
            self.slope = new_mu
            self.fitted_sigma_ms = new_sigma
            self._cached_region_size = None    # size depends on μ
            if converged:
                break

    def _m_step(self) -> tuple[float, float]:
        """Refit (μ, σ) from residuals against the current location estimate."""
        estimate = self.get_location()
        dists_ms = []   # d / KM_PER_MS  (SOL-equivalent ms)
        rtts = []
        for vp_loc, _sigma, rtt in self.constraints:
            d = get_distance(estimate, vp_loc)
            if d < 1.0:
                continue   # co-located VP: ratio is degenerate
            dists_ms.append(d / KM_PER_MS)
            rtts.append(rtt)

        k = len(dists_ms)
        if k < 2:
            return self.prior_slope, self.fitted_sigma_ms

        x = np.array(dists_ms)
        y = np.array(rtts)
        if self.noise_model == GAUSSIAN_NOISE:
            # Least-squares slope through the origin (exact M-step)
            mu_ml = float(np.dot(x, y) / np.dot(x, x))
        else:
            # Robust slope for heavy-tailed noise: median of per-VP ratios,
            # so the same outliers the likelihood down-weights don't drag
            # the slope fit either.
            mu_ml = float(np.median(y / x))
        # Shrink toward the prior
        mu = (k * mu_ml + EM_MU_PRIOR_STRENGTH * self.prior_slope) / (
            k + EM_MU_PRIOR_STRENGTH)
        mu = min(max(mu, EM_MU_BOUNDS[0]), EM_MU_BOUNDS[1])

        residuals = y - mu * x
        if self.noise_model == GAUSSIAN_NOISE:
            var_ml = float(np.mean(residuals ** 2))
        else:
            # Robust scale (MAD, scaled to σ-equivalent) for heavy tails
            mad = float(np.median(np.abs(residuals - np.median(residuals))))
            var_ml = (1.4826 * mad) ** 2
        var = (k * var_ml + EM_SIGMA_PRIOR_STRENGTH * GLOBAL_SIGMA_MS ** 2) / (
            k + EM_SIGMA_PRIOR_STRENGTH)
        sigma = max(math.sqrt(var), EM_SIGMA_FLOOR_MS)
        return mu, sigma

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _append_constraint(
        self, vp_loc: LatLon, min_rtt: float, sigma_ms: float
    ) -> None:
        if self.mode == HARD_CIRCLE:
            self._append_constraint_hard(vp_loc, min_rtt)
        else:
            # gaussian and em_gaussian share the ProbConstraint format
            self._append_constraint_gaussian(vp_loc, min_rtt, sigma_ms)

    def _update_estimate(self) -> None:
        if not self.constraints:
            return
        if self.mode == HARD_CIRCLE:
            self._update_estimate_hard()
        elif self.mode == EM_GAUSSIAN:
            self._update_estimate_em()
        else:
            self._update_estimate_gaussian()
