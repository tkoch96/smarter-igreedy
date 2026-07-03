import math
import os
import numpy as np
from scipy.optimize import minimize
from typing import Optional

from utils import LatLon, get_distance, _normalize_latlon
from probabilistic_helpers import (
    gaussian_nll,
    mean_absolute_residual,
    additive_map_location,
    AdditiveLatencyModel,
    GLOBAL_SIGMA_MS,
    GAUSSIAN_NOISE,
    STUDENT_T_NOISE,
    ASYMMETRIC_NOISE,
    ProbConstraint,
)

# Verbose per-measurement debugging for one target id; disabled unless the
# env var is set (a hardcoded '85.93.215.0' used to spam real-mesh runs
# whenever that /24 landed in the subsample).
TARGET_OF_INTEREST = os.environ.get('FEASIBLE_REGION_DEBUG_TARGET')

EARTH_RADIUS_KM = 6371.0


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
ADDITIVE    = 'additive'

# Hypothesis-set settings (additive mode): a small support set of plausible
# locations, kept alongside the MAP point. On cluster-degenerate geometry
# the likelihood has a flat ridge (offset and distance exactly confounded),
# which is invisible to any local measure — the set is built from the
# ambiguity structure (rings around the best VP) and scored with the
# PROFILED objective (per-target offset marginalised out, clamped ≥ 0:
# rtt cannot beat SOL).
HYP_RING_BEARINGS = 16     # ring sample density around the best VP
HYP_RADIUS_FACTORS = (0.4, 0.7, 1.0)   # inward fractions — fits overshoot OUT
HYP_SUPPORT_DELTA = 2.0    # keep candidates within this (misfit-scaled) NLL
HYP_MAX = 8                # support-set size cap
HYP_MIN_SEP_KM = 200.0     # dedupe near-identical hypotheses

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

    mode='additive'     — the two-way model rtt = d/100 + X_src + X_dst.
                          Per-node (μ, σ²) live in a SHARED
                          AdditiveLatencyModel (constructor param `model`)
                          because X_src pools across all targets; the region
                          only stores its own (vp_loc, src_id, rtt)
                          constraints and consults the model for offsets and
                          per-measurement variance (MAP weight
                          1/(σ_s² + σ_t²)).  The model owner (the greedy /
                          converter) is responsible for refitting it and
                          calling reoptimize().

    The public API (add_measurement, add_measurements_batch, get_region_size,
    get_location, clone, distance_to) is identical in all modes.
    add_measurement accepts an optional sigma_ms argument (gaussian modes
    only) and an optional src id (additive mode only).
    """

    def __init__(
        self,
        target_id: str,
        prior_guess: LatLon = (0.0, 0.0),
        mode: str = HARD_CIRCLE,
        radius_multiplier: float = RADIUS_MULTIPLIER,
        slope: float = DEFAULT_SLOPE,
        noise_model: str = GAUSSIAN_NOISE,
        model: Optional[AdditiveLatencyModel] = None,
        hypothesis_size: bool = False,
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
        # Shared cross-target parameter state (additive mode only); the
        # region never mutates it.
        self.model = model
        if mode == ADDITIVE and model is None:
            self.model = AdditiveLatencyModel()   # standalone/test use
        # Support set of plausible locations (additive mode; maintained by
        # _update_estimate_additive). `hypothesis_size=True` additionally
        # folds the set's spread into get_region_size() — the ridge-aware
        # honest uncertainty used by info-gain selection.
        self.hypotheses: list[LatLon] = []
        self.hypothesis_size = hypothesis_size
        # Track record of this target's PROMISED vs REALIZED gains (EWMA of
        # realized/promised, in [RELIABILITY_FLOOR, 1]).  Maintained by the
        # greedy; consumed by risk-adjusted selection: a target whose
        # promises keep failing to pay out gets its future promises
        # discounted — model-free evidence the model cannot fake.
        self.gain_reliability: float = 1.0
        self.best_guess: np.ndarray = np.array(prior_guess)
        # hard_circle: list[HardConstraint]   (vp_loc, max_radius_km)
        # gaussian:    list[ProbConstraint]   (vp_loc, sigma_ms, rtt_ms)
        # additive:    list[(vp_loc, src_id, rtt_ms)]
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
        src: Optional[str] = None,
        update_estimate: bool = True,
    ) -> None:
        """Add a single RTT measurement and update the location estimate.
        `src` (the VP's id) is required in additive mode — the shared model
        keys per-source parameters by it; other modes ignore it.
        `update_estimate=False` defers the location step (caller must
        `reoptimize()` later) — the additive greedy needs the parameter
        refit to see residuals against the PRE-ping estimate, so the offset
        lands in μ̂_t instead of being absorbed into distance."""
        if self.target_id == TARGET_OF_INTEREST:
            print(f"[{self.target_id}] add_measurement vp={vp_loc} rtt={min_rtt:.2f}ms")
        self._append_constraint(vp_loc, min_rtt, sigma_ms, src)
        if update_estimate:
            self._update_estimate()

    def add_measurements_batch(
        self,
        measurements: list[tuple[LatLon, float]],
        sigma_ms: float = GLOBAL_SIGMA_MS,
        src: Optional[str] = None,
    ) -> None:
        """Batch-add measurements, re-optimising only once at the end.
        `src` applies to every entry (additive mode: replicated samples of
        one pair enter as individual constraints)."""
        for vp_loc, min_rtt in measurements:
            self._append_constraint(vp_loc, min_rtt, sigma_ms, src)
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
        elif self.mode == ADDITIVE:
            result = self._additive_region_size()
            if self.hypothesis_size:
                # Ridge-aware honesty: a self-consistent wrong fit has
                # clean residuals, but its support set stays spread.
                result = max(result, self.hypothesis_spread_km())
        else:
            result = (mean_absolute_residual(self.get_location(), self.constraints,
                                             slope=self.slope)
                      * KM_PER_MS / self.slope)

        # Trilateration bound: with fewer than 3 VPs the position is
        # geometrically ambiguous no matter how well the RTTs fit
        # (1 ping -> a ring of candidates, 2 pings -> two mirror
        # intersection points).  Without this floor, two consistent
        # pings give a near-zero gaussian residual and the region
        # falsely reports "geolocated".  Floor the reported
        # uncertainty at the best ping's model-implied distance.
        # Additive mode counts DISTINCT VPs — replicated samples of one
        # pair are several constraints but zero extra geometry.
        if self.mode == ADDITIVE:
            n_geom = len({src for _, src, _ in self.constraints})
        else:
            n_geom = len(self.constraints)
        if n_geom < 3:
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
                                    noise_model=self.noise_model,
                                    model=self.model,
                                    hypothesis_size=self.hypothesis_size)
        new_region.prior_slope = self.prior_slope
        new_region.fitted_sigma_ms = self.fitted_sigma_ms
        new_region.best_guess = self.best_guess.copy()
        new_region.constraints = self.constraints.copy()
        new_region.hypotheses = list(self.hypotheses)
        new_region.gain_reliability = self.gain_reliability
        new_region._cached_region_size = self._cached_region_size
        return new_region

    def distance_to(self, vp_loc: LatLon) -> float:
        """Great-circle distance from the current estimate to vp_loc (km)."""
        return get_distance(vp_loc, self.get_location())

    def set_location(self, loc: LatLon) -> None:
        """Adopt an externally computed estimate (e.g. the additive batch
        fit's MAP) and invalidate the size cache."""
        self.best_guess = np.array(_normalize_latlon(loc[0], loc[1]))
        self._cached_region_size = None

    def reoptimize(self) -> None:
        """Re-run the location fit under the current model parameters.
        The additive model owner calls this after a refit — the shared
        (μ, σ²) changed under the region's feet, so both the MAP location
        and the cached size are stale."""
        self._cached_region_size = None
        self._update_estimate()

    def _min_implied_distance_km(self) -> float:
        """Model-implied distance of the best (lowest-RTT) constraint.
        Hard constraints store the radius = implied distance × multiplier,
        so the implied distance is recovered by undoing the multiplier."""
        if self.mode == HARD_CIRCLE:
            return min(radius for _, radius in self.constraints) / self.radius_multiplier
        if self.mode == ADDITIVE:
            return min(
                max(0.0, rtt - self.model.mean_offset(src, self.target_id))
                * KM_PER_MS
                for _, src, rtt in self.constraints)
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
    # Internal — additive mode (shared two-way src/dst model)
    # ------------------------------------------------------------------

    def _additive_rows(self) -> list:
        """(vp_loc, rtt, mean_offset, var_sum) rows for the MAP objective."""
        t = self.target_id
        return [(vp_loc, rtt, self.model.mean_offset(src, t),
                 self.model.var_sum(src, t))
                for vp_loc, src, rtt in self.constraints]

    def _nn_anchor(self) -> LatLon:
        """Location of the lowest-RTT constraint — the NN start that keeps
        multi-start MAP from being trapped by an early wrong fixed point."""
        return min(self.constraints, key=lambda c: c[2])[0]

    def _update_estimate_additive(self) -> None:
        # Distinct VPs, not raw constraints: replicated samples of one pair
        # are one bearing.  Optimising a single-VP set would park the
        # estimate on an arbitrary ring point at the model-implied distance,
        # whose near-zero residuals then rob μ̂_t of the offset it should
        # claim (the params-first pitfall through the back door).  Anchor at
        # the VP itself instead.
        if len({src for _, src, _ in self.constraints}) == 1:
            self.best_guess = np.array(self.constraints[0][0])
            self._update_hypotheses()
            return
        estimate = additive_map_location(
            self._additive_rows(),
            [self.get_location(), self._nn_anchor()])
        self.best_guess = np.array(estimate)
        self._update_hypotheses()

    def _profiled_nll(self, x: LatLon) -> float:
        """Fit score of candidate location x with the per-target offset
        MARGINALISED OUT (clamped ≥ 0 — rtt cannot beat SOL).  On a ridge
        the shared offset absorbs any common distance shift, so this is
        flat along the ridge (the point) while ordinary NLL under FIXED
        offsets wrongly rejects its near end."""
        offs, ws = [], []
        t = self.target_id
        for vp_loc, src, rtt in self.constraints:
            offs.append(rtt - get_distance(x, vp_loc) / KM_PER_MS
                        - self.model.mu_s.get(src, 5.0))
            ws.append(1.0 / self.model.var_sum(src, t))
        w = np.array(ws)
        r = np.array(offs)
        mu_prof = max(0.0, float(np.sum(w * r) / np.sum(w)))
        return float(np.sum(w * (r - mu_prof) ** 2) / 2.0)

    def _update_hypotheses(self) -> None:
        """Rebuild the support set: MAP + NN anchor + rings around the best
        (lowest-RTT) VP, scored by profiled NLL, kept within
        HYP_SUPPORT_DELTA of the best.  Bowl targets collapse to ~1 point;
        ridge targets keep a spread — which is exactly the signal info-gain
        selection and honest sizing need."""
        if not self.constraints:
            self.hypotheses = []
            return
        best_vp_loc, best_src, best_rtt = min(self.constraints,
                                              key=lambda c: c[2])
        implied = max(0.0, best_rtt - self.model.mean_offset(
            best_src, self.target_id)) * KM_PER_MS
        map_pt = self.get_location()
        d_map = get_distance(best_vp_loc, map_pt)

        pool: list[LatLon] = [map_pt, best_vp_loc]
        radii = {round(implied * f) for f in HYP_RADIUS_FACTORS}
        radii.add(round(d_map))
        for r_km in radii:
            if r_km < HYP_MIN_SEP_KM:
                continue
            for bearing in np.linspace(0.0, 2 * np.pi, HYP_RING_BEARINGS,
                                       endpoint=False):
                pool.append(_destination_point(best_vp_loc, bearing, r_km))

        scored = sorted((self._profiled_nll(p), i, p)
                        for i, p in enumerate(pool))
        best_nll = scored[0][0]
        # Misfit-scaled tolerance (reduced chi-square): under real,
        # misspecified noise the per-constraint NLL differences inflate
        # with the residual level AND accumulate with constraint count, so
        # a fixed Δ degenerates the support to the MAP point while the
        # geometric ambiguity is fully intact (measured: a 67-ping ridge
        # target ended with ONE hypothesis and zero utility everywhere).
        n_c = len(self.constraints)
        chi2_red = max(1.0, 2.0 * best_nll / max(n_c - 1, 1))
        delta = HYP_SUPPORT_DELTA * chi2_red * max(1.0, n_c / 8.0)
        support: list[LatLon] = []
        for nll, _, p in scored:
            if nll > best_nll + delta or len(support) >= HYP_MAX:
                break
            if all(get_distance(p, q) >= HYP_MIN_SEP_KM for q in support):
                support.append(p)
        self.hypotheses = support

    def hypothesis_spread_km(self) -> float:
        """Largest distance between two support points — the ridge length."""
        if len(self.hypotheses) < 2:
            return 0.0
        return max(get_distance(p, q)
                   for i, p in enumerate(self.hypotheses)
                   for q in self.hypotheses[i + 1:])

    def _additive_region_size(self) -> float:
        """
        Uncertainty proxy in km, precision-aware so the greedy's utility
        (expected size reduction of a simulated ping) inherits the σ̂
        signal:

          fit_ms   = precision-WEIGHTED rms residual at the estimate.  A
                     simulated zero-residual ping to a pathological target
                     carries weight 1/(σ_s² + σ̂_t²) ≈ 0, so it cannot fake
                     a reduction the way an unweighted mean would.
          floor_ms = 1/sqrt(Σ w) — the statistical uncertainty of the
                     pooled constraint set.  Another ping to a high-σ̂_t
                     target adds almost no precision, so its expected gain
                     vanishes and the greedy redirects budget.
        """
        estimate = self.get_location()
        w_sum = 0.0
        wr2_sum = 0.0
        for vp_loc, rtt, mean_off, var_sum in self._additive_rows():
            r = rtt - get_distance(estimate, vp_loc) / KM_PER_MS - mean_off
            w = 1.0 / var_sum
            w_sum += w
            wr2_sum += w * r * r
        fit_ms = math.sqrt(wr2_sum / w_sum)
        floor_ms = math.sqrt(1.0 / w_sum)
        return (fit_ms + floor_ms) * KM_PER_MS

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _append_constraint(
        self, vp_loc: LatLon, min_rtt: float, sigma_ms: float,
        src: Optional[str] = None,
    ) -> None:
        if self.mode == HARD_CIRCLE:
            self._append_constraint_hard(vp_loc, min_rtt)
        elif self.mode == ADDITIVE:
            self.constraints.append((vp_loc, src, min_rtt))
            self._cached_region_size = None
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
        elif self.mode == ADDITIVE:
            self._update_estimate_additive()
        else:
            self._update_estimate_gaussian()
