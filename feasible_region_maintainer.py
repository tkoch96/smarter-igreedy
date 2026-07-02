import numpy as np
from scipy.optimize import minimize
from typing import Optional

from utils import LatLon, get_distance
from probabilistic_helpers import (
    gaussian_nll,
    mean_absolute_residual,
    GLOBAL_SIGMA_MS,
    ProbConstraint,
)

TARGET_OF_INTEREST = '85.93.215.0'

# Hard-circle mode: (vp_location, max_radius_km)
HardConstraint = tuple[LatLon, float]
# Gaussian mode:   (vp_location, sigma_ms, rtt_ms)
# ProbConstraint imported from probabilistic_helpers

HARD_CIRCLE = 'hard_circle'
GAUSSIAN    = 'gaussian'
RADIUS_MULTIPLIER = 1.05   # hard-circle slack factor


class FeasibleRegion:
    """
    Tracks the estimated geographic region for a target based on RTT constraints.

    mode='hard_circle'  — original behaviour: each RTT becomes a maximum-radius
                          circle; Nelder-Mead minimises a penalty that fires
                          when the point falls outside any circle.

    mode='gaussian'     — probabilistic model: each RTT contributes a Gaussian
                          log-likelihood term; Nelder-Mead finds the MAP
                          estimate.  Outliers add a broad weak term instead of
                          breaking the intersection.

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
    ) -> None:
        self.target_id = target_id
        self.mode = mode
        self.radius_multiplier = radius_multiplier
        self.best_guess: np.ndarray = np.array(prior_guess)
        # hard_circle: list[HardConstraint]   (vp_loc, max_radius_km)
        # gaussian:    list[ProbConstraint]   (vp_loc, sigma_ms, rtt_ms)
        self.constraints: list = []
        self._cached_region_size: Optional[float] = None

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
        Uncertainty proxy for the current estimate.

        hard_circle: slack of the tightest constraint (km).  Lower = tighter.
        gaussian:    mean absolute RTT residual at current estimate (ms).
                     Lower = more confident.

        Returns a large sentinel (20037 or inf) when there are no constraints.
        """
        if self._cached_region_size is not None:
            return self._cached_region_size

        if not self.constraints:
            return 20037.0 if self.mode == HARD_CIRCLE else float('inf')

        if self.mode == HARD_CIRCLE:
            result = self._hard_circle_region_size()
        else:
            result = mean_absolute_residual(self.get_location(), self.constraints)

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
        new_region = FeasibleRegion(self.target_id, mode=self.mode, radius_multiplier=self.radius_multiplier)
        new_region.best_guess = self.best_guess.copy()
        new_region.constraints = self.constraints.copy()
        new_region._cached_region_size = self._cached_region_size
        return new_region

    def distance_to(self, vp_loc: LatLon) -> float:
        """Great-circle distance from the current estimate to vp_loc (km)."""
        return get_distance(vp_loc, self.get_location())

    # ------------------------------------------------------------------
    # Internal — hard_circle mode
    # ------------------------------------------------------------------

    def _append_constraint_hard(self, vp_loc: LatLon, min_rtt: float) -> None:
        max_radius_km = min_rtt * 100.0 * self.radius_multiplier
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
        self.best_guess = result.x

    def _hard_circle_region_size(self) -> float:
        centroid = self.get_location()
        tightest = float('inf')
        for (src_lat, src_lon), max_radius in self.constraints:
            dist_to_vp = get_distance(centroid, (src_lat, src_lon))
            slack = max_radius - dist_to_vp
            if slack < tightest:
                tightest = slack
        return max(tightest, 0.0)

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
            return gaussian_nll((point[0], point[1]), self.constraints)

        result = minimize(
            loss,
            self.best_guess,
            method='Nelder-Mead',
            tol=1e-4,
            options={'maxiter': 500},
        )
        self.best_guess = result.x

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _append_constraint(
        self, vp_loc: LatLon, min_rtt: float, sigma_ms: float
    ) -> None:
        if self.mode == HARD_CIRCLE:
            self._append_constraint_hard(vp_loc, min_rtt)
        else:
            self._append_constraint_gaussian(vp_loc, min_rtt, sigma_ms)

    def _update_estimate(self) -> None:
        if not self.constraints:
            return
        if self.mode == HARD_CIRCLE:
            self._update_estimate_hard()
        else:
            self._update_estimate_gaussian()
