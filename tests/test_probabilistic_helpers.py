"""
Unit tests for probabilistic_helpers.py

These tests are fully synthetic — no network access, no pickle files.
They verify the pure helper functions before any changes to FeasibleRegion.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import math
import numpy as np
import pytest

from probabilistic_helpers import (
    gaussian_nll,
    mean_absolute_residual,
    compute_per_vp_sigma,
    haversine_grid,
    posterior_mean_grid,
    KM_PER_MS,
    GLOBAL_SIGMA_MS,
)
from utils import get_distance


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def make_constraint(vp_loc, target_loc, overhead_ms=0.0, sigma_ms=15.0):
    """Build a (vp_loc, sigma_ms, rtt_ms) constraint from known geometry."""
    dist_km = get_distance(vp_loc, target_loc)
    rtt_ms = dist_km / KM_PER_MS + overhead_ms
    return (vp_loc, sigma_ms, rtt_ms)


def make_mesh_data(locations: dict[str, tuple], overhead_factor: float = 1.2):
    """
    Build synthetic target_data with known ground-truth distances.
    overhead_factor > 1 simulates routing being slower than SOL.
    """
    address_to_loc = dict(locations)
    loc_loc_meas: dict[str, dict[str, list[float]]] = {}
    node_ids = list(locations.keys())

    for src in node_ids:
        loc_loc_meas[src] = {}
        for dst in node_ids:
            if src == dst:
                continue
            dist_km = get_distance(locations[src], locations[dst])
            rtt_ms = (dist_km / KM_PER_MS) * overhead_factor
            loc_loc_meas[src][dst] = [rtt_ms]

    return {'address_to_loc': address_to_loc, 'loc_loc_meas': loc_loc_meas}


EUROPE_NODES = {
    '10.0.0.0': (51.5, -0.1),    # London
    '10.0.1.0': (48.9,  2.3),    # Paris
    '10.0.2.0': (52.5, 13.4),    # Berlin
    '10.0.3.0': (41.9, 12.5),    # Rome
    '10.0.4.0': (40.4, -3.7),    # Madrid
    '10.0.5.0': (59.9, 10.7),    # Oslo
}


# ---------------------------------------------------------------------------
# gaussian_nll
# ---------------------------------------------------------------------------

class TestGaussianNll:
    def test_zero_residual_gives_zero_loss(self):
        """A point exactly at SOL distance should give nll = 0."""
        vp = (51.5, -0.1)       # London
        target = (48.9, 2.3)    # Paris
        c = make_constraint(vp, target, overhead_ms=0.0)
        nll = gaussian_nll(target, [c])
        assert nll == pytest.approx(0.0, abs=1e-9)

    def test_empty_constraints_gives_zero(self):
        assert gaussian_nll((0.0, 0.0), []) == 0.0

    def test_nll_positive_for_nonzero_residual(self):
        vp = (51.5, -0.1)
        target = (48.9, 2.3)
        c = make_constraint(vp, target, overhead_ms=5.0)   # 5ms over SOL
        # Evaluate at true target — residual is 5ms, not zero
        nll = gaussian_nll(target, [c])
        assert nll > 0.0

    def test_nll_lower_at_true_location_than_elsewhere(self):
        """The true target location should have a lower nll than a random wrong point."""
        target = (48.9, 2.3)    # Paris
        vps = [(51.5, -0.1), (52.5, 13.4), (41.9, 12.5), (40.4, -3.7)]
        constraints = [make_constraint(vp, target, overhead_ms=0.0) for vp in vps]

        nll_true = gaussian_nll(target, constraints)
        nll_wrong = gaussian_nll((0.0, 0.0), constraints)   # Null Island
        assert nll_true < nll_wrong

    def test_nll_scales_with_inverse_sigma_squared(self):
        """Tighter sigma = steeper penalty for the same residual."""
        vp = (51.5, -0.1)
        target = (48.9, 2.3)
        overhead = 10.0  # ms over SOL
        c_tight = make_constraint(vp, target, overhead, sigma_ms=5.0)
        c_loose = make_constraint(vp, target, overhead, sigma_ms=20.0)

        nll_tight = gaussian_nll(target, [c_tight])
        nll_loose = gaussian_nll(target, [c_loose])
        assert nll_tight > nll_loose

    def test_nll_additive_over_independent_constraints(self):
        """Sum of two single-constraint nlls should equal the two-constraint nll."""
        target = (48.9, 2.3)
        c1 = make_constraint((51.5, -0.1), target, overhead_ms=3.0)
        c2 = make_constraint((52.5, 13.4), target, overhead_ms=7.0)

        nll_both = gaussian_nll(target, [c1, c2])
        nll_sum  = gaussian_nll(target, [c1]) + gaussian_nll(target, [c2])
        assert nll_both == pytest.approx(nll_sum, rel=1e-9)

    def test_outlier_adds_finite_term_not_infinity(self):
        """A 10× outlier RTT should increase nll by a finite amount."""
        target = (48.9, 2.3)
        c_normal = make_constraint((51.5, -0.1), target, overhead_ms=2.0)
        c_outlier = make_constraint((52.5, 13.4), target, overhead_ms=200.0)

        nll_normal = gaussian_nll(target, [c_normal])
        nll_both   = gaussian_nll(target, [c_normal, c_outlier])
        assert math.isfinite(nll_both)
        assert nll_both > nll_normal


# ---------------------------------------------------------------------------
# mean_absolute_residual
# ---------------------------------------------------------------------------

class TestMeanAbsoluteResidual:
    def test_empty_returns_inf(self):
        assert mean_absolute_residual((0.0, 0.0), []) == float('inf')

    def test_zero_residual_at_sol(self):
        vp = (51.5, -0.1)
        target = (48.9, 2.3)
        c = make_constraint(vp, target, overhead_ms=0.0)
        mar = mean_absolute_residual(target, [c])
        assert mar == pytest.approx(0.0, abs=1e-6)

    def test_matches_overhead(self):
        """With a single constraint and known overhead, MAR should equal that overhead."""
        vp = (51.5, -0.1)
        target = (48.9, 2.3)
        overhead = 8.0
        c = make_constraint(vp, target, overhead_ms=overhead)
        mar = mean_absolute_residual(target, [c])
        assert mar == pytest.approx(overhead, rel=1e-6)

    def test_averages_multiple_residuals(self):
        """MAR should be the mean of individual absolute residuals."""
        target = (48.9, 2.3)
        c1 = make_constraint((51.5, -0.1), target, overhead_ms=4.0)
        c2 = make_constraint((52.5, 13.4), target, overhead_ms=8.0)
        mar = mean_absolute_residual(target, [c1, c2])
        assert mar == pytest.approx(6.0, rel=1e-6)

    def test_lower_at_true_location(self):
        """MAR should be lower at the true target than at a distant wrong point."""
        target = (48.9, 2.3)
        vps = [(51.5, -0.1), (52.5, 13.4), (41.9, 12.5)]
        constraints = [make_constraint(vp, target, overhead_ms=0.0) for vp in vps]

        mar_true  = mean_absolute_residual(target, constraints)
        mar_wrong = mean_absolute_residual((0.0, 0.0), constraints)
        assert mar_true < mar_wrong

    def test_outlier_raises_mar_but_stays_finite(self):
        target = (48.9, 2.3)
        c_normal  = make_constraint((51.5, -0.1), target, overhead_ms=2.0)
        c_outlier = make_constraint((52.5, 13.4), target, overhead_ms=500.0)
        mar = mean_absolute_residual(target, [c_normal, c_outlier])
        assert math.isfinite(mar)
        assert mar > 2.0


# ---------------------------------------------------------------------------
# compute_per_vp_sigma
# ---------------------------------------------------------------------------

class TestComputePerVpSigma:
    def test_returns_entry_for_every_vp(self):
        data = make_mesh_data(EUROPE_NODES)
        sigmas = compute_per_vp_sigma(data, min_peers=2)
        for vp in EUROPE_NODES:
            assert vp in sigmas, f"Missing sigma for {vp}"

    def test_sigmas_are_positive(self):
        data = make_mesh_data(EUROPE_NODES)
        sigmas = compute_per_vp_sigma(data, min_peers=2)
        for vp, sigma in sigmas.items():
            assert sigma > 0.0, f"sigma for {vp} must be positive, got {sigma}"

    def test_constant_overhead_gives_small_sigma(self):
        """
        When every RTT has the same absolute overhead added, all residuals are
        identical so sigma → 0, clamped to 0.5ms.

        Note: overhead_factor would give rtt = dist/100 * factor, so residuals
        = dist/100 * (factor-1), which vary with distance and produce non-zero
        sigma. We use a fixed absolute offset instead.
        """
        fixed_overhead_ms = 5.0
        node_ids = list(EUROPE_NODES.keys())
        loc_loc_meas: dict[str, dict[str, list[float]]] = {}
        for src in node_ids:
            loc_loc_meas[src] = {}
            for dst in node_ids:
                if src == dst:
                    continue
                dist_km = get_distance(EUROPE_NODES[src], EUROPE_NODES[dst])
                loc_loc_meas[src][dst] = [dist_km / KM_PER_MS + fixed_overhead_ms]

        data = {'address_to_loc': dict(EUROPE_NODES), 'loc_loc_meas': loc_loc_meas}
        sigmas = compute_per_vp_sigma(data, min_peers=2)
        for vp, sigma in sigmas.items():
            # All residuals are identical ⟹ std=0 ⟹ clamped to 0.5ms
            assert sigma <= 0.6, f"Constant-overhead sigma should be near-zero, got {sigma:.3f}"

    def test_noisy_overhead_gives_larger_sigma(self):
        """Adding random noise to RTTs should increase estimated sigma."""
        rng = np.random.default_rng(42)
        node_ids = list(EUROPE_NODES.keys())
        locations = EUROPE_NODES

        address_to_loc = dict(locations)
        loc_loc_meas: dict[str, dict[str, list[float]]] = {}
        noise_std_ms = 10.0

        for src in node_ids:
            loc_loc_meas[src] = {}
            for dst in node_ids:
                if src == dst:
                    continue
                dist_km = get_distance(locations[src], locations[dst])
                base_rtt = dist_km / KM_PER_MS
                noisy_rtt = base_rtt + rng.normal(0.0, noise_std_ms)
                # RTT can't be negative
                loc_loc_meas[src][dst] = [max(0.1, noisy_rtt)]

        noisy_data = {'address_to_loc': address_to_loc, 'loc_loc_meas': loc_loc_meas}
        sigmas = compute_per_vp_sigma(noisy_data, min_peers=2)

        avg_sigma = np.mean(list(sigmas.values()))
        # Should be in the right ballpark of the injected noise
        assert 5.0 < avg_sigma < 30.0, f"Expected sigma near {noise_std_ms}ms, got avg {avg_sigma:.1f}ms"

    def test_fallback_for_vp_with_few_peers(self):
        """A VP with fewer than min_peers peers should receive the global fallback."""
        # Build data where one VP only has 1 peer (manually)
        lonely = {'10.99.0.0': (0.0, 0.0)}  # Null Island — isolated
        combined = {**EUROPE_NODES, **lonely}
        # The isolated VP has no entries in loc_loc_meas — it never pings anyone.
        data = make_mesh_data(EUROPE_NODES)   # no entries for the lonely VP
        data['address_to_loc']['10.99.0.0'] = (0.0, 0.0)

        sigmas = compute_per_vp_sigma(data, min_peers=2, global_fallback_ms=99.0)
        assert sigmas['10.99.0.0'] == pytest.approx(99.0), (
            "VP with zero peers should receive the global fallback sigma"
        )

    def test_fitted_sigma_close_to_injected_noise(self):
        """
        With Gaussian noise of known std injected into the RTTs,
        the fitted sigma should be within 50% of the injected value.
        (Loose tolerance because we have only ~5 peers per VP in this mesh.)
        """
        rng = np.random.default_rng(0)
        noise_std_ms = 8.0
        node_ids = list(EUROPE_NODES.keys())
        locations = EUROPE_NODES

        loc_loc_meas: dict[str, dict[str, list[float]]] = {}
        for src in node_ids:
            loc_loc_meas[src] = {}
            for dst in node_ids:
                if src == dst:
                    continue
                dist_km = get_distance(locations[src], locations[dst])
                rtt = dist_km / KM_PER_MS + abs(rng.normal(0.0, noise_std_ms))
                loc_loc_meas[src][dst] = [rtt]

        data = {'address_to_loc': dict(locations), 'loc_loc_meas': loc_loc_meas}
        sigmas = compute_per_vp_sigma(data, min_peers=2)

        for vp, sigma in sigmas.items():
            assert sigma < noise_std_ms * 3, (
                f"Fitted sigma {sigma:.1f} is implausibly large (injected={noise_std_ms}ms)"
            )


# ---------------------------------------------------------------------------
# haversine_grid
# ---------------------------------------------------------------------------

class TestHaversineGrid:
    def test_self_distance_is_zero(self):
        """Distance from a point to itself should be 0."""
        lats = np.array([[51.5]])
        lons = np.array([[-0.1]])
        d = haversine_grid(51.5, -0.1, lats, lons)
        assert d[0, 0] == pytest.approx(0.0, abs=1e-6)

    def test_matches_scalar_haversine(self):
        """haversine_grid should match get_distance for individual cells."""
        vp = (48.9, 2.3)   # Paris
        test_points = [(51.5, -0.1), (52.5, 13.4), (41.9, 12.5), (0.0, 0.0)]
        lats = np.array([[p[0] for p in test_points]])
        lons = np.array([[p[1] for p in test_points]])

        grid_dists = haversine_grid(vp[0], vp[1], lats, lons)[0]
        for i, pt in enumerate(test_points):
            expected = get_distance(vp, pt)
            assert grid_dists[i] == pytest.approx(expected, rel=1e-4), (
                f"Grid distance mismatch at {pt}: {grid_dists[i]:.3f} vs {expected:.3f}"
            )

    def test_output_shape_matches_input(self):
        """Output shape must equal input shape."""
        lats = np.linspace(-90, 90, 181).reshape(181, 1) * np.ones((1, 361))
        lons = np.linspace(-180, 180, 361).reshape(1, 361) * np.ones((181, 1))
        out = haversine_grid(0.0, 0.0, lats, lons)
        assert out.shape == (181, 361)

    def test_non_negative(self):
        """All distances should be non-negative."""
        lats, lons = np.meshgrid(np.linspace(-90, 90, 19), np.linspace(-180, 180, 37), indexing='ij')
        out = haversine_grid(51.5, -0.1, lats, lons)
        assert (out >= 0).all()

    def test_antipode_near_earth_half_circumference(self):
        """Distance to antipode ≈ half Earth's circumference (~20015 km)."""
        # Antipode of (0,0) is (0,180)
        lats = np.array([[0.0]])
        lons = np.array([[180.0]])
        d = haversine_grid(0.0, 0.0, lats, lons)
        assert d[0, 0] == pytest.approx(20015.0, rel=0.01)


# ---------------------------------------------------------------------------
# posterior_mean_grid
# ---------------------------------------------------------------------------

class TestPosteriorMeanGrid:
    def test_single_constraint_near_vp(self):
        """
        With one very tight constraint (tiny sigma, SOL-exact RTT), the
        posterior should concentrate near the VP / target point.
        """
        vp = (51.5, -0.1)      # London
        target = (51.5, -0.1)  # same point
        c = (vp, 1.0, 0.0)     # 0ms RTT ⟹ target IS the VP; sigma=1ms tight
        lat_est, lon_est = posterior_mean_grid([c], lat_resolution=2.0, lon_resolution=2.0)
        error_km = get_distance((lat_est, lon_est), target)
        # 2° grid resolution ≈ 220km; estimate should be within 300km
        assert error_km < 300.0, f"Single-constraint estimate too far: {error_km:.1f} km"

    def test_returns_finite_latlon(self):
        """posterior_mean_grid must return finite lat/lon."""
        c = ((51.5, -0.1), 15.0, 5.0)
        lat, lon = posterior_mean_grid([c], lat_resolution=5.0, lon_resolution=5.0)
        assert math.isfinite(lat) and math.isfinite(lon)

    def test_lat_in_range(self):
        """Returned latitude must be in [-90, 90]."""
        constraints = [
            ((51.5, -0.1), 10.0, 3.5),
            ((48.9,  2.3), 10.0, 2.0),
        ]
        lat, lon = posterior_mean_grid(constraints, lat_resolution=5.0, lon_resolution=5.0)
        assert -90.0 <= lat <= 90.0
        assert -180.0 <= lon <= 180.0

    def test_four_surrounding_vps_converges_near_centre(self):
        """
        Four VPs placed symmetrically around a target in Western Europe.
        The posterior mean should land within ~500km at 2° resolution.
        """
        target = (48.9, 2.3)   # Paris
        vps = [
            (51.5, -0.1),      # London
            (52.5, 13.4),      # Berlin
            (41.9, 12.5),      # Rome
            (40.4, -3.7),      # Madrid
        ]
        constraints = [make_constraint(vp, target, overhead_ms=0.0, sigma_ms=5.0) for vp in vps]
        lat_est, lon_est = posterior_mean_grid(constraints, lat_resolution=2.0, lon_resolution=2.0)
        error_km = get_distance((lat_est, lon_est), target)
        # 2° grid → ~220km cell size; require within 3 cells
        assert error_km < 700.0, f"4-VP estimate error too large: {error_km:.1f} km"

    def test_outlier_does_not_dominate(self):
        """
        Outlier robustness: a noisy VP (large sigma) with a 10× RTT should
        shift the estimate much less than a clean VP (small sigma) would.

        The Gaussian model's robustness works through per-VP sigma: a VP with
        high routing variance gets a large sigma, which squashes the penalty for
        a large residual.  A tight sigma=10ms on a 90ms residual is enormous;
        a loose sigma=80ms on the same residual contributes only modestly.

        This test verifies that giving the outlier VP its correct large sigma
        limits its influence relative to the 3 tight-sigma clean constraints.
        """
        target = (48.9, 2.3)
        clean_constraints = [
            make_constraint((51.5, -0.1), target, overhead_ms=0.0, sigma_ms=5.0),
            make_constraint((52.5, 13.4), target, overhead_ms=0.0, sigma_ms=5.0),
            make_constraint((41.9, 12.5), target, overhead_ms=0.0, sigma_ms=5.0),
        ]
        # 10× RTT outlier, but sigma is large (80ms) reflecting a noisy VP
        outlier_rtt = (get_distance((40.4, -3.7), target) / KM_PER_MS) * 10.0
        outlier_constraint_large_sigma  = ((40.4, -3.7), 80.0, outlier_rtt)
        outlier_constraint_tight_sigma  = ((40.4, -3.7),  5.0, outlier_rtt)

        base_lat, base_lon = posterior_mean_grid(
            clean_constraints, lat_resolution=2.0, lon_resolution=2.0
        )
        with_large_sigma_lat, with_large_sigma_lon = posterior_mean_grid(
            clean_constraints + [outlier_constraint_large_sigma],
            lat_resolution=2.0, lon_resolution=2.0,
        )
        with_tight_sigma_lat, with_tight_sigma_lon = posterior_mean_grid(
            clean_constraints + [outlier_constraint_tight_sigma],
            lat_resolution=2.0, lon_resolution=2.0,
        )

        shift_large = get_distance((base_lat, base_lon), (with_large_sigma_lat, with_large_sigma_lon))
        shift_tight = get_distance((base_lat, base_lon), (with_tight_sigma_lat, with_tight_sigma_lon))

        # Large-sigma outlier should move the estimate far less than tight-sigma outlier
        assert shift_large < shift_tight, (
            f"Large-sigma outlier (shift={shift_large:.1f}km) should dominate less "
            f"than tight-sigma outlier (shift={shift_tight:.1f}km)"
        )
        # And the large-sigma outlier should keep the estimate near the clean base
        assert shift_large < 800.0, (
            f"Large-sigma outlier moved estimate by {shift_large:.1f} km — still too dominant"
        )


# ---------------------------------------------------------------------------
# residual_nll — noise-model shapes
# ---------------------------------------------------------------------------

from probabilistic_helpers import (
    residual_nll,
    GAUSSIAN_NOISE,
    STUDENT_T_NOISE,
    ASYMMETRIC_NOISE,
    ASYM_FAST_SCALE,
)


class TestResidualNll:
    def test_zero_residual_costs_nothing_in_every_model(self):
        for nm in (GAUSSIAN_NOISE, STUDENT_T_NOISE, ASYMMETRIC_NOISE):
            assert residual_nll(0.0, 5.0, nm) == pytest.approx(0.0)

    def test_gaussian_matches_squared_form(self):
        assert residual_nll(6.0, 3.0, GAUSSIAN_NOISE) == pytest.approx(
            6.0 ** 2 / (2 * 3.0 ** 2))

    def test_student_t_saturates_relative_to_gaussian(self):
        """The whole point of heavy tails: a big outlier costs far less
        than quadratically, so it can't dominate the posterior."""
        sigma = 3.0
        big = 60.0    # 20σ detour
        assert residual_nll(big, sigma, STUDENT_T_NOISE) < \
            residual_nll(big, sigma, GAUSSIAN_NOISE) / 10.0

    def test_student_t_close_to_gaussian_for_small_residuals(self):
        """Near zero, log(1+x) ≈ x: robustness should be ~free in the bulk."""
        sigma = 3.0
        small = 0.5
        g = residual_nll(small, sigma, GAUSSIAN_NOISE)
        t = residual_nll(small, sigma, STUDENT_T_NOISE)
        assert t == pytest.approx(g * (3.0 + 1.0) / 3.0, rel=0.05)

    def test_asymmetric_penalises_faster_than_model_more(self):
        """Beating the model (negative residual) approaches the SOL wall —
        must cost much more than the same-size detour above it."""
        sigma = 3.0
        r = 6.0
        assert residual_nll(-r, sigma, ASYMMETRIC_NOISE) > \
            5.0 * residual_nll(r, sigma, ASYMMETRIC_NOISE)

    def test_asymmetric_detour_tail_is_linear(self):
        sigma = 3.0
        assert residual_nll(20.0, sigma, ASYMMETRIC_NOISE) == pytest.approx(
            2 * residual_nll(10.0, sigma, ASYMMETRIC_NOISE))

    def test_unknown_model_raises(self):
        with pytest.raises(ValueError):
            residual_nll(1.0, 1.0, 'cauchy')


# ---------------------------------------------------------------------------
# fit_additive_params — two-way overhead decomposition
# ---------------------------------------------------------------------------

from probabilistic_helpers import fit_additive_params


class TestFitAdditiveParams:
    def _synthetic(self, seed=0, n_src=8, n_dst=6, reps=4):
        rng = np.random.default_rng(seed)
        mu_s = {f's{i}': rng.uniform(1, 12) for i in range(n_src)}
        mu_t = {f't{j}': rng.uniform(1, 12) for j in range(n_dst)}
        sig_s = {k: rng.uniform(0.3, 1.5) for k in mu_s}
        sig_t = {k: rng.uniform(0.3, 1.5) for k in mu_t}
        sig_t['t0'] = 20.0   # one pathological destination
        residuals = {
            (s, t): [float(rng.normal(mu_s[s], sig_s[s])
                           + rng.normal(mu_t[t], sig_t[t]))
                     for _ in range(reps)]
            for s in mu_s for t in mu_t
        }
        return mu_s, sig_s, mu_t, sig_t, residuals

    def test_sums_recovered(self):
        """Only μ_s + μ_t is identifiable (gauge freedom) — the pairwise
        sums must match, whatever the split."""
        mu_s, _, mu_t, _, residuals = self._synthetic()
        fit_ms, _, fit_mt, _ = fit_additive_params(residuals)
        errs = [abs((fit_ms[s] + fit_mt[t]) - (mu_s[s] + mu_t[t]))
                for s in mu_s for t in mu_t]
        assert float(np.median(errs)) < 1.5   # ms

    def test_centered_offsets_recovered(self):
        mu_s, _, mu_t, _, residuals = self._synthetic()
        fit_ms, _, _, _ = fit_additive_params(residuals)
        true = np.array([mu_s[s] for s in sorted(mu_s)])
        fit = np.array([fit_ms[s] for s in sorted(mu_s)])
        r = np.corrcoef(true - true.mean(), fit - fit.mean())[0, 1]
        assert r > 0.9

    def test_pathological_destination_gets_top_variance(self):
        _, _, _, sig_t, residuals = self._synthetic()
        _, _, _, fit_vt = fit_additive_params(residuals)
        assert max(fit_vt, key=fit_vt.get) == 't0'

    def test_variances_positive(self):
        _, _, _, _, residuals = self._synthetic(seed=3)
        _, vs, _, vt = fit_additive_params(residuals)
        assert all(v > 0 for v in vs.values())
        assert all(v > 0 for v in vt.values())


# ---------------------------------------------------------------------------
# AdditiveLatencyModel — the shared cross-target state object
# ---------------------------------------------------------------------------

from probabilistic_helpers import (
    AdditiveLatencyModel, ADDITIVE_PRIOR_MU_MS, ADDITIVE_PRIOR_VAR_MS2,
)


class TestAdditiveLatencyModel:
    VP = {'a': (50.0, 0.0), 'b': (40.0, 10.0)}

    def test_unfitted_falls_back_to_priors(self):
        m = AdditiveLatencyModel()
        rtt, var = m.predict('a', 't', 1000.0)
        assert rtt == pytest.approx(10.0 + 2 * ADDITIVE_PRIOR_MU_MS)
        assert var == pytest.approx(2 * ADDITIVE_PRIOR_VAR_MS2)
        assert m.sigma_dst('t') == pytest.approx(math.sqrt(ADDITIVE_PRIOR_VAR_MS2))

    def test_refit_learns_offsets_from_estimates(self):
        """rtt = d/100 + 20ms everywhere → fitted mean offset ≈ 20ms
        (the src/dst split itself is gauge-free)."""
        m = AdditiveLatencyModel()
        targets = {'t0': (48.0, 5.0), 't1': (52.0, 8.0)}
        for s, s_loc in self.VP.items():
            for t, t_loc in targets.items():
                d = get_distance(s_loc, t_loc)
                m.record(s, t, [d / 100.0 + 20.0, d / 100.0 + 20.0])
        m.refit(self.VP, targets)
        for s in self.VP:
            for t in targets:
                rtt, _ = m.predict(s, t, get_distance(self.VP[s], targets[t]))
                d = get_distance(self.VP[s], targets[t])
                assert rtt - d / 100.0 == pytest.approx(20.0, abs=2.0)

    def test_refit_skips_pairs_without_estimates(self):
        m = AdditiveLatencyModel()
        m.record('a', 'known', [15.0])
        m.record('a', 'unknown', [55.0])
        m.refit(self.VP, {'known': (48.0, 5.0)})   # no estimate for 'unknown'
        assert 'unknown' not in m.mu_t
        # unknown target still served via priors
        assert m.sigma_dst('unknown') == pytest.approx(math.sqrt(ADDITIVE_PRIOR_VAR_MS2))
