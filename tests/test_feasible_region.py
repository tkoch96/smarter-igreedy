"""
Tests for FeasibleRegion constraint geometry.

Focus: the radius multiplier is 1.05× the speed-of-light floor
(1ms RTT → 100km one-way → 105km constraint radius).  With 1.3× the
circles were so large that every point on Earth satisfied all constraints,
making Nelder-Mead unable to localise anything.  These tests verify the
tighter geometry produces usable intersections.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import math
import pytest
import numpy as np
from feasible_region_maintainer import (
    FeasibleRegion,
    HARD_CIRCLE,
    GAUSSIAN,
    DEFAULT_SLOPE,
    _normalize_latlon,
)
from utils import get_distance

MULTIPLIER = 1.05
KM_PER_MS  = 100.0   # 1ms RTT ≈ 100km one-way (fiber SOL)
# Model-implied km per ms of RTT under the default slope
IMPLIED_KM_PER_MS = KM_PER_MS / DEFAULT_SLOPE


# ---------------------------------------------------------------------------
# Radius arithmetic
# ---------------------------------------------------------------------------

class TestConstraintRadius:
    def test_radius_formula(self):
        """Radius = model-implied distance × multiplier = rtt × 100 / slope × 1.05
        — verify the constants haven't drifted."""
        region = FeasibleRegion("test")
        rtt_ms = 10.0
        region.add_measurement((0.0, 0.0), rtt_ms)
        _, actual_radius = region.constraints[0]
        expected = rtt_ms * IMPLIED_KM_PER_MS * MULTIPLIER
        assert actual_radius == pytest.approx(expected, rel=1e-6)

    def test_radius_scales_linearly_with_rtt(self):
        """Doubling RTT should double the constraint radius."""
        r1 = FeasibleRegion("a")
        r2 = FeasibleRegion("b")
        r1.add_measurement((0.0, 0.0), 5.0)
        r2.add_measurement((0.0, 0.0), 10.0)
        _, rad1 = r1.constraints[0]
        _, rad2 = r2.constraints[0]
        assert rad2 == pytest.approx(2 * rad1, rel=1e-6)

    def test_zero_rtt_gives_zero_radius(self):
        """A 0ms RTT means the target IS the VP — radius should be 0."""
        region = FeasibleRegion("test")
        region.add_measurement((51.5, -0.1), 0.0)
        _, radius = region.constraints[0]
        assert radius == pytest.approx(0.0, abs=1e-9)

    def test_new_constraint_clears_cache(self):
        """Adding a measurement must invalidate the cached region size."""
        region = FeasibleRegion("test")
        region.add_measurement((51.5, -0.1), 50.0)
        _ = region.get_region_size()          # populate cache
        assert region._cached_region_size is not None

        region.add_measurement((48.9, 2.3), 30.0)
        assert region._cached_region_size is None


# ---------------------------------------------------------------------------
# Tightness: 1.05× should actually constrain the globe
# ---------------------------------------------------------------------------

class TestRadiusTightness:
    def test_null_island_excluded_by_single_tight_constraint(self):
        """
        A VP in London with a 5ms RTT (~404km radius under the slope model)
        should exclude Null Island (0°,0°) which is ~5570km away.  This test
        verifies the region size is meaningful (not half the Earth's
        circumference).
        """
        region = FeasibleRegion("target")
        london = (51.5, -0.1)
        rtt_ms = 5.0
        region.add_measurement(london, rtt_ms)
        size = region.get_region_size()
        # With a single constraint the size is exactly the circle radius
        assert size == pytest.approx(rtt_ms * IMPLIED_KM_PER_MS * MULTIPLIER, rel=0.01)
        assert size < 20037.0, "Region should be smaller than half the Earth"

    def test_two_constraints_tighter_than_one(self):
        """Adding a second VP from a different direction should reduce uncertainty."""
        region = FeasibleRegion("target")
        region.add_measurement((51.5, -0.1), 10.0)   # London, ~1050km radius
        size_after_one = region.get_region_size()

        region.add_measurement((48.9, 2.3), 10.0)    # Paris, ~1050km radius
        size_after_two = region.get_region_size()

        assert size_after_two <= size_after_one, (
            "A second constraint from a different angle should not increase uncertainty"
        )

    def test_very_close_vp_gives_tiny_region(self):
        """A VP 1ms away caps the region to ~105km — a tight localisation."""
        region = FeasibleRegion("target")
        region.add_measurement((52.5, 13.4), 1.0)   # Berlin, 1ms RTT
        size = region.get_region_size()
        assert size < 200.0, f"105km radius should give region < 200km, got {size:.1f}km"

    def test_multiple_constraints_from_same_direction_dont_shrink_region(self):
        """
        Redundant VPs clustered in the same city add no new geometric information.
        The region should not grow, but also shouldn't shrink much.
        """
        region = FeasibleRegion("target")
        london1 = (51.50, -0.10)
        london2 = (51.51, -0.09)   # ~1.3km apart — effectively the same spot
        region.add_measurement(london1, 20.0)
        size_before = region.get_region_size()

        region.add_measurement(london2, 20.0)
        size_after = region.get_region_size()

        assert size_after <= size_before + 1.0, (
            "A near-duplicate VP should not significantly increase region size"
        )


# ---------------------------------------------------------------------------
# Nelder-Mead localisation: 1.05× should produce a usable estimate
# ---------------------------------------------------------------------------

class TestNelderMeadLocalisation:
    """
    With 1.3× the loss landscape was nearly flat everywhere, so Nelder-Mead
    stayed at (0,0).  With 1.05× the constraint circles are tight enough
    to steer the optimiser toward the true intersection.
    """

    def _make_region_for_target(self, target: tuple, vp_locs: list, noise_factor: float = 1.02) -> FeasibleRegion:
        """
        Build a FeasibleRegion for `target` using RTTs derived from true distances.
        noise_factor > 1.0 simulates routing overhead (keeps target inside circles).
        """
        region = FeasibleRegion("synthetic")
        for vp in vp_locs:
            true_dist_km = get_distance(vp, target)
            rtt_ms = (true_dist_km / KM_PER_MS) * noise_factor  # slightly over SOL
            region.add_measurement(vp, rtt_ms)
        return region

    def test_estimate_not_at_null_island_with_multiple_constraints(self):
        """With 3+ real constraints the estimate should not be stuck at (0,0)."""
        target = (52.5, 13.4)   # Berlin
        vps = [(51.5, -0.1), (48.9, 2.3), (41.9, 12.5)]  # London, Paris, Rome
        region = self._make_region_for_target(target, vps)

        lat, lon = region.get_location()
        assert not (abs(lat) < 1.0 and abs(lon) < 1.0), (
            f"Estimate should not be at Null Island, got ({lat:.2f}, {lon:.2f})"
        )

    def test_estimate_closer_than_worst_case_with_multiple_vps(self):
        """
        With 4 surrounding VPs and near-SOL RTTs, the estimate should land
        much closer than the radius of any single constraint.
        """
        target = (48.9, 2.3)    # Paris
        vps = [
            (51.5, -0.1),   # London  (~340km)
            (52.5, 13.4),   # Berlin  (~880km)
            (41.9, 12.5),   # Rome    (~1105km)
            (40.4, -3.7),   # Madrid  (~1050km)
        ]
        region = self._make_region_for_target(target, vps, noise_factor=1.02)
        estimate = region.get_location()
        error_km = get_distance(estimate, target)

        # The tightest single constraint radius here is ~340km * 1.05 ≈ 357km.
        # With four surrounding VPs the intersection should be much smaller.
        worst_single_radius = min(
            get_distance(vp, target) * MULTIPLIER * 1.02
            for vp in vps
        )
        assert error_km < worst_single_radius, (
            f"Multi-VP estimate ({error_km:.1f}km) should beat single-constraint "
            f"radius ({worst_single_radius:.1f}km)"
        )

    def test_single_constraint_snaps_to_vp_location(self):
        """
        With exactly one constraint, the best guess is the VP's location
        (we can't do better without more information).
        """
        vp = (59.9, 10.7)   # Oslo
        region = FeasibleRegion("single")
        region.add_measurement(vp, 8.0)
        lat, lon = region.get_location()
        assert lat == pytest.approx(vp[0], abs=0.01)
        assert lon == pytest.approx(vp[1], abs=0.01)

    def test_batch_add_same_result_as_sequential(self):
        """Batch and sequential measurement ingestion should yield the same estimate."""
        vps_rtts = [
            ((51.5, -0.1), 5.0),
            ((48.9,  2.3), 4.0),
            ((52.5, 13.4), 6.0),
        ]

        seq = FeasibleRegion("seq")
        for vp, rtt in vps_rtts:
            seq.add_measurement(vp, rtt)

        bat = FeasibleRegion("bat")
        bat.add_measurements_batch(vps_rtts)

        seq_loc = seq.get_location()
        bat_loc = bat.get_location()
        assert seq_loc[0] == pytest.approx(bat_loc[0], abs=0.5)
        assert seq_loc[1] == pytest.approx(bat_loc[1], abs=0.5)


# ---------------------------------------------------------------------------
# Region size units: both modes must speak km
# ---------------------------------------------------------------------------

class TestRegionSizeUnits:
    """
    get_region_size() is thresholded by mode-agnostic callers (the greedy's
    BASICALLY_GEOLOCATED = 200), so both modes MUST return km-comparable
    values.  The original gaussian implementation returned the raw mean RTT
    residual in ms — a few ms after one ping — which read as "under 200 km"
    and made the greedy declare every target geolocated after a single
    measurement.  These tests pin the km contract in both modes.
    """

    def test_single_constraint_sizes_comparable_across_modes(self):
        """
        One 10ms measurement: hard-circle size is implied distance × 1.05,
        gaussian km-equivalent is the implied distance (rtt × 100 / slope).
        Same order of magnitude, within 5%.
        """
        vp, rtt_ms = (51.5, -0.1), 10.0

        hard = FeasibleRegion("h", mode=HARD_CIRCLE)
        hard.add_measurement(vp, rtt_ms)
        gauss = FeasibleRegion("g", mode=GAUSSIAN)
        gauss.add_measurement(vp, rtt_ms)

        hard_size = hard.get_region_size()
        gauss_size = gauss.get_region_size()

        assert gauss_size == pytest.approx(rtt_ms * IMPLIED_KM_PER_MS, rel=1e-6)
        assert hard_size == pytest.approx(rtt_ms * IMPLIED_KM_PER_MS * MULTIPLIER, rel=1e-6)
        assert gauss_size == pytest.approx(hard_size, rel=0.06)

    def test_gaussian_single_ping_not_basically_geolocated(self):
        """
        The regression that motivated the km contract: one 4ms ping must NOT
        put the region under the greedy's 200 km done-threshold.
        """
        region = FeasibleRegion("g", mode=GAUSSIAN)
        region.add_measurement((48.9, 2.3), 4.0)   # Paris, ~London RTT
        assert region.get_region_size() > 200.0, (
            "A single 4ms ping gave region size <= 200 km — gaussian mode is "
            "leaking ms units into a km threshold again"
        )

    def test_empty_region_size_is_half_earth_in_both_modes(self):
        """No constraints → same 20037 km sentinel regardless of mode."""
        for mode in (HARD_CIRCLE, GAUSSIAN):
            region = FeasibleRegion("empty", mode=mode)
            assert region.get_region_size() == pytest.approx(20037.0)

    def test_gaussian_well_triangulated_region_is_small(self):
        """
        Near-SOL RTTs from 4 surrounding VPs → residuals of a few tenths of a
        ms → tens of km.  The size should be far under one ping's worth.
        """
        target = (48.9, 2.3)    # Paris
        vps = [(51.5, -0.1), (52.5, 13.4), (41.9, 12.5), (40.4, -3.7)]
        region = FeasibleRegion("g", mode=GAUSSIAN)
        for vp in vps:
            rtt_ms = (get_distance(vp, target) / KM_PER_MS) * 1.02
            region.add_measurement(vp, rtt_ms)
        assert region.get_region_size() < 200.0


# ---------------------------------------------------------------------------
# Impossibility sanity checks: no "geolocated" claim the geometry can't back
# ---------------------------------------------------------------------------

class TestGeolocationImpossibility:
    """
    With fewer than 3 VPs a position is geometrically ambiguous no matter
    how well the RTTs fit the model: one ping constrains the target to a
    ring, two pings to two mirror intersection points.  Region size is what
    callers threshold to declare a target done, so it must never fall below
    the best ping's distance scale until a third VP breaks the ambiguity.

    Regression context: gaussian region size is a goodness-of-fit residual —
    two perfectly consistent pings gave ~0.001 km and were declared
    geolocated despite the mirror-point ambiguity.
    """

    TARGET = (48.9, 2.3)                  # Paris
    VPS = [(51.5, -0.1), (41.9, 12.5)]    # London, Rome

    def _perfect_rtts(self):
        """Model-consistent RTTs: rtt = slope × d / 100 (zero residual)."""
        return [
            (vp, DEFAULT_SLOPE * get_distance(vp, self.TARGET) / KM_PER_MS)
            for vp in self.VPS
        ]

    def test_two_perfect_pings_not_geolocated_gaussian(self):
        """Two consistent pings fit perfectly (residual ~0) but cannot
        disambiguate the mirror point — size must stay at ping scale."""
        region = FeasibleRegion("g", mode=GAUSSIAN)
        for vp, rtt in self._perfect_rtts():
            region.add_measurement(vp, rtt)
        min_rtt = min(rtt for _, rtt in self._perfect_rtts())
        assert region.get_region_size() >= min_rtt * IMPLIED_KM_PER_MS
        assert region.get_region_size() > 200.0

    def test_two_perfect_pings_not_geolocated_hard_circle(self):
        """Same bound in hard-circle mode."""
        region = FeasibleRegion("h", mode=HARD_CIRCLE)
        for vp, rtt in self._perfect_rtts():
            region.add_measurement(vp, rtt)
        min_rtt = min(rtt for _, rtt in self._perfect_rtts())
        assert region.get_region_size() >= min_rtt * IMPLIED_KM_PER_MS

    def test_third_vp_lifts_the_floor(self):
        """A third VP resolves the ambiguity: with model-consistent RTTs the
        region may then legitimately drop below the two-ping floor."""
        region = FeasibleRegion("g", mode=GAUSSIAN)
        vps = self.VPS + [(52.5, 13.4)]   # + Berlin
        for vp in vps:
            rtt = DEFAULT_SLOPE * get_distance(vp, self.TARGET) / KM_PER_MS
            region.add_measurement(vp, rtt)
        min_rtt = min(
            DEFAULT_SLOPE * get_distance(vp, self.TARGET) / KM_PER_MS for vp in vps
        )
        assert region.get_region_size() < min_rtt * IMPLIED_KM_PER_MS

    def test_close_ping_still_geolocates_with_three_vps(self):
        """The floor must not block legitimate localisation: three VPs with a
        1ms-scale nearest ping should still read as geolocated."""
        target = (52.5, 13.4)   # Berlin
        vps = [(52.52, 13.41), (51.5, -0.1), (48.9, 2.3)]  # ~2km, London, Paris
        region = FeasibleRegion("g", mode=GAUSSIAN)
        for vp in vps:
            rtt = DEFAULT_SLOPE * get_distance(vp, target) / KM_PER_MS
            region.add_measurement(vp, rtt)
        assert region.get_region_size() < 200.0


# ---------------------------------------------------------------------------
# Gaussian vs hard-circle: what the probabilistic model buys you
# ---------------------------------------------------------------------------

class TestGaussianVsHardCircle:
    """
    Concrete, deterministic demonstration of WHY the gaussian mode exists.

    Both modes share the slope model (expected rtt = slope × d / 100); they
    differ in how they absorb deviations from it.

    Hard-circle encodes deviation in the BOUND: to stay valid it must budget
    a multiplier for the worst case (e.g. 1.3× on top of the model), and its
    region size scales with that slack no matter how mutually consistent
    the measurements are.

    Gaussian encodes deviation in the NOISE: it keeps the model's slope and
    lets σ absorb the excess.  Its region size reflects cross-VP
    consistency: measurements that all agree concentrate the estimate far
    below any single circle's slack.

    Setup: target Paris, 4 surrounding VPs (London, Berlin, Rome, Madrid),
    identical measurements 5% above the assumed slope model fed to both
    modes.  Observed:

        hard(1.3×):  size ≈ 475 km   error ≈ 175 km
        hard(1.05×): size ≈ 194 km   error ≈ 110 km
        gaussian:    size ≈  37 km   error ≈  34 km
    """

    TARGET = (48.9, 2.3)    # Paris
    VPS = [(51.5, -0.1), (52.5, 13.4), (41.9, 12.5), (40.4, -3.7)]
    # True overhead runs 5% above the assumed slope — light, uniform model
    # violation, same km geometry as the pre-slope version of this test.
    OVERHEAD = DEFAULT_SLOPE * 1.05

    def _fill(self, region: FeasibleRegion) -> FeasibleRegion:
        for vp in self.VPS:
            rtt = (get_distance(vp, self.TARGET) / KM_PER_MS) * self.OVERHEAD
            region.add_measurement(vp, rtt)
        return region

    def test_gaussian_region_much_smaller_than_loose_hard_circle(self):
        """Gaussian beats the worst-case-budgeted hard bound by >5× (observed ~13×)."""
        hard = self._fill(FeasibleRegion("h", mode=HARD_CIRCLE, radius_multiplier=1.3))
        gauss = self._fill(FeasibleRegion("g", mode=GAUSSIAN))
        assert gauss.get_region_size() < hard.get_region_size() / 5.0

    def test_gaussian_region_smaller_than_tight_hard_circle(self):
        """
        Even a hard bound tuned exactly to the true overhead (1.05×, which is
        only valid BECAUSE we know the overhead here) reports >2× the
        gaussian's uncertainty (observed ~5×): the lens area is set by the
        bound's slack, while the gaussian concentrates on consistency.
        """
        hard = self._fill(FeasibleRegion("h", mode=HARD_CIRCLE, radius_multiplier=1.05))
        gauss = self._fill(FeasibleRegion("g", mode=GAUSSIAN))
        assert gauss.get_region_size() < hard.get_region_size() / 2.0

    def test_gaussian_estimate_more_accurate(self):
        """Smaller region is honest, not bravado: the estimate is also better
        (observed ~34 km vs ~175 km)."""
        hard = self._fill(FeasibleRegion("h", mode=HARD_CIRCLE, radius_multiplier=1.3))
        gauss = self._fill(FeasibleRegion("g", mode=GAUSSIAN))
        gauss_error = get_distance(gauss.get_location(), self.TARGET)
        hard_error = get_distance(hard.get_location(), self.TARGET)
        assert gauss_error < hard_error
        assert gauss_error < 100.0

    def test_hard_circle_size_tracks_its_slack_budget(self):
        """
        The structural reason hard-circle can't win this game: its region
        size grows with the multiplier, not with measurement quality — the
        same perfectly consistent data reads as more uncertain simply
        because the bound is more conservative.
        """
        loose = self._fill(FeasibleRegion("l", mode=HARD_CIRCLE, radius_multiplier=1.3))
        tight = self._fill(FeasibleRegion("t", mode=HARD_CIRCLE, radius_multiplier=1.05))
        assert loose.get_region_size() > tight.get_region_size() * 1.5

    def test_two_measurements_gaussian_capped_by_ambiguity_floor(self):
        """
        The two-measurement case, step by step (observed values):

            n=2:  hard(1.3×) ≈ 450 km    gaussian ≈ 353 km  (= the floor)
            n=4:  hard(1.3×) ≈ 475 km    gaussian ≈  37 km

        With two VPs the gaussian residual is ~0 (a perfect fit), but its
        reported size sits EXACTLY on the trilateration floor
        (min rtt × 100 km): fit quality cannot disambiguate the two mirror
        intersection points, so "≪ hard-circle" would be a lie at n=2 and
        the floor refuses it.  Gaussian is still smaller than the
        slack-budgeted hard bound — just honestly, not dramatically.

        The third and fourth VP break the ambiguity: gaussian collapses by
        ~10× while hard-circle barely moves, because hard-circle's size is
        pinned to its slack budget, not to measurement consistency.  The
        "much smaller" claim is earned exactly when the geometry allows it.
        """
        two_vps = self.VPS[:2]

        def fill(region: FeasibleRegion, vps) -> FeasibleRegion:
            for vp in vps:
                rtt = (get_distance(vp, self.TARGET) / KM_PER_MS) * self.OVERHEAD
                region.add_measurement(vp, rtt)
            return region

        hard2 = fill(FeasibleRegion("h2", mode=HARD_CIRCLE, radius_multiplier=1.3), two_vps)
        gauss2 = fill(FeasibleRegion("g2", mode=GAUSSIAN), two_vps)

        floor_km = min(
            (get_distance(vp, self.TARGET) / KM_PER_MS) * self.OVERHEAD
            for vp in two_vps
        ) * IMPLIED_KM_PER_MS

        # n=2: gaussian sits on the ambiguity floor — smaller than hard,
        # but capped well short of "much smaller".
        assert gauss2.get_region_size() == pytest.approx(floor_km, rel=0.01)
        assert gauss2.get_region_size() < hard2.get_region_size()
        assert hard2.get_region_size() < 2.0 * gauss2.get_region_size()

        # n=4: ambiguity broken — gaussian collapses, hard stays slack-bound.
        hard4 = fill(FeasibleRegion("h4", mode=HARD_CIRCLE, radius_multiplier=1.3), self.VPS)
        gauss4 = fill(FeasibleRegion("g4", mode=GAUSSIAN), self.VPS)
        assert gauss4.get_region_size() < gauss2.get_region_size() / 3.0
        assert hard4.get_region_size() > hard2.get_region_size() / 1.5
        assert gauss4.get_region_size() < hard4.get_region_size() / 5.0

    def test_slope_model_scenario(self):
        """
        The three-model story rendered by the figure, asserted numerically.

        Mixed measurements (London/Berlin/Madrid at 2.0×SOL, Rome on a
        lucky 1.0×SOL path), modeler assumes a 1.3× slope:

        (a) classic (radius = rtt×100):  always valid — overhead only
            inflates radii — but loose.
        (b) slacked (radius = rtt×100/1.3):  tighter, but Rome's near-SOL
            RTT shrinks below the true distance; its circle excludes the
            truth and the intersection is EMPTY — geolocation fails.
        (c) gaussian (rtt ≈ 1.3×SOL + noise):  same slope, soft — Rome is
            unlikely, not impossible; the MAP stays near the truth.
        """
        sys.path.insert(0, os.path.dirname(__file__))
        import plot_gaussian_vs_hard_circle as fig

        rtts = fig.measurements()
        LATS, LONS = fig.grid()
        dist_grids = fig.vp_distance_grids(LATS, LONS)

        # (a) classic is valid: every circle contains the truth...
        classic_radii = {n: r * KM_PER_MS for n, r in rtts.items()}
        for name, (vp, _) in fig.VPS.items():
            assert classic_radii[name] >= get_distance(vp, fig.TARGET)
        # ...but loose: the feasible blob is far bigger than "geolocated".
        classic_mask = fig.feasible_mask(dist_grids, classic_radii)
        classic_area = fig.cell_areas_km2(LATS)[classic_mask].sum()
        assert classic_area > 100_000.0  # km² — observed ~244k

        # (b) slacked excludes the truth via the near-SOL path...
        slacked_radii = {n: r * KM_PER_MS / fig.ASSUMED_SLOPE for n, r in rtts.items()}
        d_rome = get_distance(fig.VPS['Rome'][0], fig.TARGET)
        assert slacked_radii['Rome'] < d_rome
        # ...and the whole intersection is empty — provably (London's and
        # Rome's shrunk circles are disjoint) and on the grid.
        d_london_rome = get_distance(fig.VPS['London'][0], fig.VPS['Rome'][0])
        assert slacked_radii['London'] + slacked_radii['Rome'] < d_london_rome
        assert not fig.feasible_mask(dist_grids, slacked_radii).any()

        # (c) gaussian with the SAME slope still geolocates.
        nll = fig.gaussian_nll_grid(dist_grids)
        map_est = fig.gaussian_map_estimate(LATS, LONS, nll)
        assert get_distance(map_est, fig.TARGET) < 400.0  # observed ~197km

    def test_slope_model_scenario_via_production_class(self):
        """
        The same three models, expressed through FeasibleRegion itself now
        that the slope is plumbed in — the figure's grid math and the
        production code must agree.
        """
        sys.path.insert(0, os.path.dirname(__file__))
        import plot_gaussian_vs_hard_circle as fig

        rtts = fig.measurements()

        def fill(region: FeasibleRegion) -> FeasibleRegion:
            for name, (vp, _) in fig.VPS.items():
                region.add_measurement(vp, rtts[name])
            return region

        # (a) classic = slope 1.0, no extra slack: valid but loose.
        classic = fill(FeasibleRegion("a", mode=HARD_CIRCLE,
                                      slope=1.0, radius_multiplier=1.0))
        for (vp_loc, radius) in classic.constraints:
            assert radius >= get_distance(vp_loc, fig.TARGET)
        assert classic.get_region_size() > 200.0

        # (b) slacked = slope 1.3, no extra slack: Rome's constraint
        # excludes the truth — the hard model is invalid.
        slacked = fill(FeasibleRegion("b", mode=HARD_CIRCLE,
                                      slope=fig.ASSUMED_SLOPE,
                                      radius_multiplier=1.0))
        rome_loc = fig.VPS['Rome'][0]
        rome_radius = next(r for (vp_loc, r) in slacked.constraints
                           if vp_loc == rome_loc)
        assert rome_radius < get_distance(rome_loc, fig.TARGET)

        # (c) gaussian at the default slope (= the figure's assumed slope)
        # still produces a nearby estimate.
        assert fig.ASSUMED_SLOPE == DEFAULT_SLOPE
        gauss = fill(FeasibleRegion("c", mode=GAUSSIAN))
        assert get_distance(gauss.get_location(), fig.TARGET) < 400.0

    def test_generate_map_figure(self):
        """
        Renders the three-model scenario as a 3-panel map
        (tests/gaussian_vs_hard_circle.pdf): classic and slacked
        hard-overlap circles with their shaded feasible lens (empty in the
        slacked panel), and the gaussian posterior heat map — the same
        measurements in every panel.
        """
        sys.path.insert(0, os.path.dirname(__file__))
        from plot_gaussian_vs_hard_circle import make_figure, DEFAULT_OUTPUT
        path = make_figure(DEFAULT_OUTPUT)
        assert os.path.exists(path)
        assert os.path.getsize(path) > 10_000, "figure PDF suspiciously small"


# ---------------------------------------------------------------------------
# Coordinate normalisation: estimates must stay on the globe
# ---------------------------------------------------------------------------

class TestLatLonNormalization:
    """
    Nelder-Mead optimises lat/lon unconstrained; the periodic haversine means
    off-globe parameterisations (lat=125°) fit just as well and the optimiser
    returned them.  Downstream consumers (region-size probing, error
    calculation) then operated on garbage.  Estimates must always come back
    canonical.
    """

    def test_in_range_passthrough(self):
        assert _normalize_latlon(48.9, 2.3) == pytest.approx((48.9, 2.3))
        assert _normalize_latlon(-33.9, 151.2) == pytest.approx((-33.9, 151.2))

    def test_pole_crossing_flips_meridian(self):
        # 125.7° north = 35.7° past the pole → 54.3° on the opposite meridian
        lat, lon = _normalize_latlon(125.7, -17.3)
        assert lat == pytest.approx(54.3)
        assert lon == pytest.approx(162.7)

    def test_south_pole_crossing(self):
        lat, lon = _normalize_latlon(-100.0, 0.0)
        assert lat == pytest.approx(-80.0)
        assert lon == pytest.approx(-180.0)

    def test_full_wrap(self):
        lat, lon = _normalize_latlon(326.9, -44.5)
        assert lat == pytest.approx(-33.1)
        assert lon == pytest.approx(-44.5)

    @staticmethod
    def _unit_vec(lat_deg: float, lon_deg: float) -> np.ndarray:
        phi, lam = math.radians(lat_deg), math.radians(lon_deg)
        return np.array([
            math.cos(phi) * math.cos(lam),
            math.cos(phi) * math.sin(lam),
            math.sin(phi),
        ])

    def test_normalization_preserves_physical_point(self):
        """
        The wrapped coordinate must be the SAME point on the sphere.
        Checked via the 3D embedding — haversine itself cannot be used here
        because fast_haversine raises a math domain error on off-globe
        latitudes (another reason estimates must be normalised before they
        reach downstream consumers).
        """
        for raw in [(125.7, -17.3), (223.1, -29.8), (-100.0, 40.0), (96.6, 0.0)]:
            canonical = _normalize_latlon(*raw)
            dot = np.dot(self._unit_vec(*raw), self._unit_vec(*canonical))
            assert dot == pytest.approx(1.0, abs=1e-9), (
                f"{raw} normalised to {canonical}, a different physical point"
            )

    def test_estimates_stay_on_globe_both_modes(self):
        """End-to-end: optimised estimates are always in valid ranges."""
        target = (35.7, 139.7)   # Tokyo — far from all VPs
        vps = [(51.5, -0.1), (40.7, -74.0), (-33.9, 151.2), (-23.5, -46.6)]
        for mode in (HARD_CIRCLE, GAUSSIAN):
            region = FeasibleRegion("t", mode=mode)
            for vp in vps:
                rtt = (get_distance(vp, target) / KM_PER_MS) * 1.2
                region.add_measurement(vp, rtt)
            lat, lon = region.get_location()
            assert -90.0 <= lat <= 90.0, f"[{mode}] latitude off-globe: {lat}"
            assert -180.0 <= lon <= 180.0, f"[{mode}] longitude off-globe: {lon}"


# ---------------------------------------------------------------------------
# Noise-model toggle: gaussian / student_t / asymmetric
# ---------------------------------------------------------------------------

from probabilistic_helpers import (
    GAUSSIAN_NOISE,
    STUDENT_T_NOISE,
    ASYMMETRIC_NOISE,
)


class TestNoiseModels:
    """
    The per-residual likelihood shape is toggleable on the soft modes.
    RTT-vs-model residuals are one-sided (SOL is a hard floor) and
    heavy-tailed (detours), which gaussian handles worst.

    Calibrated single-detour scenario (4 model-consistent VPs + one 10×
    detour): gaussian error ≈ 1524 km, student_t ≈ 356 km, asymmetric ≈ 0 km.
    """

    TARGET = (48.9, 2.3)    # Paris
    VPS = [(51.5, -0.1), (52.5, 13.4), (41.9, 12.5), (40.4, -3.7), (52.37, 4.9)]

    def _region_with_detour(self, noise_model: str) -> FeasibleRegion:
        region = FeasibleRegion("t", mode=GAUSSIAN, noise_model=noise_model)
        for i, vp in enumerate(self.VPS):
            rtt = DEFAULT_SLOPE * get_distance(vp, self.TARGET) / KM_PER_MS
            if i == len(self.VPS) - 1:
                rtt *= 10.0    # long-routed detour
            region.add_measurement(vp, rtt)
        return region

    def test_noise_model_plumbs_through_and_clones(self):
        region = FeasibleRegion("t", mode=GAUSSIAN, noise_model=STUDENT_T_NOISE)
        assert region.noise_model == STUDENT_T_NOISE
        assert region.clone().noise_model == STUDENT_T_NOISE
        # default stays gaussian
        assert FeasibleRegion("t", mode=GAUSSIAN).noise_model == GAUSSIAN_NOISE

    def test_student_t_resists_detour_better_than_gaussian(self):
        err_g = get_distance(
            self._region_with_detour(GAUSSIAN_NOISE).get_location(), self.TARGET)
        err_t = get_distance(
            self._region_with_detour(STUDENT_T_NOISE).get_location(), self.TARGET)
        assert err_t < err_g / 2.0, (
            f"student_t ({err_t:.0f} km) should resist the detour far better "
            f"than gaussian ({err_g:.0f} km)"
        )

    def test_asymmetric_ignores_detour_almost_entirely(self):
        """The linear detour tail plus four consistent measurements should
        pin the estimate essentially at the truth."""
        err_a = get_distance(
            self._region_with_detour(ASYMMETRIC_NOISE).get_location(), self.TARGET)
        assert err_a < 50.0

    def test_gaussian_dragged_by_detour(self):
        """Document the failure the robust models fix: quadratic loss
        chases the outlier by >500 km."""
        err_g = get_distance(
            self._region_with_detour(GAUSSIAN_NOISE).get_location(), self.TARGET)
        assert err_g > 500.0


# ---------------------------------------------------------------------------
# Region-convergence filmstrip
# ---------------------------------------------------------------------------

class TestGenerateConvergenceFigure:
    """
    tests/region_convergence.pdf — rows = methods, columns = measurement
    counts, with a +70ms detour injected as measurement #4.  The assertions
    pin the four stories the figure tells (deterministic scenario):

      hard-circle : detour circle is harmless (contains everything);
                    the lens keeps shrinking → converges.
      gaussian    : quadratic loss chases the detour and NEVER recovers.
      asymmetric  : dragged briefly at k=4, then snaps back to the truth.
      em_gaussian : absorbs the detour into an inflated fitted μ̂ → stuck.
    """

    @pytest.fixture(scope='class')
    def trajectories(self):
        sys.path.insert(0, os.path.dirname(__file__))
        from plot_region_convergence import run_method, METHODS, TARGET
        out = {}
        for label, kwargs in METHODS:
            snaps = run_method(kwargs)
            out[label] = {
                k: {'err': get_distance(s['location'], TARGET),
                    'slope': s['slope']}
                for k, s in snaps.items()
            }
        return out

    def test_hard_circle_converges_despite_detour(self, trajectories):
        assert trajectories['hard-circle'][10]['err'] < 300.0

    def test_gaussian_never_recovers_from_detour(self, trajectories):
        assert trajectories['gaussian'][10]['err'] > 800.0

    def test_asymmetric_recovers_from_detour(self, trajectories):
        t = trajectories['asymmetric noise']
        assert t[4]['err'] > 1000.0     # briefly dragged when detour lands
        assert t[10]['err'] < 300.0     # then snaps back to the truth
        assert t[10]['err'] < t[4]['err'] / 5.0

    def test_em_absorbs_detour_into_inflated_slope(self, trajectories):
        """The EM × contamination interaction (TODOS #1.2), demonstrated:
        μ_true = 1.4 but the fitted μ̂ inflates well past it to explain the
        detour, and the estimate stays far off."""
        t = trajectories['em_gaussian']
        assert t[10]['slope'] > 1.5
        assert t[10]['err'] > 800.0

    def test_figure_renders(self):
        sys.path.insert(0, os.path.dirname(__file__))
        from plot_region_convergence import make_figure, OUT_PATH
        path = make_figure(OUT_PATH)
        assert os.path.exists(path)
        assert os.path.getsize(path) > 10_000
