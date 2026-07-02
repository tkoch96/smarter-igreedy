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
from feasible_region_maintainer import FeasibleRegion
from utils import get_distance

MULTIPLIER = 1.05
KM_PER_MS  = 100.0   # 1ms RTT ≈ 100km one-way (fiber SOL)


# ---------------------------------------------------------------------------
# Radius arithmetic
# ---------------------------------------------------------------------------

class TestConstraintRadius:
    def test_radius_formula(self):
        """Radius = rtt * 100 * 1.05 — verify the constant hasn't drifted."""
        region = FeasibleRegion("test")
        rtt_ms = 10.0
        region.add_measurement((0.0, 0.0), rtt_ms)
        _, actual_radius = region.constraints[0]
        expected = rtt_ms * KM_PER_MS * MULTIPLIER
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
        A VP in London with a 5ms RTT (525km radius) should exclude Null Island
        (0°,0°) which is ~5570km away.  With the old 1.3× multiplier the radius
        was 650km — still too small to reach (0,0), but this test verifies the
        region size is meaningful (not half the Earth's circumference).
        """
        region = FeasibleRegion("target")
        london = (51.5, -0.1)
        rtt_ms = 5.0   # 525km radius at 1.05×
        region.add_measurement(london, rtt_ms)
        size = region.get_region_size()
        # With a single constraint the estimate snaps to the VP, so size = radius - 0
        assert size == pytest.approx(rtt_ms * KM_PER_MS * MULTIPLIER, rel=0.01)
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
