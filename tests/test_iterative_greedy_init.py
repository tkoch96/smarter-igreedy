"""
Tests for Iterative_Greedy_Geolocator initialization and measurements().

The bug: best_vp_cache was never seeded in solve(), causing measurements()
to infinite-loop (focus_group always empty → best_global_dst always None →
focus_group reset → repeat).

The fix: solve() now calls _update_best_vp_for_target(dst) for every target
after building the initial state, so measurements() has a non-empty cache
to work from on the very first iteration.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from iterative_greedy_geolocator import Iterative_Greedy_Geolocator
from feasible_region_maintainer import HARD_CIRCLE, GAUSSIAN, DEFAULT_SLOPE


def make_synthetic_data(n_nodes=6, locations=None):
    """
    Build a tiny all-pairs mesh.

    By default nodes are placed on a rough grid across Europe so RTTs are
    physically plausible (distance / 100 km per ms).  Every node pings every
    other node.  Pass `locations` to override the node placement.
    """
    # (lat, lon) for n_nodes cities
    if locations is None:
        locations = [
            (51.5, -0.1),   # London
            (48.9,  2.3),   # Paris
            (52.5, 13.4),   # Berlin
            (41.9, 12.5),   # Rome
            (40.4, -3.7),   # Madrid
            (59.9, 10.7),   # Oslo
        ]
    locations = locations[:n_nodes]

    node_ids = [f"10.0.{i}.0" for i in range(n_nodes)]
    address_to_loc = {nid: loc for nid, loc in zip(node_ids, locations)}

    from utils import get_distance
    loc_loc_meas = {}
    for i, src in enumerate(node_ids):
        loc_loc_meas[src] = {}
        for j, dst in enumerate(node_ids):
            if i == j:
                continue
            dist_km = get_distance(locations[i], locations[j])
            # Ground truth follows the model slope with 20 % extra overhead
            # on top (so hard circles stay valid: radius = implied distance
            # × 1.05 = 1.26 × d, the same km geometry as the pre-slope data)
            rtt_ms = (dist_km / 100.0) * DEFAULT_SLOPE * 1.2
            # Real pipeline wraps the min RTT in a list (assess_geolocators.py line ~31)
            loc_loc_meas[src][dst] = [rtt_ms]

    return {'address_to_loc': address_to_loc, 'loc_loc_meas': loc_loc_meas}


@pytest.fixture
def small_data():
    return make_synthetic_data(n_nodes=6)


@pytest.fixture
def geolocator(small_data):
    ig = Iterative_Greedy_Geolocator(max_workers=1)
    ig.set_data(small_data)
    ig.solve()
    return ig


# ---------------------------------------------------------------------------
# Cache initialization tests (the bug / the fix)
# ---------------------------------------------------------------------------

class TestCacheInit:
    def test_cache_populated_after_solve(self, geolocator, small_data):
        """best_vp_cache must be non-empty after solve() — one entry per target."""
        all_targets = {
            dst
            for dsts in small_data['loc_loc_meas'].values()
            for dst in dsts
        }
        assert len(geolocator.best_vp_cache) == len(all_targets), (
            "best_vp_cache should have one entry per target after solve()"
        )

    def test_cache_entries_have_valid_src(self, geolocator, small_data):
        """Every cache entry should point to a real source node."""
        all_srcs = set(small_data['loc_loc_meas'].keys())
        for dst, (src, utility) in geolocator.best_vp_cache.items():
            assert src in all_srcs, (
                f"Cache entry for {dst} has unknown src {src!r}"
            )

    def test_cache_entries_have_finite_utility(self, geolocator):
        """Every cache entry must have a finite (non -inf) utility score."""
        for dst, (src, utility) in geolocator.best_vp_cache.items():
            assert utility > -float('inf'), (
                f"Cache entry for {dst} has -inf utility (VP evaluation failed)"
            )


# ---------------------------------------------------------------------------
# measurements() liveness tests
# ---------------------------------------------------------------------------

class TestMeasurementsLiveness:
    def test_measurements_returns_within_budget(self, geolocator):
        """measurements(budget) must return without hanging."""
        budget = 10
        result = geolocator.measurements(budget)
        # Count total (src, dst) pairs returned
        total = sum(len(dsts) for dsts in result.values())
        assert total == budget, (
            f"Expected {budget} measurements, got {total}"
        )

    def test_measurements_covers_multiple_targets(self, geolocator):
        """With budget >= n_targets, every target should receive at least one ping."""
        n_targets = len(geolocator.targets)
        result = geolocator.measurements(n_targets)

        covered_dsts = {dst for dsts in result.values() for dst in dsts}
        assert len(covered_dsts) == n_targets, (
            f"Expected all {n_targets} targets covered, got {len(covered_dsts)}"
        )

    def test_measurements_no_duplicate_src_dst_pairs(self, geolocator):
        """The same (src, dst) pair should never appear twice in one budget."""
        result = geolocator.measurements(20)
        seen = set()
        for src, dsts in result.items():
            for dst in dsts:
                pair = (src, dst)
                assert pair not in seen, f"Duplicate pair {pair}"
                seen.add(pair)

    def test_measurements_only_uses_available_pairs(self, geolocator, small_data):
        """Every returned (src, dst) pair must exist in the input data."""
        result = geolocator.measurements(15)
        loc_loc_meas = small_data['loc_loc_meas']
        for src, dsts in result.items():
            for dst in dsts:
                assert src in loc_loc_meas and dst in loc_loc_meas[src], (
                    f"Pair ({src}, {dst}) not in input data"
                )

    def test_measurements_history_grows_monotonically(self, geolocator):
        """Calling measurements() twice with increasing budgets should extend history."""
        geolocator.measurements(5)
        len_after_5 = len(geolocator.measurement_history)
        geolocator.measurements(10)
        len_after_10 = len(geolocator.measurement_history)
        assert len_after_5 == 5
        assert len_after_10 == 10


# ---------------------------------------------------------------------------
# region_mode plumbing tests (the overlap-methodology knob)
# ---------------------------------------------------------------------------

class TestRegionModePlumbing:
    def test_default_mode_is_hard_circle(self, geolocator):
        """Omitting region_mode preserves the original hard-circle behaviour."""
        assert geolocator.region_mode == HARD_CIRCLE
        for region in geolocator.target_regions.values():
            assert region.mode == HARD_CIRCLE

    def test_gaussian_mode_propagates_to_all_regions(self, small_data):
        """region_mode=GAUSSIAN must reach every target's FeasibleRegion."""
        ig = Iterative_Greedy_Geolocator(max_workers=1, region_mode=GAUSSIAN)
        ig.set_data(small_data)
        ig.solve()
        try:
            assert ig.target_regions, "solve() produced no regions"
            for region in ig.target_regions.values():
                assert region.mode == GAUSSIAN
        finally:
            ig.cleanup()

    def test_gaussian_mode_selection_loop_runs(self, small_data):
        """
        The selection loop must work end-to-end on gaussian regions,
        including budgets above the target count.  Region sizes are
        km-equivalents in both modes (see TestRegionSizeUnits in
        test_feasible_region.py), so a single ping no longer drops a target
        under BASICALLY_GEOLOCATED; and once every target genuinely is
        done, measurements() returns what it has instead of hanging.
        """
        n_targets = 6
        ig = Iterative_Greedy_Geolocator(max_workers=1, region_mode=GAUSSIAN)
        ig.set_data(small_data)
        ig.solve()
        try:
            budget = 2 * n_targets
            result = ig.measurements(budget)
            total = sum(len(dsts) for dsts in result.values())
            assert 0 < total <= budget

            # A single ping must not "finish" a target: with the ms→km fix,
            # at least one target should have received a second ping.
            assert any(
                len(region.constraints) >= 2
                for region in ig.target_regions.values()
            ), "No target got a second ping — one ping is 'finishing' targets again"

            # gaussian constraints are (vp_loc, sigma_ms, rtt_ms) 3-tuples
            for region in ig.target_regions.values():
                for constraint in region.constraints:
                    assert len(constraint) == 3
        finally:
            ig.cleanup()

    def test_impossible_mesh_geolocates_nothing(self):
        """
        Sanity check: when it is literally impossible to geolocate anything,
        the greedy must not claim otherwise.

        Why impossible — circles CAN pin a target down when they overlap the
        right way, but only if some circle has small absolute slack.  A
        circle can never exclude points closer to the VP than its radius,
        and here radius = implied distance × 1.05 with the true RTT carrying
        20% overhead beyond the assumed slope, i.e.
        radius = 1.26 × true_distance.  So every point within
        0.26 × d_v of the true location satisfies constraint v, and a fully
        feasible ball of radius min_v 0.26 × d_v surrounds the truth no
        matter how the circles intersect.  With every node >= 5570km from
        every other (min pair: London-NYC), that ball has radius >= ~1450km.
        Localising to 200km would need a VP within ~770km — none exists.

        After exhausting every available ping, no target may be declared
        geolocated (dropped from the VP cache), and every hard-circle
        region must report at least the guaranteed-ball bound.
        """
        global_cities = [
            (51.5,  -0.1),    # London
            (40.7, -74.0),    # New York
            (35.7, 139.7),    # Tokyo
            (-33.9, 151.2),   # Sydney
            (-23.5, -46.6),   # São Paulo
            (28.6,  77.2),    # Delhi
        ]
        data = make_synthetic_data(n_nodes=6, locations=global_cities)
        n_targets = 6
        n_pairs = sum(len(d) for d in data['loc_loc_meas'].values())

        for mode in (HARD_CIRCLE, GAUSSIAN):
            ig = Iterative_Greedy_Geolocator(max_workers=1, region_mode=mode)
            ig.set_data(data)
            ig.solve()
            try:
                ig.measurements(n_pairs + 10)   # run to exhaustion
                assert len(ig.best_vp_cache) == n_targets, (
                    f"[{mode}] {n_targets - len(ig.best_vp_cache)} target(s) "
                    "declared geolocated on a mesh where that is impossible"
                )
                # Hard-circle sizes are lower-bounded by the guaranteed
                # feasible ball (min_v 0.26 × d_v >= ~1450km; assert 1300
                # for slack).  Gaussian sizes are residual-based, so only
                # the done-threshold bound applies.
                size_floor = 1300.0 if mode == HARD_CIRCLE else 200.0
                for dst, region in ig.target_regions.items():
                    size = region.get_region_size()
                    assert size > size_floor, (
                        f"[{mode}] {dst} reports region size {size:.1f}km on "
                        f"an impossible mesh (floor {size_floor:.0f}km)"
                    )
            finally:
                ig.cleanup()

    def test_measurements_terminates_when_all_pairs_exhausted(self, small_data):
        """
        A budget larger than every available (src, dst) pair must return
        early rather than loop forever once the VP cache is exhausted.
        Applies to both modes; exercised here with the default hard_circle.
        """
        ig = Iterative_Greedy_Geolocator(max_workers=1)
        ig.set_data(small_data)
        ig.solve()
        try:
            n_pairs = sum(len(d) for d in small_data['loc_loc_meas'].values())
            result = ig.measurements(n_pairs + 100)
            total = sum(len(dsts) for dsts in result.values())
            assert 0 < total <= n_pairs
        finally:
            ig.cleanup()
