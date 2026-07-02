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


def make_synthetic_data(n_nodes=6):
    """
    Build a tiny all-pairs mesh.

    Nodes are placed on a rough grid across Europe so RTTs are physically
    plausible (distance / 100 km per ms).  Every node pings every other node.
    """
    # (lat, lon) for n_nodes cities
    locations = [
        (51.5, -0.1),   # London
        (48.9,  2.3),   # Paris
        (52.5, 13.4),   # Berlin
        (41.9, 12.5),   # Rome
        (40.4, -3.7),   # Madrid
        (59.9, 10.7),   # Oslo
    ][:n_nodes]

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
            # 1 ms ≈ 100 km propagation; add 20 % routing overhead
            rtt_ms = (dist_km / 100.0) * 1.2
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
