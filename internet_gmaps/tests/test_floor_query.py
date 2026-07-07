"""Unit tests for floor_query.py.

The load-bearing test is exactness: FloorEstimator (fields + pruned
KD-tree expansion) must agree with a brute-force reference that evaluates
the floor definition directly over all node pairs. If pruning or the
virtual-source construction is wrong, these disagree.
"""

import numpy as np
import pytest
from scipy.sparse.csgraph import dijkstra

import geo
from fiber_graph import GraphBuilder
from floor_query import FloorEstimator, floor_path_ms

EQ_DEG_KM = np.pi * geo.EARTH_RADIUS_KM / 180  # km per equator degree, derived — never hardcode


def brute_force_floor(graph, vp, target, direct_km_max, lastmile_km_max=300.0):
    """The floor definition, evaluated the slow way: full APSP + min over
    all entry/exit node pairs (last-mile legs capped), plus the
    short-range direct option."""
    km_vp = geo.haversine_km(vp[0], vp[1], graph.node_lat, graph.node_lon)
    km_t = geo.haversine_km(target[0], target[1], graph.node_lat, graph.node_lon)
    lm_vp = np.where(km_vp <= lastmile_km_max, geo.rtt_ms(km_vp), np.inf)
    lm_t = np.where(km_t <= lastmile_km_max, geo.rtt_ms(km_t), np.inf)
    via = np.inf
    if graph.n_nodes:
        d = dijkstra(graph.csr, directed=True)
        via = float(np.min(lm_vp[:, None] + d + lm_t[None, :]))
    d_direct = float(geo.haversine_km(vp[0], vp[1], target[0], target[1]))
    direct = geo.rtt_ms(d_direct) if d_direct <= direct_km_max else np.inf
    return min(via, direct)


def equator_chain(lon_start, lon_stop):
    """Unit-slack chain of 1-degree segments along the equator."""
    b = GraphBuilder(snap_tolerance_km=1.0)
    b.add_path([(0.0, float(lon)) for lon in range(lon_start, lon_stop + 1)])
    return b.build()


class TestHandComputedCases:
    def test_vp_and_target_off_both_ends_of_a_chain(self):
        g = equator_chain(0, 10)
        est = FloorEstimator(g, [0.0], [-0.5], direct_km_max=0.0)
        # last mile 0.5 deg + chain 10 deg + last mile 0.5 deg = 11 equator degrees
        expected = geo.rtt_ms(11 * EQ_DEG_KM)
        assert est.floor_ms(0.0, 10.5)[0] == pytest.approx(expected, rel=1e-6)

    def test_direct_option_short_range(self):
        g = equator_chain(0, 10)
        est = FloorEstimator(g, [0.0], [5.0], direct_km_max=300.0)
        # target ~111 km from the VP: direct geodesic beats a backbone round trip
        assert est.floor_ms(0.0, 6.0)[0] == pytest.approx(geo.rtt_ms(EQ_DEG_KM), rel=1e-6)

    def test_same_point_is_zero(self):
        g = equator_chain(0, 10)
        est = FloorEstimator(g, [0.0], [5.0], direct_km_max=300.0)
        assert est.floor_ms(0.0, 5.0)[0] == pytest.approx(0.0, abs=1e-9)

    def test_unreachable_island_stays_inf(self):
        # VP snaps to an isolated 2-node island; the target is beyond both
        # the direct range and the last-mile cap of every node -> inf floor.
        # (This is the guard against the beeline collapse: without the
        # last-mile cap, vp -> node -> target at pure fiber speed would
        # produce a finite "floor" using no infrastructure at all.)
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0.0, 0.0), (0.0, 1.0)])  # island near VP
        g = b.build()
        est = FloorEstimator(g, [0.0], [0.2], direct_km_max=100.0, lastmile_km_max=100.0)
        assert np.isinf(est.floor_ms(0.0, 90.0)[0])

    def test_island_near_target_does_not_stop_expansion(self):
        # nearest graph nodes to the target are an island unreachable from
        # the VP; the pruned expansion must keep going to the mainland
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0.0, float(l)) for l in range(0, 9)])  # mainland: lon 0..8
        b.add_path([(0.0, 10.0), (0.0, 10.5)])  # island right next to target
        g = b.build()
        est = FloorEstimator(g, [0.0], [0.0], direct_km_max=0.0)
        got = est.floor_ms(0.0, 10.2)[0]
        expected = brute_force_floor(g, (0.0, 0.0), (0.0, 10.2), direct_km_max=0.0)
        assert np.isfinite(got)
        assert got == pytest.approx(expected, rel=1e-9)


class TestExactnessAgainstBruteForce:
    @pytest.mark.parametrize("seed", [0, 1, 2, 3, 4])
    def test_random_graphs(self, seed):
        rng = np.random.default_rng(seed)
        b = GraphBuilder(snap_tolerance_km=1.0)
        n = 30
        lats = rng.uniform(-20, 20, n)
        lons = rng.uniform(-20, 20, n)
        ids = [b.node_id(lat, lon) for lat, lon in zip(lats, lons)]
        for _ in range(45):  # random edges; disconnection happens naturally
            i, j = rng.integers(0, n, 2)
            if ids[i] != ids[j]:
                km = geo.haversine_km(lats[i], lons[i], lats[j], lons[j])
                b.add_edge(ids[i], ids[j], geo.rtt_ms(km * rng.uniform(1.0, 1.5)))
        g = b.build()

        vps = list(zip(rng.uniform(-25, 25, 4), rng.uniform(-25, 25, 4)))
        est = FloorEstimator(
            g, [v[0] for v in vps], [v[1] for v in vps], direct_km_max=300.0
        )
        for _ in range(12):
            t = (rng.uniform(-25, 25), rng.uniform(-25, 25))
            got = est.floor_ms(*t)
            want = [brute_force_floor(g, vp, t, 300.0) for vp in vps]
            np.testing.assert_allclose(got, want, rtol=1e-9)

    def test_floor_many_matches_floor(self):
        g = equator_chain(0, 5)
        est = FloorEstimator(g, [0.0, 10.0], [0.0, 3.0], direct_km_max=300.0)
        targets = [(0.0, 2.0), (5.0, 5.0)]
        many = est.floor_many_ms([t[0] for t in targets], [t[1] for t in targets])
        assert many.shape == (2, 2)
        for row, t in zip(many, targets):
            np.testing.assert_allclose(row, est.floor_ms(*t))


class TestFloorPath:
    def test_rtt_matches_estimator_on_random_graphs(self):
        rng = np.random.default_rng(7)
        b = GraphBuilder(snap_tolerance_km=1.0)
        n = 25
        lats, lons = rng.uniform(-20, 20, n), rng.uniform(-20, 20, n)
        ids = [b.node_id(lat, lon) for lat, lon in zip(lats, lons)]
        for _ in range(35):
            i, j = rng.integers(0, n, 2)
            if ids[i] != ids[j]:
                km = geo.haversine_km(lats[i], lons[i], lats[j], lons[j])
                b.add_edge(ids[i], ids[j], geo.rtt_ms(km * rng.uniform(1.0, 1.5)))
        g = b.build()
        vps = list(zip(rng.uniform(-25, 25, 3), rng.uniform(-25, 25, 3)))
        est = FloorEstimator(g, [v[0] for v in vps], [v[1] for v in vps])
        for _ in range(8):
            t = (rng.uniform(-25, 25), rng.uniform(-25, 25))
            floors = est.floor_ms(*t)
            for v, vp in enumerate(vps):
                rtt, path = floor_path_ms(g, vp, t)
                if np.isinf(floors[v]):
                    assert np.isinf(rtt) and path == []
                else:
                    assert rtt == pytest.approx(floors[v], rel=1e-9)
                    assert path[0] == vp and path[-1] == t

    def test_path_interior_follows_graph_edges(self):
        # NB: endpoints sit just off the chain ends with a tight last-mile
        # cap, so the entry/exit nodes are unique. (With a generous cap on a
        # collinear chain, beelining to a deeper node costs exactly the same
        # as entering at the end — Dijkstra may legitimately skip nodes.)
        g = equator_chain(0, 10)
        edge_set = {
            (int(s), int(d)) for s, d in zip(g.edge_src, g.edge_dst)
        }
        rtt, path = floor_path_ms(
            g, (0.0, -0.05), (0.0, 10.05), direct_km_max=0.0, lastmile_km_max=50.0
        )
        assert rtt == pytest.approx(geo.rtt_ms(10.1 * EQ_DEG_KM), rel=1e-6)
        assert len(path) == 13  # src + 11 chain nodes + dst
        interior = path[1:-1]
        node_of = {(g.node_lat[k], g.node_lon[k]): k for k in range(g.n_nodes)}
        for a, c in zip(interior[:-1], interior[1:]):
            i, j = node_of[a], node_of[c]
            assert (min(i, j), max(i, j)) in edge_set

    def test_direct_win_is_two_point_path(self):
        g = equator_chain(0, 10)
        rtt, path = floor_path_ms(g, (0.0, 5.0), (0.0, 6.0), direct_km_max=300.0)
        assert len(path) == 2
        assert rtt == pytest.approx(geo.rtt_ms(EQ_DEG_KM), rel=1e-6)

    def test_no_route_returns_inf_and_empty(self):
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0.0, 0.0), (0.0, 1.0)])
        g = b.build()
        rtt, path = floor_path_ms(
            g, (0.0, 0.2), (0.0, 90.0), direct_km_max=100.0, lastmile_km_max=100.0
        )
        assert np.isinf(rtt) and path == []


class TestFloorInvariants:
    def test_admissibility_floor_never_below_geodesic(self):
        # no fiber path beats the great circle at fiber speed — the graph
        # only detours, the direct option is the geodesic itself
        rng = np.random.default_rng(2718)
        b = GraphBuilder(snap_tolerance_km=1.0)
        pts = [(rng.uniform(-30, 30), rng.uniform(-30, 30)) for _ in range(20)]
        for k in range(len(pts) - 1):
            b.add_path([pts[k], pts[k + 1]])
        g = b.build()
        est = FloorEstimator(g, [0.0], [0.0], direct_km_max=300.0)
        for _ in range(50):
            t = (rng.uniform(-40, 40), rng.uniform(-40, 40))
            floor = est.floor_ms(*t)[0]
            geodesic = geo.rtt_ms(geo.haversine_km(0.0, 0.0, *t))
            assert floor >= geodesic - 1e-9

    def test_field_at_node_equals_graph_distance_plus_lastmile(self):
        # VP exactly on node 0: floor at node k (direct disabled) must be
        # the pure graph distance
        g = equator_chain(0, 6)
        est = FloorEstimator(g, [0.0], [0.0], direct_km_max=0.0)
        d = g.single_source_rtt_ms([0])[0]
        for k in range(g.n_nodes):
            got = est.floor_ms(g.node_lat[k], g.node_lon[k])[0]
            assert got == pytest.approx(d[k], rel=1e-9, abs=1e-9)
