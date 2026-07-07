"""Unit tests for fiber_graph.py: node snapping, path construction, Dijkstra."""

import numpy as np
import pytest

import geo
from fiber_graph import FiberGraph, GraphBuilder

EQ_DEG_KM = np.pi * geo.EARTH_RADIUS_KM / 180  # km per equator degree, derived — never hardcode
EQ_DEG_MS = EQ_DEG_KM / geo.KM_PER_MS


class TestNodeSnapping:
    def test_within_tolerance_merges(self):
        b = GraphBuilder(snap_tolerance_km=5.0)
        a = b.node_id(0.0, 0.0)
        assert b.node_id(0.0, 0.01) == a  # ~1.1 km away
        assert b.n_nodes == 1

    def test_beyond_tolerance_is_new_node(self):
        b = GraphBuilder(snap_tolerance_km=5.0)
        a = b.node_id(0.0, 0.0)
        c = b.node_id(0.0, 0.1)  # ~11 km away
        assert c != a
        assert b.n_nodes == 2

    def test_nearest_candidate_wins(self):
        b = GraphBuilder(snap_tolerance_km=5.0)
        left = b.node_id(0.0, 0.0)
        right = b.node_id(0.0, 0.06)  # ~6.7 km from left: distinct
        # 0.04 deg is within tolerance of both; right (2.2 km) beats left (4.4 km)
        assert b.node_id(0.0, 0.04) == right
        assert left != right

    def test_snapping_across_kdtree_reindex(self):
        b = GraphBuilder(snap_tolerance_km=5.0)
        b._REINDEX_EVERY = 4  # force index rebuilds during the run
        first = b.node_id(0.0, 0.0)
        for i in range(10):
            b.node_id(10.0 + i, 10.0 + i)  # far-apart filler nodes
        assert b.node_id(0.0, 0.001) == first


class TestAddPath:
    def test_chain_edges_and_lengths(self):
        b = GraphBuilder(snap_tolerance_km=1.0)
        ids = b.add_path([(0, 0), (0, 1), (0, 2)])
        g = b.build()
        assert len(ids) == 3
        assert g.n_edges == 2
        np.testing.assert_allclose(g.edge_rtt_ms, [EQ_DEG_MS, EQ_DEG_MS], rtol=1e-6)

    def test_slack_scales_rtt(self):
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0, 0), (0, 1)], slack=1.5)
        g = b.build()
        assert g.edge_rtt_ms[0] == pytest.approx(1.5 * EQ_DEG_MS, rel=1e-6)

    def test_collapsed_vertices_accumulate_length(self):
        # middle vertex snaps onto the first: its segment length must fold
        # into the next edge, not vanish or self-loop
        b = GraphBuilder(snap_tolerance_km=5.0)
        b.add_path([(0, 0), (0, 0.01), (0, 1)])
        g = b.build()
        assert g.n_nodes == 2
        assert g.n_edges == 1
        assert g.edge_rtt_ms[0] == pytest.approx(EQ_DEG_MS, rel=1e-6)

    def test_parallel_edges_keep_minimum(self):
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0, 0), (0, 1)], slack=2.0)
        b.add_path([(0, 0), (0, 1)], slack=1.2)
        g = b.build()
        assert g.n_edges == 1
        assert g.edge_rtt_ms[0] == pytest.approx(1.2 * EQ_DEG_MS, rel=1e-6)

    def test_shared_vertex_stitches_paths(self):
        b = GraphBuilder(snap_tolerance_km=5.0)
        b.add_path([(0, 0), (0, 1)])
        b.add_path([(0, 1.001), (0, 2)])  # endpoint snaps onto (0, 1)
        g = b.build()
        assert g.n_nodes == 3
        assert len(np.unique(g.component_labels)) == 1


class TestFiberGraph:
    def _two_component_graph(self):
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0, 0), (0, 1), (0, 2)])  # chain A: nodes 0,1,2
        b.add_path([(20, 0), (20, 1)])  # chain B: nodes 3,4
        return b.build()

    def test_components(self):
        g = self._two_component_graph()
        labels = g.component_labels
        assert labels[0] == labels[1] == labels[2]
        assert labels[3] == labels[4]
        assert labels[0] != labels[3]

    def test_single_source_distances(self):
        g = self._two_component_graph()
        d = g.single_source_rtt_ms([0])
        assert d.shape == (1, 5)
        np.testing.assert_allclose(d[0, :3], [0, EQ_DEG_MS, 2 * EQ_DEG_MS], rtol=1e-6)
        assert np.isinf(d[0, 3]) and np.isinf(d[0, 4])

    def test_dijkstra_prefers_shortcut(self):
        b = GraphBuilder(snap_tolerance_km=1.0)
        ids = b.add_path([(0, 0), (0, 1), (0, 2)])  # 2 hops, ~2.22 ms
        b.add_edge(ids[0], ids[2], rtt_ms=1.0)  # direct shortcut, cheaper
        g = b.build()
        d = g.single_source_rtt_ms([ids[0]])[0]
        assert d[ids[2]] == pytest.approx(1.0)
        # and via-shortcut beats the chain for reaching the middle from the end
        assert d[ids[1]] == pytest.approx(EQ_DEG_MS, rel=1e-6)

    def test_empty_builder_builds(self):
        g = GraphBuilder().build()
        assert g.n_nodes == 0 and g.n_edges == 0
