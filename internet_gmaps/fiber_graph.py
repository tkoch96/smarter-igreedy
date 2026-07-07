"""Fiber infrastructure as a graph: nodes at lat/lon, edges weighted in floor-RTT ms.

GraphBuilder accumulates geometry from cartographic sources, merging any
two nodes closer than snap_tolerance_km (sources never land twice on the
exact same coordinate — cable ends vs. landing points, duplicate city
digitizations, etc.). FiberGraph is the immutable result with the scipy
plumbing: CSR adjacency, KD-tree over node positions, connected
components, single-source Dijkstra.
"""

from dataclasses import dataclass
from functools import cached_property

import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components, dijkstra
from scipy.spatial import cKDTree

import geo


@dataclass(eq=False)
class FiberGraph:
    node_lat: np.ndarray
    node_lon: np.ndarray
    edge_src: np.ndarray  # one row per undirected edge, src < dst
    edge_dst: np.ndarray
    edge_rtt_ms: np.ndarray
    edge_feature: np.ndarray = None  # per-edge index into feature_names; -1 = unknown
    feature_names: tuple = ()  # source feature labels, e.g. "TG:2africa", "ITU"

    @property
    def n_nodes(self):
        return len(self.node_lat)

    @property
    def n_edges(self):
        return len(self.edge_src)

    @cached_property
    def csr(self):
        """Symmetric adjacency, both directions materialized."""
        r = np.concatenate([self.edge_src, self.edge_dst])
        c = np.concatenate([self.edge_dst, self.edge_src])
        w = np.concatenate([self.edge_rtt_ms, self.edge_rtt_ms])
        return csr_matrix((w, (r, c)), shape=(self.n_nodes, self.n_nodes))

    @cached_property
    def kdtree(self):
        return cKDTree(geo.unit_xyz(self.node_lat, self.node_lon))

    @cached_property
    def component_labels(self):
        _, labels = connected_components(self.csr, directed=False)
        return labels

    def single_source_rtt_ms(self, source_nodes):
        """Dijkstra from each source node; returns (len(sources), n_nodes),
        np.inf where unreachable."""
        sources = np.atleast_1d(np.asarray(source_nodes, dtype=int))
        return dijkstra(self.csr, directed=True, indices=sources)


class GraphBuilder:
    _REINDEX_EVERY = 512  # rebuild the KD-tree once this many nodes are unindexed

    def __init__(self, snap_tolerance_km=5.0):
        self.snap_tolerance_km = float(snap_tolerance_km)
        self._chord_tol = geo.km_to_chord(self.snap_tolerance_km)
        self._lat = []
        self._lon = []
        self._xyz = []
        self._edges = []
        self._feature_ids = {}
        self._feature_names = []
        self._tree = None
        self._n_indexed = 0

    def _feature_idx(self, feature):
        if feature is None:
            return -1
        if feature not in self._feature_ids:
            self._feature_ids[feature] = len(self._feature_names)
            self._feature_names.append(feature)
        return self._feature_ids[feature]

    @property
    def n_nodes(self):
        return len(self._lat)

    def node_id(self, lat, lon):
        """Id of an existing node within snap tolerance (nearest wins), else a new node."""
        p = geo.unit_xyz(lat, lon)
        cand_ids, cand_d = [], []
        if self._tree is not None:
            for i in self._tree.query_ball_point(p, r=self._chord_tol):
                cand_ids.append(i)
                cand_d.append(np.linalg.norm(self._xyz[i] - p))
        if self.n_nodes > self._n_indexed:
            tail = np.asarray(self._xyz[self._n_indexed :])
            d = np.linalg.norm(tail - p, axis=1)
            for j in np.flatnonzero(d <= self._chord_tol):
                cand_ids.append(self._n_indexed + int(j))
                cand_d.append(d[j])
        if cand_ids:
            return cand_ids[int(np.argmin(cand_d))]

        self._lat.append(float(lat))
        self._lon.append(float(lon))
        self._xyz.append(p)
        if self.n_nodes - self._n_indexed >= self._REINDEX_EVERY:
            self._tree = cKDTree(np.asarray(self._xyz))
            self._n_indexed = self.n_nodes
        return self.n_nodes - 1

    def add_edge(self, id_a, id_b, rtt_ms, feature=None):
        if id_a != id_b:
            self._edges.append((id_a, id_b, float(rtt_ms), self._feature_idx(feature)))

    def add_path(self, latlons, slack=1.0, feature=None):
        """Chain of edges along a polyline of (lat, lon) vertices.

        Segment lengths come from the raw vertex coordinates (not the
        snapped node positions) so drawn geometry length is preserved;
        vertices that snap to the same node accumulate their length into
        the next edge instead of creating self-loops.
        """
        latlons = list(latlons)
        if len(latlons) < 2:
            return [self.node_id(*ll) for ll in latlons]
        ids = [self.node_id(*ll) for ll in latlons]
        chain_from = ids[0]
        seg_km = 0.0
        for k in range(1, len(latlons)):
            seg_km += float(
                geo.haversine_km(latlons[k - 1][0], latlons[k - 1][1], latlons[k][0], latlons[k][1])
            )
            if ids[k] != chain_from:
                self.add_edge(chain_from, ids[k], geo.rtt_ms(seg_km * slack), feature=feature)
                chain_from = ids[k]
                seg_km = 0.0
        return ids

    def build(self):
        """Freeze into a FiberGraph; parallel edges collapse to the minimum RTT
        (keeping the winning edge's source feature)."""
        lat = np.asarray(self._lat)
        lon = np.asarray(self._lon)
        if not self._edges:
            e = np.empty(0, dtype=int)
            return FiberGraph(lat, lon, e, e.copy(), np.empty(0), e.copy(), ())
        a, b, w, f = (np.asarray(x) for x in zip(*self._edges))
        src, dst = np.minimum(a, b), np.maximum(a, b)
        order = np.lexsort((w, dst, src))
        src, dst, w, f = src[order], dst[order], w[order], f[order]
        first = np.ones(len(src), dtype=bool)
        first[1:] = (src[1:] != src[:-1]) | (dst[1:] != dst[:-1])
        return FiberGraph(
            lat, lon, src[first], dst[first], w[first], f[first], tuple(self._feature_names)
        )
