"""Exact minimum-fiber-RTT queries against a FiberGraph.

Two-stage design for the asymmetric workload (few stable VPs, very many
targets):

1. Per-VP distance fields, precomputed once per graph version. Each field
   is exact: a virtual source node is connected to every graph node
   within lastmile_km_max at last-mile cost (geodesic at fiber speed),
   then one Dijkstra gives
   field_vp[j] = min over entry nodes i of (lastmile(vp, i) + dist(i, j)).

2. Arbitrary target lat/lon answered by KD-tree candidate expansion with
   an admissible stopping rule: candidates arrive in increasing last-mile
   order, and once lastmile alone exceeds the best total found (or the
   last-mile cap), no farther node can win (field values are
   nonnegative). Exact, and component-aware for free — island nodes carry
   inf fields and never terminate the expansion.

floor(vp, t) = min( direct geodesic RTT if geodesic(vp,t) <= direct_km_max,
                    min_j field_vp[j] + lastmile(t, j) )
with last-mile legs only allowed up to lastmile_km_max.

The two knobs model "dense unmapped local fiber exists within X km":

- direct_km_max: below this separation the endpoints may connect without
  touching the mapped graph at all. Without it, a target one street from
  a VP would be charged a round trip through the nearest backbone node.
- lastmile_km_max: an endpoint farther than this from all mapped
  infrastructure gets an inf floor (the model refuses to invent a route;
  the caller chooses a fallback). This cap is load-bearing: with
  unlimited last mile, any pair could meet at a single bend node near
  their great circle at pure fiber speed, collapsing the floor back to
  the geodesic model the atlas exists to replace. Pairs within
  ~2*lastmile_km_max of a common node can still effectively beeline —
  keep both knobs stated in figures.
"""

import hashlib
import os
from pathlib import Path

import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra

import geo

DEFAULT_DIRECT_KM_MAX = 300.0
DEFAULT_LASTMILE_KM_MAX = 300.0


def _expand_floor(graph, vp_lat, vp_lon, fields, lat, lon,
                  direct_km_max, lastmile_km_max):
    """Stage-2 lookup shared by FloorEstimator and PolicyFloorEstimator:
    KD-tree candidate expansion with the admissible stopping rule, over
    precomputed per-VP distance fields (shape (n_vps, n_nodes))."""
    d_direct = geo.haversine_km(vp_lat, vp_lon, lat, lon)
    best = np.where(d_direct <= direct_km_max, geo.rtt_ms(d_direct), np.inf)

    n = graph.n_nodes
    if n == 0:
        return best
    xyz = geo.unit_xyz(lat, lon)
    k_prev, k = 0, min(16, n)
    while True:
        chord, idx = graph.kdtree.query(xyz, k=k)
        chord, idx = np.atleast_1d(chord), np.atleast_1d(idx)
        km = geo.chord_to_km(chord[k_prev:])
        within = km <= lastmile_km_max
        if np.any(within):
            lastmile = geo.rtt_ms(km[within])
            cand = fields[:, idx[k_prev:][within]] + lastmile[None, :]
            best = np.minimum(best, cand.min(axis=1))
        if k >= n or km[-1] > lastmile_km_max or geo.rtt_ms(km[-1]) >= best.max():
            return best
        k_prev, k = k, min(2 * k, n)


class FloorEstimator:
    def __init__(
        self,
        graph,
        vp_lat,
        vp_lon,
        direct_km_max=DEFAULT_DIRECT_KM_MAX,
        lastmile_km_max=DEFAULT_LASTMILE_KM_MAX,
    ):
        self.graph = graph
        self.vp_lat = np.atleast_1d(np.asarray(vp_lat, dtype=float))
        self.vp_lon = np.atleast_1d(np.asarray(vp_lon, dtype=float))
        self.direct_km_max = float(direct_km_max)
        self.lastmile_km_max = float(lastmile_km_max)
        self._fields = self._compute_fields()  # (n_vps, n_nodes)

    @property
    def n_vps(self):
        return len(self.vp_lat)

    def _compute_fields(self):
        n = self.graph.n_nodes
        base = self.graph.csr.tocoo()
        fields = np.empty((self.n_vps, n))
        for v in range(self.n_vps):
            km = geo.haversine_km(
                self.vp_lat[v], self.vp_lon[v], self.graph.node_lat, self.graph.node_lon
            )
            entry = np.flatnonzero(km <= self.lastmile_km_max)
            # Augmented graph: virtual source (index n) -> entry nodes at last-mile cost
            rows = np.concatenate([base.row, np.full(len(entry), n)])
            cols = np.concatenate([base.col, entry])
            data = np.concatenate([base.data, geo.rtt_ms(km[entry])])
            aug = csr_matrix((data, (rows, cols)), shape=(n + 1, n + 1))
            fields[v] = dijkstra(aug, directed=True, indices=n)[:n]
        return fields

    def floor_ms(self, lat, lon):
        """Fiber-floor RTT from every VP to (lat, lon); returns shape (n_vps,).
        inf where neither the graph (within lastmile_km_max) nor the direct
        option (within direct_km_max) provides a route."""
        return _expand_floor(self.graph, self.vp_lat, self.vp_lon, self._fields,
                             lat, lon, self.direct_km_max, self.lastmile_km_max)

    def floor_many_ms(self, lats, lons):
        """Floors for many targets; returns shape (n_targets, n_vps)."""
        return np.vstack([self.floor_ms(lat, lon) for lat, lon in zip(lats, lons)])


class PolicyFloorEstimator:
    """Policy-aware floors for arbitrary query points — the geolocation
    counterpart of transit_policy.policy_floor_matrix (which only handles
    fixed location sets).

    Same two-stage design as FloorEstimator, but the per-VP field depends
    on the query point's country through the policy's class signature
    (~35 classes): fields[(v, sig)] is one Dijkstra on the node-masked,
    distrust-scaled graph. Fields are computed lazily per (VP, class) —
    a process pays only for the classes it actually queries — and can be
    disk-cached (`cache_dir=`) keyed by (policy name, vp, graph, class);
    bump the policy name on any rule change or you'll read stale physics.

    inf semantics: a policy floor of inf means "no allowed route", which
    is not a usable likelihood. floor_ms() falls back to the OPEN floor
    for exactly those (vp, point) entries — never to bare geodesic, which
    would reintroduce the geodesic ridge precisely where the policy is
    most opinionated. policy_floor_ms() exposes the raw (inf-preserving)
    floors for validation. Both include the policy-free direct option
    within direct_km_max, matching policy_floor_matrix. The OPEN floors
    are themselves lazy per-VP fields (banned set = ∅, unscaled edges) —
    nothing is precomputed for VPs that are never queried.

    Scale: `vp_indices=` restricts any query to a subset of VPs — a
    geolocation region only ever asks about its own handful of VPs, so
    with hundreds of VPs never stack fields for all of them.
    `max_cached_fields` LRU-bounds the in-memory fields (each is
    n_nodes floats); with a cache_dir, evicted fields reload from disk.

    Country attribution: `node_cc`/`vp_cc` may be given directly (tests
    inject synthetic codes); `point_cc_fn(lats, lons) -> [cc]` resolves
    query points, defaulting to offline reverse_geocoder. Lookups are
    memoized per rounded coordinate.
    """

    _OPEN_SIG = "__open__"

    def __init__(
        self,
        graph,
        vp_lat,
        vp_lon,
        node_cc=None,
        vp_cc=None,
        policy=None,
        point_cc_fn=None,
        direct_km_max=DEFAULT_DIRECT_KM_MAX,
        lastmile_km_max=DEFAULT_LASTMILE_KM_MAX,
        cache_dir=None,
        max_cached_fields=None,
    ):
        # deferred import: transit_policy imports this module for the knobs
        from transit_policy import DEFAULT_POLICY, scaled_base_data

        self.graph = graph
        self.vp_lat = np.atleast_1d(np.asarray(vp_lat, dtype=float))
        self.vp_lon = np.atleast_1d(np.asarray(vp_lon, dtype=float))
        self.policy = DEFAULT_POLICY if policy is None else policy
        self.direct_km_max = float(direct_km_max)
        self.lastmile_km_max = float(lastmile_km_max)
        self.cache_dir = Path(cache_dir) if cache_dir is not None else None
        self.max_cached_fields = max_cached_fields
        self._point_cc_fn = point_cc_fn or _reverse_geocode_ccs
        self._cc_memo = {}

        self.node_cc = (
            np.asarray(node_cc)
            if node_cc is not None
            else np.asarray(self._point_cc_fn(graph.node_lat, graph.node_lon))
        )
        self.vp_cc = (
            np.asarray(vp_cc)
            if vp_cc is not None
            else np.asarray(self._point_cc_fn(self.vp_lat, self.vp_lon))
        )
        self._uniq_node_ccs = np.unique(self.node_cc)
        self._node_cc_digest = hashlib.md5(
            "".join(self.node_cc.tolist()).encode()
        ).hexdigest()[:8]

        base = graph.csr.tocoo()
        self._base_row, self._base_col = base.row, base.col
        self._open_data = base.data
        self._base_data = scaled_base_data(self.policy, graph, base, self.node_cc)
        from collections import OrderedDict

        self._fields = OrderedDict()   # (v, sig) -> (n_nodes,) field, LRU

    @property
    def n_vps(self):
        return len(self.vp_lat)

    # -- country / class resolution -------------------------------------

    def point_cc(self, lat, lon):
        key = (round(float(lat), 4), round(float(lon), 4))
        cc = self._cc_memo.get(key)
        if cc is None:
            cc = self._point_cc_fn([lat], [lon])[0]
            self._cc_memo[key] = cc
        return cc

    # -- per-(VP, class) fields ------------------------------------------

    def _cache_path(self, v, sig):
        if self.cache_dir is None:
            return None
        name = "open" if sig == self._OPEN_SIG else self.policy.name
        key = "|".join(
            [
                name,
                f"{self.vp_lat[v]:.6f},{self.vp_lon[v]:.6f}",
                str(self.vp_cc[v]),
                repr(sig),
                f"{self.graph.n_nodes}n{self.graph.n_edges}e",
                f"lm{self.lastmile_km_max}",
                self._node_cc_digest,
            ]
        )
        h = hashlib.md5(key.encode()).hexdigest()[:16]
        return self.cache_dir / f"pfield_{h}.npy"

    def _compute_field(self, v, cc):
        """One Dijkstra: virtual VP source on the graph masked for a pair
        with endpoint countries {vp_cc[v], cc} (mirrors the worker in
        transit_policy.policy_floor_matrix_parallel).  cc=None computes
        the OPEN field: no bans, unscaled edge weights — identical to
        FloorEstimator's per-VP field."""
        n = self.graph.n_nodes
        if cc is None:
            ok = np.ones(n, dtype=bool)
            data = self._open_data
        else:
            banned = np.array(
                sorted(self.policy.banned_set(self._uniq_node_ccs,
                                              {self.vp_cc[v], cc})),
                dtype=self.node_cc.dtype,
            )
            ok = ~np.isin(self.node_cc, banned)
            data = self._base_data
        emask = ok[self._base_row] & ok[self._base_col]
        km = geo.haversine_km(
            self.vp_lat[v], self.vp_lon[v], self.graph.node_lat, self.graph.node_lon
        )
        entry_rtt = np.where(km <= self.lastmile_km_max, geo.rtt_ms(km), np.inf)
        entry = np.flatnonzero(ok & np.isfinite(entry_rtt))
        aug = csr_matrix(
            (
                np.concatenate([data[emask], entry_rtt[entry]]),
                (
                    np.concatenate([self._base_row[emask], np.full(len(entry), n)]),
                    np.concatenate([self._base_col[emask], entry]),
                ),
            ),
            shape=(n + 1, n + 1),
        )
        return dijkstra(aug, directed=True, indices=n)[:n]

    def _field(self, v, sig, cc):
        f = self._fields.get((v, sig))
        if f is not None:
            self._fields.move_to_end((v, sig))
            return f
        path = self._cache_path(v, sig)
        if path is not None and path.exists():
            f = np.load(path)
        else:
            f = self._compute_field(v, cc)
            if path is not None:
                path.parent.mkdir(parents=True, exist_ok=True)
                tmp = path.parent / f"{path.stem}.{os.getpid()}.tmp.npy"
                with open(tmp, "wb") as fh:
                    np.save(fh, f)
                os.replace(tmp, path)
        self._fields[(v, sig)] = f
        if (self.max_cached_fields is not None
                and len(self._fields) > self.max_cached_fields):
            self._fields.popitem(last=False)
        return f

    def _stack(self, sig, cc, vp_indices):
        return np.vstack([self._field(v, sig, cc) for v in vp_indices])

    # -- queries -----------------------------------------------------------

    def _resolve_vps(self, vp_indices):
        if vp_indices is None:
            return np.arange(self.n_vps)
        return np.atleast_1d(np.asarray(vp_indices, dtype=int))

    def policy_floor_ms(self, lat, lon, cc=None, vp_indices=None):
        """Raw policy floors to (lat, lon) from every VP (or the given VP
        subset); shape (len(vp_indices),).  inf where the policy leaves no
        allowed route (and the direct option is out of range) — use
        floor_ms for a likelihood-safe value."""
        cc = cc or self.point_cc(lat, lon)
        sig = self.policy.class_signature(cc)
        vps = self._resolve_vps(vp_indices)
        fields = self._stack(sig, cc, vps)
        return _expand_floor(self.graph, self.vp_lat[vps], self.vp_lon[vps],
                             fields, lat, lon,
                             self.direct_km_max, self.lastmile_km_max)

    def open_floor_ms(self, lat, lon, vp_indices=None):
        """Unrestricted floors (FloorEstimator semantics), lazy per VP."""
        vps = self._resolve_vps(vp_indices)
        fields = self._stack(self._OPEN_SIG, None, vps)
        return _expand_floor(self.graph, self.vp_lat[vps], self.vp_lon[vps],
                             fields, lat, lon,
                             self.direct_km_max, self.lastmile_km_max)

    def floor_ms(self, lat, lon, cc=None, vp_indices=None):
        """Policy floors with the OPEN-floor fallback where the policy
        floor is inf.  Still inf where even the open graph offers no route
        (endpoint beyond lastmile_km_max of everything)."""
        vps = self._resolve_vps(vp_indices)
        pf = self.policy_floor_ms(lat, lon, cc, vps)
        blocked = ~np.isfinite(pf)
        if not np.any(blocked):
            return pf
        out = pf.copy()
        out[blocked] = self.open_floor_ms(lat, lon, vps[blocked])
        return out

    def floor_ms_subset(self, lat, lon, vp_indices, cc=None):
        """floor_ms restricted to a VP subset (the geolocation hot path:
        a region only queries its own constraints' VPs)."""
        return self.floor_ms(lat, lon, cc, vp_indices)

    def floor_many_ms(self, lats, lons):
        """Floors (with fallback) for many targets; shape (n_targets, n_vps)."""
        lats = np.atleast_1d(np.asarray(lats, dtype=float))
        lons = np.atleast_1d(np.asarray(lons, dtype=float))
        ccs = self._point_cc_fn(lats, lons)
        return np.vstack(
            [self.floor_ms(lat, lon, cc) for lat, lon, cc in zip(lats, lons, ccs)]
        )


def _reverse_geocode_ccs(lats, lons):
    """Default country attribution: offline nearest-city reverse geocoding
    (same source as the transit analysis / policy validation)."""
    import reverse_geocoder as rg

    res = rg.search(
        list(zip(map(float, lats), map(float, lons))), mode=1, verbose=False
    )
    return [r["cc"] for r in res]


def floor_path_ms(
    graph,
    src_latlon,
    dst_latlon,
    direct_km_max=DEFAULT_DIRECT_KM_MAX,
    lastmile_km_max=DEFAULT_LASTMILE_KM_MAX,
):
    """Exact floor and the path achieving it, for a single pair (the
    handoff's `floor(src, dst) -> (rtt_ms, path)` API). Same semantics as
    FloorEstimator; use that for bulk queries, this for inspection.

    Returns (rtt_ms, [(lat, lon), ...]) with both endpoints included.
    A two-point path means the direct option won; (inf, []) means no route.
    """
    n = graph.n_nodes
    src_id, dst_id = n, n + 1
    base = graph.csr.tocoo()
    rows, cols, data = [base.row], [base.col], [base.data]

    km_src = geo.haversine_km(src_latlon[0], src_latlon[1], graph.node_lat, graph.node_lon)
    entry = np.flatnonzero(km_src <= lastmile_km_max)
    rows.append(np.full(len(entry), src_id))
    cols.append(entry)
    data.append(geo.rtt_ms(km_src[entry]))

    km_dst = geo.haversine_km(dst_latlon[0], dst_latlon[1], graph.node_lat, graph.node_lon)
    exit_ = np.flatnonzero(km_dst <= lastmile_km_max)
    rows.append(exit_)
    cols.append(np.full(len(exit_), dst_id))
    data.append(geo.rtt_ms(km_dst[exit_]))

    d_direct = float(geo.haversine_km(*src_latlon, *dst_latlon))
    if d_direct <= direct_km_max:
        rows.append([src_id])
        cols.append([dst_id])
        data.append([geo.rtt_ms(d_direct)])

    aug = csr_matrix(
        (np.concatenate(data), (np.concatenate(rows), np.concatenate(cols))),
        shape=(n + 2, n + 2),
    )
    dist, pred = dijkstra(aug, directed=True, indices=src_id, return_predecessors=True)
    if np.isinf(dist[dst_id]):
        return np.inf, []

    path, node = [], dst_id
    while node != src_id:
        path.append(node)
        node = pred[node]
    path.append(src_id)
    coords = []
    for node in reversed(path):
        if node == src_id:
            coords.append(tuple(src_latlon))
        elif node == dst_id:
            coords.append(tuple(dst_latlon))
        else:
            coords.append((float(graph.node_lat[node]), float(graph.node_lon[node])))
    return float(dist[dst_id]), coords
