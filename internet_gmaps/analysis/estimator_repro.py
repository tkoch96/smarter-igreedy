"""Cross-check the two independent floor computations against each other
and against measurements, on real data.

The batch matrix code (used by the validation suites) and the on-demand
estimator (used by the geolocation project, with its disk cache) must
produce identical floors. This samples probe locations from the dense
mesh and compares: unrestricted floors, batch-matrix floors, freshly
computed estimator floors, and estimator floors read through the shared
disk cache. Any disagreement localizes a bug; agreement plus a low
violation rate exonerates this codebase.

How to run (from internet_gmaps/):
    ~/Documents/venv312/bin/python analysis/estimator_repro.py
Runtime ~1 min.
"""
import os
import sys

import numpy as np


def main():
    sys.path.insert(0, os.getcwd())
    sys.path.insert(0, os.path.join(os.getcwd(), "tests"))
    import geo
    import reverse_geocoder as rg
    from fiber_graph import FiberGraph
    from floor_query import FloorEstimator, PolicyFloorEstimator
    from mesh_data import load_target_data
    from test_mesh_validation import GRAPH_NPZS, MESH_PKL
    from transit_policy import V32_POLICY, policy_floor_matrix_parallel

    d = load_target_data(MESH_PKL, include_campaign=False)
    addr_loc = d["address_to_loc"]
    locs = sorted({tuple(map(float, v)) for v in addr_loc.values()})
    rng = np.random.default_rng(7)
    vps = [locs[i] for i in rng.choice(len(locs), 40, replace=False)]
    tgts = [locs[i] for i in rng.choice(len(locs), 60, replace=False)]
    vlat = np.array([p[0] for p in vps]); vlon = np.array([p[1] for p in vps])
    tlat = np.array([p[0] for p in tgts]); tlon = np.array([p[1] for p in tgts])

    loc_id = {l: k for k, l in enumerate(locs)}
    meas_of = {}
    for src, dsts in d["loc_loc_meas"].items():
        i = loc_id[tuple(map(float, addr_loc[src]))]
        for dst, rtt in dsts.items():
            j = loc_id[tuple(map(float, addr_loc[dst]))]
            r = float(np.min(rtt) if hasattr(rtt, "__len__") else rtt)
            k = (i, j)
            meas_of[k] = min(meas_of.get(k, np.inf), r)

    npz = np.load(GRAPH_NPZS[-1])
    g = FiberGraph(
        npz["node_lat"], npz["node_lon"], npz["edge_src"], npz["edge_dst"],
        npz["edge_rtt_ms"],
        edge_feature=npz["edge_feature"] if "edge_feature" in npz else None,
        feature_names=tuple(npz["feature_names"]) if "feature_names" in npz else (),
    )
    node_cc = np.array([r["cc"] for r in rg.search(
        list(zip(map(float, g.node_lat), map(float, g.node_lon))), mode=1, verbose=False)])
    vcc = np.array([r["cc"] for r in rg.search(list(vps), mode=1, verbose=False)])
    tcc = [r["cc"] for r in rg.search(list(tgts), mode=1, verbose=False)]

    # M: matrix path (VPs as locs, targets as... matrix is loc x loc — use
    # combined loc set: floors from vp v to target t)
    all_lat = np.concatenate([vlat, tlat]); all_lon = np.concatenate([vlon, tlon])
    all_cc = np.concatenate([vcc, np.array(tcc)])
    M = policy_floor_matrix_parallel(g, node_cc, all_lat, all_lon, all_cc, V32_POLICY)
    m_floor = M[len(vps):, : len(vps)]  # [t, v]

    def run_est(cache_dir):
        est = PolicyFloorEstimator(
            g, vlat, vlon, node_cc=node_cc, vp_cc=vcc, policy=V32_POLICY,
            cache_dir=cache_dir, no_route="open",
        )
        out = np.empty((len(tgts), len(vps)))
        for t, (la, lo) in enumerate(tgts):
            out[t] = est.policy_floor_ms(la, lo, cc=tcc[t])
        return out

    e1 = run_est(None)
    e2 = run_est("data/cache/policy_fields")

    open_est = FloorEstimator(g, vlat, vlon)
    o_floor = np.vstack([open_est.floor_ms(la, lo) for la, lo in tgts])

    gd = geo.rtt_ms(geo.haversine_km(tlat[:, None], tlon[:, None], vlat[None, :], vlon[None, :]))
    meas = np.full((len(tgts), len(vps)), np.nan)
    for t, tp in enumerate(tgts):
        for v, vp in enumerate(vps):
            r = meas_of.get((loc_id[vp], loc_id[tp]))
            if r is not None:
                meas[t, v] = r
    have = np.isfinite(meas)
    short = have & (gd / 1.0 * geo.KM_PER_MS if False else have)  # placeholder
    short = have & (geo.haversine_km(tlat[:, None], tlon[:, None], vlat[None, :], vlon[None, :]) < 1500)

    print(f"measured vp-target pairs in sample: {have.sum():,} (short: {short.sum():,})")
    for name, f in (("open", o_floor), ("M matrix v3.2", m_floor),
                    ("E1 estimator fresh", e1), ("E2 estimator + policy_fields cache", e2)):
        fin = have & np.isfinite(f)
        viol = np.mean(meas[fin] < f[fin])
        sh = short & np.isfinite(f) & (gd > 0.5)
        ratio = np.median((f[sh] / gd[sh])) if sh.sum() else np.nan
        print(f"  {name:<36s} viol {viol:6.2%} | median floor/geodesic (short) {ratio:5.2f}")
    d12 = np.nanmax(np.abs(e1 - m_floor)[np.isfinite(e1) & np.isfinite(m_floor)])
    d13 = np.nanmax(np.abs(e2 - e1)[np.isfinite(e2) & np.isfinite(e1)])
    print(f"\nmax |E1 - matrix| = {d12:.3f} ms   max |E2 - E1| = {d13:.3f} ms")


if __name__ == "__main__":
    main()
