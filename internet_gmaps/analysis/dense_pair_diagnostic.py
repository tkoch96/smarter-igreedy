"""Attribute floor-validity violations on the dense probe mesh to their
cause, rule by rule.

A floor is only useful if it sits BELOW the measured RTT. This computes
floors at the exact locations of the dense (pre-campaign) probe mesh
under several policy variants — full current policy, rules without the
distrust multipliers, rules minus individual suspects — so each
violating pair can be blamed on a specific mechanism. Violations are
also split by pair distance, since short tight pairs are the most
sensitive to small detours.

How to run (from internet_gmaps/):
    ~/Documents/venv312/bin/python analysis/dense_pair_diagnostic.py
Runtime ~3-10 min (one floor matrix per variant).
"""
import os
import sys
from collections import defaultdict

import numpy as np


def main():
    sys.path.insert(0, os.getcwd())
    sys.path.insert(0, os.path.join(os.getcwd(), "tests"))
    import geo
    import reverse_geocoder as rg
    from mesh_data import load_target_data
    from test_mesh_validation import GRAPH_NPZS, MESH_PKL
    from fiber_graph import FiberGraph
    from floor_query import FloorEstimator
    from transit_policy import (
        DEFAULT_POLICY,
        SUEZ_CORRIDOR,
        PACIFIC_RELAY_ISLANDS,
        TransitPolicy,
        africa_containment,
        indian_ocean_containment,
        no_transit,
        policy_floor_matrix_parallel,
        small_country,
        small_island_transit,
        soviet_bloc,
    )

    d = load_target_data(MESH_PKL, include_campaign=False)
    addr_loc = d["address_to_loc"]
    locs = sorted({tuple(map(float, v)) for v in addr_loc.values()})
    loc_id = {l: k for k, l in enumerate(locs)}
    lat = np.array([l[0] for l in locs])
    lon = np.array([l[1] for l in locs])
    print(f"dense mesh: {len(addr_loc):,} addresses, {len(locs):,} exact locations")

    npz = np.load(GRAPH_NPZS[-1])
    g = FiberGraph(
        npz["node_lat"], npz["node_lon"], npz["edge_src"], npz["edge_dst"],
        npz["edge_rtt_ms"],
        edge_feature=npz["edge_feature"] if "edge_feature" in npz else None,
        feature_names=tuple(npz["feature_names"]) if "feature_names" in npz else (),
    )
    loc_cc = np.array([r["cc"] for r in rg.search(
        list(zip(map(float, lat), map(float, lon))), mode=1, verbose=False)])
    node_cc = np.array([r["cc"] for r in rg.search(
        list(zip(map(float, g.node_lat), map(float, g.node_lon))), mode=1, verbose=False)])

    # pairs
    src_i, dst_i, meas = [], [], []
    for src, dsts in d["loc_loc_meas"].items():
        i = loc_id[tuple(map(float, addr_loc[src]))]
        for dst, rtt in dsts.items():
            j = loc_id[tuple(map(float, addr_loc[dst]))]
            if i != j:
                src_i.append(i)
                dst_i.append(j)
                meas.append(float(np.min(rtt) if hasattr(rtt, "__len__") else rtt))
    src_i, dst_i, meas = np.array(src_i), np.array(dst_i), np.array(meas)
    dist_km = geo.haversine_km(lat[src_i], lon[src_i], lat[dst_i], lon[dst_i])
    print(f"pairs: {len(meas):,}")

    P = DEFAULT_POLICY
    rules_no_small = tuple(r for r in P.rules if "small-country" not in r.name)
    rules_no_island = tuple(r for r in P.rules if r.name != "no-small-island-transit")
    EU_SMALL = frozenset("LU MT CY EE LV LT SI HR".split())
    eu_fix_rules = tuple(
        small_country(5.0, exempt=SUEZ_CORRIDOR | PACIFIC_RELAY_ISLANDS | EU_SMALL,
                      terrestrial_only=True)
        if "small-country" in r.name else r
        for r in P.rules
    )
    variants = {
        "B full v3.8": P,
        "C rules-only (no factors)": TransitPolicy(
            "diag-rules-only", P.rules, node_cc_remaps=P.node_cc_remaps),
        "D rules minus small-country": TransitPolicy(
            "diag-no-small", rules_no_small, node_cc_remaps=P.node_cc_remaps),
        "E rules minus island-rule": TransitPolicy(
            "diag-no-island", rules_no_island, node_cc_remaps=P.node_cc_remaps),
        "F full + EU-exempt small-country": TransitPolicy(
            "diag-eu-fix", eu_fix_rules,
            cable_factors=P.cable_factors,
            terrestrial_factors=P.terrestrial_factors,
            corridor_factors=P.corridor_factors,
            node_cc_remaps=P.node_cc_remaps),
    }

    est = FloorEstimator(g, lat, lon)
    open_mat = est.floor_many_ms(lat, lon)
    floors = {"A open": open_mat[dst_i, src_i]}
    for name, pol in variants.items():
        mat = policy_floor_matrix_parallel(g, node_cc, lat, lon, loc_cc, pol)
        floors[name] = mat[dst_i, src_i]
        print(f"  computed {name}")

    buckets = [(0, 1000), (1000, 3000), (3000, 20037)]
    print(f"\n{'variant':<34}", end="")
    for lo, hi in buckets:
        print(f"  <{hi:>5}km", end="")
    print("   overall  (violation = measured < RAW floor)")
    for name, f in floors.items():
        fin = np.isfinite(f)
        row = f"{name:<34}"
        for lo, hi in buckets:
            m = fin & (dist_km >= lo) & (dist_km < hi)
            row += f"  {np.mean(meas[m] < f[m]):7.2%}"
        row += f"  {np.mean(meas[fin] < f[fin]):7.2%}"
        print(row)

    # attribution on the violating pairs of the full policy
    fB = floors["B full v3.8"]
    vB = np.isfinite(fB) & (meas < fB)
    vC = np.isfinite(floors["C rules-only (no factors)"]) & (meas < floors["C rules-only (no factors)"])
    vD = np.isfinite(floors["D rules minus small-country"]) & (meas < floors["D rules minus small-country"])
    vE = np.isfinite(floors["E rules minus island-rule"]) & (meas < floors["E rules minus island-rule"])
    vF = np.isfinite(floors["F full + EU-exempt small-country"]) & (meas < floors["F full + EU-exempt small-country"])
    print(f"\nviolating pairs under full v3.8: {vB.sum():,} ({vB.mean():.2%})")
    print(f"  caused by distrust FACTORS (fixed when factors stripped): {(vB & ~vC).sum():,}")
    print(f"  fixed by removing small-country rule:                     {(vC & ~vD).sum():,}")
    print(f"  fixed by removing island rule:                            {(vC & ~vE).sum():,}")
    print(f"  remaining under EU-exempt fix (F):                        {vF.sum():,} ({vF.mean():.2%})")

    # top endpoint countries among still-violating pairs under F
    cnt = defaultdict(int)
    for k in np.flatnonzero(vF):
        cnt[loc_cc[src_i[k]]] += 1
        if loc_cc[dst_i[k]] != loc_cc[src_i[k]]:
            cnt[loc_cc[dst_i[k]]] += 1
    print("\ntop endpoint countries among F-violations:",
          sorted(cnt.items(), key=lambda x: -x[1])[:10])


if __name__ == "__main__":
    main()
