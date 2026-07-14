"""Check whether noisy/slow measurements can actually distort the model's
conclusions, or are just cosmetic.

Three questions: (1) how many noisy measurements survive into the
analysis dataset (co-located probes absorb an outlier when a healthier
neighbor measured the same route); (2) worst-case share of any single
country's analysis pairs that are noisy (a country median only moves if
a large fraction of its pairs are contaminated); (3) regions served
ONLY by chronically-slow probes (no healthy neighbor exists to absorb).

How to run (from internet_gmaps/):
    ~/Documents/venv312/bin/python analysis/noise_power_check.py
Runtime ~1 min.
"""
import os
import sys
from collections import defaultdict

import numpy as np


def main():
    sys.path.insert(0, os.getcwd())
    sys.path.insert(0, os.path.join(os.getcwd(), "tests"))
    import reverse_geocoder as rg
    from mesh_data import load_target_data
    from test_mesh_validation import MESH_PKL, MeshEval, place_name

    ev = MeshEval()
    d = load_target_data(MESH_PKL, include_campaign=True)
    addr_loc = d["address_to_loc"]
    loc_id = {loc: k for k, loc in enumerate(ev.locs)}
    cl = {
        a: loc_id.get(ev._rep_of.get(tuple(map(float, v))))
        for a, v in addr_loc.items()
    }
    meas_of = {(i, j): m for i, j, m in zip(ev.src_idx, ev.dst_idx, ev.meas)}

    rtts = defaultdict(list)
    noisy = []
    for src, dsts in d["loc_loc_meas"].items():
        i = cl.get(src)
        if i is None:
            continue
        for dst, rtt in dsts.items():
            j = cl.get(dst)
            if j is None or i == j:
                continue
            rtt = float(rtt)
            rtts[src].append(rtt)
            rtts[dst].append(rtt)
            floor = ev.fiber_mat[j, i]
            if np.isfinite(floor) and rtt >= 350.0 and rtt - 1.3 * floor >= 200.0:
                noisy.append((i, j, rtt))

    survived = [(i, j, r) for i, j, r in noisy if abs(meas_of.get((i, j), np.inf) - r) < 1e-9]
    print(f"noisy measurements: {len(noisy):,}; survive cluster-min into the "
          f"analysis: {len(survived):,} ({len(survived)/max(1,len(noisy)):.1%})")
    print(f"analysis pairs total: {len(ev.meas):,} -> surviving-noisy share: "
          f"{len(survived)/len(ev.meas):.3%}")

    res = rg.search(list(zip(map(float, ev.lat), map(float, ev.lon))), mode=1, verbose=False)
    ccs = np.array([r["cc"] for r in res])
    pair_cc = defaultdict(int)
    for i, j in zip(ev.src_idx, ev.dst_idx):
        pair_cc[ccs[i]] += 1
        if ccs[j] != ccs[i]:
            pair_cc[ccs[j]] += 1
    noisy_cc = defaultdict(int)
    for i, j, _ in survived:
        noisy_cc[ccs[i]] += 1
        if ccs[j] != ccs[i]:
            noisy_cc[ccs[j]] += 1
    rows = sorted(
        ((n / pair_cc[cc], n, cc) for cc, n in noisy_cc.items() if pair_cc[cc] >= 300),
        reverse=True,
    )
    print("\nworst contamination of any rule-relevant country group "
          "(share of that country's analysis pairs that are surviving-noisy):")
    for share, n, cc in rows[:8]:
        print(f"  {cc}: {share:6.2%}  ({n:,} of {pair_cc[cc]:,})")

    # lone broken probes: min RTT > 200 and no healthier cluster-mate
    members = defaultdict(list)
    for a, c in cl.items():
        if c is not None and rtts[a]:
            members[c].append(a)
    lone = []
    for c, addrs in members.items():
        mins = [np.min(rtts[a]) for a in addrs]
        if min(mins) > 200.0:  # NOBODY in the cluster is ever fast
            n_pairs = sum(1 for (i, j) in meas_of if i == c or j == c)
            lone.append((c, len(addrs), float(min(mins)), n_pairs))
    print(f"\nclusters where EVERY member has min RTT > 200 ms: {len(lone)}")
    for c, na, mn, npair in sorted(lone, key=lambda x: -x[3])[:10]:
        print(f"  {place_name(ev.lat[c], ev.lon[c]):<30s} probes={na} "
              f"best-ever={mn:5.0f} ms  analysis pairs={npair:,}")


if __name__ == "__main__":
    main()
