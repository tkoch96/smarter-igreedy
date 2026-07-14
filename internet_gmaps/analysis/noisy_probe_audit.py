"""Audit slow/noisy measurements at the individual-probe level.

When to use: to check whether bad measurements (RTTs far above what the
pair's geometry warrants) are common, and whether they come from a few
bad probes or are spread across the mesh. Slow measurements can never
break floor validity (a floor must sit BELOW the measurement), but they
inflate the error statistics.

Definitions:
  excess  = measured RTT minus 1.3x the pair's fiber floor. Distance-
            normalized "how much slower than plausible" — a raw 350 ms
            is fine for an antipodal pair, so raw thresholds mislead.
  NOISY   = RTT >= 350 ms AND excess >= 200 ms.
  pop. A  = probes whose MEDIAN RTT > 200 ms (chronically slow links,
            e.g. satellite; kept in the data by decision, just listed).
  pop. B  = probes that look healthy overall but carry NOISY samples.

How to run (from internet_gmaps/):

    ~/Documents/venv312/bin/python analysis/noisy_probe_audit.py

Runtime ~1 min. A figure version lives in noisy_probe_figure.py.
"""
import os
import sys
from collections import defaultdict

import numpy as np


def main():
    sys.path.insert(0, os.getcwd())
    sys.path.insert(0, os.path.join(os.getcwd(), "tests"))
    from mesh_data import load_target_data
    from test_mesh_validation import MESH_PKL, MeshEval, place_name

    # Floors come from the cluster-level mesh eval; individual probes map
    # to their cluster to look up the pair's floor.
    ev = MeshEval()
    d = load_target_data(MESH_PKL, include_campaign=True)
    addr_loc = d["address_to_loc"]
    loc_id = {loc: k for k, loc in enumerate(ev.locs)}
    cl = {a: loc_id.get(ev._rep_of.get(tuple(map(float, v))))
          for a, v in addr_loc.items()}

    rtts = defaultdict(list)      # probe -> RTTs of all its pairs
    excesses = defaultdict(list)  # probe -> excess values of its pairs
    noisy_of = defaultdict(int)   # probe -> count of NOISY measurements
    n_meas = n_floorable = 0
    exc_all = []
    for src, dsts in d["loc_loc_meas"].items():
        i = cl.get(src)
        if i is None:
            continue
        for dst, rtt in dsts.items():
            j = cl.get(dst)
            if j is None or i == j:
                continue
            rtt = float(rtt)
            n_meas += 1
            rtts[src].append(rtt)
            rtts[dst].append(rtt)
            floor = ev.fiber_mat[j, i]
            if not np.isfinite(floor):
                continue
            n_floorable += 1
            exc = rtt - 1.3 * floor
            exc_all.append(exc)
            excesses[src].append(exc)
            excesses[dst].append(exc)
            if rtt >= 350.0 and exc >= 200.0:
                noisy_of[src] += 1
                noisy_of[dst] += 1

    exc_all = np.asarray(exc_all)
    print(f"measurements: {n_meas:,} directed pairs ({n_floorable:,} with "
          f"finite floors), {len(rtts):,} addresses")
    print("\n== how common is 'noisy' overall ==")
    for t in (100, 200, 300, 500, 1000):
        print(f"  excess > {t:>4} ms: {np.mean(exc_all > t):6.2%}  "
              f"({int((exc_all > t).sum()):,})")
    nn = sum(noisy_of.values()) // 2
    print(f"  NOISY (rtt>=350 AND excess>=200): {nn:,} measurements "
          f"({nn / n_floorable:.2%})")

    med_rtt = {a: float(np.median(v)) for a, v in rtts.items()}
    min_rtt = {a: float(np.min(v)) for a, v in rtts.items()}
    med_exc = {a: float(np.median(v)) for a, v in excesses.items() if v}

    # Population A: chronically slow probes. The min-RTT>200 subset is
    # the hard core — never fast to anyone, whatever the distance.
    slow = sorted((a for a in med_rtt if med_rtt[a] > 200.0),
                  key=lambda a: -med_rtt[a])
    broken = [a for a in slow if min_rtt[a] > 200.0]
    print(f"\n== A. systemically slow (median RTT > 200 ms): {len(slow)} "
          f"({len(slow)/len(rtts):.1%}); min RTT also > 200 ms: {len(broken)} ==")
    for a in slow[:15]:
        lat, lon = map(float, addr_loc[a])
        print(f"  {a:<20s} {place_name(lat, lon):<28s} median {med_rtt[a]:6.0f}  "
              f"min {min_rtt[a]:6.0f}  n={len(rtts[a]):,}")

    # Population B: healthy probes carrying occasional noisy samples.
    sporadic = sorted(
        (a for a in noisy_of
         if med_exc.get(a, 1e9) < 50.0 and med_rtt.get(a, 1e9) <= 200.0),
        key=lambda a: -noisy_of[a])
    print(f"\n== B. otherwise-reasonable probes with noisy measurements: "
          f"{len(sporadic)} ==")
    hist = np.array([noisy_of[a] for a in sporadic])
    if len(hist):
        print(f"   per-probe noisy counts: median {np.median(hist):.0f}, "
              f"P90 {np.percentile(hist, 90):.0f}, max {hist.max()}")
        for a in sporadic[:15]:
            lat, lon = map(float, addr_loc[a])
            print(f"  {a:<20s} {place_name(lat, lon):<28s} noisy {noisy_of[a]:>4}  "
                  f"of n={len(excesses[a]):,}  med_exc {med_exc[a]:5.1f}")

    # Isolation: if a few probes hold most of the noise, per-probe fixes
    # help; if it is spread thin, it is pair-level congestion instead.
    counts = np.array(sorted(noisy_of.values(), reverse=True))
    print(f"\n== isolation ==")
    print(f"  addresses touched by >=1 noisy measurement: {len(noisy_of):,} "
          f"/ {len(rtts):,}")
    if len(counts):
        for k in (10, 50, 200):
            if len(counts) > k:
                print(f"  top {k:>3} addresses hold "
                      f"{counts[:k].sum()/counts.sum():.1%} of noisy endpoints")


if __name__ == "__main__":
    main()
