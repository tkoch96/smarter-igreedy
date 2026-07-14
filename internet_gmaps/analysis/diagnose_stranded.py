"""List which countries' probes are left with NO allowed route by the
current transit policy.

When to use: after any policy change, if test_no_policy_stranded_pairs
fails, run this to see WHO got cut off. It groups the stranded pairs
(pairs the unrestricted graph can route but the policy cannot) by
endpoint country, and names the policy rule restricting each country.

How to run (from internet_gmaps/, after the policy validation suite has
cached the current policy's floor matrix under data/cache/):

    ~/Documents/venv312/bin/python analysis/diagnose_stranded.py

Prints: stranded count, the most-stranded country pairs, and per-country
counts with the restricting rule names.
"""
import hashlib
import os
import sys
from collections import Counter
from pathlib import Path

import numpy as np


def main():
    sys.path.insert(0, os.getcwd())
    sys.path.insert(0, os.path.join(os.getcwd(), "tests"))
    from test_mesh_validation import MeshEval
    from transit_policy import DEFAULT_POLICY
    import reverse_geocoder as rg

    # Load the mesh (probe locations + measured RTTs) and the country of
    # every probe cluster.
    ev = MeshEval()
    res = rg.search(list(zip(map(float, ev.lat), map(float, ev.lon))),
                    mode=1, verbose=False)
    loc_cc = np.array([r["cc"] for r in res])

    # Load the current policy's floor matrix from the test suite's disk
    # cache (the suite must have run once at this mesh size).
    key = f"{DEFAULT_POLICY.name}|{len(ev.locs)}|{len(ev.meas)}|{ev.graph.n_edges}"
    h = hashlib.md5(key.encode()).hexdigest()[:12]
    path = Path("data/cache") / f"floors_{DEFAULT_POLICY.name}_{h}.npy"
    if not path.exists():
        # The mesh drifts slightly between days even with unchanged data
        # files (the campaign export consults the LIVE probe inventory,
        # so probe churn changes the merged mesh and thus this key).
        # Remedy: rerun the policy validation suite once to cache the
        # matrix at today's mesh, then rerun this script.
        have = sorted(p.name for p in Path("data/cache").glob(
            f"floors_{DEFAULT_POLICY.name}_*.npy"))
        sys.exit(
            f"no cached floor matrix for today's mesh ({key}).\n"
            f"cached for this policy: {have or 'none'}\n"
            "run: python -m pytest tests/test_policy_validation.py -q  "
            "(~7 min) and retry."
        )
    mat = np.load(path)

    # Same 120k-pair sample the validation suite uses (same seed), so the
    # numbers here match the failing test exactly.
    rng = np.random.default_rng(31415)
    sample = rng.choice(len(ev.meas), size=min(120000, len(ev.meas)), replace=False)
    sample = sample[np.argsort(ev.src_idx[sample], kind="stable")]
    s, d = ev.src_idx[sample], ev.dst_idx[sample]

    # Stranded = the open (rule-free) graph has a route but the policy
    # does not. These are the pairs the NoRouteError contract protects.
    pol = mat[d, s]
    open_f = ev.fiber[sample]
    stranded = np.isfinite(open_f) & ~np.isfinite(pol)
    print(f"stranded: {stranded.sum():,} / {len(sample):,}")

    # Group stranded pairs by their endpoint countries.
    cc_pairs, cc_any = Counter(), Counter()
    for i in np.flatnonzero(stranded):
        a, b = loc_cc[s[i]], loc_cc[d[i]]
        cc_pairs[tuple(sorted((a, b)))] += 1
        cc_any[a] += 1
        if b != a:
            cc_any[b] += 1

    print("\ntop endpoint-country pairs among stranded:")
    for (a, b), n in cc_pairs.most_common(15):
        print(f"  {a}-{b}: {n:,}")

    print("\ntop endpoint countries among stranded:")
    for cc, n in cc_any.most_common(15):
        # Which rules would restrict this country in the worst case
        # (no exempting endpoints)? Points at the responsible rule.
        rules = [r.name for r in DEFAULT_POLICY.rules if r.banned(cc, frozenset())]
        print(f"  {cc}: {n:,}   restricted-by: {rules or '-'}")


if __name__ == "__main__":
    main()
