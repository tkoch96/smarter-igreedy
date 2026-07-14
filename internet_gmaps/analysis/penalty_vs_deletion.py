"""Compare two ways of distrusting bad cables: a cost penalty (the
multipliers in the current policy) versus deleting them from the graph
outright. Produces side-by-side error CDFs and a summary table.

Finding when first run: at 2x penalty the two are globally identical —
a doubled cable already loses every route with an alternative — and
deletion only hurts the few pairs with no alternative at all.

How to run (from internet_gmaps/):
    ~/Documents/venv312/bin/python analysis/penalty_vs_deletion.py
Writes figures/cable_penalty_vs_deletion.pdf. Runtime ~7 min (computes
one extra floor matrix; the current policy's matrix comes from cache).
"""
import hashlib
import os
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def main():
    sys.path.insert(0, os.getcwd())
    sys.path.insert(0, os.path.join(os.getcwd(), "tests"))
    import reverse_geocoder as rg
    from test_mesh_validation import FIG_DIR, FIBER_SLOPE, MeshEval, ecdf
    from transit_policy import (
        DEFAULT_POLICY,
        TransitPolicy,
        policy_floor_matrix_parallel,
    )

    DELETION = TransitPolicy(
        DEFAULT_POLICY.name + "-deletion-experiment",
        DEFAULT_POLICY.rules,
        cable_factors=tuple((n, 1e12) for n, _f in DEFAULT_POLICY.cable_factors),
        terrestrial_factors=DEFAULT_POLICY.terrestrial_factors,
        corridor_factors=DEFAULT_POLICY.corridor_factors,
    )

    ev = MeshEval()
    res = rg.search(list(zip(map(float, ev.lat), map(float, ev.lon))), mode=1, verbose=False)
    loc_cc = np.array([r["cc"] for r in res])
    nres = rg.search(
        list(zip(map(float, ev.graph.node_lat), map(float, ev.graph.node_lon))),
        mode=1, verbose=False,
    )
    node_cc = np.array([r["cc"] for r in nres])

    def cached_matrix(policy):
        key = f"{policy.name}|{len(ev.locs)}|{len(ev.meas)}|{ev.graph.n_edges}"
        h = hashlib.md5(key.encode()).hexdigest()[:12]
        path = Path("data/cache") / f"floors_{policy.name}_{h}.npy"
        if path.exists():
            return np.load(path)
        mat = policy_floor_matrix_parallel(
            ev.graph, node_cc, ev.lat, ev.lon, loc_cc, policy
        )
        np.save(path, mat)
        return mat

    pen_mat = cached_matrix(DEFAULT_POLICY)   # cache hit from the suite run
    del_mat = cached_matrix(DELETION)         # ~6 min compute

    pen = pen_mat[ev.dst_idx, ev.src_idx]
    dele = del_mat[ev.dst_idx, ev.src_idx]
    dele = np.where(dele >= 1e11, np.inf, dele)

    meas = ev.meas
    open_f = ev.fiber
    ok = np.isfinite(open_f)

    def stats(name, floor):
        fin = ok & np.isfinite(floor)
        r = meas[fin] - FIBER_SLOPE * floor[fin]
        stranded = int((ok & ~np.isfinite(floor)).sum())
        print(f"{name:>22}: median {np.median(r):6.1f}  P90 {np.percentile(r, 90):6.1f} ms | "
              f"overshoot {np.mean(r < 0):5.1%} | raw viol "
              f"{np.mean(meas[fin] < floor[fin]):5.2%} | stranded {stranded:,}")
        return r, fin

    print(f"pairs (open-finite): {ok.sum():,}")
    r_open = meas[ok] - FIBER_SLOPE * open_f[ok]
    print(f"{'open':>22}: median {np.median(r_open):6.1f}  P90 {np.percentile(r_open, 90):6.1f} ms")
    r_pen, fin_pen = stats(f"{DEFAULT_POLICY.name} penalties", pen)
    r_del, fin_del = stats(f"{DEFAULT_POLICY.name} deletion", dele)

    both = fin_pen & fin_del
    affected = both & (np.abs(dele - pen) > 0.5)
    print(f"\naffected pairs (floor changed by deletion): {affected.sum():,} "
          f"({affected.sum()/both.sum():.1%} of comparable)")
    ra_pen = meas[affected] - FIBER_SLOPE * pen[affected]
    ra_del = meas[affected] - FIBER_SLOPE * dele[affected]
    print(f"{'affected, penalties':>22}: median {np.median(ra_pen):6.1f} | raw viol "
          f"{np.mean(meas[affected] < pen[affected]):5.2%}")
    print(f"{'affected, deletion':>22}: median {np.median(ra_del):6.1f} | raw viol "
          f"{np.mean(meas[affected] < dele[affected]):5.2%}")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.5, 5))
    for ax, (series, subtitle) in zip(
        (ax1, ax2),
        (
            (
                [("open", r_open, "0.6", "--"),
                 (f"{DEFAULT_POLICY.name}, penalties (current)", r_pen, "tab:red", "-"),
                 (f"{DEFAULT_POLICY.name}, cables DELETED", r_del, "tab:blue", "-")],
                f"all comparable pairs (n={both.sum():,})",
            ),
            (
                [("x2 penalties", ra_pen, "tab:red", "-"),
                 ("deleted", ra_del, "tab:blue", "-")],
                f"pairs whose floor the deletion changed (n={affected.sum():,})",
            ),
        ),
    ):
        for label, r, color, ls in series:
            rr = r[np.isfinite(r)]
            ecdf(ax, np.clip(rr, -100, 300), color=color, ls=ls, lw=1.7,
                 label=f"{label}  [med {np.median(rr):.0f}]")
        ax.axvline(0, color="k", lw=0.8, ls=":")
        ax.set_xlabel("measured − 1.3·floor (ms, clipped)")
        ax.set_ylabel("CDF over pairs")
        ax.set_title(subtitle, fontsize=10)
        ax.legend(fontsize=9, loc="lower right")
        ax.grid(alpha=0.3)
    fig.suptitle(
        "Distrust penalties vs deletion — the 8 named cable systems "
        "(Red Sea cut series + PEACE + RJCN + KJCN)",
        fontsize=11,
    )
    fig.tight_layout()
    FIG_DIR.mkdir(exist_ok=True)
    fig.savefig(FIG_DIR / "cable_penalty_vs_deletion.pdf", bbox_inches="tight")
    print(f"\nsaved {FIG_DIR / 'cable_penalty_vs_deletion.pdf'}")


if __name__ == "__main__":
    main()
