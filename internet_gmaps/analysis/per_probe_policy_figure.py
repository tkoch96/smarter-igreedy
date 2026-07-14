"""Per-probe error figure measured against the CURRENT policy's floors
(the standard mesh_validation per-probe figure uses rule-free floors on
purpose, as a probe-quality check — this variant shows how much of each
probe's error the routing rules explain).

How to run (from internet_gmaps/, after the policy validation suite has
cached the current policy's floor matrix):
    ~/Documents/venv312/bin/python analysis/per_probe_policy_figure.py
Writes figures/mesh_validation_cdfs_per_probe_policy.pdf. Runtime ~1 min.
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
    from test_mesh_validation import FIG_DIR, FIBER_SLOPE, MeshEval, ecdf, pct
    from transit_policy import DEFAULT_POLICY

    ev = MeshEval()
    key = f"{DEFAULT_POLICY.name}|{len(ev.locs)}|{len(ev.meas)}|{ev.graph.n_edges}"
    h = hashlib.md5(key.encode()).hexdigest()[:12]
    path = Path("data/cache") / f"floors_{DEFAULT_POLICY.name}_{h}.npy"
    if not path.exists():
        # Mesh drifts between days (live-inventory dependence in the
        # campaign export) — rerun the policy suite to cache today's
        # matrix, then retry.
        sys.exit(
            f"no cached floor matrix for today's mesh ({key}); "
            "run: python -m pytest tests/test_policy_validation.py -q and retry."
        )
    mat = np.load(path)
    pol = mat[ev.dst_idx, ev.src_idx]

    finite = np.isfinite(pol) & np.isfinite(ev.fiber)
    metrics = {
        f"(c) measured − 1.3·open floor": (ev.meas - FIBER_SLOPE * ev.fiber)[finite],
        f"(c*) measured − 1.3·{DEFAULT_POLICY.name} floor":
            (ev.meas - FIBER_SLOPE * pol)[finite],
    }
    idx2 = np.concatenate([ev.src_idx[finite], ev.dst_idx[finite]])
    order = np.argsort(idx2, kind="stable")
    bounds = np.searchsorted(idx2[order], np.arange(len(ev.locs) + 1))
    has_pairs = np.flatnonzero(np.diff(bounds) > 0)

    colors = {"(c)": "tab:green", "(c*": "tab:red"}
    fig, ax = plt.subplots(figsize=(7.5, 5))
    meds = {}
    for name, v in metrics.items():
        v2 = np.concatenate([v, v])[order]
        med = np.array([np.median(v2[bounds[l]: bounds[l + 1]]) for l in has_pairs])
        meds[name] = med
        color = colors[name[:3]]
        ecdf(ax, np.clip(med, -100, 250), label=f"{name} (per-probe median)",
             color=color, lw=1.8)
        ecdf(ax, np.clip(v, -100, 250), color=color, ls="--", lw=0.9, alpha=0.45,
             label=f"{name[:4]} all pairs")
    ax.axvline(0, color="k", lw=0.8, ls=":")
    ax.set_xlabel("difference (ms, clipped to [-100, 250])")
    ax.set_ylabel("CDF over probes (solid) / pairs (dashed)")
    ax.set_title(
        f"Per-probe error, open vs {DEFAULT_POLICY.name} floors — "
        f"{len(has_pairs)} metro clusters\n"
        "(the policy CDF shifting left = rules explaining residual)",
        fontsize=10,
    )
    ax.legend(loc="lower right", fontsize=8)
    ax.grid(alpha=0.3)
    FIG_DIR.mkdir(exist_ok=True)
    fig.savefig(FIG_DIR / "mesh_validation_cdfs_per_probe_policy.pdf",
                bbox_inches="tight")

    for name, med in meds.items():
        print(f"{name}: per-probe median {np.median(med):5.1f}  "
              f"P10 {pct(med, 10):5.1f}  P90 {pct(med, 90):5.1f} ms | "
              f">100 ms: {int((med > 100).sum())}/{len(med)}")
    print(f"saved {FIG_DIR / 'mesh_validation_cdfs_per_probe_policy.pdf'}")


if __name__ == "__main__":
    main()
