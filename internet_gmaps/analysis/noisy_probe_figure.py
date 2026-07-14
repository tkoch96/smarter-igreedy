"""Figure version of noisy_probe_audit.py: four panels quantifying how
common noisy measurements are and whether they concentrate in a few
probes.

Panels: (a) how often measurements exceed their pair's plausible RTT,
(b) probe populations (chronically slow vs healthy-with-outliers),
(c) noisy-sample count per otherwise-healthy probe, (d) how concentrated
the noise is across probes. NOISY := RTT >= 350 ms AND >= 200 ms above
1.3x the pair's fiber floor (distance-normalized, so long healthy paths
don't get flagged).

How to run (from internet_gmaps/): 
    ~/Documents/venv312/bin/python analysis/noisy_probe_figure.py
Writes figures/noise_audit.pdf. Runtime ~1 min.
"""
import os
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def main():
    sys.path.insert(0, os.getcwd())
    sys.path.insert(0, os.path.join(os.getcwd(), "tests"))
    from mesh_data import load_target_data
    from test_mesh_validation import FIG_DIR, MESH_PKL, MeshEval

    ev = MeshEval()
    d = load_target_data(MESH_PKL, include_campaign=True)
    addr_loc = d["address_to_loc"]
    loc_id = {loc: k for k, loc in enumerate(ev.locs)}
    cl = {
        a: loc_id.get(ev._rep_of.get(tuple(map(float, v))))
        for a, v in addr_loc.items()
    }

    rtts, excesses, noisy_of = defaultdict(list), defaultdict(list), defaultdict(int)
    exc_all = []
    n_floorable = 0
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

    exc_all = np.sort(np.asarray(exc_all))
    addrs = [a for a in rtts if excesses[a]]
    med_rtt = np.array([np.median(rtts[a]) for a in addrs])
    med_exc = np.array([np.median(excesses[a]) for a in addrs])
    n_noisy = np.array([noisy_of.get(a, 0) for a in addrs])
    pop_a = med_rtt > 200.0
    pop_b = ~pop_a & (med_exc < 50.0) & (n_noisy > 0)
    healthy = ~pop_a & ~pop_b

    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    (ax1, ax2), (ax3, ax4) = axes

    # (a) CCDF of excess over the floor
    ccdf = 1.0 - np.arange(1, len(exc_all) + 1) / len(exc_all)
    ax1.semilogy(np.clip(exc_all, -50, 1500), np.maximum(ccdf, 1e-7),
                 color="tab:blue", lw=1.8)
    for t in (100, 200, 300):
        frac = float(np.mean(exc_all > t))
        ax1.axvline(t, color="0.6", lw=0.8, ls=":")
        ax1.annotate(f">{t}: {frac:.2%}", xy=(t, frac), xytext=(t + 40, frac * 1.6),
                     fontsize=8, color="0.25")
    ax1.set_xlim(-50, 1500)
    ax1.set_xlabel("excess = measured − 1.3·fiber floor (ms)")
    ax1.set_ylabel("CCDF over measurements (log)")
    ax1.set_title(f"(a) how common is noise — {n_floorable:,} measurements", fontsize=10)
    ax1.grid(alpha=0.3)

    # (b) per-address populations
    ax2.scatter(med_rtt[healthy], np.clip(med_exc[healthy], -60, None), s=4,
                c="0.75", lw=0, label=f"healthy ({healthy.sum():,})", rasterized=True)
    ax2.scatter(med_rtt[pop_b], np.clip(med_exc[pop_b], -60, None), s=14,
                c="tab:orange", marker="^", lw=0,
                label=f"B: reasonable + noisy meas. ({pop_b.sum():,})")
    ax2.scatter(med_rtt[pop_a], np.clip(med_exc[pop_a], -60, None), s=14,
                c="tab:red", marker="s", lw=0,
                label=f"A: median RTT > 200 ms ({pop_a.sum():,})")
    ax2.axvline(200, color="0.6", lw=0.8, ls=":")
    ax2.axhline(50, color="0.6", lw=0.8, ls=":")
    ax2.set_xscale("log")
    ax2.set_xlabel("per-address median RTT (ms, log)")
    ax2.set_ylabel("per-address median excess (ms)")
    ax2.set_title("(b) probe populations", fontsize=10)
    ax2.legend(fontsize=8, loc="upper left")
    ax2.grid(alpha=0.3)

    # (c) noisy-measurement count per reasonable probe
    counts_b = n_noisy[pop_b]
    bins = np.arange(1, counts_b.max() + 2)
    ax3.hist(counts_b, bins=bins, color="tab:orange", alpha=0.85)
    ax3.set_yscale("log")
    ax3.set_xlabel("noisy measurements per probe (population B)")
    ax3.set_ylabel("probes (log)")
    ax3.set_title(
        f"(c) isolation per probe — median {np.median(counts_b):.0f}, "
        f"P90 {np.percentile(counts_b, 90):.0f}, max {counts_b.max()}",
        fontsize=10,
    )
    ax3.grid(alpha=0.3, axis="y")

    # (d) concentration (Lorenz-style)
    counts_sorted = np.sort(np.array(list(noisy_of.values())))[::-1]
    cum = np.cumsum(counts_sorted) / counts_sorted.sum()
    ax4.semilogx(np.arange(1, len(cum) + 1), cum, color="tab:blue", lw=1.8)
    for k in (10, 50, 200):
        if len(cum) > k:
            ax4.plot([k], [cum[k - 1]], "o", color="tab:blue", ms=5)
            ax4.annotate(f"top {k}: {cum[k - 1]:.0%}", xy=(k, cum[k - 1]),
                         xytext=(k * 1.3, cum[k - 1] - 0.06), fontsize=8, color="0.25")
    ax4.set_ylim(0, 1.02)
    ax4.set_xlabel("addresses, ranked by noisy-measurement count (log)")
    ax4.set_ylabel("cumulative share of noisy endpoints")
    ax4.set_title(
        f"(d) concentration — {len(noisy_of):,} of {len(addrs):,} addresses touched",
        fontsize=10,
    )
    ax4.grid(alpha=0.3)

    fig.suptitle(
        "Noisy-measurement audit — NOISY := rtt ≥ 350 ms AND excess ≥ 200 ms "
        f"({int(counts_sorted.sum() / 2):,} measurements, "
        f"{counts_sorted.sum() / 2 / n_floorable:.2%} of all)",
        fontsize=11,
    )
    fig.tight_layout()
    FIG_DIR.mkdir(exist_ok=True)
    fig.savefig(FIG_DIR / "noise_audit.pdf", bbox_inches="tight")
    print(f"saved {FIG_DIR / 'noise_audit.pdf'}")


if __name__ == "__main__":
    main()
