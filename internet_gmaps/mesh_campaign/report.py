"""Campaign verification artifacts:

  pilot(n)              real 10x10-style mesh among n diverse probes
                        (executes actual RIPE Atlas measurements; isolated
                        state db so early polling can't strike probes in
                        the main campaign state)
  simulate_coverage()   scheduler simulation on the real probe inventory:
                        projected coverage per diversity metric over days
                        -> figures/mesh_campaign_coverage.pdf
"""

import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from . import atlas_api
from .inventory import fetch_inventory
from .results import pull_open_measurements
from .scheduler import DiversityScheduler
from .state import CampaignState

FIG_DIR = Path(__file__).parent.parent / "figures"
PILOT_DB = Path(__file__).parent / "data" / "pilot_state.sqlite"


def pick_pilot_probes(probes, n=10, seed=31415):
    """n probes, all in different countries and ASNs."""
    import random

    rng = random.Random(seed)
    shuffled = probes[:]
    rng.shuffle(shuffled)
    chosen, ccs, asns = [], set(), set()
    for p in shuffled:
        if p["cc"] not in ccs and p["asn"] not in asns:
            chosen.append(p)
            ccs.add(p["cc"])
            asns.add(p["asn"])
        if len(chosen) == n:
            break
    return chosen


def pilot(n=10, dry_run=False):
    probes = fetch_inventory()
    chosen = pick_pilot_probes(probes, n)
    print(f"pilot probes ({len(chosen)}):")
    for p in chosen:
        print(f"  prb {p['id']:>7}  {p['cc']}  AS{p['asn']:<8} {p['ip']:<16} ({p['lat']:.2f},{p['lon']:.2f})")

    state = CampaignState(PILOT_DB)
    sched = DiversityScheduler(chosen, attempted_pairs=state.attempted_pairs(), seed=1)
    batches = sched.plan_batches(n * (n - 1) // 2)
    print(f"\nfull pilot mesh: {sum(b.n_pairs for b in batches)} pairs in "
          f"{len(batches)} measurements, {sum(b.credits for b in batches)} credits")
    if dry_run:
        return chosen, batches, []

    msm_ids = []
    date = time.strftime("%Y-%m-%d")
    for b in batches:
        ok, resp = atlas_api.create_ping(
            b.dst_ip, b.src_prbs, f"fiber-atlas-mesh|pilot|{date}|dst:{b.dst_prb}"
        )
        if ok:
            state.record_scheduled(resp, b.dst_prb, b.dst_ip, b.src_prbs)
            msm_ids.append(resp)
            print(f"  launched msm {resp}: {len(b.src_prbs)} srcs -> {b.dst_ip} (prb {b.dst_prb})")
        else:
            print(f"  FAILED for dst {b.dst_prb}: {resp}")
    return chosen, batches, msm_ids


def pilot_results(grace_s=0):
    """Pull whatever pilot results exist right now."""
    probes = fetch_inventory()
    by_id = {p["id"]: p for p in probes}
    state = CampaignState(PILOT_DB)
    return pull_open_measurements(state, by_id, grace_s=grace_s)


def simulate_coverage(days=30, pairs_per_day=100_000, seed=31415, out_pdf=None):
    """Run the real scheduler on the real inventory for `days` simulated
    days and plot cumulative coverage per diversity metric."""
    probes = fetch_inventory()
    sched = DiversityScheduler(probes, seed=seed)
    metrics = list(sched.coverage())
    history = {m: [] for m in metrics}
    t0 = time.time()
    for day in range(days):
        sched.plan(pairs_per_day)
        cov = sched.coverage()
        for m in metrics:
            history[m].append(cov[m])
        if day in (0, 4, 9, days - 1):
            print(f"  day {day + 1}: " + "  ".join(
                f"{m} {got:,}/{tot:,}" for m, (got, tot) in cov.items()))
    print(f"  simulated {days} days x {pairs_per_day:,} pairs in {time.time()-t0:.0f}s")

    fig, ax = plt.subplots(figsize=(8, 5))
    x = range(1, days + 1)
    styles = {
        "country_pairs": ("tab:blue", "country ↔ country"),
        "asn_pairs": ("tab:orange", "ASN ↔ ASN"),
        "cc_asn_pairs": ("tab:green", "(country, ASN) ↔ (country, ASN)"),
        "probe_pairs": ("tab:red", "probe ↔ probe"),
    }
    for m, (color, label) in styles.items():
        got_tot = history[m]
        frac = [g / max(t, 1) for g, t in got_tot]
        final_g, final_t = got_tot[-1]
        ax.plot(x, frac, color=color, lw=2,
                label=f"{label}  ({final_g:,}/{final_t:,} by day {days})")
    ax.set_xlabel("campaign day")
    ax.set_ylabel("fraction of achievable pairs covered")
    ax.set_ylim(0, 1.02)
    ax.set_title(
        f"Projected mesh coverage — {len(probes):,} usable probes, "
        f"{pairs_per_day:,} pairs/day ({pairs_per_day * atlas_api.PING_PACKETS:,} credits/day)\n"
        "diversity-first scheduling: T1 country, T2 ASN, T3 (country,ASN), T4 probe pairs"
    )
    ax.legend(loc="center right", fontsize=9)
    ax.grid(alpha=0.3)
    FIG_DIR.mkdir(exist_ok=True)
    out = out_pdf or FIG_DIR / "mesh_campaign_coverage.pdf"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved {out}")
    return history
