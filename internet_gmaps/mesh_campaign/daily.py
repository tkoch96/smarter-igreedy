"""Daily mesh-campaign orchestrator — the future cronjob.

    (a) pull results of measurements we started earlier
    (b) update pair outcomes + probe health (strikes, SOL checks)
    (c) schedule + execute new pings up to the daily credit budget,
        diversity-first (see scheduler.py)

Usage:
    python -m mesh_campaign.daily --budget-credits 30000            # real run
    python -m mesh_campaign.daily --budget-credits 300 --dry-run    # plan only
    python -m mesh_campaign.daily --max-pairs 100                   # cap pairs

Suggested crontab (mind the 1M/day account limit; stay well under):
    17 6 * * *  cd <internet_gmaps> && <venv>/bin/python -m mesh_campaign.daily --budget-credits 300000 >> mesh_campaign/data/daily.log 2>&1
"""

import argparse
import time

from . import atlas_api
from .inventory import fetch_inventory
from .results import pull_open_measurements
from .scheduler import DiversityScheduler
from .state import CampaignState

DESCRIPTION_FMT = "fiber-atlas-mesh|{date}|dst:{dst}"

# Account cap discovered empirically 2026-07-06, refined 07-07: max 100k
# RESULTS per ROLLING 24H WINDOW (1 result = 1 src-dst pair, regardless of
# packets) — a run at 10:06 UTC was rejected with 99,985 results counted
# even though midnight UTC had passed. This — not the 1M credit limit —
# is the binding throughput constraint at 3 packets.
DAILY_RESULTS_LIMIT = 100_000
STOP_ERRORS = ("credit", "quota", "results limit", "daily results")


def run_daily(
    budget_credits=300_000,
    max_pairs=None,
    max_measurements=1200,
    dry_run=False,
    seed=None,
    state=None,
):
    state = state or CampaignState()
    probes = fetch_inventory()
    by_id = {p["id"]: p for p in probes}

    print("== (a)+(b) pulling results of open measurements")
    outcomes = pull_open_measurements(state, by_id)
    n_ok = sum(1 for *_, r in outcomes if r is not None)
    print(f"   parsed {len(outcomes)} pair outcomes ({n_ok} ok)")

    print("== (c) scheduling new measurements")
    n_pairs = budget_credits // atlas_api.PING_PACKETS
    if max_pairs is not None:
        n_pairs = min(n_pairs, max_pairs)
    used_24h = state.db.execute(
        "SELECT COALESCE(SUM(n_src),0) FROM measurements WHERE created >= ? AND msm_id > 0",
        (time.time() - 86400,),
    ).fetchone()[0]
    headroom = DAILY_RESULTS_LIMIT - used_24h
    if headroom < n_pairs:
        print(f"   results-limit headroom {headroom:,} (used {used_24h:,} in last 24h); capping")
        n_pairs = max(headroom, 0)
    if n_pairs == 0:
        print("   rolling 24h results limit reached; nothing to schedule")
        return [], []
    successful = {
        (min(s, d), max(s, d)) for s, d, _ in state.results("ok")
    }
    anchors = {p["id"] for p in probes if p.get("anchor")}
    sched = DiversityScheduler(
        probes,
        attempted_pairs=state.attempted_pairs(),
        successful_pairs=successful,
        benched=state.benched_probes(),
        verified_dsts=state.verified_dsts() | anchors,
        seed=seed if seed is not None else int(time.time()),
    )
    batches = sched.plan_batches(n_pairs)
    if len(batches) > max_measurements:
        # concurrency caps make measurement count the real bottleneck; carry
        # unspent budget to tomorrow rather than babysit a 10h submit loop
        batches = batches[:max_measurements]
    total_pairs = sum(b.n_pairs for b in batches)
    total_credits = sum(b.credits for b in batches)
    print(
        f"   planned {len(batches)} measurements, {total_pairs} pairs, "
        f"{total_credits} credits"
    )
    for name, (got, total) in sched.coverage().items():
        # NB: optimistic — assumes every planned pair succeeds. True coverage
        # is recomputed from status='ok' pairs on the next run.
        print(
            f"   planned coverage IF all succeed: {name}: {got:,}/{total:,} "
            f"({got/max(total,1):.1%})"
        )

    if dry_run:
        return batches, []

    date = time.strftime("%Y-%m-%d")
    msm_ids = []
    for i, b in enumerate(batches):
        if i % 10 == 9:
            atlas_api.wait_for_capacity()
        ok, resp = atlas_api.create_ping(
            b.dst_ip, b.src_prbs, DESCRIPTION_FMT.format(date=date, dst=b.dst_prb)
        )
        if not ok and "concurrent" in str(resp):
            atlas_api.wait_for_capacity()
            ok, resp = atlas_api.create_ping(
                b.dst_ip, b.src_prbs, DESCRIPTION_FMT.format(date=date, dst=b.dst_prb)
            )
        if ok:
            state.record_scheduled(resp, b.dst_prb, b.dst_ip, b.src_prbs)
            msm_ids.append(resp)
        else:
            print(f"   create failed for dst {b.dst_prb}: {resp}")
            detail = str(resp).lower()
            if any(k in detail for k in STOP_ERRORS):
                print("   stopping: account limit reached")
                break
    print(f"   launched {len(msm_ids)} measurements")
    return batches, msm_ids


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget-credits", type=int, default=300_000)
    ap.add_argument("--max-pairs", type=int, default=None)
    ap.add_argument("--max-measurements", type=int, default=1200)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    run_daily(
        args.budget_credits, args.max_pairs, args.max_measurements, args.dry_run, args.seed
    )


if __name__ == "__main__":
    main()
