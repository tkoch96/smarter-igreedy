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
from .anycast import is_anycast, load_anycast_slash24s
from .inventory import fetch_inventory
from .results import pull_open_measurements
from .scheduler import DiversityScheduler
from .state import CampaignState

DESCRIPTION_FMT = "fiber-atlas-mesh|{date}|dst:{dst}"
REMEASURE_FMT = "fiber-atlas-mesh-remeasure|{date}|dst:{dst}"

# Re-measures use min-of-10 instead of min-of-3: credits are not the
# binding constraint (results are), and these are exactly the pairs whose
# floor needs corroborating. Batch count is capped so the long tail of
# single-pair dsts can't eat the concurrency slots (~15 msm/min submit).
REMEASURE_PACKETS = 10
REMEASURE_MAX_MEAS = 300

# Account cap discovered empirically 2026-07-06, refined 07-07: max 100k
# RESULTS per ROLLING 24H WINDOW (1 result = 1 src-dst pair, regardless of
# packets) — a run at 10:06 UTC was rejected with 99,985 results counted
# even though midnight UTC had passed. This — not the 1M credit limit —
# is the binding throughput constraint at 3 packets.
DAILY_RESULTS_LIMIT = 100_000
STOP_ERRORS = ("credit", "quota", "results limit", "daily results")


def _create_ping_retry(dst_ip, src_prbs, description, packets=atlas_api.PING_PACKETS):
    ok, resp = atlas_api.create_ping(dst_ip, src_prbs, description, packets=packets)
    if not ok and "concurrent" in str(resp):
        atlas_api.wait_for_capacity()
        ok, resp = atlas_api.create_ping(dst_ip, src_prbs, description, packets=packets)
    return ok, resp


def run_daily(
    budget_credits=300_000,
    max_pairs=None,
    max_measurements=1200,
    dry_run=False,
    seed=None,
    state=None,
    remeasure_pairs=15_000,
):
    state = state or CampaignState()
    probes = fetch_inventory()
    by_id = {p["id"]: p for p in probes}

    # dead listed addresses get their verified in-network surrogate
    # (surrogates.py); anycast-listed probes are sources only (anycast.py)
    surr = state.surrogates()
    n_sub = 0
    for p in probes:
        if p["id"] in surr:
            p["ip"] = surr[p["id"]]
            n_sub += 1
    anycast_24s = load_anycast_slash24s()
    no_dst = {p["id"] for p in probes if is_anycast(p["ip"], anycast_24s)}
    if n_sub or no_dst:
        print(f"   {n_sub} surrogate dst addresses; {len(no_dst)} anycast probes src-only")

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

    date = time.strftime("%Y-%m-%d")
    if remeasure_pairs:
        print("== (c') re-measuring uncorroborated pairs (min-of-10)")
        cands = state.remeasure_candidates()
        budget = min(remeasure_pairs, n_pairs)
        benched = state.benched_probes()
        by_dst = {}
        for s, d, _gap in cands:
            if s in benched or d in benched or s not in by_id or d not in by_id:
                continue
            if d in no_dst:
                continue
            by_dst.setdefault(d, []).append(s)
        print(f"   {len(cands)} candidate pairs across {len(by_dst)} dsts; "
              f"pair budget {budget}")
        n_re = n_meas = 0
        limit_hit = False
        # densest dst batches first: measurement count, not pairs, is the
        # submit-loop bottleneck; the single-pair tail drains over days
        for dst, srcs in sorted(by_dst.items(), key=lambda kv: -len(kv[1])):
            if n_re >= budget or n_meas >= REMEASURE_MAX_MEAS or limit_hit:
                break
            for i in range(0, len(srcs), atlas_api.MAX_PROBES_PER_MEAS):
                chunk = srcs[i:i + atlas_api.MAX_PROBES_PER_MEAS][: budget - n_re]
                if not chunk or n_meas >= REMEASURE_MAX_MEAS:
                    break
                if dry_run:
                    n_re += len(chunk)
                    n_meas += 1
                    continue
                if n_meas % 10 == 9:
                    atlas_api.wait_for_capacity()
                ok, resp = _create_ping_retry(
                    by_id[dst]["ip"], chunk,
                    REMEASURE_FMT.format(date=date, dst=dst),
                    packets=REMEASURE_PACKETS,
                )
                if ok:
                    state.record_scheduled(
                        resp, dst, by_id[dst]["ip"], chunk, kind="remeasure"
                    )
                    n_re += len(chunk)
                    n_meas += 1
                else:
                    print(f"   remeasure create failed for dst {dst}: {resp}")
                    if any(k in str(resp).lower() for k in STOP_ERRORS):
                        print("   stopping: account limit reached")
                        limit_hit = True
                        break
        print(f"   launched {n_meas} re-measurements covering {n_re} pairs")
        n_pairs = 0 if limit_hit else n_pairs - n_re

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
        no_dst=no_dst,
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

    msm_ids = []
    for i, b in enumerate(batches):
        if i % 10 == 9:
            atlas_api.wait_for_capacity()
        ok, resp = _create_ping_retry(
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
    ap.add_argument("--remeasure-pairs", type=int, default=15_000,
                    help="pair budget for re-measuring uncorroborated RTTs (0 disables)")
    args = ap.parse_args()
    run_daily(
        args.budget_credits, args.max_pairs, args.max_measurements, args.dry_run,
        args.seed, remeasure_pairs=args.remeasure_pairs,
    )


if __name__ == "__main__":
    main()
