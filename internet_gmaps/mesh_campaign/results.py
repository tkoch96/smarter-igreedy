"""Pull and parse results of our mesh measurements; maintain probe health.

Interpretation rules (campaign spec caveats 5/6/8):
- a source probe that produced no result in a finished measurement gets a
  src strike; a destination that answered no source at all gets a dst
  strike; probes with MAX_STRIKES and zero successes are benched.
- every successful RTT is checked against the speed-of-light geodesic
  floor between the two probes' CLAIMED locations. rtt < geodesic/KM_PER_MS
  is impossible physics => at least one location is wrong; both probes
  accumulate sol_violations and are benched past a threshold. (The floor
  model itself never trains on pairs involving distrusted locations.)
"""

import math
import time

from . import atlas_api

KM_PER_MS = 100.0  # keep consistent with the fiber atlas / smarter-igreedy
SOL_MARGIN_MS = 2.0  # measurement jitter allowance before calling violation
RESULTS_GRACE_S = 20 * 60  # one-offs complete in ~15 min (caveat 8)


def _haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(min(1.0, math.sqrt(a)))


def min_rtt_of(result):
    """Minimum RTT (ms) from one probe's ping result, or None."""
    rtts = [r["rtt"] for r in result.get("result", []) if isinstance(r, dict) and "rtt" in r]
    m = result.get("min", -1)
    if m and m > 0:
        rtts.append(m)
    return min(rtts) if rtts else None


def resweep_closed_measurements(state, by_id, verbose=True):
    """Second-chance pull: re-fetch results for CLOSED measurements that
    still have failed pairs. Probes that were disconnected at measurement
    time can upload their results hours later; any late arrival flips the
    pair from failed to ok (strikes already recorded stay — the probe DID
    miss the window — but successes are credited so it never gets benched
    for slowness alone)."""
    rows = list(
        state.db.execute(
            "SELECT DISTINCT m.msm_id, m.dst_prb FROM measurements m "
            "JOIN pairs p ON p.msm_id = m.msm_id "
            "WHERE m.done=1 AND m.msm_id > 0 AND p.status='failed'"
        )
    )
    checked = recovered = 0
    for msm_id, dst_prb in rows:
        failed_srcs = {
            s
            for (s,) in state.db.execute(
                "SELECT src FROM pairs WHERE msm_id=? AND status='failed'", (msm_id,)
            )
        }
        try:
            results = atlas_api.measurement_results(msm_id)
        except Exception:
            continue
        checked += 1
        for res in results:
            src = res["prb_id"]
            if src not in failed_srcs:
                continue
            rtt = min_rtt_of(res)
            if rtt is None:
                continue
            state.record_result(src, dst_prb, rtt)
            state.credit_ok(src)
            state.credit_ok(dst_prb)
            recovered += 1
    if verbose:
        print(f"resweep: {checked} measurements rechecked, {recovered} pairs recovered")
    return checked, recovered


def pull_open_measurements(state, by_id, grace_s=RESULTS_GRACE_S, verbose=True):
    """Fetch results for in-flight measurements old enough to be complete;
    update pair outcomes and probe health. Returns list of
    (src, dst, min_rtt or None)."""
    outcomes = []
    for msm_id, dst_prb, dst_ip in state.open_measurements(older_than_s=grace_s):
        try:
            results = atlas_api.measurement_results(msm_id)
        except Exception as e:  # transient API failure: retry next run
            if verbose:
                print(f"  msm {msm_id}: fetch failed ({e}); will retry")
            continue
        expected = {
            src for src, dst, *_ in state.db.execute(
                "SELECT src, dst FROM pairs WHERE msm_id=?", (msm_id,)
            )
        }
        answered = set()
        for res in results:
            src = res["prb_id"]
            rtt = min_rtt_of(res)
            answered.add(src)
            state.record_result(src, dst_prb, rtt)
            outcomes.append((src, dst_prb, rtt))
            if rtt is None:
                continue
            state.credit_ok(src)
            state.credit_ok(dst_prb)
            ps, pd = by_id.get(src), by_id.get(dst_prb)
            if ps and pd:
                floor = _haversine_km(ps["lat"], ps["lon"], pd["lat"], pd["lon"]) / KM_PER_MS
                if rtt < floor - SOL_MARGIN_MS:
                    state.sol_violation(src)
                    state.sol_violation(dst_prb)
        for src in expected - answered:
            state.record_result(src, dst_prb, None)
            state.strike_src(src)
            outcomes.append((src, dst_prb, None))
        if results == [] and expected:
            state.strike_dst(dst_prb)
        state.close_measurement(msm_id)
        if verbose:
            n_ok = sum(1 for s, d, r in outcomes if d == dst_prb and r is not None)
            print(f"  msm {msm_id} -> dst {dst_prb}: {n_ok}/{len(expected)} pairs ok")
    return outcomes
