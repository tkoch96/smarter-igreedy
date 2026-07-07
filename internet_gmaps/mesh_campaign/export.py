"""Export campaign measurements in the parent project's TargetData shape:

    {'address_to_loc': {addr: (lat, lon)}, 'loc_loc_meas': {src: {dst: min_rtt}}}

— the exact structure produced by smarter-igreedy's
pull_ripe_atlas_measurement_data pipeline, so downstream consumers stay
oblivious to whether an RTT came from the free daily dumps or from this
campaign. Seeded pairs (msm_id=0, which CAME from the dumps) are excluded
to avoid double counting; pairs touching probes with distrusted locations
(SOL violations) are excluded because a wrong location poisons any
geography-based validation.
"""

from collections import defaultdict

from .inventory import fetch_inventory
from .state import MAX_SOL_VIOLATIONS, CampaignState


def sol_suspect_probes(state):
    return {
        prb
        for prb, sol in state.db.execute(
            "SELECT prb_id, sol_violations FROM probe_health"
        )
        if sol >= MAX_SOL_VIOLATIONS
    }


def campaign_target_data(state=None, probes=None, exclude_sol_suspect=True):
    state = state or CampaignState()
    probes = probes or fetch_inventory(verbose=False)
    by_id = {p["id"]: p for p in probes}
    suspect = sol_suspect_probes(state) if exclude_sol_suspect else set()

    addr_to_loc, meas = {}, defaultdict(dict)
    q = state.db.execute(
        "SELECT src, dst, min_rtt FROM pairs WHERE status='ok' AND msm_id > 0"
    )
    for src, dst, rtt in q:
        ps, pd = by_id.get(src), by_id.get(dst)
        if ps is None or pd is None or src in suspect or dst in suspect:
            continue
        addr_to_loc[ps["ip"]] = (ps["lat"], ps["lon"])
        addr_to_loc[pd["ip"]] = (pd["lat"], pd["lon"])
        prev = meas[ps["ip"]].get(pd["ip"])
        meas[ps["ip"]][pd["ip"]] = rtt if prev is None else min(prev, rtt)
    return {"address_to_loc": addr_to_loc, "loc_loc_meas": dict(meas)}
