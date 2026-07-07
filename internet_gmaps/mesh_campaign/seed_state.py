"""Seed campaign state with the existing daily-dump mesh so we never spend
credits re-measuring pairs we already have (campaign caveat 4).

The parent project's cache (smarter-igreedy/cache/cached_target_data.pkl)
stores min RTTs between /24-truncated probe addresses. We map those /24s
onto current inventory probe ids (skipping ambiguous /24s that contain
several probes) and record the pairs as successful with msm_id=0. Their
destinations become verified (their addresses demonstrably answered).

    python -m mesh_campaign.seed_state
"""

import pickle
from collections import defaultdict
from pathlib import Path

from .inventory import fetch_inventory
from .state import CampaignState

MESH_PKL = Path(__file__).parent.parent.parent / "cache" / "cached_target_data.pkl"


def slash24(ip):
    return ".".join(ip.split(".")[:3])


def seed_from_dumps(state=None, mesh_pkl=MESH_PKL, verbose=True):
    state = state or CampaignState()
    probes = fetch_inventory(verbose=False)
    by_24 = defaultdict(list)
    for p in probes:
        by_24[slash24(p["ip"])].append(p["id"])
    prb_of = {k: v[0] for k, v in by_24.items() if len(v) == 1}

    d = pickle.load(open(mesh_pkl, "rb"))
    addr_loc = d["address_to_loc"]
    n_pairs = n_matched = 0
    rows = []
    for src_addr, dsts in d["loc_loc_meas"].items():
        src_prb = prb_of.get(slash24(src_addr))
        for dst_addr, rtt in dsts.items():
            n_pairs += 1
            dst_prb = prb_of.get(slash24(dst_addr))
            if src_prb is None or dst_prb is None or src_prb == dst_prb:
                continue
            n_matched += 1
            rows.append((src_prb, dst_prb, float(rtt)))
    state.db.executemany(
        "INSERT OR IGNORE INTO pairs VALUES (?,?,'ok',?,0,0)", rows
    )
    state.db.commit()
    matched_probes = {r[0] for r in rows} | {r[1] for r in rows}
    if verbose:
        print(
            f"seeded {n_matched:,}/{n_pairs:,} dump pairs onto {len(matched_probes)} "
            f"current probes ({len(addr_loc)} dump addresses, "
            f"{len(prb_of):,} unambiguous /24 matches in inventory)"
        )
    return n_matched


if __name__ == "__main__":
    seed_from_dumps()
