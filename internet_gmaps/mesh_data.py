"""Single entry point for mesh ground-truth RTT data.

Everything downstream (MeshEval, transit analysis, policy validation)
loads through here and stays oblivious to the source: the parent
project's daily-dump mesh and the mesh_campaign's live measurements are
merged into one TargetData structure

    {'address_to_loc': {addr: (lat, lon)}, 'loc_loc_meas': {src: {dst: min_rtt}}}

with min-RTT winning where both sources know a pair.
"""

import pickle
from pathlib import Path

MESH_PKL = Path(__file__).parent.parent / "cache" / "cached_target_data.pkl"
CAMPAIGN_DB = Path(__file__).parent / "mesh_campaign" / "data" / "state.sqlite"


def merge_target_data(base, extra):
    """Merge two TargetData dicts; min RTT wins on overlapping pairs."""
    out = {
        "address_to_loc": dict(base["address_to_loc"]),
        "loc_loc_meas": {s: dict(t) for s, t in base["loc_loc_meas"].items()},
    }
    out["address_to_loc"].update(extra["address_to_loc"])
    for src, dsts in extra["loc_loc_meas"].items():
        row = out["loc_loc_meas"].setdefault(src, {})
        for dst, rtt in dsts.items():
            prev = row.get(dst)
            row[dst] = rtt if prev is None else min(float(prev), float(rtt))
    return out


def load_target_data(mesh_pkl=MESH_PKL, include_campaign=True):
    data = pickle.load(open(mesh_pkl, "rb"))
    data = {
        "address_to_loc": dict(data["address_to_loc"]),
        "loc_loc_meas": {s: dict(t) for s, t in data["loc_loc_meas"].items()},
    }
    if include_campaign and CAMPAIGN_DB.exists():
        from mesh_campaign.export import campaign_target_data

        data = merge_target_data(data, campaign_target_data())
    return data
