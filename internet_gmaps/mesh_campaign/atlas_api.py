"""Thin REST client for the RIPE Atlas API v2.

Plain `requests` instead of ripe.atlas.cousteau: cousteau is in maintenance
mode and the API dropped query-param auth (keys go in the
`Authorization: Key <k>` header now — verified 2026-07-06; the old
`?key=` pattern returns 401). Four endpoints cover everything the mesh
campaign needs.

The key is read from $RIPE_ATLAS_KEY or ~/.ripe_atlas_key.
"""

import os
import time
from pathlib import Path

import requests

BASE = "https://atlas.ripe.net/api/v2"
KEY_FILE = Path.home() / ".ripe_atlas_key"

# RIPE Atlas platform limits (see atlas.ripe.net/docs/udm; mirrors the
# constants in the old atlas_pinger.py)
MAX_PROBES_PER_MEAS = 100
MAX_ACTIVE_MEAS = 90  # platform cap 100; leave headroom
PING_PACKETS = 3  # credits per ping result = packets


def api_key():
    key = os.environ.get("RIPE_ATLAS_KEY")
    if not key and KEY_FILE.exists():
        key = KEY_FILE.read_text().strip()
    return key or None


def _headers():
    key = api_key()
    return {"Authorization": f"Key {key}"} if key else {}


def get(path, **params):
    r = requests.get(f"{BASE}{path}", params=params, headers=_headers(), timeout=90)
    r.raise_for_status()
    return r.json()


def get_paged(path, max_pages=200, **params):
    """Yield results across paginated endpoints."""
    url = f"{BASE}{path}"
    for _ in range(max_pages):
        r = requests.get(url, params=params, headers=_headers(), timeout=90)
        r.raise_for_status()
        data = r.json()
        yield from data["results"]
        url, params = data.get("next"), {}
        if not url:
            return


def create_ping(target_ip, probe_ids, description, packets=PING_PACKETS):
    """One-off ping from the given probes to target_ip. Returns (ok, msm_id
    or error dict). Costs len(probe_ids) * packets credits."""
    payload = {
        "definitions": [
            {
                "type": "ping",
                "af": 4,
                "target": target_ip,
                "description": description,
                "packets": packets,
                "resolve_on_probe": False,
            }
        ],
        "probes": [
            {
                "type": "probes",
                "value": ",".join(str(p) for p in probe_ids),
                "requested": len(probe_ids),
            }
        ],
        "is_oneoff": True,
    }
    r = requests.post(
        f"{BASE}/measurements/", json=payload, headers=_headers(), timeout=90
    )
    if r.status_code // 100 == 2:
        return True, r.json()["measurements"][0]
    try:
        return False, r.json()
    except ValueError:
        return False, {"error": {"status": r.status_code, "detail": r.text[:200]}}


def measurement_results(msm_id):
    return get(f"/measurements/{msm_id}/results/")


def measurement_status(msm_id):
    return get(f"/measurements/{msm_id}/")


def n_active_measurements():
    try:
        return get("/measurements/my/", status=2, page_size=1)["count"]
    except Exception:
        # transient API failure: report "at capacity" so callers back off
        # instead of crashing a long submit loop
        return MAX_ACTIVE_MEAS


def wait_for_capacity(poll_s=30, max_wait_s=1800):
    """Block until the account is under the active-measurement cap."""
    waited = 0
    while n_active_measurements() >= MAX_ACTIVE_MEAS and waited < max_wait_s:
        time.sleep(poll_s)
        waited += poll_s
