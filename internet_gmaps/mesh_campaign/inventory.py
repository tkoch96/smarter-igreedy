"""Probe inventory: connected RIPE Atlas probes with a public, pingable-in-
principle IPv4 address, a claimed location, and an ASN.

Trust model (caveat 5 of the campaign spec): the listed address and
location are claims. Address responsiveness is verified by the campaign
itself (probes that never answer accumulate strikes, results.py);
locations are cross-checked with speed-of-light violations against the
fiber-atlas geodesic floor (also results.py). This module only filters to
probes whose claims are worth testing.
"""

import json
import time
from pathlib import Path

from . import atlas_api

CACHE = Path(__file__).parent / "data" / "probe_inventory.json"
FIELDS = "id,address_v4,asn_v4,country_code,geometry,status,tags,is_anchor"


def fetch_inventory(cache_path=CACHE, max_age_h=24.0, verbose=True):
    """List of probe dicts: id, ip, asn, cc, lat, lon. Cached to disk."""
    cache_path = Path(cache_path)
    if cache_path.exists():
        cached = json.loads(cache_path.read_text())
        if time.time() - cached["fetched_at"] < max_age_h * 3600:
            return cached["probes"]

    probes = []
    raw = atlas_api.get_paged(
        "/probes/", status=1, is_public=True, page_size=500, fields=FIELDS
    )
    for p in raw:
        tags = {t["slug"] for t in p.get("tags", [])}
        if (
            p.get("address_v4")
            and p.get("asn_v4")
            and p.get("country_code")
            and p.get("geometry")
            and "system-ipv4-works" in tags
        ):
            lon, lat = p["geometry"]["coordinates"]
            probes.append(
                {
                    "id": p["id"],
                    "ip": p["address_v4"],
                    "asn": int(p["asn_v4"]),
                    "cc": p["country_code"],
                    "lat": float(lat),
                    "lon": float(lon),
                    # anchors are servers with deliberately pingable static
                    # addresses: pre-verified destinations
                    "anchor": bool(p.get("is_anchor")),
                }
            )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps({"fetched_at": time.time(), "probes": probes}))
    if verbose:
        ccs = {p["cc"] for p in probes}
        asns = {p["asn"] for p in probes}
        print(
            f"inventory: {len(probes)} usable probes, {len(ccs)} countries, {len(asns)} ASNs"
        )
    return probes
