"""Anycast prefix screen (MAnycast²/LACeS census, ut-dacs/Anycast-Census).

An anycast address is poison as a mesh DESTINATION: replies come from
whichever instance is nearest the source, so RTTs are attributed to the
probe's claimed location wrongly — and only violate the speed-of-light
check when the near instance is impossibly close. The census pre-filter
catches the plausible-but-wrong cases the SOL check cannot. Source-side
the probe pings outward from its real location, so sources stay eligible.

The census publishes detected anycast /24s daily; membership is a set
lookup on the /24-truncated address. 16 inventory probes matched on
2026-07-07 — including four probes on ONE anycast /24 claiming four
different countries.
"""

import csv
import ipaddress
import subprocess
import time
from pathlib import Path

CENSUS_URL = "https://raw.githubusercontent.com/ut-dacs/Anycast-Census/main/IPv4-latest.csv"
CACHE = Path(__file__).parent / "data" / "anycast_v4_latest.csv"
MAX_AGE_H = 7 * 24.0


def _refresh(cache_path=CACHE):
    subprocess.run(
        ["curl", "-sL", "-o", str(cache_path), CENSUS_URL], check=True, timeout=120
    )


def load_anycast_slash24s(cache_path=CACHE, max_age_h=MAX_AGE_H):
    """Set of anycast /24s as address-int >> 8. Refreshes weekly; a stale
    or missing census degrades to the cached (or empty) set rather than
    blocking the campaign."""
    cache_path = Path(cache_path)
    stale = (
        not cache_path.exists()
        or time.time() - cache_path.stat().st_mtime > max_age_h * 3600
    )
    if stale:
        try:
            _refresh(cache_path)
        except Exception:
            pass
    if not cache_path.exists():
        return set()
    nets = set()
    with open(cache_path) as f:
        for row in csv.DictReader(f):
            try:
                nets.add(int(ipaddress.ip_network(row["prefix"]).network_address) >> 8)
            except (ValueError, KeyError):
                continue
    return nets


def is_anycast(ip, slash24s):
    try:
        return (int(ipaddress.ip_address(ip)) >> 8) in slash24s
    except ValueError:
        return False
