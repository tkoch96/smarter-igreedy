"""Surrogate targets for probes whose listed address never answers.

~6% of usable probes are proven-good SOURCES whose own `address_v4` is
dead (pilot: listed address != pingable address for ~1/3 of probes; most
screen out via probation, but 711 groups lose their only destination).
Recipe: walk the probe's public built-in traceroutes outward and take the
first hop that is

  (a) globally routable,
  (b) in the probe's own network (same ASN via RIPEstat, with a same-/24
      fast path that skips the lookup),
  (c) within MAX_HOP_RTT_MS of the probe.

The RTT cap doubles as a location-slack bound: pinging a router r ms from
the probe attributes the result to the probe's location with <= r ms of
error. Keeping MAX_HOP_RTT_MS <= results.SOL_MARGIN_MS guarantees a
surrogate can never manufacture a false speed-of-light violation.

Candidates are screened with a local ping before being admitted; verified
surrogates live in the state db and daily.py substitutes them as the
probe's destination address (probation still applies — local
reachability does not guarantee Atlas-probe reachability).
"""

import ipaddress
import json
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from . import atlas_api

# built-in IPv4 traceroutes every connected probe runs (verified live
# 2026-07-07): topology discovery + root servers
BUILTIN_TRACEROUTES = (5051, 5151, 5010, 5004, 5005, 5016)
MAX_HOP_RTT_MS = 2.0  # keep <= results.SOL_MARGIN_MS (see module docstring)
RIPESTAT_URL = "https://stat.ripe.net/data/network-info/data.json"
ASN_CACHE = Path(__file__).parent / "data" / "ip_asn_cache.json"


def _is_global(ip):
    try:
        return ipaddress.ip_address(ip).is_global
    except ValueError:
        return False


class AsnResolver:
    """IP -> origin ASNs via RIPEstat, disk-cached (one lookup per IP ever)."""

    def __init__(self, cache_path=ASN_CACHE):
        self.cache_path = Path(cache_path)
        self.cache = {}
        if self.cache_path.exists():
            self.cache = json.loads(self.cache_path.read_text())

    def asns(self, ip):
        if ip not in self.cache:
            try:
                r = requests.get(RIPESTAT_URL, params={"resource": ip}, timeout=30)
                r.raise_for_status()
                self.cache[ip] = [int(a) for a in r.json()["data"]["asns"]]
            except Exception:
                return []  # transient failure: don't cache, don't match
        return self.cache[ip]

    def save(self):
        self.cache_path.write_text(json.dumps(self.cache))


def dead_dst_probes(state, inventory_by_id, min_attempts=5):
    """Probes worth a surrogate: in inventory, proven ok as a SOURCE, but
    never answered as a destination despite >= min_attempts tries."""
    ok_src = {s for (s,) in state.db.execute("SELECT DISTINCT src FROM pairs WHERE status='ok'")}
    dead = [
        d
        for (d,) in state.db.execute(
            "SELECT dst FROM pairs GROUP BY dst HAVING SUM(status='ok')=0 AND SUM(status='failed')>=?",
            (min_attempts,),
        )
        if d in inventory_by_id and d in ok_src
    ]
    return dead


def hops_from_traceroute(res):
    """Yield (ip, min_rtt_ms) per responding hop, in hop order."""
    for hop in res.get("result", []):
        replies = [x for x in hop.get("result", []) if "from" in x]
        rtts = [x["rtt"] for x in replies if isinstance(x.get("rtt"), (int, float))]
        if replies and rtts:
            yield replies[0]["from"], min(rtts)


def candidate_surrogate(probe, resolver, max_rtt_ms=MAX_HOP_RTT_MS):
    """First traceroute hop in the probe's network within the RTT cap, or
    None. Tries each built-in measurement until one has a fresh result."""
    own_net24 = ipaddress.ip_network(f"{probe['ip']}/24", strict=False)
    for msm in BUILTIN_TRACEROUTES:
        try:
            results = atlas_api.get(f"/measurements/{msm}/latest/", probe_ids=probe["id"])
        except Exception:
            continue
        for res in results or []:
            for ip, rtt in hops_from_traceroute(res):
                if rtt > max_rtt_ms:
                    break  # hops only get farther; next traceroute won't help either
                if not _is_global(ip) or ip == probe["ip"]:
                    continue
                same_24 = ipaddress.ip_address(ip) in own_net24
                if same_24 or probe["asn"] in resolver.asns(ip):
                    return {"ip": ip, "hop_rtt_ms": rtt}
        if results:
            return None  # had a traceroute, no qualifying hop
    return None


PING_RE = re.compile(r"min/avg/max[^=]*= ([\d.]+)/")


def local_ping(ip, count=3, timeout_s=4):
    """Ping from this machine; returns min rtt ms or None. Responsiveness
    screen only — local reachability, not location evidence."""
    try:
        out = subprocess.run(
            ["ping", "-c", str(count), "-t", str(timeout_s), ip],
            capture_output=True,
            text=True,
            timeout=timeout_s + count + 5,
        )
    except Exception:
        return None
    if out.returncode != 0:
        return None
    m = PING_RE.search(out.stdout)
    return float(m.group(1)) if m else None


def discover(state, inventory_by_id, max_rtt_ms=MAX_HOP_RTT_MS, workers=8, verbose=True):
    """Full pipeline: dead dsts -> traceroute candidates -> local ping
    screen -> state db. Returns {prb_id: surrogate dict}."""
    resolver = AsnResolver()
    have = state.surrogates()
    todo = [p for p in dead_dst_probes(state, inventory_by_id) if p not in have]
    if verbose:
        print(f"surrogates: {len(todo)} dead-dst probes to try ({len(have)} already mapped)")

    def find(pid):
        return pid, candidate_surrogate(inventory_by_id[pid], resolver, max_rtt_ms)

    found = {}
    with ThreadPoolExecutor(workers) as ex:
        for pid, cand in ex.map(find, todo):
            if cand:
                found[pid] = cand
    resolver.save()
    if verbose:
        print(f"surrogates: {len(found)} traceroute candidates; local ping screen...")

    admitted = {}
    with ThreadPoolExecutor(workers) as ex:
        rtts = ex.map(lambda kv: (kv[0], local_ping(kv[1]["ip"])), found.items())
        for pid, rtt in rtts:
            if rtt is not None:
                cand = dict(found[pid], local_rtt_ms=rtt)
                state.record_surrogate(pid, cand["ip"], cand["hop_rtt_ms"], rtt)
                admitted[pid] = cand
    if verbose:
        print(f"surrogates: {len(admitted)} admitted after local screen")
    return admitted


def main():
    import argparse

    from .inventory import fetch_inventory
    from .state import CampaignState

    ap = argparse.ArgumentParser()
    ap.add_argument("--max-rtt-ms", type=float, default=MAX_HOP_RTT_MS)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()
    state = CampaignState()
    by_id = {p["id"]: p for p in fetch_inventory()}
    discover(state, by_id, args.max_rtt_ms, args.workers)


if __name__ == "__main__":
    main()
