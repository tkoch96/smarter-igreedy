"""Snapshot the free public fiber-atlas datasets into data/raw/<YYYY-MM-DD>/.

Sources (all verified live 2026-07-04, see DATA_SOURCES.md):
  - TeleGeography submarine cable map API v3: cable geometries,
    landing points, and per-cable metadata (published lengths, RFS, owners).
  - ITU BBmaps terrestrial transmission links via open WFS
    (~40k LineStrings, fibre + microwave, with status).

Idempotent: skips files that already exist for today's snapshot.
Per-cable metadata is ~700 small requests; a polite delay keeps it ~2 min.
"""

import json
import sys
import time
import urllib.request
from pathlib import Path

TG_BASE = "https://www.submarinecablemap.com/api/v3"
ITU_WFS = (
    "https://bbmaps.itu.int/geoserver/itu-geocatalogue/ows"
    "?service=WFS&version=2.0.0&request=GetFeature"
    "&typeNames=itu-geocatalogue:trx_geocatalogue"
    "&outputFormat=application/json"
)

OUT_DIR = Path(__file__).parent / "data" / "raw" / time.strftime("%Y-%m-%d")


def fetch(url: str, timeout: int = 120) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "fiber-atlas-research/0.1"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def save(name: str, url: str) -> dict:
    path = OUT_DIR / name
    if path.exists():
        print(f"  [cached] {name}")
        return json.loads(path.read_text())
    raw = fetch(url)
    data = json.loads(raw)  # validate before writing
    path.write_bytes(raw)
    print(f"  [fetched] {name} ({len(raw) / 1e6:.1f} MB)")
    return data


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Snapshot dir: {OUT_DIR}")

    print("TeleGeography:")
    cable_geo = save("tg_cable_geo.json", f"{TG_BASE}/cable/cable-geo.json")
    save("tg_landing_point_geo.json", f"{TG_BASE}/landing-point/landing-point-geo.json")
    cables = save("tg_cable_index.json", f"{TG_BASE}/cable/all.json")

    detail_path = OUT_DIR / "tg_cable_details.json"
    if detail_path.exists():
        print("  [cached] tg_cable_details.json")
    else:
        details = {}
        for i, c in enumerate(cables):
            details[c["id"]] = json.loads(fetch(f"{TG_BASE}/cable/{c['id']}.json"))
            if (i + 1) % 100 == 0:
                print(f"  ...cable details {i + 1}/{len(cables)}")
            time.sleep(0.1)
        detail_path.write_text(json.dumps(details))
        print(f"  [fetched] tg_cable_details.json ({len(details)} cables)")

    print("ITU BBmaps WFS:")
    itu = save("itu_trx_geocatalogue.json", ITU_WFS)
    n_returned = itu.get("numberReturned", len(itu.get("features", [])))
    n_matched = itu.get("numberMatched")
    if n_matched is not None and n_returned < n_matched:
        # GeoServer capped the response; page through the remainder.
        feats = itu["features"]
        while len(feats) < n_matched:
            page = json.loads(fetch(f"{ITU_WFS}&startIndex={len(feats)}"))
            got = page.get("features", [])
            if not got:
                sys.exit(f"WFS paging stalled at {len(feats)}/{n_matched}")
            feats.extend(got)
            print(f"  ...WFS paging {len(feats)}/{n_matched}")
        itu["numberReturned"] = len(feats)
        (OUT_DIR / "itu_trx_geocatalogue.json").write_text(json.dumps(itu))

    print("\nSummary:")
    print(f"  TG cables (geo):     {len(cable_geo['features'])}")
    print(f"  ITU terrestrial links: {len(itu['features'])}")


if __name__ == "__main__":
    main()
