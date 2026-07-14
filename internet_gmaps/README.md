# internet_gmaps — the Fiber Atlas

"Google Maps for packets": queryable minimum-plausible fiber RTT between
any two points, built from public infrastructure data, with a
falsifier-validated geopolitical transit policy. Consumed by the parent
geolocation project through `probabilistic_helpers.FiberFloorRtt`
(results: `../.claude/FIBER_GEOLOCATOR_RESULTS.md`; open research
directions for the routing model: `../.claude/HANDOFF_routing_realism.md`).

Modules expect this directory on `sys.path` (`conftest.py` handles pytest;
external callers `sys.path.append(<repo>/internet_gmaps)`).

## Files

- `DATA_SOURCES.md` — survey of what data exists (free / paid /
  ask-nicely), with live-verified endpoints. Start here for data.
- `fetch_public_data.py` — snapshot the free datasets into
  `data/raw/<date>/` (TeleGeography submarine cables + ITU terrestrial
  WFS). Raw snapshots are not in git; refetch with this script.
- `geo.py` — geodesics; reuses the parent project's conventions
  (KM_PER_MS from probabilistic_helpers, fast_haversine/get_distance from
  utils, Earth radius 6371.0) and adds only the array/KD-tree helpers the
  parent lacks. Consistency is pinned by tests.
- `fiber_graph.py` — GraphBuilder (node snapping/dedup) + FiberGraph
  (CSR, KD-tree, components, Dijkstra).
- `build_graph.py` — loaders (TeleGeography w/ published-length slack
  factors, ITU operational fibre) + build/report/save script; writes
  `data/graph_<date>.npz` (the current built graph IS in git).
- `transit_policy.py` + `TRANSIT_POLICY.md` — geopolitical transit
  restrictions (whose fiber may carry through-traffic): country/region
  rules with endpoint exemptions, cable- and terrestrial-distrust RTT
  factors, `DEFAULT_POLICY` (v3.7), and the per-rule evidence. Also the
  parallel policy-floor matrix/paths machinery used by validation.
- `floor_query.py` — the query layer:
  - `FloorEstimator` — OPEN floors: exact per-VP distance fields +
    pruned KD-tree expansion for arbitrary lat/lon. Knobs
    `direct_km_max` / `lastmile_km_max` (default 300 km each) are
    documented in the module docstring; state them in figures.
  - `PolicyFloorEstimator` — policy-aware floors for arbitrary points:
    lazy one-Dijkstra-per-(VP, country-class) fields, LRU-bounded
    (`max_cached_fields=`) with optional disk cache (`cache_dir=`,
    keyed by policy NAME — bump the name on rule changes), VP-subset
    queries (`floor_ms_subset`). Where the policy leaves no allowed
    route while the open graph has one, `floor_ms` raises
    `NoRouteError` (a KeyError) — an unroutable pair is a policy bug,
    never a silent number; `no_route="open"` restores the legacy
    OPEN-floor fallback (never bare geodesic). Points off the graph
    entirely stay inf under both modes. Country attribution is
    injectable (`node_cc`/`vp_cc`/`point_cc_fn`), defaulting to
    offline reverse_geocoder.
    ⚠️ the disk cache grows at ~250 KB × n_vps × realized classes
    (measured 22 GB at 1000 VPs); it is safe to delete — fields
    rebuild lazily.
- `mesh_data.py` — single entry point for ground-truth RTTs: merges the
  parent's daily-dump mesh (`../cache/cached_target_data.pkl`) with the
  live campaign (min-RTT wins, SOL-suspect probes excluded).
- `mesh_campaign/` — the RIPE Atlas measurement campaign (inventory,
  scheduling, results, export). Measurement state lives in
  `mesh_campaign/data/` (not in git). No cron is installed; runs are
  manual via `python -m mesh_campaign.daily`.
- `analysis/` — standalone diagnostic/figure scripts (stranded-pair
  diagnosis, noise audits, regional cable attribution, floor
  cross-checks, penalty-vs-deletion). Each file's docstring says when
  to use it and how to run it; see also
  `../.claude/HANDOFF_atlas_policy_v38.md` for the iteration playbook.
- `tests/` — unit tests (brute-force floor references, policy rules
  incl. terrestrial-only bans, PolicyFloorEstimator
  exactness/NoRouteError/cache) plus the mesh- and
  graph-dependent validation suites (skip without the mesh cache /
  built graph / reverse_geocoder). Run
  `~/Documents/venv312/bin/python -m pytest tests/` from this dir.

Ground-truth validation mesh lives in the parent project:
`../cache/cached_target_data.pkl` (see `../CLAUDE.md` for loading
conventions).
