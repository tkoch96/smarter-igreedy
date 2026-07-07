# internet_gmaps — the Fiber Atlas

"Google Maps for packets": queryable minimum-plausible fiber RTT between
any two points, built from public infrastructure data. Vision, motivation,
architecture, and milestones: `../.claude/HANDOFF_fiber_atlas.md`.

- `DATA_SOURCES.md` — survey of what data exists (free / paid /
  ask-nicely), with live-verified endpoints. Start here.
- `fetch_public_data.py` — snapshot the free datasets into
  `data/raw/<date>/` (TeleGeography submarine cables + ITU terrestrial WFS).
- `geo.py` — geodesics; reuses the parent project's conventions
  (KM_PER_MS from probabilistic_helpers, fast_haversine/get_distance from
  utils, Earth radius 6371.0) and adds only the array/KD-tree helpers the
  parent lacks. Consistency is pinned by tests.
- `fiber_graph.py` — GraphBuilder (node snapping/dedup) + FiberGraph
  (CSR, KD-tree, components, Dijkstra).
- `build_graph.py` — loaders (TeleGeography w/ published-length slack
  factors, ITU operational fibre) + build/report/save script.
- `transit_policy.py` + `TRANSIT_POLICY.md` — geopolitical transit
  restrictions (whose fiber may carry through-traffic) and the evidence
  for/against each rule.
- `floor_query.py` — FloorEstimator: exact per-VP distance fields +
  pruned KD-tree snapping for arbitrary lat/lon targets. The two
  modeling knobs (direct_km_max, lastmile_km_max) are documented in its
  module docstring.
- `tests/` — unit tests incl. a brute-force floor reference;
  run `~/Documents/venv312/bin/python -m pytest tests/` from this dir.

Ground-truth validation mesh lives in the parent project:
`../cache/cached_target_data.pkl` (see `../CLAUDE.md` for loading
conventions).
