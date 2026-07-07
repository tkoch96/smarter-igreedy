# Fiber-floor geolocation — integrating the learned atlas into smarter-igreedy

> Handoff for a fresh agent. Written 2026-07-06, the day the fiber atlas
> (internet_gmaps/) reached policy v3.2 and the RIPE mesh campaign grew
> ground truth 10x. This is the "integration experiment (the customer)"
> promised in HANDOFF_fiber_atlas.md: swap the geolocator's
> geodesic-at-fiber-speed distance model for the learned fiber-floor
> model and measure whether the ridge pathology shrinks. Read this
> standalone; verify file/line pointers before editing (both codebases
> moved fast).

## What you are given (all under internet_gmaps/)

- **A validated fiber graph**: `data/graph_2026-07-04.npz` — 31k nodes,
  38k edges (RFS-only cables + operational ITU terrestrial), loaded via
  `FiberGraph` (fiber_graph.py). Rebuildable: `python build_graph.py`.
- **The learned routing policy**: `transit_policy.DEFAULT_POLICY`
  (v3.2-geopolitical) — country bans (CN/MN/AF/TW, soviet-bloc-minus-EU,
  small-country w/ relay exemptions), country-granular Africa
  containment, cable distrust (Red Sea cut series x2), terrestrial
  distrust (Iraq overland x2). Every rule was adopted only after
  surviving the raw-floor-violation falsifier on ~1M mesh pairs;
  evidence + literature in TRANSIT_POLICY.md.
- **Model quality** (merged mesh, 2,965 clustered sites, 120k-pair
  sample): residual (measured − 1.3·floor) median 8.9 ms / P90 48 ms
  under policy, vs 16.2/83 open, vs ~30/122 for the 1.3·geodesic
  baseline. Raw-floor violations 4.2% (open: 0.4%). ~15% of pairs have
  an INF policy floor (no allowed route) — see "inf semantics" below.
- **Query machinery**:
  - `floor_query.FloorEstimator(graph, vp_lats, vp_lons)` — OPEN floors:
    one Dijkstra per VP, then `floor_ms(lat, lon)` is a microsecond
    lookup for arbitrary points. Knobs: `direct_km_max=300`,
    `lastmile_km_max=300` (documented in its module docstring; state
    them in figures).
  - `transit_policy.policy_floor_matrix_parallel(...)` — POLICY floors
    between fixed location sets (used by validation; matrix-shaped).
  - What does NOT exist yet: a policy-aware FloorEstimator for arbitrary
    query points. **Building it is your first task** (spec below).
- **Ground truth**: `mesh_data.load_target_data()` — the daily-dump mesh
  merged with the live campaign (897 -> 8,567 addresses; min-RTT wins;
  SOL-suspect probes excluded). Use THIS, not the raw pkl, so campaign
  targets are eligible.

## The idea (user's spec)

(a) import the learned model as the distance-between-two-points model
    instead of SOL; (b) call that a new strategy; (c) unit tests,
    including the harness that generates
    tests/error_over_measurements_additive_large.pdf; (d) rerun
    assess_geolocators with ~100-target large mesh including the new
    strategy.

## Task 1 — PolicyFloorEstimator (new, in internet_gmaps/floor_query.py)

Geolocation needs `expected_floor(vp, arbitrary_latlon)` under the
policy. The policy mask depends on the pair's endpoint countries, so:

- Per VP, precompute one field per target-class signature
  (`policy.class_signature(cc)`, ~35 classes; reuse `_build_banned_of`
  and the worker machinery in transit_policy.py). Cost: n_vps x 35
  Dijkstras — ~30 s for 100 VPs with the parallel pool; cache per
  (graph, policy, vp-set) like test_policy_validation does.
- Query: reverse-geocode the query point's cc (reverse_geocoder, offline,
  vectorize over hypothesis grids), pick the class field, then the same
  candidate-expansion lookup FloorEstimator.floor_ms uses.
- **inf semantics**: policy floor = inf means "no allowed route", which
  is not a usable likelihood. Fall back to the OPEN floor for that
  (vp, point) — compute both fields; `floor = policy_floor if finite
  else open_floor`. Never fall back to bare geodesic (that reintroduces
  the ridge exactly where the policy is most opinionated).
- Unit-test it against `policy_floor_matrix_parallel` on the mesh locs
  (equality where finite) and against `FloorEstimator` under
  OPEN_POLICY (exact match — the pattern in
  tests/test_transit_policy.py::test_policy_paths_floors_and_attribution).

## Task 2 — the strategy hook in smarter-igreedy

The model currently converts distance to expected RTT in a handful of
places (verify lines; grep `KM_PER_MS`):

- feasible_region_maintainer.py ~L180 `expected_rtt_ms(dist_km)` =
  slope * d / KM_PER_MS (and the ring geometry that consumes it)
- probabilistic_helpers.py ~L120/150/324 (likelihoods), ~L440/488/535
  (additive residuals), `AdditiveLatencyModel.predict(src, dst, dist_km)`
- iterative_greedy_geolocator.py ~L223 `preds = [get_distance(vp_loc, h)
  / KM_PER_MS for h in hyps]`

**Interface caveat that shapes the whole change**: these all take
`dist_km`. The fiber model needs coordinates — floor(vp, x) is not a
function of distance. Introduce one injectable model object:

    class RttModel:            # default = today's behavior
        def expected_ms(self, vp_loc, loc) -> float
    GeodesicRtt(slope)         # slope * get_distance(...) / KM_PER_MS
    FiberFloorRtt(estimator, slope=1.3, offset_ms=...)   # slope * floor + offset

and thread it through those call sites instead of `dist_km` (pass locs
down; keep `dist_km` variants delegating to GeodesicRtt so every
existing test passes unchanged). The additive model then learns its
per-VP offsets on top of the fiber floor exactly as it does on top of
the geodesic today — that is the whole point: the floor replaces the
d/100 term, the offset machinery is reused as-is.

Strategy naming: follow the existing region-mode pattern
(hard_circle / gaussian / em_gaussian -> add `fiber_additive` or
expose `--rtt-model fiber` orthogonally; read how assess_geolocators.py
selects strategies before choosing). Batch queries: hypothesis grids can
be thousands of points — PolicyFloorEstimator lookups are vectorizable;
avoid per-point python loops in the likelihood inner loop (profile
first; the EM tests are runtime-sensitive).

## Task 3 — tests

- Extend tests/test_e2e_additive_large.py (the
  error_over_measurements_additive_large.pdf generator). NOTE: its world
  is SYNTHETIC with ground truth rtt = slope*d/100 + noise — a fiber
  strategy there can only sanity-check plumbing. Do two things:
  1. plumbing test: FiberFloorRtt with a mock estimator that returns
     geodesic/100 must reproduce the gaussian/additive baseline curves
     exactly (proves the injection changes nothing when the model is
     the old model);
  2. a small synthetic world where ground truth uses a toy fiber graph
     (equator chain, tests/test_floor_query.py has builders) — fiber
     strategy must beat geodesic there.
- The real evidence comes from the real-mesh harness (assess harnesses
  ending in `_real` / the n=100 comparisons — see CLAUDE.md "File map").

## Task 4 — the experiment

Rerun assess_geolocators on ~100 large-mesh targets, strategies =
existing set + the fiber one. Selection: draw targets from
`mesh_data.load_target_data()` (includes campaign coverage; excludes
SOL-suspects). Seed 31415 per project convention. Compare
error-over-measurements curves; also slice by target class —
**island/isolated and East-Asia targets are where the hypothesis says
fiber-isochrones beat circles** (Colombo/Cape-Town/Guam class; the
10,000 km ridge failures documented in HANDOFF_fiber_atlas.md).

Honest expectations, so you report faithfully:
- The floor is VALID nearly everywhere (0.4% open violations) but LOOSE
  unevenly: Gulf (~67 ms endpoint trombone), East Asia (~60+ ms), and
  anywhere the campaign's min-of-3 single-visit RTTs are the only data.
  A fixed 1.3 slope will under/over-shoot per region — if the additive
  per-VP offsets don't absorb that, a per-region offset is the first
  thing to try (TRANSIT_POLICY.md "deliberate non-rules" explains why
  this is a slack problem, not a routing problem).
- Data hygiene (from HANDOFF_fiber_atlas.md, still binding): the atlas
  is public-infrastructure prior — legitimate. But do NOT tune slope or
  offsets on the same mesh pairs you evaluate on; the additive model's
  existing train/eval split discipline applies to fiber terms too.

## Practicalities

- venv: `~/Documents/venv312` (pytest, scipy, reverse_geocoder all in).
- Imports: internet_gmaps modules assume internet_gmaps/ on sys.path
  (its conftest.py does this for ITS tests; from smarter-igreedy code do
  `sys.path.insert(0, <repo>/internet_gmaps)` or make a tiny shim).
- geo conventions are already shared (internet_gmaps/geo.py imports
  KM_PER_MS and haversine from THIS project; consistency is pinned by
  tests/test_geo.py::TestParentProjectConsistency).
- Floor-matrix caches: internet_gmaps/data/cache/*.npy, keyed by
  (policy name, loc count, pair count, edge count) — bump the policy
  name on any rule change or you'll read stale physics.
- Runtime intuition: per-VP-per-class Dijkstra ~5-8 ms; 100 VPs x 35
  classes parallel ~30 s once, then lookups are microseconds. The full
  clustered validation suite is ~5 min if you want to re-verify the
  atlas first (`pytest tests/ -q` inside internet_gmaps; 100+ tests).
- Run only the tests you touch (user preference); the mesh-dependent
  suites need cache/cached_target_data.pkl and a built graph npz.
- The campaign cron is NOT installed; you don't need new measurements
  for this task, but `python -m mesh_campaign.daily --dry-run` shows
  where coverage would grow next if the experiment wants more pairs.

## Definition of done

1. PolicyFloorEstimator + unit tests (exactness vs matrix, OPEN
   equivalence, inf-fallback behavior).
2. RttModel injection with all existing tests green unchanged.
3. New strategy in the e2e harness with the two synthetic tests.
4. The n~100 real-mesh comparison figure with the fiber strategy
   overlaid, plus a per-region error breakdown, and a short written
   verdict: did fiber-isochrones shrink the ridge failures, where did
   they not, and is the failure mode slack (offsets) or structure
   (missing rules) — that verdict feeds the next atlas iteration.
