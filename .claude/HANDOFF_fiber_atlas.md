# Fiber Atlas — a driving-time estimator for the Internet

> Handoff for a fresh agent/project. Written 2026-07-04 from inside the
> smarter-igreedy geolocation project, which is both the motivation and
> the first customer. Read this standalone; pointers into smarter-igreedy
> are given where its data or lessons matter.

## Vision

Google Maps for packets: given any two points on the globe, return the
**minimum plausible fiber RTT between them and the path that achieves
it** — not the geodesic-at-light-speed fantasy, but the shortest route
over infrastructure that actually exists: submarine cables, terrestrial
long-haul runs, landing stations, IXPs.

Today the standard cheap model is `rtt ≥ geodesic_km / 100` ms (RTT at
~2/3 c in fiber, the "1 ms per 100 km" rule). It is accurate if you
squint and badly wrong in the tails: Colombo–Frankfurt traffic does not
burrow through the planet, it follows submarine corridors via the Red
Sea; two city pairs with identical geodesics can have fiber floors that
differ by tens of ms. A queryable fiber floor turns "distance implies
latency" into "the network implies latency."

## Why this matters (measured, not hypothetical)

The smarter-igreedy project (RIPE-Atlas geolocation under a ping budget,
`~/Documents/smarter-igreedy/`, see its `CLAUDE.md` and
`SIMULATION_ENVIRONMENT.md`) spent this week discovering that the
geodesic model is the binding constraint on real data:

- Real RTTs run **~67 ms mean overhead** above the geodesic floor
  (median 53 ms), one-sided and heavy-tailed, scaling ~ d^0.67.
- The estimator's core failure mode is a **likelihood ridge**: with the
  geodesic model, "target is far" and "target has routing overhead" are
  exactly confounded when VPs are clustered — errors of 10,000+ km from
  self-consistent wrong fits. A fiber floor is geographically
  *structured*: fiber-isochrones are not circles, so the ridge collapses.
- At n=200, a third of targets ended with model estimates worse than
  their own nearest-neighbor answer — the model's ms→km translation,
  not the measurements, was the bottleneck.

Beyond geolocation: detour detection (measured ≫ fiber floor = routing
inefficiency, a sellable observable), latency SLA sanity checks, and
what-if analysis for new cables.

## Ground truth you already have

`~/Documents/smarter-igreedy/cache/cached_target_data.pkl` (~25 MB):

```python
import pickle
d = pickle.load(open('cache/cached_target_data.pkl', 'rb'))
d['address_to_loc']   # /24 subnet -> (lat, lon), ~909 probes, global
d['loc_loc_meas']     # src -> dst -> min RTT ms (bare float in cache)
```

- A dense all-pairs mesh (every survivor pings ≥80% of the others in
  both directions), built from one day of RIPE Atlas hourly dumps, each
  pair reduced to its **min RTT** over ~10 hourly files, each hourly
  value already min-of-~3 probes (queueing largely stripped).
- Regeneration / more days: `pull_ripe_atlas_measurement_data.py`
  (`RipeAtlasPipeline`, date-ranged) + `pull_ripe_atlas_probe_data.py`.
- **Interpretation discipline**: an observed min RTT is an UPPER bound
  on the fiber floor for that pair. Well-routed pairs sit near the
  floor; badly-routed pairs sit far above it. So the mesh cannot tell
  you the floor directly, but it gives ~800k directed pair constraints
  `floor(src, dst) ≤ observed`, and the lower envelope across many
  pairs traces the floor itself.

## The system, in the three intended parts

### 1. Fiber path data sources

- **Submarine (best-mapped, do this first)**: TeleGeography's
  submarinecablemap.com — the underlying GeoJSON (cable geometries +
  landing points) has historically been public in the
  `telegeography/www.submarinecablemap.com` GitHub repo; check current
  licensing before redistribution. ITU and Infrapedia as cross-checks.
- **Terrestrial long-haul (hard, mostly proprietary)**: the InterTubes
  dataset (US long-haul fiber map, IMC 2015) for the US; national NREN
  topology maps (GÉANT, Internet2, etc.); Infrapedia for a global but
  license-encumbered view.
- **Where no data exists, use rights-of-way priors**: long-haul fiber
  follows highways, railways and power corridors — OpenStreetMap
  motorway/rail geometries are a defensible proxy, or as a v0, connect
  major population centers / IXP cities (PeeringDB is public and has
  facility coordinates) within each country with geodesic × detour
  factor edges.

### 2. Overlay onto a geographic structure

- Graph: nodes = landing points, IXPs/major POPs, cities; edges = cable
  or corridor segments weighted by **RTT = length_km × 2 / v_fiber**
  with v_fiber ≈ c/1.468 ≈ 204,000 km/s (that is what the 1 ms/100 km
  rule encodes; smarter-igreedy's `KM_PER_MS = 100` must stay
  consistent with whatever you choose).
- Arbitrary query points snap to the graph: nearest node(s) within some
  radius, plus a last-mile segment charged at fiber speed (or slightly
  worse) over the geodesic. Spatial index (H3/S2 or a KD-tree over
  nodes) makes snapping trivial.
- Optional realism knobs, all additive per-hop constants: landing
  station / regeneration / router traversal penalties. Keep them
  explicit and default-zero — the product is a FLOOR, so every penalty
  must be defensible as physically unavoidable.
- Start with `networkx` + GeoJSON files in a repo; optimize later.

### 3. Shortest paths

- Dijkstra to start; A* with the **geodesic-at-fiber-speed heuristic**,
  which is admissible by construction (no fiber path beats the great
  circle at fiber speed), so A* stays exact.
- Many-to-many mode for validation runs against the mesh (~900×900);
  contraction hierarchies or landmark heuristics only if performance
  ever matters.
- API shape: `floor(src_latlon, dst_latlon) -> (rtt_ms, path)`.

## Validation loop (this is where the mesh earns its keep)

1. **Consistency (hard constraint)**: for every mesh pair, predicted
   floor ≤ observed min RTT. A violation is impossible physics — it
   means your graph is missing a link or a segment length is wrong.
   Violations are therefore a *gift*: each one localizes missing
   infrastructure. Expect the submarine-only v1 to violate on
   terrestrial-heavy pairs; that's the roadmap ordering itself.
2. **Tightness (usefulness metric)**: distribution of
   `observed − predicted` vs the geodesic baseline's
   `observed − geodesic/100`. The win is the shift of that whole
   distribution toward zero, reported per pair-category
   (intra-continent / inter-continent / island-and-isolated — the
   Colombo/Cape-Town/Guam class is exactly where geodesic fails worst
   and where smarter-igreedy bled 10,000 km errors).
3. **Integration experiment (the customer)**: swap smarter-igreedy's
   `expected rtt = slope × geodesic/100` for
   `expected rtt = fiber_floor + offset` in the additive model and
   rerun the n=100 real-mesh comparison. The hypothesis-set rings
   become fiber-isochrones; the ridge pathology should shrink
   measurably. This is a one-file change on that side
   (`probabilistic_helpers` / `feasible_region_maintainer` consume a
   distance-to-rtt function in a handful of places).

## Suggested milestones

0. Repo scaffold; load the mesh; reproduce the geodesic baseline
   tightness report (one script, one figure) so every later step has a
   before/after.
1. Submarine-only graph + landing points; floors for inter-continental
   pairs; violation + tightness report.
2. Terrestrial v0 (IXP-city graph with detour-factor edges, or OSM
   rights-of-way where ambition allows); repeat the report.
3. Snapping + query API + a cached many-to-many matrix for the mesh.
4. The smarter-igreedy integration experiment.

## Notes from the trenches (so you don't re-learn them)

- Min-of-observations is your friend for stripping queueing but does
  nothing about geometric detours — never treat an observed RTT as the
  floor itself, only as its ceiling.
- One reference point to sanity-check pipelines: smarter-igreedy's
  seed-31415 subsamples and its `assess_geolocators.py` harness — the
  mesh loading conventions (bare floats wrapped into lists on load) are
  documented in its `CLAUDE.md`.
- Information-boundary framing (matters if results feed geolocation):
  this atlas is built from PUBLIC infrastructure data, so using it
  inside inference is legitimate prior knowledge — unlike calibrating
  on the mesh's own VP-to-VP distances, which smarter-igreedy forbids
  as cheating. Keep the two data diets cleanly separated.
- Fiber speed conventions drift across papers (0.66c vs 0.68c vs
  2/3 c); pick one, name the constant, and state it in every figure.
