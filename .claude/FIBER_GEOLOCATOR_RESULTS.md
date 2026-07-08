# Fiber-floor geolocation — integration results (2026-07-06/07)

What happened when the geolocator's geodesic-at-fiber-speed distance
model was swapped for the learned fiber-floor atlas (internet_gmaps),
and what the experiments showed. Open follow-ups live in
`.claude/TODOS.md`; the atlas-side agenda in
`.claude/HANDOFF_routing_realism.md`.

## What was built

1. **PolicyFloorEstimator** (internet_gmaps/floor_query.py): policy-aware
   floors for arbitrary query points. Lazy one-Dijkstra-per-(VP, class
   signature) fields with LRU + disk cache (data/cache/policy_fields/),
   VP-subset queries (`floor_ms_subset`) for the geolocation hot path,
   OPEN-floor fallback exactly where the policy floor is inf (never bare
   geodesic). Unit-tested for exact equality vs policy_floor_matrix,
   bit-exact OPEN_POLICY equivalence with FloorEstimator, per-VP
   inf-fallback, disk-cache roundtrip, LRU-eviction exactness
   (tests/test_policy_floor_estimator.py, 12 tests).

2. **RttModel injection** (probabilistic_helpers.py): `RttModel` /
   `GeodesicRtt` / `FiberFloorRtt` — an injectable base-RTT term replacing
   d/KM_PER_MS everywhere (gaussian NLL, EM M-step, additive MAP /
   batch-EM / shared-model refit, hypothesis-benefit predictions,
   posterior grid). Every call site keeps `rtt_model=None` = the original
   geodesic expression bit-for-bit; the full pre-existing suite passes
   unchanged. FiberFloorRtt pickles as (factory, token) only — greedy
   workers rebuild the estimator once per process and share the disk
   field cache. Floors are memoized per exact query point; MAP inner
   loops batch one subset lookup per optimizer point (`base_ms_rows`).

3. **Synthetic tests** (tests/test_e2e_additive_large.py):
   - plumbing: FiberFloorRtt with a mock geodesic atlas reproduces the
     additive batch fit, the gaussian MAP, and full greedy curves
     EXACTLY (float-equality) — the injection is a no-op when the model
     is the old model.
   - toy fiber world (C-shaped detour cable, truth = 1.0·floor +
     offsets): fiber 76 km vs geodesic 423 km vs NN 414 km.
     Instructive detail: with VPs on only one end of a single cable the
     world is ridge-degenerate — all floors shift by the same constant
     along the cable and the per-target offset absorbs position exactly.
     A cable cannot trilaterate itself; the far-end VP pins position.

4. **Real-mesh experiment** (seed 31415, budgets 200..2500, fiber = v3.2
   policy floors × 1.3, open fallback; originally a standalone
   assess_fiber_real.py, since FOLDED INTO assess_geolocators.py as
   GEOLOC_* env settings — use e.g. `GEOLOC_DATA=merged GEOLOC_NSRC=100
   GEOLOC_NTGT=100 GEOLOC_FIBER=1 python assess_geolocators.py`):
   100 targets = 58 dense-mesh + 42 campaign, per-target VP subsets
   (shared 100-VP dense pool; ≤15 own sources per campaign target —
   campaign sources are thin and disjoint, median 6 targets per source,
   so no shared core exists). Outputs (renamed to the shape convention):
   figures/geolocator_results_668src_100dst.pdf,
   cache/geolocator_run_668src_100dst.pkl (per-target errors per budget).

## Headline numbers (mean / median km at b=2500)

| strategy                | all targets    | dense (58)   | campaign (42) |
|-------------------------|----------------|--------------|---------------|
| smart_perfect (oracle)  | 1174 / 272     |  341 / 119   | 2324 /  804   |
| **greedy_phased_fiber** | **1532 / 748** |  871 / 599   | 2444 / 1388   |
| random+additive_fiber   | 1695 / 785     | 1006 / 628   | 2647 / 1317   |
| greedy_phased           | 1933 / 1246    | 1607 / 970   | 2384 / 1471   |
| random+nn               | 1993 / 608     |  918 / 322   | 3477 / 1428   |
| random+additive         | 2303 / 1472    | 1607 / 1113  | 3263 / 2033   |

- The pure distance-model swap (random order, additive estimator,
  geodesic → fiber) improves the mean 2303 → 1695 and the median
  1472 → 785. A fiber estimator on RANDOM pings beats the geodesic
  greedy's selected pings.
- greedy_phased_fiber is the best honest strategy on both stats and
  does NOT plateau: the geodesic greedy flat-lines ≈1950 from b≈1600
  while the fiber greedy is still falling at b=2500 (1616 → 1532 over
  the last 500 pings). The ridge plateau is broken on dense targets:
  mean 1607 → 871, median 970 → 599.
- Per-target ledger (estimator pair): 55 targets helped >100 km,
  33 hurt >100 km, net +608 km mean. Wins are exactly the hypothesis
  class — ridge failures collapsing: Guadeloupe 9689→713, Japan
  11937→3005, Taiwan 6884→2578, South Africa 9161→4390, Bolivia
  9044→3770. East-Asia region mean 9328→3413.

## Where fiber did NOT help, and the verdict

- **Campaign targets are a wash for the greedy** (2384 → 2444 mean,
  1471 → 1388 median): with ≤15 thin, single-visit sources the oracle
  itself only reaches 2324/804 — the binding constraint there is
  measurement geometry and RTT quality, not the distance model. The
  fiber ESTIMATOR still wins on campaign targets (3263 → 2647 mean),
  so the model is right; selection just has nothing extra to exploit.
- **The failure mode is slack, not structure.** The big per-target
  losses (NZ −7132, MY −6453, SG −9556, scattered US/EU regressions)
  sit in regions the atlas validation already flags as LOOSE (East-Asia
  /Oceania endpoint trombone, ~60+ ms; campaign min-of-3 RTT noise) —
  and the same countries appear on BOTH the win and loss lists (JP, TW,
  SG, ZA, US each have targets fixed and targets broken). That is the
  signature of uneven endpoint slack under a fixed 1.3 slope, not of a
  missing transit rule: when the floor is loose, fiber isochrones move
  the MAP a long way ALONG a cable, so slack errors are amplified
  directionally (worst case: a US campaign target thrown 907→7773 km).
  No new-ban candidate emerges from the loss set.
  (The follow-ups this verdict implies are tracked in
  `.claude/TODOS.md`.)

## Scaling runs (2026-07-07 overnight; mean/median km at final budget)

Same six strategies, merged mesh, seed 31415, via the now-unified
`assess_geolocators.py` settings (`configs/fiber_200x1000.json`,
`configs/fiber_1000x1000.json`; results pickles
`cache/geolocator_run_{200src_1000dst,1000src_1000dst}.pkl`, figures
`figures/geolocator_results_<shape>.pdf`).

**(b) 200 sources × 1000 targets** (11,482 pairs, coverage-greedy source
selection, b=10000 ≈ 87% of pairs):

| strategy | all | dense (622) | campaign (378) |
|---|---|---|---|
| smart_perfect | 2048 / 1247 | 1828 / 1303 | 2410 / 1144 |
| greedy_phased_fiber | 2191 / 1322 | 2022 / 1367 | 2469 / 1298 |
| random+nn | 2232 / 1322 | 2032 / 1345 | 2561 / 1148 |
| random+additive_fiber | 2285 / 1343 | 2146 / 1386 | 2513 / 1298 |
| greedy_phased | 2741 / 2134 | 2817 / 2344 | 2616 / 1673 |
| random+additive | 2855 / 2294 | 2943 / 2499 | 2709 / 1649 |

Source-scarcity regime (~11 VPs/target): the oracle itself is only ~8%
better than random+NN — there is little estimation/selection headroom
when coverage is the binding constraint. Within that headroom the fiber
models still dominate their geodesic twins (fiber greedy −20% mean /
−38% median vs geodesic greedy; per-target ledger 628 helped vs 294
hurt, net +550 km), and the fiber greedy lands within 7% of the oracle.
Fiber vs NN is a statistical tie here (~2%, below the documented 5–10%
greedy cross-run jitter).

**(a) 1000 × 1000** (24,982 pairs, VP cap 25/target, b=15000):

| strategy | all | dense (622) | campaign (378) |
|---|---|---|---|
| smart_perfect | 1523 / 612 | 1408 / 523 | 1713 / 818 |
| greedy_phased_fiber | 1675 / 800 | 1483 / 685 | 1992 / 1081 |
| random+additive_fiber | 1782 / 787 | 1669 / 649 | 1968 / 1102 |
| random+nn | 1894 / 815 | 1805 / 663 | 2041 / 1040 |
| greedy_phased | 2086 / 1142 | 1950 / 965 | 2309 / 1467 |
| random+additive | 2033 / 1152 | 1920 / 1047 | 2219 / 1454 |

The n=100 conclusions replicate at 10× scale: the geodesic additive
plateau reappears (greedy ≈ random order ≈ 2050/1150) and the fiber
base term breaks it (−20% mean / −30% median for the greedy); the fiber
greedy is the best honest strategy on both stats and beats random+NN on
means. East-Asia region mean: geodesic greedy 7602 → fiber 3354. The
fiber win concentrates on dense targets again (1483/685 vs 1950/965);
campaign targets stay coverage-limited (oracle 1713 there).

**Scaling costs** (MacBook Air, 6 workers): n=100 ≈ 25 min, 200×1000
≈ 3 h, 1000×1000 ≈ 2.7 h wall. The one hazard is the policy-field disk
cache: 22 GB at 1000 VPs (≈ n_vps × ~90 realized country-classes ×
250 KB — a 1000-target sample realizes nearly the full signature space
per VP). It was deleted after the runs. Prune the cache between runs
with different VP sets — keys include VP coordinates, so cross-run
reuse is nil (footprint reduction options are tracked in TODOS.md).

## Floor-matched world: 200 best-placed sources × 100 targets (2026-07-07)

Follow-up to the corrected oracle-floor sweep (source budgets = best-k by
greedy facility location, nested targets; the earlier sweep's rising
floors were an artifact of the coverage-heuristic source choice).  The
comparison harness gained `--source-selection facility` (constructs an
oracle-placed-VP world; reads target truth, so benchmark-construction
only).  Sized per the sweep so the perfect floor ≈ 300 km: at seed 31415,
200 facility-placed sources × 100 targets has floor 245/21 (mean/median
km) — and smart_perfect in the harness lands exactly there (245/21),
validating floor math and world construction against each other.

Results (b=6000 ≈ 56% of 10,679 pairs; cache/geolocator_run_
200src_100dst_facility.pkl):

| strategy | mean / median | note |
|---|---|---|
| smart_perfect | 245 / 21 | the designed floor |
| random+nn | 1066 / 185 | needs near-full coverage to get there |
| **greedy_phased_fiber** | 1105 / 732 | reaches this mean by b≈2500 |
| random+additive_fiber | 1778 / 789 | |
| greedy_phased | 2702 / 1366 | plateaus from b≈1500 |
| random+additive | 2884 / 1555 | |

Reading: in a world with a VP ~20 km from the median target, min-RTT NN
is nearly unbeatable on MEDIANS once coverage is complete — model-based
estimators sit at their residual scale (~5–15 ms ≈ several hundred km)
and cannot localize below it.  The fiber greedy's value is budget
efficiency on means: it reaches random+NN's full-coverage mean with ~40%
of the pings (1254 at b=2500 vs NN's 2299), and it beats the geodesic
greedy everywhere regionally (Africa 9283→1992, East Asia 6106→1516).
Everyone remains ~4× above the oracle floor on means — finding WHICH VP
is the lowest-RTT one cheaply is exactly the selection problem, and
nobody solves it yet at low budget.  Fiber-vs-geodesic conclusions are
unchanged in this world; NN-vs-model medians flip in NN's favor by
construction (well-placed VPs are NN's best case).

## Practicalities discovered

- Merged-mesh structure: 9,764 sources / 2,146 distinct targets;
  campaign sources never overlap the 909 daily-mesh probes.
- Policy-field cost on graph_2026-07-04: ~8 ms per (VP, class) Dijkstra,
  fields 250 KB each. ⚠️ The n=100 run realized 24k fields = **5.4 GB**
  under internet_gmaps/data/cache/policy_fields/ (668 VPs × ~36 classes:
  Nelder-Mead excursions cross country borders, touching many classes
  per VP). Safe to `rm -rf` if space is needed — reruns rebuild lazily;
  keyed by policy NAME, so bump the name on rule changes or you'll read
  stale physics.
- Full n=100 run ≈ 25 min on the laptop (6 workers), fiber greedy
  ≈ 13 min of it.
