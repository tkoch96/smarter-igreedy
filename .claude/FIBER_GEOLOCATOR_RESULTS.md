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
   VP-subset queries (`floor_ms_subset`) for the geolocation hot path.
   No-route semantics (since 2026-07-09 / policy v3.3): `floor_ms`
   raises NoRouteError (KeyError) where the policy strands an
   open-routable pair; `no_route="open"` restores the OPEN-floor
   fallback used by the n=100 experiment below (never bare geodesic).
   Unit-tested for exact equality vs policy_floor_matrix, bit-exact
   OPEN_POLICY equivalence with FloorEstimator, NoRouteError contract +
   per-VP inf-fallback, terrestrial-only bans, disk-cache roundtrip,
   LRU-eviction exactness (tests/test_policy_floor_estimator.py).

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

## 2026-07-10 dense collapse: root cause + resolution (2026-07-11)

Fiber "stopped working" on every world sampled from 2026-07-09 on
(dense targets ~9,300 km mean AND median). **The fiber model never
regressed — the sampled worlds did.** The asymmetric coverage-greedy in
`get_random_subsample` broke its equal-gain ties lexicographically by
IP string, so the ~coverage_depth(10) sources serving ALL dense targets
were always the lowest-address daily probes — AFRINIC 102.x / APNIC
103.x, an Africa/Asia cluster. Which probes escaped depended on the
daily target draw (the campaign DB reseeds the eligible pool): Jul 6-7
drew spread sets (fiber won), Jul 10 drew the cluster (dense
nearest-VP median 4,805 km) and EVERY strategy collapsed on dense
targets — random+NN 7,292 km median, the ground-truth oracle 7,291.
The fiber greedy amplified the bad geometry worst (×1.3 floor
over-predicts 47% of >5,000 km dense constraints; clamped-≥0 offsets
push the excess into position), which masqueraded as a fiber
regression. Both prior investigations were right: 24-41% "violations"
measured the ×1.3 SLOPED base (raw floor: 2-3%); 1.5×geodesic was the
>5,000 km dense mix (<1,500 km pairs: 1.01-1.04); working-tree
V32_POLICY floors are bit-identical to the winning-era default.

Fixes (assess_geolocators.py): seeded jitter tie-break in the
coverage greedy; VP-coordinate hash in the fiber estimator token +
sidecar (stale-registry footgun). Tests:
tests/test_fiber_beats_geo_real.py (hermetic sampler-geography
regression; registry freshness; GEOLOC_E2E_REAL=1 e2e on pinned
snapshot cache/world_100src_300dst_fibergeo.pkl — fiber 2350/1189 vs
geodesic 2490/1318 mean/median km at b=2500).

⚠️ Worlds snapshotted 2026-07-09..07-11 (sizecheck / san300 /
300×2209 / 200×999) carry the degenerate geometry — resample; do not
use for fiber-vs-geodesic conclusions.

Full-scale rerun (identical settings, resampled world,
cache/geolocator_run_300src_2204dst_resampled.pkl; mean/median km at
b=32000, broken → resampled):

| strategy | broken world | resampled |
|---|---|---|
| random+NN | 3773/2364 | 2310/987 |
| smart_perfect | 3566/2095 | 2161/869 |
| greedy_phased_geo | 2718/1915 | 2281/1200 |
| greedy_phased_fiber | 5187/2836 | **2235/1075** |

## Plateau mechanism + uncertainty repair (2026-07-11)

Why every strategy flattens (probe audits: at 30% budget, 10% of
remaining candidate pings are worth >1,000 km and the auction scores
90% of them <1 km, Spearman ≈ 0):

1. Degenerate VP geometry (10 shared VPs per dense target) creates
   offset-position likelihood ridges (not capacity overfitting: ~3
   params vs ~10 measurements).
2. The additive fit launders position error into μ̂_dst (err>5,000 km
   targets: median μ̂_dst 27 ms vs 0.5 ms for err<500 km;
   corr(err, μ̂)=0.48) → residuals quiet → promises 0 → phased greedy
   devolves to random exploration by ~25% of budget.
3. The batch polish OVERWROTE rescues (params-first alternation judges
   all starts under the incumbent basin's offsets; measured 2,977 km
   ping-time → 13,827 polished).

Measured outcomes (100×300 healthy world, fiber arm, b=2500):

- **RECOMMENDED for real-mesh runs: GEOLOC_HYP_OUTER_RINGS=1
  GEOLOC_POLISH_LIVE_STARTS=1** — hypothesis rings at the zero-offset
  physics bound + live-estimate polish starts with penalized-NLL basin
  arbitration. 3 runs 2134-2251/1137-1196 vs baselines 2394/1206,
  2350/1189 (~7% mean); rescued-then-relost 12 → 4-7. Off by default:
  honest synthetic worlds re-inflate pathological promises (4 pinned
  sweep behaviors move).
- Negative results (all measured, don't re-try blind): stronger L2
  (GEOLOC_PRIOR_STRENGTH 10/30) shrinks laundered weights, rescues 0
  targets, hurts medians; size-weighted exploration
  (explore_bias='size') leaves the stranded count unchanged; fiber
  prior_mu_ms=0 ("atlas needs no correction") 2457/1215 at S=2,
  2745/1742 at S=15 — the ×1.3 floor is a bound, not a mean; real
  paths carry ~5 ms genuine overhead above it. Default prior stays
  5 ms (knobs: GEOLOC_PRIOR_MU_MS / GEOLOC_PRIOR_STRENGTH /
  GEOLOC_MU_FLOOR_MS; per-model prior via FiberFloorRtt(prior_mu_ms=)).
- Residual stranded targets are likelihood-limited (wrong basin
  genuinely fits better under conical VP geometry) — only
  bearing-diverse probes fix them (see TODOS).

Oracle (smart_perfect) fixes: candidate simulation now uses the same
radius it commits (was rtt×100 vs rtt×100/1.3×1.05); scoring converter
is settable (--oracle-converter / GEOLOC_ORACLE_CONVERTER). Measured
shoot-out under oracle selection (100×300, b=2500): NN 2382/1208 <
additive_em 2604/1518 < hard_circle 2906/1540 (degrades with budget —
validity cliff) < em_gaussian 4222/3036 < gaussian 4619/3586. NN stays
the default.

## Graph-node search (map-matching) — default fiber location step (2026-07-11)

The additive MAP for fiber models now scores EVERY atlas graph node in
one vectorized pass (per-VP open-field rows), seeds Nelder-Mead from
the best node, rescored top nodes compete under the full policy base,
and off-infrastructure winners (the geodesic-fallback zone — where the
mid-ocean estimates lived) are rejected.  Default on;
GEOLOC_NODE_SEARCH=0 restores free search; test mocks lack node fields
so plumbing pins stay bit-exact.  Measured on the pinned resampled
300×2204 world, greedy_phased_fiber, identical settings
(cache/geolocator_run_300src_2204dst_resampled_nodesearch.pkl):

- final b=32000: 2078/1022 vs 2235/1075 km (mean/median), stranded
  >5000 km 254 vs 276; dense 2126/979 vs 2395/1037.
- the win is BUDGET EFFICIENCY: new curve dominates at every budget,
  mid-range medians −25..−33% (b=2617: 3243 vs 4858; b=7962: 1724 vs
  2331); reaches the old estimator's final median with ~25% fewer
  pings.
- paired ledger: 803 helped / 898 hurt >100 km, net +157 km mean —
  node granularity slightly perturbs many well-located targets while
  hemisphere-rescuing the stranded tail.
- figure: figures/node_search_before_after.pdf (case-study map).

Model grid (2026-07-11, 42 arms × 2 200×500 worlds, estimation-only,
unbiased priors, node-search positions; scripts in session scratch):
winning structure = 1.3×raw-floor + δ_src + δ_dst (≥0, prior 0, S=2)
with HUBER loss — 2792/993, stranded 87 vs NN's 2716/936/104.
Marginals: adders −1041 km, fixed-1.3 multiplier −465, huber −50
(−107 on top arm), learned μ vs fixed 1.3 +6 (don't learn it), free
sign +9 (clamp costs nothing), stiffer priors monotonically worse
(S=32: +658 mean).  Production delta implied: Huber loss in the
additive fit/MAP (not yet implemented).

Fiber toggle for the synthetic additive sweep:
tests/test_e2e_additive_em.py helpers take rtt_model= (None = bit-
identical geodesic); TestFiberToggleSweep pins fiber-base additive_em
441 km vs geodesic-base 1560 on toy-C-cable truth; figure
tests/error_over_measurements_additive_fiber.pdf
(plot_error_additive.py --fiber).
