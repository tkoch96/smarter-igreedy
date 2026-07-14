# smarter-igreedy — Claude Context

## What this project does

Benchmarks IP geolocation strategies under a ping budget. Given N pings from
RIPE Atlas probes to unknown-location targets, how accurately can you locate
the targets — and which probe-selection strategy spends the budget best?

**Read `SIMULATION_ENVIRONMENT.md` first** — it explains the research problem,
the information boundary (what's allowed during inference), why several
intuitive approaches are considered cheating, and the model ladder (how each
estimation idea motivated the next).

Entry point: `assess_geolocators.py` → `Geolocator_Comparator.run()`.
Latest results: `.claude/FIBER_GEOLOCATOR_RESULTS.md` (fiber-floor
integration + scaling runs); atlas research agenda:
`.claude/HANDOFF_routing_realism.md`; open items: `.claude/TODOS.md`.

---

## Data pipeline

`pull_ripe_atlas_measurement_data.py` — `RipeAtlasPipeline`:
- Downloads hourly `.bz2` ping dumps from `data-store.ripe.net` for a date range
- Parses each dump into a small `*_summary.json` (src_24 → dst_24 → [rtts])
- Filters to a dense bidirectional mesh: prunes nodes until every survivor pings
  ≥80% of the others in both directions (relaxes to 72% if fewer than 500 survive)
- Outputs `{'address_to_loc': {subnet/24 → (lat, lon)}, 'loc_loc_meas': {src → {dst → min_rtt}}}`

Probe metadata (lat/lon) from `pull_ripe_atlas_probe_data.py`.

**RTT value format**: `loc_loc_meas[src][dst]` is a `list[float]`. When loading
from the pickle cache, `assess_geolocators.py` wraps the stored bare float in
`[...]`. Code that calls `min(rtts)` depends on it being a list.

The full dataset is cached at `cache/cached_target_data.pkl`. Cache stores bare
`numpy.float64` values; `assess_geolocators.py` wraps them on load.

**Note**: `load_parsed_target_data` has an early-exit `if fni == 10: break`
limiting it to the first 10 hourly files. The full day has 24.

---

## FeasibleRegion (`feasible_region_maintainer.py`)

Tracks the estimated location of a target given RTT measurements from VPs.
Both modes share one predictive RTT model — `expected rtt = slope × d / 100`
with `slope = DEFAULT_SLOPE = 1.3` (realistic fiber overhead; slope 1.0 =
pure SOL never happens in practice) — and differ in how they treat
deviations from it:

### `mode='hard_circle'` (default)

Each RTT becomes a maximum-radius circle at the model-implied distance
(`rtt × 100 / slope`) × `radius_multiplier` (safety slack, default 1.05).
Nelder-Mead minimises a penalty that fires when the estimate falls outside
any circle. A measurement faster than the slope allows makes the truth
infeasible — hard models trade informativeness against validity.

**Known issue**: loss landscape is nearly flat inside all circles — Nelder-Mead
barely moves from its starting point. Multiplier 1.3 is too loose; 1.05 is
tighter but still flatter than Gaussian.

Region size = largest displacement from the estimate that satisfies every
constraint, found by probing 8 bearings (geometric ladder + bisection);
single constraint returns the circle radius exactly. (The old "tightest
slack" proxy measured distance to one boundary and badly underestimated
uncertainty near the edge of a large feasible lens.)

### `mode='gaussian'`

Each RTT contributes a term to the negative log-posterior:
```
NLL(x) = Σ_v  (rtt_v - slope × d(x, v) / 100)² / (2 σ_v²)
```
Nelder-Mead minimises NLL. Always has gradient (proper bowl). σ defaults to
`GLOBAL_SIGMA_MS = 15ms` (fixed global constant — no per-VP calibration).
Slope-beating measurements are unlikely, not impossible — no validity cliff.

**Important**: per-VP sigma estimation from the mesh would require knowing
VP-to-VP distances, which is disallowed (see `SIMULATION_ENVIRONMENT.md`).
The honest baseline uses global σ only.

### `mode='em_gaussian'`

Gaussian, but the per-target slope μ and noise σ are UNKNOWN and fitted
online (honestly — from residuals against the target's own estimated
location, no ground truth). Each measurement update alternates:

- **E-step**: MAP location via Nelder-Mead under the current μ
- **M-step**: closed-form least-squares refit of μ (through the origin)
  and σ from the residuals, shrunk toward priors with pseudo-counts
  (`EM_MU_PRIOR_STRENGTH = 3`) so the cold start (1–2 pings can't identify
  μ jointly with location) stays well-posed; μ clamped to [1.0, 2.0].

`region.slope` holds the current μ; `region.fitted_sigma_ms` the current σ.
A full refit per update is used instead of an EWMA because a target sees at
most tens of pings — no forgetting needed.

### `mode='additive'`

The two-way model `rtt = d/100 + X_src + X_dst` with per-node (μ, σ²) held
in a SHARED `probabilistic_helpers.AdditiveLatencyModel` (constructor param
`model=`) — X_src pools across all targets, so the parameters cannot live in
a per-target region. The region stores `(vp_loc, src_id, rtt)` constraints
(`add_measurement(..., src=)`), consults the model for offsets and
per-measurement variance (MAP weight `1/(σ̂_s² + σ̂_t²)`), and its
`get_region_size()` is precision-aware: weighted-rms residual + statistical
floor `1/sqrt(Σw)`, in km. The model owner refits (`model.refit(vp_locs,
estimates)` — fresh, never warm-started) and calls `region.reoptimize()`.
Location MAP multi-starts from [previous estimate, NN anchor]
(`probabilistic_helpers.additive_map_location`). Single-VP constraint sets
(replicated samples) anchor at the VP itself — optimising them parks the
estimate on an arbitrary ring point whose zero residuals rob μ̂_t of its
offset (the params-first pitfall through the back door).

### `noise_model` toggle (soft modes)

The per-residual likelihood shape is a constructor toggle on gaussian and
em_gaussian modes (`noise_model=`, from `probabilistic_helpers`):

- `GAUSSIAN_NOISE` (default) — r²/2σ²; thin-tailed, dragged by detours.
- `STUDENT_T_NOISE` — heavy-tailed (ν=3), saturating loss: outlier-robust,
  essentially free on clean data.
- `ASYMMETRIC_NOISE` — steep quadratic below the model (SOL is a hard
  floor), linear Laplace tail above (detours are common): matches
  one-sided RTT overhead dynamics.

Calibrated (single 10× detour among 5 pings): gaussian err ≈ 1524km,
student_t ≈ 356km, asymmetric ≈ 0km. Under 20% Exp(40ms) detour
contamination (e2e): gaussian ≈ 925, student_t ≈ 420, asymmetric ≈ 213 km;
clean-world cost: student_t ~0%, asymmetric ~16%. In em_gaussian mode the
M-step switches to robust fits (median-of-ratios slope, MAD scale) for the
non-gaussian models. ⚠️ Known interaction: under heavy contamination,
EM's μ-fit absorbs some detour bias — robust noise + fixed slope currently
beats robust noise + EM there.

Calibrated result (per-target μ ~ U(1.01,1.4), σ ~ U(1,6)ms, 80 seeds, full
budget, `test_e2e_adaptive_em.py`): random=281, sol(slope 1.0)=446,
const gaussian=186, **em=133**, oracle(true μ,σ)=147 km. Two notable
findings pinned by tests: misspecified slope-1.0 triangulation is WORSE
than nearest-neighbour, and EM can beat the parameter-oracle in-sample
(fitting the realised noise beats knowing the true μ).

### API

```python
region = FeasibleRegion(target_id, mode='gaussian')
region.add_measurement(vp_loc, rtt_ms, sigma_ms=GLOBAL_SIGMA_MS)
region.get_location()      # → (lat, lon), always canonical (on-globe)
region.get_region_size()   # uncertainty proxy, km in BOTH modes
region.clone()             # fast isolated copy
```

Invariants (pinned by `TestRegionSizeUnits`, `TestGeolocationImpossibility`,
`TestLatLonNormalization`):

- `get_region_size()` returns km in both modes (gaussian = mean residual ×
  100 / slope); empty region = 20037.0 sentinel.
- **Trilateration floor**: with <3 constraints the size never drops below
  the best ping's model-implied distance (min rtt × 100 / slope km) —
  1 ping is a ring,
  2 pings are two mirror points, regardless of fit quality.
- Estimates are wrapped to canonical lat/lon after every optimisation
  (Nelder-Mead otherwise wanders off-globe, which breaks probing and can
  crash `fast_haversine`).

`add_measurements_batch([(vp_loc, rtt), ...])` re-optimises once at the end.

---

## Geolocators

| Name | File | What it does |
|---|---|---|
| `random` | `random_geolocator.py` | Shuffles all (VP, target) pairs; returns first `budget` |
| `smart_perfect` | `perfect_geolocator.py` | Oracle: has ground truth; greedily picks the ping that minimises actual error |
| `iterative_greedy` | `iterative_greedy_geolocator.py` | Main algorithm: estimates expected FeasibleRegion reduction per candidate ping |

Each exposes: `set_data(data)`, `solve()`, `measurements(budget) → MeasData`.

**`Random_Geolocator` does no estimation** — it only shuffles measurement
ordering. Estimation is done by `Geolocator_Comparator.convert_measurements_to_locations()`.

---

## Evaluation loop (`Geolocator_Comparator`)

Two-phase design (train/test-style split — see "The two phases" in
`SIMULATION_ENVIRONMENT.md`): selection runs under realistic information
limits; evaluation scores against ground truth, which is never fed back.
Each strategy is a complete system of selection + estimation — baselines
intentionally use dumb nearest-neighbor estimation, while the greedy
supplies its own overlap-based estimates via `get_current_estimates()`
(bypassing the converter entirely).

Sweeps budget from 100 to 2500 in steps of 100 (`run()` also takes
`n_subsample=` — the probe-count knob, threaded to `get_random_subsample`).
At each budget, estimates locations and computes great-circle error vs known
probe locations. Missing estimates incur a 10,000 km penalty.

The default `self.geolocators` list compares three greedy variants
(gaussian slope 1.3 / 1.05 / em_gaussian, distinguishable via the greedy's
`name=` param) against `Perfect_Geolocator` and `Random_Geolocator`.
Result caches include the subsample size in the filename
(`cached_results_<name>_<mode>_n<N>.pkl`) — results from different
subsample sizes are not interchangeable (a stale-cache bug once mixed
20-probe baselines into a 100-probe figure).

Real-mesh scaling result (2026-07-04, means at b=2500, seed 31415;
figures/geolocator_results_n{200,300}.pdf): greedy_additive_risk breaks
the greedy plateau at scale — n=200: 2,265 vs greedy_em/simulate's
~3,240 and beats random+NN at every budget until b≈2,100 (2.5× budget
efficiency: risk@800 ≈ random@2000); n=300: 2,398, ahead of random
(2,628) even at full budget (random is coverage-starved at 8.3
pings/target) with an 800-1,200 km lead through the mid-range;
greedy_em closes to 2,413 only at the very end. The mesh floor
(smart_perfect) tightens 615 → 424 → 382 across n=100/200/300.
The phased selection (promise-collapse → random exploration) closes
risk_gain's flat segment on the real mesh (2026-07-04 reruns, new
promise-based switch): n=100 phased 2,066 vs risk's 3,312; n=200 phased
2,100 vs risk 2,318 AND vs random+NN's 2,184 — the greedy family's
first full-budget win at that scale. Random still wins n=100 full
budget (1,249; 25 pings/target is exhaustion's home regime); the
crossover to greedy dominance happens by n=200 (12.5 pings/target).
Cross-run jitter on greedy curves is ~5-10% (as_completed ties).

Real-mesh findings at n=100 (means; medians are far kinder — isolated
targets dominate means, and run() does not report medians yet):
greedy_em drops to ~2,650 by b=300 and plateaus ~2,800; greedy_additive
(2026-07-03 run) starts best of all honest strategies (3,942 at b=100 vs
random+NN's 7,185), dips to 2,731 at b=600 and plateaus ~2,750-2,990 —
PARITY with greedy_em on means, the plateau is not broken. Suspected
causes: the gaussian additive M-step feels real one-sided detours (same
reason em_asymmetric wins means in the estimator-only comparison), and
means bury the additive model's median advantage. Fixed-slope greedys
DIVERGE (4,161→4,778 and 4,426→5,433); random+NN grinds to 1,620 at
b=2,000 and wins means from ~b=900 on; smart_perfect (scored through the
NN converter) sits at ~615.

Fiber-floor result (2026-07-06, assess_fiber_real.py, merged mesh n=100 =
58 dense + 42 campaign, seed 31415, b=2500): swapping the geodesic d/100
base term for the internet_gmaps policy floor (v3.2 × 1.3 — the then-
current DEFAULT_POLICY; v3.5 since 2026-07-09, RttModel
injection) breaks the greedy plateau — greedy_phased_fiber 1532/748
(mean/median km) vs geodesic greedy_phased 1933/1246, and the fiber
additive estimator on RANDOM pings (1695/785) beats the geodesic greedy.
Dense targets: 871/599 vs 1607/970. Ridge failures collapse (Guadeloupe
9689→713, Japan 11937→3005); losses concentrate where the atlas floor is
loose (NZ/MY/SG endpoint trombone) — slack, not structure. Replicates at
10× scale (2026-07-07, 1000×1000, b=15000): fiber greedy 1675/800 vs
geodesic greedy 2086/1142, oracle 1523/612; at 200 sources × 1000 targets
coverage binds (oracle only ~8% better than NN) and fiber ties NN.
⚠️ fiber runs grow internet_gmaps/data/cache/policy_fields/ at ~250KB ×
n_vps × ~90 classes (22GB at 1000 VPs) — prune between VP sets. Full
report: `.claude/FIBER_GEOLOCATOR_RESULTS.md`.

`get_random_subsample(n=100)` **mutates `target_data` in place** — subsequent
re-runs operate on already-pruned data.

### `measurement_converter_mode` options

| Mode | What it does |
|---|---|
| `'nearest_neighbor'` | Estimate = location of lowest-RTT VP seen |
| `'hard_circle'` | FeasibleRegion(mode='hard_circle') on all pings |
| `'gaussian'` | FeasibleRegion(mode='gaussian', σ=GLOBAL_SIGMA_MS) on all pings |
| `'em_gaussian'` | FeasibleRegion(mode='em_gaussian'), batch-added |
| `'em_asymmetric'` | em_gaussian + one-sided asymmetric noise model |
| `'additive_em'` | cross-target two-way fit (`_convert_additive_em`): params-first alternation, fresh NN-anchored inits per call |

Default is `'nearest_neighbor'`. The `random` geolocator baseline always uses
this mode. `assess_additive_real.py` compares the estimator modes on
identical random-ordered measurements (mean AND median per budget; medians
matter — isolated probes' ~10,000 km errors dominate means).

Real-mesh estimator findings (seed 31415, random order, full budget):
at n=20 additive_em (mean 1927 / median 575) is the first model-based
estimator to beat NN (2496 / 604) on both stats; em_asymmetric 1894 / 791.
At n=100 / budget 2500, NN stays ahead overall (1249 / 467, dense-coverage
regime) but additive_em is the best model-based median at every budget ≥
1000 (933 vs em_asymmetric's 1091 at 2500) and breaks the per-target-em
floor (2081 vs 3247 mean). Cached: `cache/additive_real_results_n{20,100}.pkl`.

---

## Probabilistic helpers (`probabilistic_helpers.py`)

Pure functions, no dependency on FeasibleRegion:

- `gaussian_nll(point, constraints)` — NLL for MAP estimation
- `mean_absolute_residual(point, constraints)` — uncertainty proxy
- `compute_per_vp_mu(target_data)` — ⚠️ uses VP distances, cheating per simulation rules
- `compute_per_vp_sigma(target_data)` — ⚠️ uses VP distances, cheating per simulation rules
- `haversine_grid(vp_lat, vp_lon, lats, lons)` — vectorised distance to grid
- `posterior_mean_grid(constraints)` — Path 2: grid integration (alternative to MAP)

The `compute_per_vp_*` functions exist for analysis (they are used in
`assess_probabilistic.py` and `analyze_latency_distance.py`) but must NOT be
used inside inference — doing so requires VP geographic distances.

---

## Information boundary enforcement

`utils.py` exports `LockedLocationDict` and `simulation_mode()`.

`LockedLocationDict` raises `ValueError` on any key access while locked.
Wrap all ground-truth location dicts in this type; lock during inference;
unlock for assessment.

```python
with simulation_mode(ALL_LOCS):
    estimate = run_inference(rtts, vp_locs)   # vp_locs pre-resolved, ALL_LOCS locked

error = get_distance(estimate, ALL_LOCS['_target'])  # fine: lock released
```

---

## Tests

### `tests/test_probabilistic_helpers.py` (29 tests)

Unit tests for pure functions in `probabilistic_helpers.py`. No FeasibleRegion.
Covers: NLL arithmetic, mean absolute residual, per-VP sigma estimation,
vectorised haversine, grid posterior. Completes in ~1 second.

### `tests/test_e2e_probabilistic.py` (12 tests + figure)

**Integration test** — uses the real pipeline (`Geolocator_Comparator`,
`Random_Geolocator`) with synthetic data. Synthetic data is generated in the
real `{'address_to_loc': ..., 'loc_loc_meas': ...}` format.

What the tests guarantee (80 seeds, 10 VPs, Prague target, correctly-specified
model `rtt = DEFAULT_SLOPE × d/100 + N(0, σ_vp²)`, same slope in ground
truth and estimators):

| Claim | Test |
|---|---|
| Gaussian produces a finite, in-bounds estimate | `test_estimate_finite_and_in_bounds` |
| Gaussian median error < 250km | `test_median_error_under_250km` |
| Gaussian p90 error < 600km | `test_p90_under_600km` |
| Gaussian median beats nearest-neighbour (random) | `TestTriangulationBeatsNearestNeighbour` |
| Hard-circle median beats nearest-neighbour (random) | `TestTriangulationBeatsNearestNeighbour` |
| Hard-circle is >1.3× worse than oracle (calibration gap) | `test_hard_circle_notably_worse_than_oracle` |
| Gaussian mean beats random | `TestGaussianBeatsRandom` |
| Gaussian wins majority of seeds vs random | `TestGaussianBeatsRandom` |
| Oracle median beats Gaussian | `TestOracleBetterThanGaussian` |
| Oracle wins majority of seeds vs Gaussian | `TestOracleBetterThanGaussian` |
| Full ranking: oracle ≤ {gaussian, hard_circle} ≤ random | `TestFullRanking` |

**Location locking** is enforced throughout: `ALL_LOCS` is a
`LockedLocationDict`. VP locations are resolved outside the simulation block
and passed explicitly. The target location is only accessed after the lock
is released. Any accidental location lookup during inference raises `ValueError`.

The figure test (`TestGenerateFigure`) runs a 200-seed simulation and writes
`tests/error_over_measurements.pdf`. This runs automatically with pytest.

**Note on Gaussian vs hard-circle**: without per-VP sigma calibration,
Gaussian and hard-circle are roughly comparable (hard-circle wins by ~10km
median on this synthetic problem). The Gaussian advantage emerges with
calibration — which requires knowing VP distances (cheating). The oracle
quantifies this gap.

### Running tests

```bash
cd ~/Documents/smarter-igreedy
source ~/Documents/venv312/bin/activate
python3 -m pytest tests/ -v
```

---

## Iterative Greedy — key mechanics

- `region_mode` constructor param (`HARD_CIRCLE` default, `GAUSSIAN`,
  `EM_GAUSSIAN`, or `ADDITIVE`) selects the overlap methodology for the
  greedy's own regions — both the selection utility and its reported
  estimates. `get_region_size()` returns km-equivalents in all modes, so
  `BASICALLY_GEOLOCATED = 200` km applies uniformly.
- `ADDITIVE` mode: one shared `AdditiveLatencyModel` across all regions,
  refit from all accumulated measurements after every actual ping
  (`model_refit_every=`). The pinged region's location update is DEFERRED
  until after the refit (`add_measurement(..., update_estimate=False)` +
  `reoptimize()`) — params-first, or the location absorbs the pair's
  offset and μ̂_t collapses. Incremental updates drive SELECTION only;
  the estimates handed out get a final `additive_batch_em` polish at the
  end of `measurements()` (incremental location steps ratchet offsets
  into distance over a run; the fresh NN-anchored batch alternation
  recovers them — measured: patho μ̂_t 16 vs true 35 before polish).
  `additive_utility_evaluator` predicts RTT via the model and discounts
  the simulated km gain by `prior_var/(prior_var + σ̂_t²)` — without the
  discount a pathological target OUTBIDS finished ones (its statistical
  floor promises big absolute reductions). Measured effect: patho ping
  share median 0.29 / max 0.375 (fair 0.25) vs em-greedy's 0.33-0.50 on
  identical scenarios.
- `selection='info_gain'` (ADDITIVE only): exploration-aware utility over
  a per-region hypothesis SUPPORT SET (MAP + NN anchor + rings around the
  best VP, scored by PROFILED NLL — offset marginalised out, clamped ≥ 0;
  fixed offsets would wrongly reject the near end of a ridge; tolerance
  is misfit-scaled or the support degenerates to the MAP as constraints
  accumulate). Utility = MEAN per-hypothesis partition benefit (km): how
  much of the support spread a candidate's reading would eliminate.
  Fixes the measured geometry-blindness of the simulate utility (real
  mesh: ~90 candidates within 0.35%, the ridge-collapsing 7 km VP ranked
  #9, never pinged). Regression test: TestRidgeEscape — lone VP found as
  the far target's 2nd ping 9-10/10 seeds (err ~464 km vs simulate's
  3068, 0/10). ⚠️ On the real mesh, mean-benefit selection SINKS budget
  into uncuttable ridges (50-88 pings; median target starved to 1-2) —
  its promises are large under a lucky hypothesis and never pay out.
- `selection='risk_gain'`: the fix for that sink — same benefits, scored
  at their 25th PERCENTILE ("gain I can count on in most worlds") ×
  the target's `gain_reliability` (EWMA of realized/promised spread
  reduction — model-free track record a bad fit cannot fake). Prefers a
  500±100 km benefit to a 2000±5000 km one. TestUncuttableRidge: declines
  the unpayable target at exactly 2 pings in 10/10 seeds (info_gain: 6)
  while still finding the lone VP when it exists (9/10). Bounded premium
  in the gaussian synthetic (patho promises DO pay there): ≤1.25× pinned.
  Both selections: parity at full coverage (shared batch polish), cheaper
  than simulate (no per-candidate NM).
- `BASICALLY_GEOLOCATED` (200km region size) DEPRIORITISES a target rather
  than dropping it: done targets rank below every unfinished one, and
  leftover budget flows to the least-certain done target via its nearest
  VP. `measurements()` returns early only when every (VP, target) pair is
  exhausted.
- `AdaptiveRTTModel`: predicts RTT as `distance × DEFAULT_SLOPE / 100ms`, with per-target
  EMA correction (α=0.3)
- `default_utility_evaluator`: simulates adding a candidate constraint to a
  cloned `FeasibleRegion`, measures area reduction. Targets within 200km
  (`BASICALLY_GEOLOCATED`) return −1,000,000 utility.
- Parallelises with `ProcessPoolExecutor`. Very slow at 900×900 VPs on laptop.
- "Focus batch" trick: re-sorts targets by cached utility every 50 pings.
- `solve()` must call `_update_best_vp_for_target(dst)` for every target to
  seed `best_vp_cache` — without this, `measurements()` infinite-loops.

**Current status**: greedy_phased beats random+NN on means from n=200
scale up (see "Real-mesh scaling result" above); with the fiber-floor
base model it is the best honest strategy at every tested shape
(`.claude/FIBER_GEOLOCATOR_RESULTS.md`). Random+NN keeps the median
lead wherever a VP sits close to the median target (its home regime).

---

## Type aliases

```python
LatLon     = tuple[float, float]               # (latitude, longitude)
MeasData   = dict[str, dict[str, list[float]]] # src → dst → [rtt_ms, ...]
TargetData = dict[str, Any]                    # 'address_to_loc' + 'loc_loc_meas'
```

---

## File map

```
assess_geolocators.py              entry point / comparator harness.  Fully
                                   parametrized — argparse CLI (--help), --config
                                   JSON (see configs/), GEOLOC_* env vars (legacy);
                                   precedence CLI > config > env > defaults; no
                                   settings = the historical default run.  Covers:
                                   data source legacy|merged, independent
                                   n_sources/n_targets (lazy-greedy coverage source
                                   selection for the asymmetric merged mesh),
                                   per-target VP cap, fiber-floor variants
                                   (--fiber), budget grids, fig-name/tag, per-target
                                   error recording + per-region breakdown.  All
                                   artifacts named by shape <srcs>src_<dsts>dst
                                   (figures/geolocator_results_<shape>.pdf,
                                   cache/geolocator_run_<shape>.pkl).  Estimation
                                   variants are per-instance settings
                                   (Random_Geolocator converter_mode= / rtt_model=
                                   / order_seed=), NOT separate harnesses.  README
                                   "Running experiments" documents the settings
                                   matrix.
assess_additive_real.py            real-mesh estimator comparison at matched
                                   measurements (NN / em / em_asym / additive)
assess_probabilistic.py            real-data Gaussian vs hard-circle sweep (analysis only)
analyze_latency_distance.py        offline RTT vs distance model fitting (analysis only)
feasible_region_maintainer.py      FeasibleRegion: hard_circle + gaussian modes
probabilistic_helpers.py           pure NLL/sigma/grid helpers (no FeasibleRegion dep);
                                   also RttModel / GeodesicRtt / FiberFloorRtt — the
                                   injectable base-RTT term (replaces d/100; every call
                                   site keeps rtt_model=None = old behavior bit-for-bit)
iterative_greedy_geolocator.py     main algorithm
perfect_geolocator.py              oracle baseline (has ground truth)
random_geolocator.py               random baseline (shuffles measurement order only)
pull_ripe_atlas_measurement_data.py  hourly ping data pipeline
pull_ripe_atlas_probe_data.py        probe metadata (lat/lon)
utils.py                           haversine, LatLon, LockedLocationDict, simulation_mode
plot_results.py                    matplotlib output functions

tests/test_probabilistic_helpers.py   unit tests for pure functions (29 tests, ~1s)
tests/test_e2e_probabilistic.py       integration test using real pipeline (12 tests + figure)
tests/plot_error_over_measurements.py figure generator (called by integration test)
tests/plot_gaussian_vs_hard_circle.py 3-panel map: hard-circle lenses vs gaussian
                                      posterior (called by TestGaussianVsHardCircle;
                                      writes tests/gaussian_vs_hard_circle.pdf)
tests/test_e2e_additive_em.py         additive two-way model rtt = SOL + X_src + X_dst,
                                      ONE sample per pair (a practical ping is already
                                      min-of-3 against queueing; X_src/X_dst model path
                                      inefficiency that replication can't average out):
                                      σ̂_dst flags pathological destinations 100%; best
                                      model-based estimator (917 vs 1647 mean batch;
                                      722 vs 1655/1686 in the sweep) though NN keeps
                                      the full-coverage lead in this small synthetic
                                      (646 mean) — the real n=20 mesh flips that.
                                      Greedy sweep: selection dominates early (1932 vs
                                      3475+ at b=10), matches additive_em at full
                                      coverage, patho ping share ≤ 0.375 (fair 0.25).
                                      Oracle = Perfect_Geolocator selection + true
                                      (μ, σ), pinned to dominate at every budget.
                                      FIBER TOGGLE: the sweep helpers take rtt_model=
                                      (None = geodesic d/100, bit-for-bit) and scenarios
                                      carry vp_locs + their truth base; TestFiberToggleSweep
                                      reruns the sweep on a toy-C-cable-truth world with
                                      both bases and pins fiber-base additive_em < 0.5×
                                      its geodesic twin (measured: 441 vs 1560 km at
                                      full coverage)
tests/plot_error_additive.py          error-vs-budget curves under the additive world
                                      (writes tests/error_over_measurements_additive.pdf);
                                      `--fiber` / make_fiber_figure: the fiber toggle —
                                      same estimators on a toy-fiber-truth world, geodesic
                                      base (dashed) vs injected fiber base (solid)
                                      (writes tests/error_over_measurements_additive_fiber.pdf)
tests/test_e2e_adaptive_em.py         online-EM e2e (single-target estimator comparison
                                      + noise models under contamination) AND the
                                      multi-target budget-allocation comparison:
                                      random+NN vs greedy(hard/gaussian/em) vs oracle
                                      over a shared total budget, avg error across
                                      5 random targets (the project objective)
tests/plot_error_adaptive_em.py       error-vs-total-budget curves for the multi-target
                                      comparison, with greedy stop markers
                                      (writes tests/error_over_measurements_adaptive.pdf)
tests/plot_em_edge_vs_mismatch.py     em/gaussian error ratio vs μ-range mismatch,
                                      with σ-only variants (writes
                                      tests/em_edge_vs_mismatch.pdf)
tests/plot_region_convergence.py      filmstrip: 1:1 spatial companion to the curves —
                                      5 targets per cell, estimates + each region's own
                                      uncertainty circle, per strategy over budget
                                      (writes tests/region_convergence.pdf)

configs/                           ready-made experiment configs for
                                   assess_geolocators --config (fiber n=100,
                                   200x1000, 1000x1000)

internet_gmaps/                    the fiber atlas: infrastructure graph
                                   (fiber_graph.py + build_graph.py from
                                   TeleGeography/ITU snapshots), exact floor
                                   queries (floor_query.py: FloorEstimator +
                                   PolicyFloorEstimator — policy-aware floors
                                   for arbitrary points, lazy per-(VP, class)
                                   fields, disk cache under data/cache/),
                                   geopolitical transit policy
                                   (transit_policy.py + TRANSIT_POLICY.md),
                                   merged mesh loading (mesh_data.py), the
                                   live RIPE measurement campaign
                                   (mesh_campaign/), and its own test suite.
                                   Its modules expect internet_gmaps/ on
                                   sys.path; see internet_gmaps/README.md.

SIMULATION_ENVIRONMENT.md         ← read this to understand what's allowed during inference
.claude/FIBER_GEOLOCATOR_RESULTS.md fiber integration + scaling results/verdict
.claude/HANDOFF_routing_realism.md  atlas research agenda (routing realism)
.claude/TODOS.md                   ordered fix list

cache/cached_target_data.pkl       full mesh (~909 nodes)
cache/cached_results_*.pkl         per-geolocator sweep results
figures/                           output PDFs
```
