# smarter-igreedy — Claude Context

## What this project does

Benchmarks IP geolocation strategies under a ping budget. Given N pings from
RIPE Atlas probes to unknown-location targets, how accurately can you locate
the targets — and which probe-selection strategy spends the budget best?

**Read `SIMULATION_ENVIRONMENT.md` first** — it explains the research problem,
the information boundary (what's allowed during inference), and why several
intuitive approaches are considered cheating.

Entry point: `assess_geolocators.py` → `Geolocator_Comparator.run()`.

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
Supports two modes:

### `mode='hard_circle'` (default)

Each RTT becomes a maximum-radius circle: target must be within
`rtt × 100km × radius_multiplier` of the VP. Nelder-Mead minimises a penalty
that fires when the estimate falls outside any circle.

**Known issue**: loss landscape is nearly flat inside all circles — Nelder-Mead
barely moves from its starting point. Multiplier 1.3 is too loose; 1.05 is
tighter but still flatter than Gaussian.

### `mode='gaussian'`

Each RTT contributes a term to the negative log-posterior:
```
NLL(x) = Σ_v  (rtt_v - d(x, v) / 100)² / (2 σ_v²)
```
Nelder-Mead minimises NLL. Always has gradient (proper bowl). σ defaults to
`GLOBAL_SIGMA_MS = 15ms` (fixed global constant — no per-VP calibration).

**Important**: per-VP sigma estimation from the mesh would require knowing
VP-to-VP distances, which is disallowed (see `SIMULATION_ENVIRONMENT.md`).
The honest baseline uses global σ only.

### API

```python
region = FeasibleRegion(target_id, mode='gaussian')
region.add_measurement(vp_loc, rtt_ms, sigma_ms=GLOBAL_SIGMA_MS)
region.get_location()      # → (lat, lon)
region.get_region_size()   # uncertainty proxy
region.clone()             # fast isolated copy
```

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

Sweeps budget from 100 to 2500 in steps of 100. At each budget, estimates
locations and computes great-circle error vs known probe locations. Missing
estimates incur a 10,000 km penalty.

`get_random_subsample(n=100)` **mutates `target_data` in place** — subsequent
re-runs operate on already-pruned data.

### `measurement_converter_mode` options

| Mode | What it does |
|---|---|
| `'nearest_neighbor'` | Estimate = location of lowest-RTT VP seen |
| `'hard_circle'` | FeasibleRegion(mode='hard_circle') on all pings |
| `'gaussian'` | FeasibleRegion(mode='gaussian', σ=GLOBAL_SIGMA_MS) on all pings |

Default is `'nearest_neighbor'`. The `random` geolocator baseline always uses
this mode.

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
model `rtt = d/100 + N(0, σ_vp²)`):

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

- `AdaptiveRTTModel`: predicts RTT as `distance × 1.5 / 100ms`, with per-target
  EMA correction (α=0.3)
- `default_utility_evaluator`: simulates adding a candidate constraint to a
  cloned `FeasibleRegion`, measures area reduction. Targets within 200km
  (`BASICALLY_GEOLOCATED`) return −1,000,000 utility.
- Parallelises with `ProcessPoolExecutor`. Very slow at 900×900 VPs on laptop.
- "Focus batch" trick: re-sorts targets by cached utility every 50 pings.
- `solve()` must call `_update_best_vp_for_target(dst)` for every target to
  seed `best_vp_cache` — without this, `measurements()` infinite-loops.

**Current status**: iterative greedy does not yet beat random. See `.claude/TODOS.md`.

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
assess_geolocators.py              entry point / comparator harness
assess_probabilistic.py            real-data Gaussian vs hard-circle sweep (analysis only)
analyze_latency_distance.py        offline RTT vs distance model fitting (analysis only)
feasible_region_maintainer.py      FeasibleRegion: hard_circle + gaussian modes
probabilistic_helpers.py           pure NLL/sigma/grid helpers (no FeasibleRegion dep)
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

SIMULATION_ENVIRONMENT.md         ← read this to understand what's allowed during inference
.claude/TODOS.md                   ordered fix list

cache/cached_target_data.pkl       full mesh (~909 nodes)
cache/cached_results_*.pkl         per-geolocator sweep results
figures/                           output PDFs
```
