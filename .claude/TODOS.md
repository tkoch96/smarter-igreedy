# Agent TODOs

Items are ordered by priority. Fix blocking issues before ergonomics.

---

## ✅ Done

- **Cache init bug** — `solve()` never seeded `best_vp_cache`, causing
  `measurements()` to infinite-loop. Fixed.
- **Type annotations** — all function signatures carry argument and return types.
- **Unit tests** — `tests/test_iterative_greedy_init.py` covers cache
  population, VP validity, finite utilities, liveness, coverage, deduplication,
  pair validity, history monotonicity.
- **Probabilistic FeasibleRegion** — `FeasibleRegion` now supports
  `mode='hard_circle'` and `mode='gaussian'`. Gaussian uses Nelder-Mead on
  the NLL with configurable per-measurement sigma.
- **Probabilistic helper functions** — `probabilistic_helpers.py` with unit
  tests (29 tests, `tests/test_probabilistic_helpers.py`).
- **Gaussian mode wired into comparator** — `Geolocator_Comparator` supports
  `measurement_converter_mode = 'gaussian'` using `GLOBAL_SIGMA_MS`.
- **Integration test restructured** — `tests/test_e2e_probabilistic.py` now
  calls the real `Geolocator_Comparator.convert_measurements_to_locations()`
  and `Random_Geolocator` with synthetic data in the real pipeline format.
- **Location locking** — `LockedLocationDict` and `simulation_mode()` in
  `utils.py`. Integration test enforces the information boundary at runtime.
- **Real-data analysis** — `analyze_latency_distance.py` fits 4 RTT models,
  shows per-VP heterogeneity, confirms overhead ∝ d^0.67, not constant.
- **Honest baseline** — removed per-VP mu/sigma calibration from inference
  (it required VP-to-VP distances = cheating). Gaussian now uses global σ only.
- **Documentation** — `SIMULATION_ENVIRONMENT.md` explains the research
  problem, simulation setup, and information boundary. `CLAUDE.md` updated.

---

## 🔴 High priority

### 1. Algorithm/evaluator mismatch

The greedy optimises *FeasibleRegion area reduction* but the default evaluator
uses `nearest_neighbor` (estimate = location of closest-RTT VP). These
objectives are orthogonal.

**Option A — align evaluator to greedy**: switch to `'gaussian'` mode and use
`get_current_estimates()` on the greedy (already wired).

**Option B — align greedy to evaluator**: change greedy utility to select the
VP closest to the current centroid estimate. Simpler, abandons triangulation.

### 2. Online EM for per-VP calibration (honest version)

Per-VP sigma and mu can be estimated honestly via EM:
- **E-step**: estimate target locations using current (μ_v, σ_v)
- **M-step**: update (μ_v, σ_v) from residuals against those location estimates

Cold start: global σ, no mu correction. As target estimates accumulate and
improve, the VP parameters refine automatically. No VP-to-VP distances needed.

Not yet implemented. Would likely close most of the gap between Gaussian
(honest) and oracle.

### 3. Real-data Gaussian performance

The Gaussian MAP with global σ and no mu correction performs poorly on real
RIPE Atlas data: mean overhead 67ms (not zero), overhead ∝ d^0.67 (not
constant). Some targets get 10,000+ km errors.

Root cause: `rtt = d/100 + N(0, σ²)` is badly misspecified. Real overhead
is large, VP-dependent, and scales with distance.

Pending fixes (in order of impact):
1. Online EM for per-VP calibration (see #2 above)
2. Per-VP affine model: `rtt ≈ a_v × d + b_v` — slope + intercept per VP.
   Estimated honestly via EM against accumulating target estimates.
3. Clip catastrophically wrong estimates (e.g. >5000km from any VP) as a
   robustness fallback

---

## 🟡 Medium priority

### 4. RTT model quality in greedy

`AdaptiveRTTModel` uses `distance × 1.5 / 100` as base RTT. The 1.5× routing
factor is a rough constant; real overhead is path-dependent. EMA correction
(α=0.3) helps per-target but can't fix systematic regional bias.

Ideas:
- Per-region (continent pair) routing factors from empirical data
- Use SOL floor (d/100) as hard lower bound for constraint radius

### 5. Single-ping corner cases in greedy

When a target has exactly one constraint, `_update_estimate` short-circuits to
the VP's location. The greedy's utility for a second ping is computed against a
region centred at the first VP — causing wildly overestimated area reduction.

Fix: record the VP location as the "prior centre" and compute utility relative
to the constraint circle, not the current guess.

### 6. `get_random_subsample` modifies `target_data` in place

`assess_geolocators.py` calls `get_random_subsample()` which overwrites
`self.target_data['loc_loc_meas']`. Re-running `run()` operates on
already-pruned data. Should return a new dict or deepcopy.

---

## 🟢 Low priority / ergonomics

### 7. Hardcoded debug target

`'85.93.215.0'` appears in `feasible_region_maintainer.py`. Replace with an
env var or constructor argument.

### 8. `get_distance` dead code in `utils.py`

Everything after `return fast_haversine(...)` is unreachable. Delete it.

### 9. Data coverage

The dense-mesh filter requires ≥80% bidirectional coverage. Some RTT pairs are
genuinely missing (probe offline). Consider:
- Merging more than 10 hourly files (current `fni == 10` early-exit)
- Symmetric imputation: RTT(A→B) ≈ RTT(B→A)

### 10. Compute scaling

`_update_best_vp_for_target` fans out one `ProcessPoolExecutor` job per VP per
target. With 900×900 = 810k tasks this is very slow locally. During development:
- Use `max_workers=1` to avoid fork overhead
- Consider batching VP evaluations into vectorised numpy ops
