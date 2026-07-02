# Agent TODOs

Open work items, ordered by priority. Completed work is recorded in git
history and reflected in `CLAUDE.md` / `SIMULATION_ENVIRONMENT.md` — do not
maintain a "done" list here.

---

## 🔴 High priority

### 1. Per-VP extension of the EM framework + real-data performance

`FeasibleRegion(mode='em_gaussian')` fits a per-TARGET (μ, σ) online. Real
RIPE Atlas data needs the per-VP version: overhead is VP-dependent and
scales with distance (mean 67ms, ∝ d^0.67), so the next steps are:

1. Per-VP affine model `rtt ≈ a_v × d + b_v`, fitted via the same
   EM/shrinkage machinery but pooling residuals per VP across targets
   (honest: uses estimated target locations, never VP-to-VP distances).
2. Fix the EM × robust-noise interaction: under heavy detour
   contamination, EM's μ-fit absorbs some detour bias, so robust noise +
   fixed slope currently beats robust noise + EM (asymmetric: 213 vs
   300km median). Candidate fixes: fit μ on the trimmed/lower-quantile
   residuals, or down-weight detour-suspect measurements in the M-step
   using the likelihood itself.
3. Re-run the real-data sweep (`assess_probabilistic.py`) with the slope +
   EM + asymmetric-noise estimators; the old catastrophic 10,000km
   failures came from the slope-1.0 gaussian placing rings ~6700km too far
   out. (`noise_model=ASYMMETRIC_NOISE` is likely the right default for
   real data.)
4. General predictive-model interface: estimation and selection both
   consume "what RTT do I expect and how sure am I" — a `LatencyModel`
   protocol with `nll(vp, rtt, candidate_loc)`, `predict(vp, loc) →
   (expected_rtt, sigma)`, and `update(vp, rtt, loc_estimate, weight)`
   would let slope/EM/per-VP-affine/learned models swap freely under
   FeasibleRegion and the greedy. Sketched, not implemented.

### 2. Oracle's estimation half is unclear

The oracle *selects* pings by minimising the true error of an internal
hard-circle FeasibleRegion estimate (`perfect_geolocator.py`), but is then
*scored* through the `nearest_neighbor` converter. Its selection objective
and its scored estimator disagree, so it may not be a tight upper bound for
the overlap methodology. Decide what estimator the oracle should be scored
with — probably the same overlap estimate it optimises during selection.

(Note: comparing greedy+overlap against random+NN whole-system is the
intended experiment, not a confound — see "The two phases" in
`SIMULATION_ENVIRONMENT.md`.)

### 3. Greedy still doesn't beat random

The selection algorithm is the main open research problem. Now that the
estimation stack is in decent shape (slope model, km-consistent region
sizes, em_gaussian), run the greedy sweep with `region_mode=GAUSSIAN` /
`'em_gaussian'` and compare against random ordering under the same
estimator to isolate selection quality.

---

## 🟡 Medium priority

### 4. Two RTT prediction paths in the greedy

`AdaptiveRTTModel` predicts `distance × DEFAULT_SLOPE / 100` + per-target
EMA correction — a second, parallel copy of the predictive model that now
lives in `FeasibleRegion.expected_rtt_ms()`. Unify: the greedy should ask
the region for its model prediction (which em_gaussian keeps calibrated
per target) instead of maintaining its own.

### 5. Single-ping corner cases in greedy

When a target has exactly one constraint, `_update_estimate` short-circuits
to the VP's location. The greedy's utility for a second ping is computed
against a region centred at the first VP — causing overestimated area
reduction. Fix: compute utility relative to the constraint circle, not the
current guess.

### 6. Hard-circle empty intersection reads as geolocated

If constraints conflict (one slope-beating RTT makes the circles fail to
all intersect), Nelder-Mead's estimate satisfies nothing, the region-size
probe finds no feasible displacement, and the size reads ~0 → falsely
"geolocated". Fix idea: if the estimate itself is infeasible, return the
sentinel (or distance to nearest feasible point). Gaussian modes are immune
(misfit raises their size).

### 7. `get_random_subsample` modifies `target_data` in place

`assess_geolocators.py` calls `get_random_subsample()` which overwrites
`self.target_data['loc_loc_meas']`. Re-running `run()` operates on
already-pruned data. Should return a new dict or deepcopy.

---

## 🟢 Low priority / ergonomics

### 8. Hardcoded debug target

`'85.93.215.0'` appears in `feasible_region_maintainer.py`. Replace with an
env var or constructor argument.

### 9. `get_distance` dead code in `utils.py`

Everything after `return fast_haversine(...)` is unreachable. Delete it.

### 10. Data coverage

The dense-mesh filter requires ≥80% bidirectional coverage. Some RTT pairs
are genuinely missing (probe offline). Consider merging more than 10 hourly
files (current `fni == 10` early-exit) and symmetric imputation
RTT(A→B) ≈ RTT(B→A).

### 11. Compute scaling

`_update_best_vp_for_target` fans out one `ProcessPoolExecutor` job per VP
per target. With 900×900 = 810k tasks this is very slow locally. Use
`max_workers=1` during development; consider batching VP evaluations into
vectorised numpy ops.
