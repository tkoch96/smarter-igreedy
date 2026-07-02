# Agent TODOs

Open work items, ordered by priority. Completed work is recorded in git
history and reflected in `CLAUDE.md` / `SIMULATION_ENVIRONMENT.md` — do not
maintain a "done" list here.

---

## ⏭️ Immediate (see `.claude/HANDOFF_next_steps.md` for full context)

### 0a. Additive model under greedy selection

The additive src/dst estimator currently runs only under a shared RANDOM
measurement order (`run_additive_budget_seed`) because it needs per-source
state shared across targets, which per-target `FeasibleRegion`s can't
hold. Integrate it into `Iterative_Greedy_Geolocator` via a shared model
object, use σ̂_dst in the utility (fixes the measured budget-sink), then
add `greedy_additive` lines to both budget figures.
(Note: `error_over_measurements_adaptive.pdf` already uses greedy
selection — only the new additive figure is random-order, as a stopgap.)

### 0b. Additive methodology on the real mesh

Run the additive estimator on RIPE data via `assess_geolocators`
(recommended: a cross-target `'additive_em'` converter mode) against
NN / per-target-em / em_asymmetric. Reference numbers, replication
caveats and pitfalls are in the handoff.

---

## 🔴 High priority

### 1. Per-VP extension of the EM framework + real-data performance

`FeasibleRegion(mode='em_gaussian')` fits a per-TARGET (μ, σ) online. The
ADDITIVE two-way model rtt = SOL + X_src + X_dst (X_n ~ N(μ_n, σ_n²)) is
now implemented and validated on synthetic data
(`probabilistic_helpers.fit_additive_params` +
`tests/test_e2e_additive_em.py`): it identifies pathological destinations
via σ̂_dst 100% of the time (the honest "stop wasting pings here" signal
for selection), and halves the per-target EM's location error (512 vs
1149 km mean). Key implementation lesson recorded in the test: run the
PARAMETER step before the location step each EM iteration, otherwise a
pathological target's offset gets absorbed into distance (a wrong
self-consistent fixed point; μ̂_t collapse).

Next steps:

1. Integrate the additive model into the greedy (shared per-source state
   across targets — this is the LatencyModel interface, see #4 below) and
   use σ̂_dst in the utility to fix the budget-sink pathology.
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

### 3. Greedy beats random on synthetic — port the win to the real mesh

The old headline ("greedy doesn't beat random") is RESOLVED on synthetic
multi-target budget allocation (`TestMultiTargetBudgetAllocation`,
5 targets / 10 VPs / 20 seeds) after making `BASICALLY_GEOLOCATED` a
deprioritisation rather than a hard stop (leftover budget flows to the
least-certain "done" targets). Medians: greedy_em beats random+NN at every
budget (k=10: 893 vs 1214; k=25: 437 vs 699 — random's FULL-budget
accuracy at half budget; k=50: 174 vs 477, winning every seed) and
statistically ties the parameter-oracle (170). greedy_hard remains worse
than random at mid-budget — estimation quality gates selection quality.

Remaining:
1. Selection utility should use the region's own model
   (`region.expected_rtt_ms`) instead of the separate AdaptiveRTTModel
   (see #4) — for em regions the fitted μ̂ should drive choices.
2. Plumb `noise_model` through the greedy constructor (asymmetric noise
   for real data).
3. Port the comparison to the real mesh via assess_geolocators (structure
   to be agreed before editing — see session history).

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

### 8. `get_distance` dead code in `utils.py`

Everything after `return fast_haversine(...)` is unreachable. Delete it.

### 9. Data coverage

The dense-mesh filter requires ≥80% bidirectional coverage. Some RTT pairs
are genuinely missing (probe offline). Consider merging more than 10 hourly
files (current `fni == 10` early-exit) and symmetric imputation
RTT(A→B) ≈ RTT(B→A).

### 10. Compute scaling

`_update_best_vp_for_target` fans out one `ProcessPoolExecutor` job per VP
per target. With 900×900 = 810k tasks this is very slow locally. Use
`max_workers=1` during development; consider batching VP evaluations into
vectorised numpy ops.
