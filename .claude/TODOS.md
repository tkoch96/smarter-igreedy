# Agent TODOs

Open work items, ordered by priority. Completed work is recorded in git
history and reflected in `CLAUDE.md` / `SIMULATION_ENVIRONMENT.md` — do not
maintain a "done" list here.

---

## ⏭️ Immediate (see `.claude/HANDOFF_next_steps.md` for full context)

### 0. Real-mesh run of greedy_additive

The additive model is now integrated into the greedy (shared
`AdditiveLatencyModel`, σ̂_dst-discounted utility — fixes the budget sink
on synthetic data) and the additive_em ESTIMATOR is validated on the real
mesh (best model-based median at n=20 and n=100; beats NN outright at
n=20). The missing piece is the whole-SYSTEM real-mesh comparison: add an
`Iterative_Greedy_Geolocator(region_mode=ADDITIVE)` variant to
`Geolocator_Comparator.self.geolocators` and rerun
`assess_geolocators.run()` at n=100 against greedy_em / random+NN.
Watch: per-ping model refits are O(pairs) each — bump `model_refit_every`
(constructor param) to ~10-25 at real-mesh scale.

---

## 🔴 High priority

### 1. Additive model on real data — robustness follow-ups

Real residuals are one-sided and heavy-tailed; the additive M-step
(`fit_additive_params`) uses plain shrunk means, so detours inflate μ̂ and
σ̂. At n=100 the additive median is the best model-based one, but
em_asymmetric still wins the MEAN at high budget (1702 vs 2081 at 2500) —
the gaussian additive parameter step is feeling the detours the
asymmetric noise model shrugs off.

Next steps:

1. Robustify the additive parameter step (median/trimmed means, or
   likelihood-weighted residuals) the way the per-target EM's M-step
   already handles non-gaussian noise models.
2. Replication: the cached mesh stores ONE rtt per pair; per-pair variance
   from a single residual pools weakly. Lift the `fni == 10` early-exit in
   `load_parsed_target_data` (see #9) to get more hourly samples.
3. Fix the EM × robust-noise interaction: under heavy detour
   contamination, EM's μ-fit absorbs some detour bias, so robust noise +
   fixed slope currently beats robust noise + EM (asymmetric: 213 vs
   300km median). Candidate fixes: fit μ on the trimmed/lower-quantile
   residuals, or down-weight detour-suspect measurements in the M-step
   using the likelihood itself.
4. General predictive-model interface: `AdditiveLatencyModel` now realises
   the shared-model idea for the additive class (`predict(src, dst, d) →
   (expected_rtt, var)`, `record`, `refit`); a common protocol so slope /
   EM / per-VP-affine models can swap under FeasibleRegion and the greedy
   the same way is still open (see #4 below).

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
