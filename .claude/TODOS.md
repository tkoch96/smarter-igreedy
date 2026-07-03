# Agent TODOs

Open work items, ordered by priority. Completed work is recorded in git
history and reflected in `CLAUDE.md` / `SIMULATION_ENVIRONMENT.md` — do not
maintain a "done" list here.

---

## ⏭️ Immediate (see `.claude/HANDOFF_next_steps.md` for full context)

### 0. Break the real-mesh greedy plateau — DIAGNOSED 2026-07-03

Hindsight debug run (n=100, b=1000, belief trail joined with truth):

- **Median vindicated**: greedy_additive median 859 km BEATS random+NN's
  907 at the same budget; its mean (3,087 vs 2,090) is entirely the tail
  — the worst 10 targets carry 46% of total error.
- **Dominant failure = out-of-mesh identifiability, not detours.** The
  catastrophic targets (Mangalore, Colombo, Cape Town, ...) sit far
  outside the Europe-heavy VP cluster. With every VP in one cluster,
  a shared offset and extra distance are EXACTLY confounded (moving away
  along the cluster bearing mimics +offset for all VPs at once); the fit
  absorbs the offset into distance (μ̂ fitted 5-100 ms vs needed 44-197),
  residuals then read ≈ 0, believed region size reads 550-970 km while
  the true error is 12-18,000 km. Overconfidence also wastes budget
  (10-13 pings each on "improvable" hopeless targets) and evades the
  trust discount (clean residuals → low σ̂_t).
- Detour chasing is real but SECONDARY: pings with residual > +20 ms
  moved the estimate away from truth 54% of the time vs 47% baseline.
- Batch polish is load-bearing on real data: median 1,901 → 859.
- No cheap estimate-level fix: SOL clamp recovers ~150 km of mean (the
  bound is too loose at 150-220 ms best-RTTs); a perfect per-target
  min(MAP, NN) switch only reaches mean 2,318 — these targets' own NN
  floor is 5-10,000 km. The n=100 MEAN is geometry-bound; median is the
  honest headline metric.

Second finding (closest-VP trace, same debug run): the additive
utility is GEOMETRY-BLIND, so selection cannot recover from a wrong
estimate. Reconstructing the b=1000 state and ranking all ~90 candidate
VPs through additive_utility_evaluator: every candidate scores within
0.35% of every other (the simulated ping is added at the model-expected
RTT — zero surprise — so only σ̂_s differentiates candidates), and the
predicted RTT for the truly-closest VP is computed from distance to the
WRONG estimate (197 ms predicted where reality is 0.7 ms from 7 km
away; it ranked #9/88 and was never pinged). The oracle's 615 km mean
with plain-NN estimation proves close VPs exist for nearly every
target: the plateau is an EXPLORATION failure, not a geometry floor.
Random+NN keeps improving with budget precisely because random sampling
stumbles onto the close VPs; expected-utility-under-the-current-model
structurally cannot value a measurement whose worth is falsifying the
model.

Fix directions, in value order:
1. Exploration-aware utility: score candidates by outcome VARIANCE under
   the predictive distribution (location-ridge + σ̂), not by simulated
   zero-residual size reduction — a candidate whose measurement could
   drastically move the estimate carries the information; bearing/
   distance novelty is the cheap proxy. This is the plateau-breaker.
2. Report MEDIANS in `Geolocator_Comparator.run()` (store per-target
   errors in plot_data) — greedy already wins medians at b=1000
   (859 vs 907) and the mean-only print hides it.
3. Honest region size under cluster-degenerate geometry (narrow bearing
   cone → ring-ambiguity floor): stops the overconfidence and the
   10-13-ping waste on unfixable-looking-fixable targets.
4. Robust additive M-step (see #1) for the secondary detour effect.

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
