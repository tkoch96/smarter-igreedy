# Handoff: additive integration DONE → real-mesh greedy + robustness

> Written 2026-07-02 (supersedes the previous handoff, which asked for the
> greedy integration and the real-mesh estimator run — both landed this
> session). All 144+ tests green. Read `CLAUDE.md` for mechanics,
> `SIMULATION_ENVIRONMENT.md` for the research framing.

## What landed this session

1. **`AdditiveLatencyModel`** (`probabilistic_helpers.py`) — the shared
   cross-target state object: records raw (src, dst) samples, refits
   per-node (μ̂, σ̂²) via `fit_additive_params` against CURRENT estimates
   (fresh inits every call), serves `predict(src, dst, d) →
   (expected_rtt, var)` and `sigma_dst(t)`.
2. **`FeasibleRegion(mode='additive', model=...)`** — src-aware
   constraints, MAP weights 1/(σ̂_s²+σ̂_t²), precision-aware region size
   (weighted-rms residual + 1/sqrt(Σw) floor, km), `reoptimize()`.
3. **Greedy integration** (`region_mode=ADDITIVE`): shared model refit per
   ping (`model_refit_every=`), `additive_utility_evaluator` with the
   σ̂_dst TRUST DISCOUNT `prior_var/(prior_var+σ̂_t²)`, final
   reoptimize-all before estimates are read.
4. `greedy_additive` lines + calibrated assertions in BOTH budget figures
   (`error_over_measurements_additive.pdf`, `..._adaptive.pdf`).
5. **`'additive_em'` / `'em_gaussian'` / `'em_asymmetric'` converter
   modes** in `assess_geolocators.py` + `assess_additive_real.py` (driver:
   same random measurements through every estimator, mean AND median).

## Results (pinned by tests or cached pickles)

Synthetic additive world (10 seeds): greedy_additive 1151 → 634 → 476 km
over b = 30/120/240 vs random-order additive_em 1488 → 530 → 408.
Selection wins early; small full-budget estimation premium (min-of-reps,
see pitfalls). Budget sink FIXED: pathological ping share 0.20-0.30
(fair 0.25) vs em-greedy's 0.33-0.50 on identical scenarios.

Real mesh (seed 31415; `cache/additive_real_results_n{20,100}.pkl`):
- n=20 full coverage: additive mean 1927 / median 575 — FIRST model-based
  estimator to beat NN (2496/604) on both stats; em_asym 1894/791.
- n=100: additive is best model-based MEDIAN at every budget ≥ 1000
  (933 vs em_asym 1091 at b=2500) and breaks the per-target-em floor
  (2081 vs 3247 mean); em_asym keeps a better MEAN at high budget
  (1702 vs 2081); NN stays ahead overall (1249/467) — dense-coverage
  regime, expected.

## Pitfalls paid for THIS session (on top of the three previous ones)

- **Trust discount is load-bearing.** Ranking by raw simulated km
  reduction makes a pathological target OUTBID finished ones: its
  statistical floor is huge, so one more ping promises a large absolute
  reduction. Discount by prior_var/(prior_var+σ̂_t²)
  (`additive_utility_evaluator`).
- **Greedy regions must constrain on MIN-of-reps** while the model records
  all reps. Per-rep constraints let the pinged region's location step
  absorb the pair's full mean offset → zero residuals → μ̂_t collapse
  (measured: patho μ̂_t 8.2 vs true 35.1, errors 4400+ km). The min-vs-mean
  gap keeps residuals positive so the parameter step claims the offset.
- **Single-VP constraint sets must anchor at the VP**, not optimise: NM
  parks them on an arbitrary ring point whose zero residuals feed the same
  collapse (relevant once replicated samples make len(constraints) > 1
  with one distinct VP).

## Next steps (ordered; TODOS.md #0/#1)

1. **Real-mesh greedy_additive** — whole-system comparison: add
   `Iterative_Greedy_Geolocator(region_mode=ADDITIVE, name='greedy_additive')`
   to `Geolocator_Comparator.self.geolocators`, rerun `run()` at n=100 vs
   greedy_em / random+NN. Bump `model_refit_every` to ~10-25 (per-ping
   refits are O(pairs)). The n=100 greedy_em runs took hours on the laptop
   — budget accordingly or trim the geolocator list.
2. **Robustify the additive parameter step** (median/trimmed means): real
   residuals are one-sided heavy-tailed; the gaussian M-step feels detours
   — that's why em_asymmetric still wins the n=100 MEAN. Mirror the robust
   fits the per-target EM M-step already has.
3. **Replication**: lift the `fni == 10` early-exit in
   `load_parsed_target_data` (TODOS #9) — per-pair variance from a single
   residual pools weakly; more hourly samples sharpen the σ̂ split.

## Workflow reminders

- Smoke any real-mesh change at `n_subsample=20` first (2 s vs minutes);
  result caches embed the subsample size.
- Drive greedy runs from a real script file with an
  `if __name__ == '__main__':` guard — macOS spawn re-imports __main__
  (breaks stdin/-c drivers AND unguarded scripts).
- Report medians alongside means (isolated probes dominate means).
