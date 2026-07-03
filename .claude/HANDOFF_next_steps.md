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
   ping (`model_refit_every=`) with the pinged region's location update
   DEFERRED until after the refit (params-first),
   `additive_utility_evaluator` with the σ̂_dst TRUST DISCOUNT
   `prior_var/(prior_var+σ̂_t²)`, and a final `additive_batch_em` polish
   before estimates are read — incremental state drives selection, the
   fresh batch fit produces the answers.
4. `greedy_additive` lines + calibrated assertions in BOTH budget figures
   (`error_over_measurements_additive.pdf`, `..._adaptive.pdf`).
5. **`'additive_em'` / `'em_gaussian'` / `'em_asymmetric'` converter
   modes** in `assess_geolocators.py` + `assess_additive_real.py` (driver:
   same random measurements through every estimator, mean AND median).

## Results (pinned by tests or cached pickles)

The synthetic additive world uses ONE sample per (src, dst) pair — a
practical ping is already min-of-~3, which strips QUEUEING delay, whereas
X_src/X_dst model per-node path inefficiency that repetition cannot
average away. (An earlier 3-replicate version let estimators exploit
noise structure that doesn't exist in practice; removed 2026-07-03.)

Synthetic additive world (10 seeds, budgets in pairs, max 80; all stats
MEANS across seeds): greedy_additive 1932 → 1129 → 706 km over
b = 10/40/80 vs random-order additive_em 4281 → 1172 → 706 and random+NN
3475 → 752 → 646. Selection dominates early (greedy at b=10 ≈ NN at
b=30-40); at full coverage greedy = additive_em exactly (same estimator,
same data). NN keeps the full-coverage lead in this small synthetic
(only ~8-10 pairs of pooling per node) — the real n=20 mesh flips that.
Budget sink FIXED: pathological ping share median 0.29 / max 0.375
(fair 0.25) vs em-greedy's 0.33-0.50 on identical scenarios.

Oracle convention (both test files AND assess_geolocators): selection by
`Perfect_Geolocator` — the single selection-oracle implementation, fed
ground truth via its address_to_loc — paired in the tests with true-(μ,σ)
MAP estimation. Pinned to dominate every strategy at every budget
(sweep oracle 676 → 390 → 374): if an honest strategy beat it, the
oracle should have picked what that strategy picked. The batch
`run_param_oracle` in test_e2e_additive_em.py is NOT a selection oracle
— it is the parameter-estimation bound on full data.

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
- **Incremental location updates RATCHET offsets into distance.** Each
  per-ping MAP step under not-yet-converged offsets absorbs a little of a
  pathological target's offset; later refits can't win it back (measured:
  patho μ̂_t 16 vs true 35 at full budget). Two-part fix: defer the pinged
  region's location update until after the parameter refit
  (`add_measurement(..., update_estimate=False)`), and produce reported
  estimates via a fresh NN-anchored `additive_batch_em` at the end of
  `measurements()` — trust incremental state for selection only.
- **Single-VP constraint sets must anchor at the VP**, not optimise: NM
  parks them on an arbitrary ring point whose zero residuals feed the μ̂_t
  collapse (relevant whenever a target's constraints span one distinct VP,
  e.g. multiple samples of the same pair).

## Next steps (ordered; TODOS.md #0/#1)

1. **DONE 2026-07-03 — real-mesh greedy_additive** (n=100, seed 31415,
   ~2.5 h laptop run, `model_refit_every=25`): best honest start (3,942
   at b=100), 2,731 at b=600, then plateaus 2,750-2,990 — mean-parity
   with greedy_em, plateau NOT broken. See TODOS #0 for the two suspects
   (robust M-step; median reporting in run()).
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
