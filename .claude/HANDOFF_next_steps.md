# Handoff: additive src/dst model → greedy integration + real data

> Written 2026-07-02. State: all 147 tests green; the additive two-way
> model is implemented and validated on synthetic data but NOT yet wired
> into selection (the greedy) or the real-data harness. Read `CLAUDE.md`
> for codebase mechanics and `SIMULATION_ENVIRONMENT.md` for the research
> framing and the model ladder.

## Where things stand (one paragraph)

Estimation has climbed a ladder: fixed-slope gaussian → toggleable noise
models (asymmetric wins on real data) → per-target EM (`em_gaussian`) →
the ADDITIVE two-way model `rtt = SOL + X_src + X_dst` with per-node
(μ, σ) learned honestly by alternating EM
(`probabilistic_helpers.fit_additive_params` +
`tests/test_e2e_additive_em.py`). On synthetic additive ground truth the
additive estimator is the only model-based method that beats
nearest-neighbour (408 vs 605 km at full budget; per-target EM stalls at
1180) and it identifies pathological destinations via σ̂_dst 100% of the
time — the signal selection needs to stop sinking budget into hopeless
targets (a pathology measured on the real mesh: median target got 3
pings while one target absorbed 52).

## Immediate TODO (a): put the additive model under GREEDY selection

Context for the question "why does the new unit test use a random
measurement order — didn't we change that?":

- `error_over_measurements_adaptive.pdf` (test_e2e_adaptive_em.py,
  `run_multi_seed`) DOES use greedy selection — that was changed as
  requested. Only its `random_nn` baseline is random-ordered.
- The NEW additive sweep (`error_over_measurements_additive.pdf`,
  `run_additive_budget_seed`) uses a shared random order for ALL
  estimators **by design, as a stopgap**: it isolates the estimation
  question because the additive estimator cannot run inside
  `Iterative_Greedy_Geolocator` yet. The blocker is structural:
  `FeasibleRegion` is a per-target object, but the additive model needs
  per-SOURCE state shared across all targets (X_src is pooled).

The task: give the greedy a shared model object (this is the
`LatencyModel` interface sketched in TODOS #4):

1. A coordinator owning (μ_s, σ_s, μ_t, σ_t), refitted from all
   accumulated measurements via `fit_additive_params` after each ping
   (or every few pings).
2. Regions (or their replacement) consult it for expected rtt and
   per-measurement variance; MAP weights = 1/(σ_s² + σ_t²).
3. THE PAYOFF — use σ̂_dst in the utility: expected information gain of
   another ping to target t shrinks as σ̂_t grows, so the greedy learns
   to redirect budget away from pathological targets. This should fix
   the budget-sink measured in the flatline debugging session.
4. Then extend both budget figures with `greedy_additive` lines.

Implementation pitfalls already paid for (don't rediscover them):
- Run the PARAMETER step before the location step each EM iteration —
  otherwise a pathological target's offset is absorbed into distance
  (μ̂_t collapses to ~3ms vs true ~59; a self-consistent wrong fixed
  point). See the comment in `run_additive_em`.
- Do NOT warm-start the fit across budget points — early-budget fixed
  points carried forward degraded full-budget error 2×. Fresh
  NN-anchored inits per refit. See comment in `run_additive_budget_seed`.
- Location MAP: multi-start Nelder-Mead (previous estimate + NN start);
  normalize coordinates afterwards (`_normalize_latlon`).

## Immediate TODO (b): run the additive methodology on the REAL mesh

Goal: assess_geolocators-style numbers for the additive estimator on
RIPE data, against NN / per-target em / em_asymmetric baselines.

Recommended path (no greedy needed for a first result):
- Add converter mode `'additive_em'` to
  `Geolocator_Comparator.convert_measurements_to_locations` — that
  method receives the FULL budgeted measurement dict, so a cross-target
  fit fits naturally inside it (unlike per-target FeasibleRegion modes).
  Port the loop from `run_additive_em` (params-first, fresh inits).
- Or, quicker and dirtier: a driver mirroring `run_additive_em` directly
  on `gc.target_data` after `get_random_subsample(n)`.

Real-data practicalities:
- The measurement cache stores ONE rtt per pair (wrapped into a
  1-element list on load). `fit_additive_params` still works — per-pair
  variance from a single residual is noisy but pools across ~n pairs per
  node — but variance estimates improve a lot with replication:
  consider lifting the `fni == 10` early-exit in
  `load_parsed_target_data` (TODOS #9) to get more hourly samples.
- Real residuals are one-sided/heavy-tailed; the gaussian additive
  M-step will feel detours. If results disappoint, robustify the
  parameter step (median/trimmed means) the way the per-target EM's
  M-step already does for non-gaussian noise models.
- Reference numbers to beat (same seed-31415 20-probe subsample):
  NN mean 2496 / median 604; per-target em mean 3913; em_asymmetric
  mean 2054 / median 594 (the current best honest estimator). At
  n=100: NN@full-coverage mean ≈ 1620; per-target-em floor ≈ 2601.
- Workflow: smoke at `n_subsample=20` before 100 (cheap, same code
  path). `run(..., n_subsample=)` is threaded; result caches include
  the subsample size in the filename. Drive via a real script file
  (macOS spawn breaks stdin/-c drivers with the greedy's process pool).
- Report MEDIANS alongside means — the subsample contains isolated
  targets (Guam/Cape Town/Dallas at n=20) whose ~10,000 km errors
  dominate any mean.

## Secondary follow-ups (see TODOS.md for the full list)

- Budget-sink fix without the full additive integration: per-target
  ping cap, or utility driven by `region.expected_rtt_ms` (TODOS #3/#4).
- `noise_model` plumbing through the greedy constructor (one parameter).
- Oracle estimation-half mismatch (TODOS #2).
