# Agent TODOs

Open work items (kept deliberately terse — completed work lives in git
history, `CLAUDE.md`, and `.claude/FIBER_GEOLOCATOR_RESULTS.md`; the
atlas-side research agenda is `.claude/HANDOFF_routing_realism.md`).
Last reviewed 2026-07-11.

## Plateau follow-ups (from the 2026-07-11 investigation — see
## "Plateau mechanism" in FIBER_GEOLOCATOR_RESULTS.md)

- Bearing-diverse dense sources: the residual stranded targets (~12%
  of dense) are likelihood-limited by conical 10-VP geometry; the
  sampler's coverage_depth tie-break could prefer bearing diversity
  (or raise dense depth) — the only lever that fixes them.
- Payoff-uncertainty scoring: risk_gain scores the p25 of each
  candidate's per-hypothesis benefit distribution (pessimism, chosen
  to stop budget-sinking). A "10,000-or-500 km" candidate scores ≈500;
  optimism-under-remaining-budget (UCB-style) is a one-constant
  experiment.
- Multi-seed validation of GEOLOC_HYP_OUTER_RINGS +
  GEOLOC_POLISH_LIVE_STARTS (currently 3-runs-vs-2 evidence, ~7% mean)
  before flipping them on by default for real-mesh runs.
- Offset-doubt uncertainty floor: report region size ≥ μ̂_dst×100 km
  until the offset is VERIFIED by bearing-diverse constraints (the
  fitted offset is a confession, not an explanation) — sketched, not
  implemented.
- Mid-run rescue losses outside the polish (shared-model refit path
  re-launders; measured 617 km → 14,716 at ping 1469) — the arbitration
  only guards checkpoint polishes.

## Fiber model follow-ups (from the integration verdict)

- Per-region (or per-target-prior) offsets on top of the 1.3×floor —
  the additive per-VP offsets don't absorb destination-side trombone
  for thinly-measured targets; the fiber losses concentrate exactly in
  the loose-floor regions (NZ/MY/SG class).
- Hypothesis-ring GENERATION is still geodesic (rings around the best
  VP); scoring is already fiber-aware. Fiber isochrone rings would help
  exactly the ridge targets the support set exists for.
- Policy-field cache footprint: fields are float64, ~250 KB × n_vps ×
  ~90 realized classes (22 GB at 1000 VPs). float32 and/or capping
  realized classes per VP before running larger VP sets.
- Mid-budget selection gap: every honest strategy sits ~4× above the
  oracle floor at ~50% coverage even in the floor-matched world
  (200src×100dst: floor 245 km mean). Finding each target's lowest-RTT
  VP cheaply IS the selection problem; nobody solves it at low budget.

## Known small issues (pre-fiber, still true)

- Two RTT prediction paths in the greedy: `rtt_func`/AdaptiveRTTModel
  (selection-time heuristic + telemetry) vs the regions' `rtt_model`
  base term. Consolidation would remove a confusion hazard.
- Hard-circle mode: an empty constraint intersection reads as
  "geolocated" (near-zero region size) instead of infeasible.
- Legacy `get_random_subsample` (symmetric path) mutates `target_data`
  in place; re-running on the same comparator operates on pruned data.
- `get_distance` in `utils.py` has unreachable code after the return.
- Dense-mesh pipeline merges only the first 10 hourly dump files
  (`fni == 10` early-exit); the full day has 24.
- `_update_best_vp_for_target` fans out one executor job per VP per
  target — slow at 900×900 scale; VP evaluations could batch.
