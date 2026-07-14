# Handoff: utility calibration — why growth plateaus under the current model

You are picking this up fresh; read this fully before touching
anything. The goal is a debugging LOOP, not a one-shot fix: analyze the
per-ping logs, find where the greedy's expected utility disagrees with
what a ping actually accomplished (both directions), form one hypothesis,
change one thing, re-run, re-read the logs. The user's stated loop:
idea → run → read expected-vs-actual logs → new idea.

## Orientation

Repo `~/Documents/smarter-igreedy`, venv `~/Documents/venv312/bin/python`.
Read `CLAUDE.md` first; results and the full history of the 2026-07-10..12
investigations (sampler collapse, offset laundering, graph-node search,
model grid) live in `.claude/FIBER_GEOLOCATOR_RESULTS.md` — the sections
"2026-07-10 dense collapse", "Plateau mechanism", "Graph-node search".
Run experiments ONLY through `assess_geolocators.py`; `random` and
`smart_perfect` are force-included baselines in every run.

## Current state (all landed on main, 2026-07-12)

- The additive greedy with the 1.3×fiber-floor base uses GRAPH-NODE
  search for every location step (map-matching to the atlas;
  `GEOLOC_NODE_SEARCH=0` disables). Current headline on the pinned
  world `cache/world_300src_2204dst_resampled.pkl`:
  `greedy_phased_fiber` 2078/1022 km (mean/median) at b=32000 vs oracle
  2161/869... yes, the greedy now BEATS the oracle's mean at full
  coverage; the mid-budget gap is the open problem: at b≈4000 the
  oracle is ~2× better (oracle flatlines by b≈2500, honest strategies
  need ~10× that).
- Run pickles with full telemetry:
  `cache/geolocator_run_300src_2204dst_resampled_nodesearch.pkl`
  (current model), `..._resampled.pkl` (pre-node-search, 4 strategies),
  `..._resampled_baselines2.pkl` (fresh oracle + random). Each greedy
  entry carries `utility_tracking` (per-ping: expected_util,
  actual_util, explore flag, est/size before-after, model residuals)
  and `model_params` (fitted μ/σ² per node).

## The question

The greedy plateaus because its auction stops finding value that
demonstrably exists. Quantified on the CURRENT model
(`analysis/utility_calibration.py`, first run 2026-07-12):

- 1,030 SILENT WINS (expected < 50 km, true gain > 500 km) — all of
  them explore pings; the biggest are +14,000..18,000 km rescues taken
  at expected utility exactly 0.
- 3,081 EMPTY PROMISES (expected > 500 km, |true movement| < 50 km),
  concentrated in deciles 1–4 — the auction pays for spread reductions
  that do not move the estimate toward anything.
- By decile 3 (~30% of budget) the median expected utility is 0 and
  60–97% of pings are uniform-random exploration.
- LEAD, unexplained: several huge silent wins carry NEGATIVE believed
  utility (e.g. ping 12360: believed −6,590 km "worse", TRUE +15,311 km
  better; ping 9209: believed +24,072 vs TRUE +14,936). The belief
  system sometimes records a rescue as a regression (region size GREW
  when the estimate jumped basins — arguably honest uncertainty!) and
  sometimes wildly overstates. Whether size-delta is even the right
  "actual" is itself in question.

Diagnosed causes so far (do not re-litigate without new evidence, see
FIBER_GEOLOCATOR_RESULTS.md): offset-position laundering silences
residuals (err>5000 km targets carry ~27 ms fitted μ_dst); hypothesis
supports collapse because their tolerance is misfit-scaled and the
laundered fit has low misfit; `gain_reliability` EWMA writes targets
off. Measured dead ends: stronger L2, zero-centered priors,
size-weighted exploration. Measured helps: outer physics rings +
live-start polish arbitration (`GEOLOC_HYP_OUTER_RINGS=1
GEOLOC_POLISH_LIVE_STARTS=1`, ~7% mean, off by default — synthetic
pins move), node search (landed).

## Tools (analysis/, all runnable today)

- `analysis/utility_calibration.py RUN.pkl WORLD.pkl [strategy]` — the
  decile table + mismatch quadrants + biggest silent wins. START HERE.
- `analysis/probe_audit.py` (env `AUDIT_ARM=fiber|geo`) — at
  checkpoints, scores EVERY remaining candidate two ways: the auction's
  own evaluator (`_evaluate_vp_chunk_worker`) vs ground-truth
  clone-add-reoptimize gain; top-5 tables + rank correlation. Uses the
  100×300 world snapshot (`cache/world_100src_300dst_fibergeo.pkl`).
- `analysis/strand_trace.py` — runs the small-world fiber greedy,
  classifies stranded targets (dipped-then-relost / never-good /
  under-pinged) with per-ping true-error trajectories.

## Suggested plan

1. Reproduce the calibration table on the nodesearch pickle; then the
   same for `greedy_phased_geo` (old pickle) — is the mismatch fiber-
   specific or structural?
2. Chase the negative-believed-utility rescues: for ~10 of them, dump
   the region state around the ping (constraints, hypothesis support,
   size before/after, whether polish later kept or reverted the jump).
   Decide what "actual utility" SHOULD mean — size delta (current),
   promised-quantity delta (hypothesis spread), or NLL improvement —
   and make expected/actual commensurable. The auction currently
   promises spread-cuts but is scored on size-cuts; part of the
   miscalibration may be pure unit mismatch.
3. Empty promises (deciles 1–4): join with `gain_reliability` and
   per-target ping counts — are these the documented patho targets the
   discount should be suppressing, or well-behaved targets whose
   promised spread is stale (support built before the last refit)?
4. Silent wins: all explore pings. The auction never scores them >0
   because the support has collapsed. Test the one-line hypothesis the
   audits point at: at the phased switch, re-run `_update_hypotheses`
   with the outer physics rings enabled for HIGH-believed-size targets
   only (size > 3000 km, say), and see whether their candidates start
   bidding — that is a targeted version of GEOLOC_HYP_OUTER_RINGS that
   may not perturb the synthetic pins.
5. Whatever changes, measure via `assess_geolocators` on the pinned
   world (GEOLOC_WORLD=cache/world_300src_2204dst_resampled.pkl, tag
   your run) and re-run the calibration table: success = silent-win
   count falls materially at unchanged (or better) error curves; the
   mid-budget gap to the oracle narrows.

## Watch-outs

- One greedy at a time on this 16 GB laptop; a 300×2204 single-arm run
  is ~2 h. The 100×300 world is the 15-minute iteration vehicle.
- Greedy runs jitter 5–10% (worker arrival order breaks score ties);
  single-run deltas below that are noise. Paired/multi-run or bust.
- Worlds drift with the campaign DB: ALWAYS pin GEOLOC_WORLD.
- `tests/test_speedups.py::TestGreedyEndToEnd` is a known flake
  (arrival-order); everything else should stay green.
- Ground truth is for POST-HOC analysis only, never inside estimation
  (`SIMULATION_ENVIRONMENT.md`).
- Huber loss in the production additive fit is grid-validated but NOT
  yet implemented (`.claude/TODOS.md`) — if your investigation lands
  there, that's expected; implement behind a knob and A/B it.

## Resolution

(To be filled in by the next agent: what the mismatch was, with the
before/after calibration tables and error curves.)
