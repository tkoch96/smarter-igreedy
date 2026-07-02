# smarter-igreedy

IP geolocation under a ping budget. Given a set of RIPE Atlas vantage
points (VPs) and unknown-location targets, which VP→target pings should you
spend — and how should you turn RTTs into locations — to minimise average
geolocation error per measurement?

Two coupled sub-problems:

- **Estimation** — turn observed RTTs into a location (and an honest
  uncertainty). Implemented as `FeasibleRegion` (hard-circle / gaussian /
  online-EM modes, toggleable noise models) plus an additive per-source /
  per-destination overhead model.
- **Selection** — decide the next ping. Implemented as
  `Iterative_Greedy_Geolocator` (greedy expected-region-reduction across a
  shared multi-target budget).

## Quick start

```bash
source ~/Documents/venv312/bin/activate

# full test suite (~3 min) — every experiment figure regenerates under tests/
python -m pytest tests/

# real-mesh comparison (writes figures/geolocator_results.pdf);
# uses cache/cached_target_data.pkl (built on first run from RIPE dumps)
python assess_geolocators.py
```

## Documentation map

| File | What it covers |
|---|---|
| `SIMULATION_ENVIRONMENT.md` | The research problem, the information boundary (what inference may see — read this first), the two-phase selection/evaluation split, and the model ladder |
| `CLAUDE.md` | Codebase mechanics: every module, mode, invariant, calibrated result and test |
| `.claude/TODOS.md` | Open work items, priority-ordered |
| `.claude/HANDOFF_next_steps.md` | Current handoff: integrate the additive model into greedy selection; run it on real data |

Figures are generated artifacts (gitignored): experiment figures under
`tests/*.pdf` regenerate with pytest; `figures/` comes from
`assess_geolocators.py` runs.
