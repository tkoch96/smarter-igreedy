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

# a configured experiment, e.g. the fiber-vs-geodesic comparison
python assess_geolocators.py --config configs/fiber_merged_n100.json
```

## Running experiments (`assess_geolocators.py`)

One harness, everything a setting — a strategy is a **selection** method
paired with an **estimation** method, and both are flags/attributes, never
separate scripts. Precedence: command line > `--config` JSON (same key
names) > `GEOLOC_*` env vars (legacy interface) > defaults. With no
settings at all, the historical default comparison runs unchanged.
`configs/` holds ready-made experiment configs.

```bash
python assess_geolocators.py --help                # full flag list
python assess_geolocators.py \
  --data merged --n-sources 200 --n-targets 1000 \
  --fiber --budgets 1500,3000,6000,10000 --tag _200x1000
```

### Data & sampling

| Setting | Meaning |
|---|---|
| `--data legacy` | symmetric daily-dump mesh (`cache/cached_target_data.pkl`): the n sampled sources are also the targets |
| `--data merged` | daily mesh + live campaign via `internet_gmaps/mesh_data.py` (min-RTT wins, SOL-suspects excluded). Asymmetric: campaign targets are dst-only with thin, disjoint source sets — use `--n-targets` |
| `--n-sources N` | number of probes/VPs (default 100) |
| `--n-targets M` | independent target count; sources are then chosen by lazy-greedy coverage (each pick maximises targets still below the depth cap) |
| `--source-selection facility` | choose the n sources by greedy facility location instead (best-k placement, matches the oracle floor sweep). Reads target ground truth to CONSTRUCT the world — floors are as low as the source budget allows; default `coverage` stays location-blind |
| `--vps-per-target K` | cap each target's VP count — the runtime knob for large runs |
| `--budgets 1500,3000,...` | explicit budget grid (else `--min-budget/--max-budget/--budget-step`) |
| `--seed` | sampling + shared random ping order (default 31415, project convention) |

### Exploration (selection) types

| Strategy name | What it does |
|---|---|
| `random` | uniform shuffle of all (VP, target) pairs — the coverage baseline |
| `smart_perfect` | oracle: sees ground truth, greedily ranks pings by realised error reduction (`--oracle-candidates` caps its per-target search) |
| `greedy_phased` | the main algorithm: additive-model greedy, risk-adjusted promises (`risk_gain`) with a marginal-returns switch to random exploration when the auction's best honest bid collapses |
| `greedy_phased_fiber` | same selection, fiber-floor base model (`--fiber`) |

Other greedy selections (`simulate`, `info_gain`, `risk_gain`) and region
modes (`hard_circle`, `gaussian`, `em_gaussian`, `additive`) are
constructor settings on `Iterative_Greedy_Geolocator` — see CLAUDE.md for
their semantics and measured trade-offs.

### Estimation methods

Strategies that don't carry their own estimator are scored through a
converter (`convert_measurements`), set per instance
(`Random_Geolocator(converter_mode=...)`) or globally (`--converter-mode`):

| Mode | Estimate |
|---|---|
| `nearest_neighbor` | location of the lowest-RTT VP (dumb strong baseline) |
| `hard_circle` | feasible-circle overlap (validity cliff; see CLAUDE.md) |
| `gaussian` | MAP under rtt = 1.3·d/100 + N(0, 15ms) |
| `em_gaussian` / `em_asymmetric` | per-target online EM of (μ, σ), optional one-sided noise |
| `additive_em` | cross-target two-way model rtt = base + X_src + X_dst (batch, params-first) |

### Base RTT model (the distance term)

`rtt_model` swaps the geodesic `d/100` base term everywhere
(`probabilistic_helpers.RttModel`); `--fiber` builds a
`FiberFloorRtt` over the internet_gmaps policy-aware fiber atlas
(`--fiber-slope`, default 1.3; open-floor fallback where the transit
policy allows no route). Result (2026-07): breaks the geodesic greedy's
ridge plateau — see `.claude/FIBER_GEOLOCATOR_RESULTS.md`. ⚠️ Fiber runs
grow a per-(VP, country-class) field cache under
`internet_gmaps/data/cache/policy_fields/` — ~250 KB × n_vps × ~40
classes (22 GB at 1000 VPs); it is safe to delete (lazily rebuilt) and
keyed by policy name.

### Outputs

All artifacts are named by the experiment shape `<sources>src_<targets>dst`
(the budget curves are as dense as `--budgets` — pass more points for
smoother curves; cost ≈ one estimation pass per point per strategy):

- `figures/geolocator_results_<shape>.pdf` — average error vs number of
  pings, the objective (override with `--fig-name`)
- `cache/geolocator_run_<shape>.pkl` — full per-target errors per budget
  + sampling metadata (feeds post-hoc slicing and `--replot`)
- `--replot cache/geolocator_run_*.pkl` regenerates the figure + region
  breakdown from a recorded run without recomputing anything
- per-region mean-error table at the final budget (`--no-breakdown` to skip)
- `--floor-sweep-targets 25,100,1000,...` (with `--floor-sweep-sources`,
  default `200,1000,0`=all) computes the full-coverage "perfect" floor —
  NN over the lowest-RTT measured VP, what `smart_perfect` converges to —
  plus the geometric floor (nearest measured VP) per target count.
  Source budgets are best-k (greedy facility location) and targets are
  nested prefixes per seed, so curves are smooth and comparable;
  writes `figures/oracle_floor_sweep.pdf`
- `cache/cached_results_<strategy>_<mode>_<shape>.pkl` — cached baseline
  curves (`random`, `smart_perfect` only); the shape keys the cache, so
  different sampling shapes never mix

## Documentation map

| File | What it covers |
|---|---|
| `SIMULATION_ENVIRONMENT.md` | The research problem, the information boundary (what inference may see — read this first), the two-phase selection/evaluation split, and the model ladder |
| `CLAUDE.md` | Codebase mechanics: every module, mode, invariant, calibrated result and test |
| `internet_gmaps/README.md` | The fiber atlas sub-package: infrastructure graph, floor queries, transit policy, measurement campaign |
| `.claude/FIBER_GEOLOCATOR_RESULTS.md` | Fiber-floor integration results: n=100 verdict, scaling runs, floor-matched world |
| `.claude/HANDOFF_routing_realism.md` | Atlas research agenda: what the shortest-fiber model gets wrong and why |
| `.claude/TODOS.md` | Open work items, priority-ordered |

Figures are generated artifacts (gitignored): experiment figures under
`tests/*.pdf` regenerate with pytest; `figures/` comes from
`assess_geolocators.py` runs.
