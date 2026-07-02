"""
End-to-end test for the ADDITIVE two-way overhead model:

    rtt(s, t) = SOL(d) + X_s + X_t,   X_s ~ N(μ_s, σ_s²),  X_t ~ N(μ_t, σ_t²)

Every source AND every destination has its own overhead mean and noise.
Two destinations per scenario are PATHOLOGICAL (huge μ_t and σ_t —
think broken routing), which is what defeats the per-target multiplicative
EM: a single per-target slope can't separate "far away" from "badly routed",
and a selection algorithm without σ_t keeps wasting pings on hopeless
targets (the budget-sink pathology observed on the real mesh).

The additive estimator (probabilistic_helpers.fit_additive_params) learns
all four parameter sets online from ESTIMATED locations only (honest), by
alternating:

    location step:  MAP per target under current (μ, σ) sums
    parameter step: two-way shrunk decomposition of SOL residuals

Headline claims tested:
(a) the fitted σ̂_t ranks the pathological destinations on top — the model
    literally learns WHICH targets are tough (the signal a selection
    algorithm needs to stop wasting budget on them);
(b) centered μ̂_s tracks the true per-source overheads (gauge-invariant);
(c) additive locations beat the per-target multiplicative EM and the
    constant-slope gaussian on this ground truth;
(d) the parameter-oracle bounds it.

Measurements: 3 repeats per (src, dst) pair — variance decomposition needs
replication, and the real pipeline's rtt lists provide it.
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import pytest
from scipy.optimize import minimize

from feasible_region_maintainer import FeasibleRegion, GAUSSIAN, EM_GAUSSIAN, _normalize_latlon
from probabilistic_helpers import KM_PER_MS, fit_additive_params
from utils import get_distance, LatLon

VP_LOCS: dict[str, LatLon] = {
    'london':    (51.50,  -0.10),
    'paris':     (48.85,   2.35),
    'berlin':    (52.52,  13.41),
    'rome':      (41.90,  12.50),
    'madrid':    (40.42,  -3.70),
    'amsterdam': (52.37,   4.90),
    'warsaw':    (52.23,  21.01),
    'stockholm': (59.33,  18.07),
    'new_york':  (40.71, -74.01),
    'istanbul':  (41.01,  28.97),
}

N_TARGETS = 8
N_PATHOLOGICAL = 2
N_REPS = 3
TARGET_LAT_RANGE = (38.0, 58.0)
TARGET_LON_RANGE = (-8.0, 25.0)

# normal nodes: modest overheads; pathological destinations: huge ones
SRC_MU_RANGE = (1.0, 10.0)
SRC_SIGMA_RANGE = (0.3, 2.0)
DST_MU_RANGE = (1.0, 10.0)
DST_SIGMA_RANGE = (0.3, 2.0)
PATH_MU_RANGE = (25.0, 60.0)
PATH_SIGMA_RANGE = (12.0, 30.0)

EM_OUTER_ITERS = 8


def make_additive_scenario(seed: int) -> dict:
    rng = np.random.default_rng(seed + 3_000_000)

    sources = {
        s: {'mu': float(rng.uniform(*SRC_MU_RANGE)),
            'sigma': float(rng.uniform(*SRC_SIGMA_RANGE))}
        for s in VP_LOCS
    }
    targets = {}
    for i in range(N_TARGETS):
        pathological = i < N_PATHOLOGICAL
        targets[f'target_{i}'] = {
            'loc': (float(rng.uniform(*TARGET_LAT_RANGE)),
                    float(rng.uniform(*TARGET_LON_RANGE))),
            'mu': float(rng.uniform(*(PATH_MU_RANGE if pathological else DST_MU_RANGE))),
            'sigma': float(rng.uniform(*(PATH_SIGMA_RANGE if pathological else DST_SIGMA_RANGE))),
            'pathological': pathological,
        }

    rtts: dict[tuple, list[float]] = {}
    for s, sp in sources.items():
        for t, tp in targets.items():
            sol = get_distance(VP_LOCS[s], tp['loc']) / KM_PER_MS
            rtts[(s, t)] = [
                sol
                + max(0.0, float(rng.normal(sp['mu'], sp['sigma'])))
                + max(0.0, float(rng.normal(tp['mu'], tp['sigma'])))
                for _ in range(N_REPS)
            ]
    return {'sources': sources, 'targets': targets, 'rtts': rtts}


def _map_location(constraint_rows, starts: list[LatLon]) -> LatLon:
    """MAP under the additive model: constraint_rows are
    (vp_loc, rtt, mean_offset, var_sum) — expected rtt = d/100 + mean_offset.
    Nelder-Mead is local, so try several starts and keep the best (protects
    against early-EM estimates trapping later iterations)."""
    def nll(x):
        total = 0.0
        for vp_loc, rtt, mean_off, var_sum in constraint_rows:
            r = rtt - get_distance((x[0], x[1]), vp_loc) / KM_PER_MS - mean_off
            total += r * r / (2.0 * var_sum)
        return total

    best, best_val = None, float('inf')
    for start in starts:
        res = minimize(nll, np.array(start), method='Nelder-Mead',
                       tol=1e-4, options={'maxiter': 500})
        if res.fun < best_val:
            best, best_val = res.x, res.fun
    return _normalize_latlon(best[0], best[1])


def run_additive_em(scenario: dict) -> dict:
    """Alternating location/parameter fit; honest (uses estimated locations
    only). Returns estimates and fitted parameters."""
    rtts = scenario['rtts']
    targets = list(scenario['targets'])
    sources = list(scenario['sources'])

    mu_s = {s: 5.0 for s in sources}
    var_s = {s: 25.0 for s in sources}
    mu_t = {t: 5.0 for t in targets}
    var_t = {t: 25.0 for t in targets}
    # Initialise each target at its lowest-RTT VP (the NN guess) — far
    # better-behaved than a fixed centroid for the first location step.
    estimates = {
        t: VP_LOCS[min(sources, key=lambda s: min(rtts[(s, t)]))]
        for t in targets
    }

    for _ in range(EM_OUTER_ITERS):
        # Parameter step FIRST: with the location step first, the initial
        # prior μ̂_t (small) makes the optimiser absorb a pathological
        # target's huge offset into DISTANCE (thousands of km of location
        # error), after which residuals look small and μ̂_t never recovers —
        # a self-consistent wrong fixed point. Fitting parameters against
        # the NN-anchored locations first lets μ̂_t claim the offset before
        # the location can.
        residuals = {
            (s, t): [rtt - get_distance(VP_LOCS[s], estimates[t]) / KM_PER_MS
                     for rtt in rtts[(s, t)]]
            for s in sources for t in targets
        }
        mu_s, var_s, mu_t, var_t = fit_additive_params(residuals)

        for t in targets:
            rows = []
            for s in sources:
                for rtt in rtts[(s, t)]:
                    rows.append((VP_LOCS[s], rtt, mu_s[s] + mu_t[t],
                                 var_s[s] + var_t[t]))
            nn_start = VP_LOCS[min(sources, key=lambda s: min(rtts[(s, t)]))]
            estimates[t] = _map_location(rows, [estimates[t], nn_start])

    return {'estimates': estimates, 'mu_s': mu_s, 'var_s': var_s,
            'mu_t': mu_t, 'var_t': var_t}


def run_oracle(scenario: dict) -> dict:
    """Location MAP with the TRUE per-node parameters (explicit cheat)."""
    estimates = {}
    for t, tp in scenario['targets'].items():
        rows = []
        for s, sp in scenario['sources'].items():
            for rtt in scenario['rtts'][(s, t)]:
                rows.append((VP_LOCS[s], rtt, sp['mu'] + tp['mu'],
                             sp['sigma'] ** 2 + tp['sigma'] ** 2))
        estimates[t] = _map_location(rows, [(48.0, 10.0),
                                            VP_LOCS[min(scenario['sources'],
                                                        key=lambda s2: min(scenario['rtts'][(s2, t)]))]])
    return {'estimates': estimates}


def run_baseline(scenario: dict, mode: str) -> dict:
    """FeasibleRegion baselines (const gaussian / per-target em) on the
    per-pair min RTT, as the production converter would use them."""
    estimates = {}
    for t in scenario['targets']:
        region = FeasibleRegion(t, mode=mode)
        for s in scenario['sources']:
            region.add_measurement(VP_LOCS[s], min(scenario['rtts'][(s, t)]))
        estimates[t] = region.get_location()
    return {'estimates': estimates}


def run_additive_seed(seed: int) -> dict:
    sc = make_additive_scenario(seed)

    def errs(est):
        return {t: get_distance(est[t], sc['targets'][t]['loc'])
                for t in sc['targets']}

    additive = run_additive_em(sc)
    out = {
        'scenario': sc,
        'additive': additive,
        'errors': {
            'const_gaussian': errs(run_baseline(sc, GAUSSIAN)['estimates']),
            'per_target_em':  errs(run_baseline(sc, EM_GAUSSIAN)['estimates']),
            'additive_em':    errs(additive['estimates']),
            'oracle':         errs(run_oracle(sc)['estimates']),
        },
    }
    return out


N_ADDITIVE_SEEDS = 12

@pytest.fixture(scope='module')
def additive_results():
    return [run_additive_seed(seed) for seed in range(N_ADDITIVE_SEEDS)]


def _mean_err(rows, key):
    return float(np.mean([np.mean(list(r['errors'][key].values())) for r in rows]))


class TestAdditiveModel:
    def test_summary(self, additive_results):
        for key in ('const_gaussian', 'per_target_em', 'additive_em', 'oracle'):
            print(f"\n{key:16s} mean err = {_mean_err(additive_results, key):7.1f} km")

    def test_learns_which_destinations_are_tough(self, additive_results):
        """THE claim: fitted σ̂_t puts the pathological destinations on top —
        the signal a selection algorithm needs to stop sinking budget."""
        hits = 0
        for r in additive_results:
            fitted = r['additive']['var_t']
            top = sorted(fitted, key=fitted.get, reverse=True)[:N_PATHOLOGICAL]
            truth = {t for t, tp in r['scenario']['targets'].items()
                     if tp['pathological']}
            hits += len(set(top) & truth)
        rate = hits / (N_ADDITIVE_SEEDS * N_PATHOLOGICAL)
        assert rate >= 0.9, f"pathological targets in top-σ̂ only {100*rate:.0f}%"

    def test_source_offsets_recovered(self, additive_results):
        """Centered μ̂_s tracks true per-source overheads (gauge-invariant)."""
        cors = []
        for r in additive_results:
            true = np.array([r['scenario']['sources'][s]['mu'] for s in VP_LOCS])
            fit = np.array([r['additive']['mu_s'][s] for s in VP_LOCS])
            cors.append(np.corrcoef(true - true.mean(), fit - fit.mean())[0, 1])
        # gauge + only 8 targets of pooling limit this; calibrated 0.67
        assert float(np.median(cors)) > 0.5

    def test_additive_beats_per_target_em(self, additive_results):
        """Calibrated: 512 vs 1149 km mean — the additive decomposition
        roughly halves the per-target multiplicative EM's error here."""
        assert _mean_err(additive_results, 'additive_em') < \
            0.65 * _mean_err(additive_results, 'per_target_em')

    def test_additive_beats_constant_gaussian(self, additive_results):
        assert _mean_err(additive_results, 'additive_em') < \
            0.65 * _mean_err(additive_results, 'const_gaussian')

    def test_oracle_bounds_additive(self, additive_results):
        assert _mean_err(additive_results, 'oracle') <= \
            _mean_err(additive_results, 'additive_em')


# ===========================================================================
# Budget sweep — the error_over_measurements companion under the ADDITIVE
# world (per-destination μ/σ unknown to every estimator)
# ===========================================================================
#
# Same random measurement ORDER for every strategy (pings = (src, dst, rep)
# triples); strategies differ only in estimation. The additive EM is the
# only one whose model class can represent this world, so it should win
# once enough cross-target data has accumulated.

MISSING_PENALTY_KM = 10_000.0
TOTAL_PINGS = len(VP_LOCS) * N_TARGETS * N_REPS          # 240
BUDGET_GRID = (10, 20, 30, 45, 60, 90, 120, 160, 200, 240)
SWEEP_STRATEGIES = ('random_nn', 'const_gaussian', 'per_target_em',
                    'additive_em', 'oracle')


def run_additive_budget_seed(seed: int) -> dict:
    sc = make_additive_scenario(seed)
    rng = np.random.default_rng(seed + 4_000_000)
    pings = [(s, t, i) for s in sc['sources'] for t in sc['targets']
             for i in range(N_REPS)]
    rng.shuffle(pings)

    seen: dict[tuple, list[float]] = {}
    curves = {name: [] for name in SWEEP_STRATEGIES}


    def avg_err(estimates: dict) -> float:
        return float(np.mean([
            get_distance(estimates[t], tp['loc']) if t in estimates
            else MISSING_PENALTY_KM
            for t, tp in sc['targets'].items()
        ]))

    k = 0
    for b in BUDGET_GRID:
        while k < b:
            s, t, i = pings[k]
            seen.setdefault((s, t), []).append(sc['rtts'][(s, t)][i])
            k += 1

        # nearest neighbour
        nn_est = {}
        for t in sc['targets']:
            pairs_t = {s2: min(v) for (s2, t2), v in seen.items() if t2 == t}
            if pairs_t:
                nn_est[t] = VP_LOCS[min(pairs_t, key=pairs_t.get)]
        curves['random_nn'].append(avg_err(nn_est))

        # FeasibleRegion baselines (rebuilt on per-pair min, as production)
        for label, mode in (('const_gaussian', GAUSSIAN),
                            ('per_target_em', EM_GAUSSIAN)):
            est = {}
            for t in sc['targets']:
                region = FeasibleRegion(t, mode=mode)
                for (s2, t2), v in seen.items():
                    if t2 == t:
                        region.add_measurement(VP_LOCS[s2], min(v))
                if region.constraints:
                    est[t] = region.get_location()
            curves[label].append(avg_err(est))

        # additive EM (all seen samples) — FRESH batch-style fit at each
        # budget point (NN-anchored inits, parameters-first). Warm-starting
        # across budget points carries early-budget wrong fixed points
        # forward and degraded full-budget error ~2×.
        measured = {t2 for (_, t2) in seen}
        add_est = {t2: nn_est[t2] for t2 in measured}
        for _ in range(4):
            residuals = {
                (s2, t2): [r - get_distance(VP_LOCS[s2], add_est[t2]) / KM_PER_MS
                           for r in v]
                for (s2, t2), v in seen.items()
            }
            mu_s, var_s, mu_t, var_t = fit_additive_params(residuals)
            for t2 in measured:
                rows = [(VP_LOCS[s2], r, mu_s[s2] + mu_t[t2],
                         var_s[s2] + var_t[t2])
                        for (s2, tt), v in seen.items() if tt == t2
                        for r in v]
                add_est[t2] = _map_location(rows, [add_est[t2], nn_est[t2]])
        curves['additive_em'].append(avg_err(add_est))

        # oracle (true per-node params)
        orc = {}
        for t2 in measured:
            tp = sc['targets'][t2]
            rows = [(VP_LOCS[s2], r,
                     sc['sources'][s2]['mu'] + tp['mu'],
                     sc['sources'][s2]['sigma'] ** 2 + tp['sigma'] ** 2)
                    for (s2, tt), v in seen.items() if tt == t2
                    for r in v]
            orc[t2] = _map_location(rows, [nn_est[t2], (48.0, 10.0)])
        curves['oracle'].append(avg_err(orc))

    return {'scenario': sc, 'curves': curves}


N_SWEEP_SEEDS = 10

@pytest.fixture(scope='module')
def sweep_results():
    return [run_additive_budget_seed(seed) for seed in range(N_SWEEP_SEEDS)]


def _sweep_med(rows, strategy, budget):
    i = BUDGET_GRID.index(budget)
    return float(np.median([r['curves'][strategy][i] for r in rows]))


class TestAdditiveBudgetSweep:
    def test_summary(self, sweep_results):
        for b in (30, 120, 240):
            line = "  ".join(f"{s}={_sweep_med(sweep_results, s, b):7.1f}"
                             for s in SWEEP_STRATEGIES)
            print(f"\nb={b:3d}: {line}")

    def test_additive_wins_at_full_budget(self, sweep_results):
        """The only model class that can represent this world should win it."""
        add = _sweep_med(sweep_results, 'additive_em', 240)
        for other in ('random_nn', 'const_gaussian', 'per_target_em'):
            assert add < _sweep_med(sweep_results, other, 240)

    def test_additive_beats_per_target_em_from_mid_budget(self, sweep_results):
        assert _sweep_med(sweep_results, 'additive_em', 120) < \
            _sweep_med(sweep_results, 'per_target_em', 120)

    def test_oracle_bounds_additive_at_full_budget(self, sweep_results):
        assert _sweep_med(sweep_results, 'oracle', 240) <= \
            _sweep_med(sweep_results, 'additive_em', 240)

    def test_generate_figure(self, sweep_results):
        """Renders tests/error_over_measurements_additive.pdf from the same
        curves the assertions above checked."""
        from plot_error_additive import make_figure, OUT_PATH
        path = make_figure(sweep_results, OUT_PATH)
        assert os.path.exists(path)
        assert os.path.getsize(path) > 5_000
