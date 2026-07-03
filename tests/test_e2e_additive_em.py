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

Measurements: ONE sample per (src, dst) pair, like the real mesh. A
practical "ping" is already the min of ~3 probes — that repetition exists
to strip QUEUEING delay, which is not what X_src/X_dst model: they are
per-node path inefficiency, a property of the (src, dst) routing that
repeating the measurement would not average away. Simulating replicates
and min-taking here would let estimators exploit noise structure that
does not exist in practice. Variance decomposition instead pools
single-sample residuals across the ~n pairs touching each node.
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import pytest
from scipy.optimize import minimize

from feasible_region_maintainer import (
    FeasibleRegion, GAUSSIAN, EM_GAUSSIAN, ADDITIVE, _normalize_latlon,
)
from iterative_greedy_geolocator import (
    Iterative_Greedy_Geolocator, MARGINAL_SWITCH_KM,
)
from perfect_geolocator import Perfect_Geolocator
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

    # One sample per pair, held in a 1-element list (the pipeline's
    # rtt-list format).
    rtts: dict[tuple, list[float]] = {}
    for s, sp in sources.items():
        for t, tp in targets.items():
            sol = get_distance(VP_LOCS[s], tp['loc']) / KM_PER_MS
            rtts[(s, t)] = [
                sol
                + max(0.0, float(rng.normal(sp['mu'], sp['sigma'])))
                + max(0.0, float(rng.normal(tp['mu'], tp['sigma'])))
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


def run_param_oracle(scenario: dict) -> dict:
    """PARAMETER-oracle estimation bound: location MAP with the TRUE
    per-node (μ, σ) on the FULL data (explicit cheat). No selection is
    involved here — every estimator in this batch comparison sees every
    pair. The selection oracle everywhere else is Perfect_Geolocator."""
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
            'oracle':         errs(run_param_oracle(sc)['estimates']),
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
        # gauge + only 8 targets of pooling limit this; calibrated 0.66
        assert float(np.median(cors)) > 0.5

    def test_additive_beats_per_target_em(self, additive_results):
        """Calibrated: 917 vs 1647 km mean (single sample per pair) — the
        additive decomposition still buys ~45% over the per-target
        multiplicative EM."""
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
# Same random measurement ORDER for every strategy (pings = (src, dst)
# pairs, one sample each); strategies differ only in estimation. The
# oracle cheats on BOTH halves: Perfect_Geolocator selection (error-guided
# greedy on ground truth) + MAP with the true per-node (μ, σ) — a
# whole-system upper bound that no honest strategy should beat.
# All stats are means across seeds of the per-seed avg-over-targets error
# (matching the figure and assess_geolocators.run()).
#
# The additive EM is the only model class that can represent this world
# and wins among the MODEL-BASED estimators once cross-target data has
# accumulated. Nearest-neighbour keeps the full-coverage lead in this
# small single-sample synthetic (646 vs 706 mean at b=80) — with 10 VPs
# there is only ~8-10 pairs of pooling per node, and NN error is bounded
# by VP density. On the real n=20 mesh (19 pairs/node) the additive
# estimator DOES beat NN outright; see assess_additive_real.py numbers in
# CLAUDE.md.
#
# greedy_additive is the exception: it SELECTS its own measurement order
# (Iterative_Greedy_Geolocator with additive-mode regions sharing one
# AdditiveLatencyModel); one greedy selection = one pair = one budget unit,
# same as the random order.

MISSING_PENALTY_KM = 10_000.0
TOTAL_PINGS = len(VP_LOCS) * N_TARGETS                   # 80
BUDGET_GRID = (5, 10, 15, 20, 30, 40, 50, 60, 70, 80)
SWEEP_STRATEGIES = ('random_nn', 'const_gaussian', 'per_target_em',
                    'additive_em', 'greedy_additive', 'greedy_additive_info',
                    'greedy_additive_risk', 'greedy_additive_phased', 'oracle')


def run_greedy_additive_seed(sc: dict, selection: str = 'simulate') -> tuple[list[float], dict]:
    """Greedy selection + additive estimation over BUDGET_GRID.
    Returns (error curve, cumulative pathological ping share per budget)."""
    loc_loc_meas: dict[str, dict[str, list[float]]] = {}
    for (s, t), rtts in sc['rtts'].items():
        loc_loc_meas.setdefault(s, {})[t] = list(rtts)
    data = {'address_to_loc': dict(VP_LOCS), 'loc_loc_meas': loc_loc_meas}
    patho = {t for t, tp in sc['targets'].items() if tp['pathological']}

    ig = Iterative_Greedy_Geolocator(max_workers=1, region_mode=ADDITIVE,
                                     selection=selection)
    ig.set_data(data)
    ig.solve()
    errs, patho_share = [], {}
    try:
        for b in BUDGET_GRID:
            ig.measurements(b)             # extends history incrementally
            est = ig.get_current_estimates()
            errs.append(float(np.mean([
                get_distance(est[t], tp['loc']) if t in est
                else MISSING_PENALTY_KM
                for t, tp in sc['targets'].items()
            ])))
            n_hist = len(ig.measurement_history)
            patho_share[b] = (
                sum(1 for _, t in ig.measurement_history if t in patho)
                / max(n_hist, 1))
    finally:
        ig.cleanup()
    return errs, patho_share


def oracle_measurement_order(sc: dict) -> list[tuple[str, str]]:
    """Selection order from Perfect_Geolocator — THE selection-oracle
    implementation (the same one assess_geolocators runs; no parallel
    reimplementations). Its licensed cheat is ground truth in
    address_to_loc, which guides its error-driven greedy choices. An
    oracle must cheat on BOTH halves — selection and estimation — or it
    is not an upper bound and honest strategies can legitimately beat it."""
    cheat_locs = dict(VP_LOCS)
    cheat_locs.update({t: tp['loc'] for t, tp in sc['targets'].items()})
    loc_loc_meas: dict[str, dict[str, list[float]]] = {}
    for (s, t), rs in sc['rtts'].items():
        loc_loc_meas.setdefault(s, {})[t] = list(rs)
    pg = Perfect_Geolocator()
    pg.set_data({'address_to_loc': cheat_locs, 'loc_loc_meas': loc_loc_meas})
    return pg.measurement_order


def run_additive_budget_seed(seed: int) -> dict:
    sc = make_additive_scenario(seed)
    rng = np.random.default_rng(seed + 4_000_000)
    pings = [(s, t) for s in sc['sources'] for t in sc['targets']]
    rng.shuffle(pings)

    oracle_order = oracle_measurement_order(sc)

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
            s, t = pings[k]
            seen[(s, t)] = list(sc['rtts'][(s, t)])
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

        # oracle: true per-node params on its own (cheating) selection order
        orc_by_t: dict[str, list[str]] = {}
        for s2, t2 in oracle_order[:b]:
            orc_by_t.setdefault(t2, []).append(s2)
        orc = {}
        for t2, srcs in orc_by_t.items():
            tp = sc['targets'][t2]
            rows = [(VP_LOCS[s2], r,
                     sc['sources'][s2]['mu'] + tp['mu'],
                     sc['sources'][s2]['sigma'] ** 2 + tp['sigma'] ** 2)
                    for s2 in srcs for r in sc['rtts'][(s2, t2)]]
            nn0 = VP_LOCS[min(srcs, key=lambda s2: min(sc['rtts'][(s2, t2)]))]
            orc[t2] = _map_location(rows, [nn0, (48.0, 10.0)])
        curves['oracle'].append(avg_err(orc))

    curves['greedy_additive'], patho_share = run_greedy_additive_seed(sc)
    curves['greedy_additive_info'], info_patho_share = \
        run_greedy_additive_seed(sc, selection='info_gain')
    curves['greedy_additive_risk'], risk_patho_share = \
        run_greedy_additive_seed(sc, selection='risk_gain')
    curves['greedy_additive_phased'], _ = \
        run_greedy_additive_seed(sc, selection='phased')

    return {'scenario': sc, 'curves': curves,
            'greedy_patho_share': patho_share,
            'greedy_info_patho_share': info_patho_share,
            'greedy_risk_patho_share': risk_patho_share}


N_SWEEP_SEEDS = 10

@pytest.fixture(scope='module')
def sweep_results():
    return [run_additive_budget_seed(seed) for seed in range(N_SWEEP_SEEDS)]


def _sweep_mean(rows, strategy, budget):
    """Mean across seeds of the per-seed avg-over-targets error — same
    statistic the figure plots and assess_geolocators.run() reports."""
    i = BUDGET_GRID.index(budget)
    return float(np.mean([r['curves'][strategy][i] for r in rows]))


class TestAdditiveBudgetSweep:
    def test_summary(self, sweep_results):
        for b in (10, 40, 80):
            line = "  ".join(f"{s}={_sweep_mean(sweep_results, s, b):7.1f}"
                             for s in SWEEP_STRATEGIES)
            print(f"\nb={b:3d}: {line}")

    def test_additive_wins_among_models_at_full_budget(self, sweep_results):
        """Best MODEL-BASED estimator at full coverage (706 vs 1686/1702).
        NN (646) stays ahead in this small single-sample synthetic — see the
        section comment; the real n=20 mesh flips that."""
        add = _sweep_mean(sweep_results, 'additive_em', 80)
        for other in ('const_gaussian', 'per_target_em'):
            assert add < _sweep_mean(sweep_results, other, 80)

    def test_additive_beats_per_target_em_from_mid_budget(self, sweep_results):
        assert _sweep_mean(sweep_results, 'additive_em', 40) < \
            _sweep_mean(sweep_results, 'per_target_em', 40)

    def test_oracle_bounds_additive_at_full_budget(self, sweep_results):
        assert _sweep_mean(sweep_results, 'oracle', 80) <= \
            _sweep_mean(sweep_results, 'additive_em', 80)

    # -- greedy_additive: selection + estimation as one system --------------
    # Calibrated means (10 seeds): greedy 1932 → 1129 → 706 over
    # b = 10/40/80 vs random-order additive_em's 4281 → 1172 → 706 and
    # random_nn's 3475 → 752 → 646; oracle 676 → 390 → 374 bounds all of
    # it. Selection dominates the early regime; at full coverage the
    # greedy's batch polish IS the additive_em estimator on identical
    # data, hence the exactly matching 706.
    #
    # greedy_additive_info (hypothesis-set info-gain selection): 1991 →
    # 1077 → 706 — parity here BY DESIGN: these targets sit inside the VP
    # span, so there are no ridges to exploit. Its home turf is
    # TestRidgeEscape below; this world pins "no harm".

    def test_greedy_selection_beats_random_order_early(self, sweep_results):
        """The point of selection: at b=10 the greedy places its pings
        better than the shared random order feeds ANY honest estimator
        (1932 vs additive_em's 4281 and random+NN's 3475)."""
        assert _sweep_mean(sweep_results, 'greedy_additive', 10) < \
            0.6 * _sweep_mean(sweep_results, 'additive_em', 10)
        assert _sweep_mean(sweep_results, 'greedy_additive', 10) < \
            0.6 * _sweep_mean(sweep_results, 'random_nn', 10)

    def test_oracle_dominates_every_strategy_at_every_budget(self, sweep_results):
        """An oracle that cheats on both selection and estimation must be
        an upper bound everywhere — if an honest strategy beat it, the
        oracle should have picked whatever that strategy picked. (2%
        slack: MAP location is multi-start Nelder-Mead, not exact.)"""
        for b in BUDGET_GRID:
            orc = _sweep_mean(sweep_results, 'oracle', b)
            for s in SWEEP_STRATEGIES:
                if s == 'oracle':
                    continue
                assert orc <= 1.02 * _sweep_mean(sweep_results, s, b), \
                    f"oracle {orc:.0f} beaten by {s} at b={b}"

    def test_greedy_matches_additive_em_at_full_budget(self, sweep_results):
        """At full coverage both have seen every pair, and the greedy's
        final batch polish is the same estimator — no estimation premium
        for having selected greedily."""
        assert _sweep_mean(sweep_results, 'greedy_additive', 80) <= \
            1.02 * _sweep_mean(sweep_results, 'additive_em', 80)

    def test_info_gain_no_harm_in_ridge_free_world(self, sweep_results):
        """Info-gain selection must not cost anything where there is
        nothing to explore (calibrated: 1955/1124/706 vs simulate's
        1932/1129/706)."""
        for b in (10, 40, 80):
            assert _sweep_mean(sweep_results, 'greedy_additive_info', b) <= \
                1.10 * _sweep_mean(sweep_results, 'greedy_additive', b)

    def test_risk_gain_bounded_premium_in_gaussian_world(self, sweep_results):
        """Calibrated 1948/1325/706: risk adjustment declines the
        pathological targets, whose promises DO partially pay out under
        genuinely gaussian noise (their floor shrinks as 1/sqrt(k)) — so
        it pays a bounded mid-budget premium here (1325 vs simulate's
        1129 at b=40). That is the honest cost of not trusting promises;
        the real mesh, where those promises do NOT pay, is where it
        collects (see TestUncuttableRidge). Full coverage is identical by
        construction (shared batch polish)."""
        for b in (10, 40, 80):
            assert _sweep_mean(sweep_results, 'greedy_additive_risk', b) <= \
                1.25 * _sweep_mean(sweep_results, 'greedy_additive', b)
        assert _sweep_mean(sweep_results, 'greedy_additive_risk', 80) <= \
            1.02 * _sweep_mean(sweep_results, 'additive_em', 80)

    def test_phased_fixes_risk_mid_budget_premium(self, sweep_results):
        """THE controlled-environment demonstration of the phase switch
        (promise-based: explore when the auction's top risk-adjusted bid
        collapses, which happens at ping ~12 here). Risk-adjustment's
        mid-budget premium — it declines the pathological targets whose
        gaussian promises would actually pay — is exactly a dead zone of
        collapsed bids, and exploration fills it: calibrated means
        1948 / 920 / 706 vs risk's 1948 / 1325 / 706. At b=40 phased is
        the best strategy in the figure, beating even the random-order
        additive_em (1172)."""
        assert _sweep_mean(sweep_results, 'greedy_additive_phased', 40) <= \
            0.85 * _sweep_mean(sweep_results, 'greedy_additive_risk', 40)
        for b in (10, 80):
            assert _sweep_mean(sweep_results, 'greedy_additive_phased', b) <= \
                1.05 * _sweep_mean(sweep_results, 'greedy_additive_risk', b)

    def test_greedy_does_not_sink_budget_into_pathological_targets(self, sweep_results):
        """THE payoff of σ̂_dst in the utility (trust-discounted gain): the
        pathological targets' share of greedy pings stays near their fair
        share (N_PATHOLOGICAL/N_TARGETS = 0.25) at partial budget.
        Calibrated: median 0.29, max 0.375. Reference sinks in the same
        single-sample scenarios: em-greedy takes 0.33-0.50 (median ~0.41);
        the real mesh showed one target absorbing 52 pings (median 3)."""
        shares = [r['greedy_patho_share'][40] for r in sweep_results]
        assert float(np.median(shares)) <= 0.32
        assert max(shares) <= 0.40

    def test_generate_figure(self, sweep_results):
        """Renders tests/error_over_measurements_additive.pdf from the same
        curves the assertions above checked."""
        from plot_error_additive import make_figure, OUT_PATH
        path = make_figure(sweep_results, OUT_PATH)
        assert os.path.exists(path)
        assert os.path.getsize(path) > 5_000


# ===========================================================================
# Ridge escape — the regression test for the real-mesh selection failure
# ===========================================================================
#
# Miniature of the measured pathology: a VP cluster (Europe) plus ONE far
# target with ONE lone VP near it. From the cluster alone, the target's
# offset and its distance are exactly confounded (a flat likelihood ridge);
# the lone VP is the only measurement that collapses the ridge. Measured on
# the real mesh: the simulate utility scored ~90 candidates within 0.35% of
# each other and ranked the 7 km / 0.7 ms VP #9 — it was never pinged and
# the target finished 12,450 km wrong. Info-gain selection scores candidates
# by hypothesis disagreement, which is exactly "how much of the ridge does
# this ping destroy" — the lone VP should dominate.

RIDGE_VP_LOCS: dict[str, LatLon] = {
    'london':    (51.50,  -0.10),
    'paris':     (48.85,   2.35),
    'berlin':    (52.52,  13.41),
    'madrid':    (40.42,  -3.70),
    'warsaw':    (52.23,  21.01),
    'rome':      (41.90,  12.50),
    # lone VP near the far target — inserted LAST so dict/list ordering
    # never gifts it a free first ping
    'kochi':     (9.93,  76.26),
}
RIDGE_FAR_TARGET_LOC = (13.01, 74.80)     # ~Mangalore, 460 km from kochi
RIDGE_N_EUR_TARGETS = 3
RIDGE_BUDGET = 16                         # 4 targets x 7 VPs = 28 pairs max


def make_ridge_scenario(seed: int) -> dict:
    rng = np.random.default_rng(seed + 5_000_000)
    targets = {'far_0': {'loc': RIDGE_FAR_TARGET_LOC}}
    for i in range(RIDGE_N_EUR_TARGETS):
        targets[f'eur_{i}'] = {
            'loc': (float(rng.uniform(*TARGET_LAT_RANGE)),
                    float(rng.uniform(*TARGET_LON_RANGE)))}
    for tp in targets.values():
        tp['mu'] = float(rng.uniform(*DST_MU_RANGE))
        tp['sigma'] = float(rng.uniform(*DST_SIGMA_RANGE))
    sources = {
        s: {'mu': float(rng.uniform(*SRC_MU_RANGE)),
            'sigma': float(rng.uniform(*SRC_SIGMA_RANGE))}
        for s in RIDGE_VP_LOCS
    }
    loc_loc_meas: dict[str, dict[str, list[float]]] = {}
    for s, sp in sources.items():
        loc_loc_meas[s] = {}
        for t, tp in targets.items():
            sol = get_distance(RIDGE_VP_LOCS[s], tp['loc']) / KM_PER_MS
            loc_loc_meas[s][t] = [
                sol
                + max(0.0, float(rng.normal(sp['mu'], sp['sigma'])))
                + max(0.0, float(rng.normal(tp['mu'], tp['sigma'])))]
    data = {'address_to_loc': dict(RIDGE_VP_LOCS), 'loc_loc_meas': loc_loc_meas}
    return {'targets': targets, 'data': data}


def run_ridge_seed(seed: int, selection: str) -> dict:
    sc = make_ridge_scenario(seed)
    ig = Iterative_Greedy_Geolocator(max_workers=1, region_mode=ADDITIVE,
                                     selection=selection)
    ig.set_data(sc['data'])
    ig.solve()
    ig.measurements(RIDGE_BUDGET)
    est = ig.get_current_estimates()
    far_srcs = [s for s, t in ig.measurement_history if t == 'far_0']
    ig.cleanup()
    return {
        'kochi_pos': (far_srcs.index('kochi') + 1
                      if 'kochi' in far_srcs else None),   # 1-based, in far_0's pings
        'far_pings': len(far_srcs),
        'far_err': get_distance(est['far_0'], RIDGE_FAR_TARGET_LOC),
    }


N_RIDGE_SEEDS = 10


@pytest.fixture(scope='module')
def ridge_results():
    return {sel: [run_ridge_seed(seed, sel) for seed in range(N_RIDGE_SEEDS)]
            for sel in ('simulate', 'info_gain', 'risk_gain')}


class TestRidgeEscape:
    def test_summary(self, ridge_results):
        for sel, rows in ridge_results.items():
            found = [r['kochi_pos'] for r in rows]
            errs = [r['far_err'] for r in rows]
            print(f"\n{sel:9s}: kochi_pos={found}  "
                  f"far_err med={np.median(errs):7.1f} max={max(errs):8.1f}")

    def test_info_gain_finds_the_lone_vp_early(self, ridge_results):
        """The discriminating VP should be among far_0's first pings —
        hypothesis disagreement makes it score far above cluster VPs."""
        rows = ridge_results['info_gain']
        hits = sum(1 for r in rows
                   if r['kochi_pos'] is not None and r['kochi_pos'] <= 3)
        assert hits >= 0.8 * N_RIDGE_SEEDS

    def test_info_gain_solves_the_far_target(self, ridge_results):
        """Calibrated median 477 km — essentially the 460 km distance from
        kochi to the target, i.e. the best any method could do from these
        VPs. simulate: 3068 km."""
        errs = [r['far_err'] for r in ridge_results['info_gain']]
        assert float(np.median(errs)) < 700.0

    def test_risk_gain_also_finds_the_lone_vp(self, ridge_results):
        """Risk adjustment must NOT cost the ridge escape: the lone VP
        helps under EVERY hypothesis (high mean, LOW variance benefit), so
        its 25th-percentile benefit stays dominant."""
        rows = ridge_results['risk_gain']
        hits = sum(1 for r in rows
                   if r['kochi_pos'] is not None and r['kochi_pos'] <= 3)
        errs = [r['far_err'] for r in rows]
        assert hits >= 0.8 * N_RIDGE_SEEDS
        assert float(np.median(errs)) < 700.0

    def test_simulate_utility_is_ridge_blind(self, ridge_results):
        """Pin the failure this feature fixes: the simulate utility treats
        all candidates alike (measured 0.35% spread on the real mesh), so
        it reaches the lone VP no better than chance and its far-target
        error stays ~ridge-sized. If this ever starts passing the
        info-gain criteria, the simulate path learned geometry — update
        the docs."""
        rows = ridge_results['simulate']
        hits = sum(1 for r in rows
                   if r['kochi_pos'] is not None and r['kochi_pos'] <= 3)
        errs = [r['far_err'] for r in rows]
        assert hits <= 0.5 * N_RIDGE_SEEDS
        assert float(np.median(errs)) > \
            2.0 * float(np.median([r['far_err']
                                   for r in ridge_results['info_gain']]))


# ===========================================================================
# Uncuttable ridge — the flashy-uncertain case risk adjustment must decline
# ===========================================================================
#
# Same geometry as TestRidgeEscape but WITHOUT the lone VP: the far
# target's ambiguity is real but NO available measurement can reduce it
# (every VP sees the ring from its centre). Its per-hypothesis benefits
# are the "2000 ± 5000" shape — large under a lucky hypothesis, ~zero
# otherwise — and its promises never pay out. Mean-benefit selection
# (info_gain) keeps buying them (the measured real-mesh sink: 50-88 pings
# on such targets while the median target got 1-2); the 25th-percentile ×
# track-record value declines them, so budget flows to the reliable
# 500 ± 100 European targets.

UNCUT_VP_LOCS = {k: v for k, v in RIDGE_VP_LOCS.items() if k != 'kochi'}
UNCUT_BUDGET = 16


def run_uncut_seed(seed: int, selection: str) -> dict:
    sc = make_ridge_scenario(seed)
    # strip the lone VP: the far target becomes uncuttable
    data = {'address_to_loc': dict(UNCUT_VP_LOCS),
            'loc_loc_meas': {s: dsts for s, dsts in
                             sc['data']['loc_loc_meas'].items()
                             if s != 'kochi'}}
    ig = Iterative_Greedy_Geolocator(max_workers=1, region_mode=ADDITIVE,
                                     selection=selection)
    ig.set_data(data)
    ig.solve()
    ig.measurements(UNCUT_BUDGET)
    est = ig.get_current_estimates()
    far_pings = sum(1 for _, t in ig.measurement_history if t == 'far_0')
    eur_errs = [get_distance(est[t], tp['loc'])
                for t, tp in sc['targets'].items()
                if t != 'far_0' and t in est]
    ig.cleanup()
    return {'far_pings': far_pings,
            'eur_err': float(np.mean(eur_errs))}


@pytest.fixture(scope='module')
def uncut_results():
    return {sel: [run_uncut_seed(seed, sel) for seed in range(N_RIDGE_SEEDS)]
            for sel in ('info_gain', 'risk_gain')}


class TestUncuttableRidge:
    def test_summary(self, uncut_results):
        for sel, rows in uncut_results.items():
            print(f"\n{sel:9s}: far_pings={[r['far_pings'] for r in rows]}  "
                  f"eur_err med={np.median([r['eur_err'] for r in rows]):7.1f}")

    def test_risk_gain_declines_the_unpayable_promise(self, uncut_results):
        """The far target's budget share under risk_gain stays near fair
        share (16 pings / 4 targets = 4) instead of sinking."""
        risk = [r['far_pings'] for r in uncut_results['risk_gain']]
        info = [r['far_pings'] for r in uncut_results['info_gain']]
        assert float(np.median(risk)) <= 6
        assert float(np.median(risk)) < float(np.median(info))

    def test_phased_switch_mechanism(self):
        """Pin the phase transition on the promise signal: exploit while
        the auction's top risk-adjusted bid is real, explore once it
        collapses. Debugged history (trace in the git log): the first cut
        switched on an EWMA of REALIZED size change, which tie-break
        wobble on noisy targets kept spuriously high — bids collapse at
        ping ~12 in the sweep world, the realized tape never crossed the
        threshold at all. Bids, post-risk-adjustment, are the honest
        marginal-return estimate; realized size change is not."""
        sc = make_additive_scenario(0)
        loc_loc_meas: dict[str, dict[str, list[float]]] = {}
        for (s, t), rtts in sc['rtts'].items():
            loc_loc_meas.setdefault(s, {})[t] = list(rtts)
        ig = Iterative_Greedy_Geolocator(max_workers=1, region_mode=ADDITIVE,
                                         selection='phased')
        ig.set_data({'address_to_loc': dict(VP_LOCS),
                     'loc_loc_meas': loc_loc_meas})
        ig.solve()
        try:
            # coverage/resolution phase: real bids dominate (boundary is
            # fuzzy — bids hover near the threshold around ping ~10)
            ig.measurements(10)
            assert ig.explore_pings <= 2

            # bids collapse at ping ~12 (calibrated trace) → exploration
            # takes over and spends only on unpinged pairs
            ig.measurements(30)
            assert ig.explore_pings >= 10
            assert len(set(ig.measurement_history)) == \
                len(ig.measurement_history)   # never re-pings a pair
        finally:
            ig.cleanup()

    def test_risk_gain_declining_does_not_hurt_reliable_targets(self, uncut_results):
        """Calibrated 838 vs 802 km: at this mini scale the European
        targets are already ping-saturated, so the freed budget buys ~no
        extra accuracy HERE — the payoff of declining shows at real-mesh
        scale, where sinks starved the median target to 1-2 pings. This
        pin only guards that declining costs nothing."""
        risk = float(np.median([r['eur_err'] for r in uncut_results['risk_gain']]))
        info = float(np.median([r['eur_err'] for r in uncut_results['info_gain']]))
        assert risk <= info * 1.10
