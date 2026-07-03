"""
LARGE additive world — the regime-visible companion to
test_e2e_additive_em's small sweep (which stays the fast regression
harness; its 10-VP Europe world is min-of-k-friendly, so random+NN's
estimator dominates it — see the git log discussion).

World: 20 GLOBAL VPs, 100 targets (70% near VP regions, 30% isolated,
10% pathological), one sample per pair (2,000 pairs), budgets 100-1500 =
0.5-15 pings/target. This is the scarcity regime the real mesh lives in:
random needs most of the budget just to cover everyone once, min-of-k
needs many draws to find a near VP among 20, and selection has room to
matter — the dynamics the small world cannot show.

Strategies (trimmed to the informative set; simulate/info greedys are
dominated and expensive at this scale):

    random_nn              random order + NN (the coverage baseline)
    per_target_em          per-target multiplicative EM on the same order
    additive_em            cross-target additive fit on the same order
    greedy_additive_risk   risk-adjusted selection
    greedy_additive_phased risk + promise-collapse -> random exploration
    oracle                 Perfect_Geolocator selection + true params

Writes tests/error_over_measurements_additive_large.pdf.
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import pytest

from feasible_region_maintainer import FeasibleRegion, EM_GAUSSIAN, ADDITIVE
from iterative_greedy_geolocator import Iterative_Greedy_Geolocator
from perfect_geolocator import Perfect_Geolocator
from probabilistic_helpers import KM_PER_MS, additive_batch_em
from test_e2e_additive_em import (
    _map_location, SRC_MU_RANGE, SRC_SIGMA_RANGE, DST_MU_RANGE,
    DST_SIGMA_RANGE, PATH_MU_RANGE, PATH_SIGMA_RANGE, MISSING_PENALTY_KM,
)
from utils import get_distance, LatLon

LARGE_VP_LOCS: dict[str, LatLon] = {
    # Europe-weighted like real meshes, but global
    'london':       (51.50,   -0.10),
    'paris':        (48.85,    2.35),
    'frankfurt':    (50.11,    8.68),
    'madrid':       (40.42,   -3.70),
    'warsaw':       (52.23,   21.01),
    'stockholm':    (59.33,   18.07),
    'bucharest':    (44.43,   26.10),
    'new_york':     (40.71,  -74.01),
    'chicago':      (41.88,  -87.63),
    'los_angeles':  (34.05, -118.24),
    'toronto':      (43.65,  -79.38),
    'mexico_city':  (19.43,  -99.13),
    'tokyo':        (35.68,  139.69),
    'singapore':    (1.35,   103.82),
    'mumbai':       (19.08,   72.88),
    'seoul':        (37.57,  126.98),
    'sao_paulo':    (-23.55,  -46.63),
    'buenos_aires': (-34.60,  -58.38),
    'johannesburg': (-26.20,   28.05),
    'sydney':       (-33.87,  151.21),
}

N_LARGE_TARGETS = 100
LARGE_PATHO_FRAC = 0.10
LARGE_BUDGET_GRID = (100, 200, 350, 500, 750, 1000, 1500)
LARGE_STRATEGIES = ('random_nn', 'per_target_em', 'additive_em',
                    'greedy_additive_risk', 'greedy_additive_phased',
                    'oracle')
N_LARGE_SEEDS = 3


def make_large_scenario(seed: int) -> dict:
    rng = np.random.default_rng(seed + 9_000_000)
    vp_names = list(LARGE_VP_LOCS)
    sources = {
        s: {'mu': float(rng.uniform(*SRC_MU_RANGE)),
            'sigma': float(rng.uniform(*SRC_SIGMA_RANGE))}
        for s in vp_names
    }
    targets = {}
    for i in range(N_LARGE_TARGETS):
        if rng.random() < 0.7:
            # near a VP region (mesh-like clustering)
            base = LARGE_VP_LOCS[vp_names[int(rng.integers(len(vp_names)))]]
            loc = (float(np.clip(base[0] + rng.normal(0, 6.0), -55, 65)),
                   float((base[1] + rng.normal(0, 8.0) + 180) % 360 - 180))
        else:
            # isolated
            loc = (float(rng.uniform(-45, 60)),
                   float(rng.uniform(-180, 180)))
        pathological = rng.random() < LARGE_PATHO_FRAC
        targets[f't{i:03d}'] = {
            'loc': loc,
            'mu': float(rng.uniform(*(PATH_MU_RANGE if pathological
                                      else DST_MU_RANGE))),
            'sigma': float(rng.uniform(*(PATH_SIGMA_RANGE if pathological
                                         else DST_SIGMA_RANGE))),
            'pathological': pathological,
        }

    loc_loc_meas: dict[str, dict[str, list[float]]] = {}
    for s, sp in sources.items():
        loc_loc_meas[s] = {}
        for t, tp in targets.items():
            sol = get_distance(LARGE_VP_LOCS[s], tp['loc']) / KM_PER_MS
            loc_loc_meas[s][t] = [
                sol
                + max(0.0, float(rng.normal(sp['mu'], sp['sigma'])))
                + max(0.0, float(rng.normal(tp['mu'], tp['sigma'])))]
    return {'sources': sources, 'targets': targets,
            'data': {'address_to_loc': dict(LARGE_VP_LOCS),
                     'loc_loc_meas': loc_loc_meas}}


def _avg_err(estimates: dict, targets: dict) -> float:
    return float(np.mean([
        get_distance(estimates[t], tp['loc']) if t in estimates
        else MISSING_PENALTY_KM
        for t, tp in targets.items()]))


def run_large_seed(seed: int) -> dict:
    sc = make_large_scenario(seed)
    llm = sc['data']['loc_loc_meas']
    rng = np.random.default_rng(seed + 10_000_000)
    pings = [(s, t) for s in sc['sources'] for t in sc['targets']]
    rng.shuffle(pings)

    curves = {name: [] for name in LARGE_STRATEGIES}

    # oracle selection order: THE selection-oracle implementation
    cheat_locs = dict(LARGE_VP_LOCS)
    cheat_locs.update({t: tp['loc'] for t, tp in sc['targets'].items()})
    pg = Perfect_Geolocator()
    pg.set_data({'address_to_loc': cheat_locs, 'loc_loc_meas': llm})
    oracle_order = pg.measurement_order

    seen: dict[tuple, list[float]] = {}
    k = 0
    for b in LARGE_BUDGET_GRID:
        while k < b:
            s, t = pings[k]
            seen[(s, t)] = list(llm[s][t])
            k += 1

        # nearest neighbour on the shared random order
        nn_est = {}
        best = {}
        for (s, t), v in seen.items():
            r = min(v)
            if t not in best or r < best[t][0]:
                best[t] = (r, s)
        nn_est = {t: LARGE_VP_LOCS[s] for t, (_, s) in best.items()}
        curves['random_nn'].append(_avg_err(nn_est, sc['targets']))

        # per-target multiplicative EM on the same pings
        est = {}
        by_t: dict[str, list] = {}
        for (s, t), v in seen.items():
            by_t.setdefault(t, []).append((LARGE_VP_LOCS[s], min(v)))
        for t, batch in by_t.items():
            region = FeasibleRegion(t, mode=EM_GAUSSIAN)
            region.add_measurements_batch(batch)
            est[t] = region.get_location()
        curves['per_target_em'].append(_avg_err(est, sc['targets']))

        # cross-target additive fit on the same pings
        add_est, _, _, _, _ = additive_batch_em(seen, LARGE_VP_LOCS)
        curves['additive_em'].append(_avg_err(add_est, sc['targets']))

        # oracle: PG order + true per-node params
        orc_by_t: dict[str, list[str]] = {}
        for s, t in oracle_order[:b]:
            orc_by_t.setdefault(t, []).append(s)
        orc = {}
        for t, srcs in orc_by_t.items():
            tp = sc['targets'][t]
            rows = [(LARGE_VP_LOCS[s], llm[s][t][0],
                     sc['sources'][s]['mu'] + tp['mu'],
                     sc['sources'][s]['sigma'] ** 2 + tp['sigma'] ** 2)
                    for s in srcs]
            nn0 = LARGE_VP_LOCS[min(srcs, key=lambda s: llm[s][t][0])]
            orc[t] = _map_location(rows, [nn0, (20.0, 0.0)])
        curves['oracle'].append(_avg_err(orc, sc['targets']))

    # greedy systems select their own pings
    for name, sel in (('greedy_additive_risk', 'risk_gain'),
                      ('greedy_additive_phased', 'phased')):
        ig = Iterative_Greedy_Geolocator(max_workers=1, region_mode=ADDITIVE,
                                         model_refit_every=10, selection=sel)
        ig.set_data(sc['data'])
        ig.solve()
        try:
            for b in LARGE_BUDGET_GRID:
                ig.measurements(b)
                curves[name].append(
                    _avg_err(ig.get_current_estimates(), sc['targets']))
        finally:
            ig.cleanup()

    return {'scenario': sc, 'curves': curves}


@pytest.fixture(scope='module')
def large_results():
    return [run_large_seed(seed) for seed in range(N_LARGE_SEEDS)]


def _mean_at(rows, strategy, budget):
    i = LARGE_BUDGET_GRID.index(budget)
    return float(np.mean([r['curves'][strategy][i] for r in rows]))


class TestLargeAdditiveWorld:
    def test_summary(self, large_results):
        for b in LARGE_BUDGET_GRID:
            line = "  ".join(f"{s}={_mean_at(large_results, s, b):7.1f}"
                             for s in LARGE_STRATEGIES)
            print(f"\nb={b:4d}: {line}")

    def test_selection_beats_random_under_scarcity(self, large_results):
        """The point of the bigger world: with 20 global VPs and 100
        targets, min-of-k-random needs many draws, and selection wins the
        scarcity regime on the same budget."""
        for b in (200, 350, 500):
            for g in ('greedy_additive_risk', 'greedy_additive_phased'):
                assert _mean_at(large_results, g, b) < \
                    _mean_at(large_results, 'random_nn', b)

    def test_cross_target_pooling_beats_per_target_em(self, large_results):
        """Same pings, same budget: the additive model's pooled offsets
        halve per-target EM's error once data accumulates (calibrated
        616 vs 1202 at b=1000)."""
        for b in (500, 1000, 1500):
            assert _mean_at(large_results, 'additive_em', b) < \
                0.75 * _mean_at(large_results, 'per_target_em', b)

    def test_selection_adds_on_top_of_estimation(self, large_results):
        """risk-adjusted greedy vs the SAME estimator on a random order
        (calibrated 363 vs 616 at b=1000)."""
        for b in (500, 750, 1000):
            assert _mean_at(large_results, 'greedy_additive_risk', b) < \
                _mean_at(large_results, 'additive_em', b)

    def test_oracle_dominates_everywhere(self, large_results):
        for b in LARGE_BUDGET_GRID:
            orc = _mean_at(large_results, 'oracle', b)
            for s in LARGE_STRATEGIES:
                if s != 'oracle':
                    assert orc <= 1.02 * _mean_at(large_results, s, b), \
                        f"oracle beaten by {s} at b={b}"

    def test_generate_figure(self, large_results):
        from plot_error_additive import make_figure
        out = os.path.join(os.path.dirname(__file__),
                           'error_over_measurements_additive_large.pdf')
        path = make_figure(
            large_results, out,
            budget_grid=LARGE_BUDGET_GRID, strategies=LARGE_STRATEGIES,
            n_targets=N_LARGE_TARGETS,
            title=(f'LARGE additive world: 20 global VPs, {N_LARGE_TARGETS} '
                   f'global targets (10% pathological), one sample/pair —\n'
                   f'{N_LARGE_SEEDS} seeds; shared random order for the '
                   f'estimation-only lines, greedys select their own pings'),
            ylim=6500)
        assert os.path.exists(path)
        assert os.path.getsize(path) > 5_000