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

from feasible_region_maintainer import FeasibleRegion, EM_GAUSSIAN, GAUSSIAN, ADDITIVE
from iterative_greedy_geolocator import Iterative_Greedy_Geolocator
from perfect_geolocator import Perfect_Geolocator
from probabilistic_helpers import KM_PER_MS, additive_batch_em, FiberFloorRtt
from test_e2e_additive_em import (
    _map_location, SRC_MU_RANGE, SRC_SIGMA_RANGE, DST_MU_RANGE,
    DST_SIGMA_RANGE, PATH_MU_RANGE, PATH_SIGMA_RANGE, MISSING_PENALTY_KM,
)
from utils import get_distance, LatLon

# internet_gmaps (the fiber atlas) lives inside this repo; its modules
# assume their own directory on sys.path (see its conftest.py)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'internet_gmaps'))

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


# ---------------------------------------------------------------------------
# Fiber-floor RttModel: plumbing + a toy world where the floor is the truth
# ---------------------------------------------------------------------------
# This synthetic harness's ground truth is rtt = d/100 + offsets, so a fiber
# strategy here can only prove PLUMBING: injecting FiberFloorRtt with a mock
# atlas whose floor IS the geodesic must change nothing, bit for bit.  The
# toy-fiber-world test below builds the complementary world where the truth
# follows a detouring cable — there the fiber model must win.

class _GeodesicFloorEstimator:
    """Mock atlas: floor(vp, x) = geodesic / KM_PER_MS.  Module-level so
    regions holding it survive the greedy's worker pickling."""

    def __init__(self, vp_locs: list[LatLon]) -> None:
        self.vp_locs = [(float(a), float(b)) for a, b in vp_locs]

    def floor_ms(self, lat: float, lon: float) -> np.ndarray:
        return np.array([get_distance(vp, (lat, lon)) / KM_PER_MS
                         for vp in self.vp_locs])


def _mock_fiber_model(vp_locs_dict: dict[str, LatLon]) -> FiberFloorRtt:
    locs = list(vp_locs_dict.values())
    return FiberFloorRtt(estimator=_GeodesicFloorEstimator(locs),
                         vp_locs=locs, slope=1.0, offset_ms=0.0)


class TestFiberPlumbing:
    def test_additive_batch_em_unchanged_under_mock_fiber(self):
        sc = make_large_scenario(0)
        seen = {(s, t): list(rs)
                for s, dsts in sc['data']['loc_loc_meas'].items()
                for t, rs in dsts.items()}
        base, mu_s_b, _, mu_t_b, _ = additive_batch_em(seen, LARGE_VP_LOCS)
        mock = _mock_fiber_model(LARGE_VP_LOCS)
        fib, mu_s_f, _, mu_t_f, _ = additive_batch_em(seen, LARGE_VP_LOCS,
                                                      rtt_model=mock)
        assert base.keys() == fib.keys()
        for t in base:
            assert base[t] == fib[t], t
        assert mu_s_b == mu_s_f and mu_t_b == mu_t_f

    def test_gaussian_map_unchanged_under_mock_fiber(self):
        # slope=1.0: at other slopes (slope*d)/100 vs slope*(d/100) differ
        # in the last ulp and Nelder-Mead amplifies it — the injection's
        # bit-exactness contract is defined at the shared base term
        sc = make_large_scenario(1)
        llm = sc['data']['loc_loc_meas']
        mock = _mock_fiber_model(LARGE_VP_LOCS)
        for t in list(sc['targets'])[:5]:
            batch = [(LARGE_VP_LOCS[s], llm[s][t][0]) for s in LARGE_VP_LOCS]
            plain = FeasibleRegion(t, mode=GAUSSIAN, slope=1.0)
            plain.add_measurements_batch(batch)
            fiber = FeasibleRegion(t, mode=GAUSSIAN, slope=1.0, rtt_model=mock)
            fiber.add_measurements_batch(batch)
            assert plain.get_location() == fiber.get_location(), t
            assert plain.get_region_size() == fiber.get_region_size(), t

    def test_greedy_curves_unchanged_under_mock_fiber(self):
        sc = make_large_scenario(2)
        budgets = (100, 300)
        curves = {}
        for name, model in (('plain', None),
                            ('fiber', _mock_fiber_model(LARGE_VP_LOCS))):
            ig = Iterative_Greedy_Geolocator(
                max_workers=1, region_mode=ADDITIVE, model_refit_every=10,
                selection='risk_gain', rtt_model=model)
            ig.set_data(sc['data'])
            ig.solve()
            try:
                vals = []
                for b in budgets:
                    ig.measurements(b)
                    vals.append(_avg_err(ig.get_current_estimates(),
                                         sc['targets']))
                curves[name] = vals
            finally:
                ig.cleanup()
        assert curves['plain'] == curves['fiber']


# Toy fiber world: a C-shaped cable A(0,0) -> B(0,15) -> C(10,15) -> D(10,0).
# Fiber A->D runs ~40 equator-degrees while the geodesic is ~10 — exactly
# the detour regime (Colombo/Cape-Town class) where geodesic circles and
# fiber isochrones disagree.  Ground truth rtt = 1.0*floor + offsets + noise.
# VPs sit at all four corners: with VPs on one side only, every floor
# shifts by the same constant along the cable and the per-target offset
# absorbs position exactly (a linear cable cannot trilaterate itself) —
# the far-end VP provides the opposing floor gradient that pins position.

TOY_VPS: dict[str, LatLon] = {
    'vp_a': (0.0, 0.0),
    'vp_b': (0.0, 15.0),
    'vp_c': (10.0, 15.0),
    'vp_d': (10.0, 0.0),
}


def _toy_fiber_graph():
    from fiber_graph import GraphBuilder

    b = GraphBuilder(snap_tolerance_km=1.0)
    path = ([(0.0, float(lon)) for lon in range(0, 16)]            # A -> B
            + [(float(lat), 15.0) for lat in range(1, 11)]         # B -> C
            + [(10.0, float(lon)) for lon in range(14, -1, -1)])   # C -> D
    b.add_path(path)
    return b.build()


def make_toy_fiber_scenario(seed: int) -> dict:
    from floor_query import FloorEstimator

    graph = _toy_fiber_graph()
    rng = np.random.default_rng(seed + 20_000_000)
    vp_names = list(TOY_VPS)
    est = FloorEstimator(graph,
                         [TOY_VPS[v][0] for v in vp_names],
                         [TOY_VPS[v][1] for v in vp_names])

    targets = {}
    # 8 targets along the far C->D leg (deep in detour territory), 4 near B
    spots = ([(10.0, float(lon)) for lon in (0.0, 2.0, 4.0, 6.0, 8.0, 10.0,
                                             12.0, 13.0)]
             + [(0.0, 12.0), (2.0, 15.0), (0.0, 9.0), (5.0, 15.0)])
    for i, (lat, lon) in enumerate(spots):
        targets[f't{i:02d}'] = {
            'loc': (lat + float(rng.normal(0, 0.15)),
                    lon + float(rng.normal(0, 0.15))),
            'mu': float(rng.uniform(2.0, 6.0)),
        }
    src_mu = {s: float(rng.uniform(2.0, 6.0)) for s in vp_names}

    loc_loc_meas: dict[str, dict[str, list[float]]] = {}
    for v, s in enumerate(vp_names):
        loc_loc_meas[s] = {}
        for t, tp in targets.items():
            floor = float(est.floor_ms(tp['loc'][0], tp['loc'][1])[v])
            assert np.isfinite(floor)
            loc_loc_meas[s][t] = [floor + src_mu[s] + tp['mu']
                                  + abs(float(rng.normal(0.0, 0.5)))]
    return {'targets': targets, 'graph': graph, 'estimator': est,
            'data': {'address_to_loc': dict(TOY_VPS),
                     'loc_loc_meas': loc_loc_meas}}


class TestToyFiberWorld:
    def test_fiber_model_beats_geodesic_when_truth_is_fiber(self):
        errs = {'geodesic': [], 'fiber': [], 'nn': []}
        for seed in range(2):
            sc = make_toy_fiber_scenario(seed)
            seen = {(s, t): list(rs)
                    for s, dsts in sc['data']['loc_loc_meas'].items()
                    for t, rs in dsts.items()}
            fiber_model = FiberFloorRtt(
                estimator=sc['estimator'], vp_locs=list(TOY_VPS.values()),
                slope=1.0, offset_ms=0.0)
            geo_est, *_ = additive_batch_em(seen, TOY_VPS)
            fib_est, *_ = additive_batch_em(seen, TOY_VPS,
                                            rtt_model=fiber_model)
            best = {}
            for (s, t), rs in seen.items():
                if t not in best or min(rs) < best[t][0]:
                    best[t] = (min(rs), s)
            nn_est = {t: TOY_VPS[s] for t, (_, s) in best.items()}
            errs['geodesic'].append(_avg_err(geo_est, sc['targets']))
            errs['fiber'].append(_avg_err(fib_est, sc['targets']))
            errs['nn'].append(_avg_err(nn_est, sc['targets']))
        geo_mean = float(np.mean(errs['geodesic']))
        fib_mean = float(np.mean(errs['fiber']))
        nn_mean = float(np.mean(errs['nn']))
        print(f"\ntoy fiber world: geodesic={geo_mean:.0f} "
              f"fiber={fib_mean:.0f} nn={nn_mean:.0f} km")
        # the whole point of the atlas: when routes follow the cable, the
        # fiber model dominates both the geodesic model and coverage-NN
        assert fib_mean < 0.5 * geo_mean
        assert fib_mean < 0.5 * nn_mean
        assert fib_mean < 300.0