"""
End-to-end test for the online-EM gaussian estimator (per-target μ/σ).

Data model
----------
Each target (one per seed) has its OWN unknown routing parameters:

    μ_t ~ U(1.01, 1.4)          (per-target slope over SOL)
    σ_t ~ U(1.0, 6.0) ms        (per-target noise)
    rtt_v = μ_t × d(v, target) / 100  +  N(0, σ_t²)

The estimators do not know (μ_t, σ_t).  The EM estimator
(`FeasibleRegion(mode='em_gaussian')`) fits them online from accumulating
residuals with prior-anchored least squares; the others assume a fixed model.

Strategies compared (all on the same measurements, same random order):

    random   — nearest neighbour: location of the lowest-RTT VP seen
    sol      — gaussian MAP, slope fixed at 1.0 (straight SOL conversion)
    const    — gaussian MAP, slope fixed at DEFAULT_SLOPE (what we had)
    em       — em_gaussian: slope/σ fitted online per target
    oracle   — gaussian MAP given the TRUE (μ_t, σ_t)  (explicit cheat)

Calibrated medians at full budget (80 seeds, km):
    random≈281   sol≈446   const≈186   em≈133   oracle≈147

Notable findings encoded as assertions:
- Misspecified triangulation (sol) is WORSE than nearest-neighbour: a wrong
  model loses to the dumb baseline.
- EM can match or beat the parameter-oracle in-sample: fitting μ to the
  realised noise sample can explain the data better than the true μ.

Information boundary: ALL_LOCS is locked during inference; VP and target
coordinates are resolved before the lock (target only for error
computation, never passed to estimators).

Multi-target budget allocation (second half of this file)
----------------------------------------------------------
run_multi_seed / TestMultiTargetBudgetAllocation measure the actual project
objective: N_TARGETS targets share one TOTAL ping budget, minimise AVERAGE
error. Strategies are whole systems (selection + estimation): random+NN,
Iterative_Greedy_Geolocator with hard/gaussian/em/additive regions, and a
Perfect_Geolocator-selection + true-(μ,σ) oracle. BASICALLY_GEOLOCATED deprioritises
"done" targets instead of hard-stopping, so leftover budget refines the
least-certain ones. Per-target μ_t ~ U(1.0, 2.0) — wide enough that the
fixed 1.3 slope is genuinely wrong for many targets. Findings: greedy wins
the early budget; em beats the fixed-slope greedy 2.3× at full budget
(every seed) while the fixed-slope greedy actually loses to random; em
beats random on 95% of seeds. TestEMEdgeVsModelMismatch sweeps the μ range
and shows em's edge growing with mismatch (ratios 0.93 → 0.56 → 0.31) and
that σ mismatch alone creates no edge (shared-σ MAP is σ-invariant).
plot_error_adaptive_em.py plots the curves; plot_region_convergence.py is
the 1:1 spatial filmstrip (seed 15).
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import pytest

from feasible_region_maintainer import (
    FeasibleRegion, GAUSSIAN, EM_GAUSSIAN, DEFAULT_SLOPE,
)
from probabilistic_helpers import KM_PER_MS, GLOBAL_SIGMA_MS
from utils import get_distance, LatLon, LockedLocationDict, simulation_mode


# ---------------------------------------------------------------------------
# Problem definition (same cities as test_e2e_probabilistic)
# ---------------------------------------------------------------------------

ALL_LOCS: LockedLocationDict = LockedLocationDict({
    '_target':   (50.08,  14.44),   # Prague — the unknown target
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
})

VP_NAMES = [k for k in ALL_LOCS if k != '_target']

MU_RANGE = (1.01, 1.4)      # per-target slope over SOL
SIGMA_RANGE = (1.0, 6.0)    # per-target noise, ms

STRATEGIES = ('random', 'sol', 'const', 'em', 'oracle')


# ---------------------------------------------------------------------------
# One-seed simulation (shared with the figure generator)
# ---------------------------------------------------------------------------

def make_scenario(seed: int) -> dict:
    """
    Deterministic per-seed scenario: one target with its own (μ_t, σ_t),
    a shuffled VP order and the measured RTTs.  Shared by run_seed and the
    region-convergence filmstrip (plot_region_convergence.py) so the two
    figures map 1:1.
    """
    rng = np.random.default_rng(seed)
    mu_t = float(rng.uniform(*MU_RANGE))
    sigma_t = float(rng.uniform(*SIGMA_RANGE))

    # Resolve coordinates BEFORE any lock: VP locs are measurement metadata,
    # target loc is used only for error computation.
    vp_locs: dict[str, LatLon] = {n: ALL_LOCS[n] for n in VP_NAMES}
    target_loc: LatLon = ALL_LOCS['_target']

    order = list(VP_NAMES)
    rng.shuffle(order)
    rtts = {
        n: mu_t * get_distance(vp_locs[n], target_loc) / KM_PER_MS
           + float(rng.normal(0.0, sigma_t))
        for n in order
    }
    return {
        'mu_t': mu_t,
        'sigma_t': sigma_t,
        'vp_locs': vp_locs,
        'target_loc': target_loc,
        'order': order,
        'rtts': rtts,
    }


def run_seed(seed: int) -> dict:
    """
    Simulate one target with its own (μ_t, σ_t); feed the same measurements
    to every strategy incrementally.  Returns per-strategy error curves
    (index k-1 = error after k measurements) plus the EM parameter fits.
    """
    scenario = make_scenario(seed)
    mu_t, sigma_t = scenario['mu_t'], scenario['sigma_t']
    vp_locs, target_loc = scenario['vp_locs'], scenario['target_loc']
    order, rtts = scenario['order'], scenario['rtts']

    errors: dict[str, list[float]] = {s: [] for s in STRATEGIES}

    with simulation_mode(ALL_LOCS):
        regions = {
            'sol':    FeasibleRegion('t', mode=GAUSSIAN, slope=1.0),
            'const':  FeasibleRegion('t', mode=GAUSSIAN),  # DEFAULT_SLOPE
            'em':     FeasibleRegion('t', mode=EM_GAUSSIAN),
            'oracle': FeasibleRegion('t', mode=GAUSSIAN, slope=mu_t),
        }
        seen: dict[str, float] = {}
        for name in order:
            seen[name] = rtts[name]
            for s, region in regions.items():
                sigma = sigma_t if s == 'oracle' else GLOBAL_SIGMA_MS
                region.add_measurement(vp_locs[name], rtts[name], sigma_ms=sigma)

            nearest = min(seen, key=seen.get)
            errors['random'].append(get_distance(vp_locs[nearest], target_loc))
            for s, region in regions.items():
                errors[s].append(get_distance(region.get_location(), target_loc))

    return {
        'errors': errors,
        'mu_t': mu_t,
        'sigma_t': sigma_t,
        'fitted_mu': regions['em'].slope,
        'fitted_sigma': regions['em'].fitted_sigma_ms,
    }


# ---------------------------------------------------------------------------
# Multi-seed fixture
# ---------------------------------------------------------------------------

N_SEEDS = 80

@pytest.fixture(scope='module')
def results():
    return [run_seed(seed) for seed in range(N_SEEDS)]


def _final_errs(rows, key):
    return np.array([r['errors'][key][-1] for r in rows])


def _med(rows, key) -> float:
    return float(np.median(_final_errs(rows, key)))


# ---------------------------------------------------------------------------
# (a) EM recovers the per-target parameters
# ---------------------------------------------------------------------------

class TestEMRecoversParameters:
    def test_fitted_mu_beats_constant_assumption(self, results):
        """Median |μ_EM − μ_t| must beat median |DEFAULT_SLOPE − μ_t| —
        otherwise the online fit is not learning anything."""
        em_err = np.median([abs(r['fitted_mu'] - r['mu_t']) for r in results])
        const_err = np.median([abs(DEFAULT_SLOPE - r['mu_t']) for r in results])
        assert em_err < const_err, (
            f"EM μ error {em_err:.3f} should beat constant-assumption "
            f"error {const_err:.3f}"
        )

    def test_fitted_mu_in_physical_bounds(self, results):
        for r in results:
            assert 1.0 <= r['fitted_mu'] <= 2.0
            assert r['fitted_sigma'] > 0.0


# ---------------------------------------------------------------------------
# (b) EM beats the constant-parameter gaussian
# ---------------------------------------------------------------------------

class TestEMBeatsConstantGaussian:
    def test_median_error(self, results):
        med_em, med_c = _med(results, 'em'), _med(results, 'const')
        assert med_em < med_c, (
            f"EM ({med_em:.1f} km) should beat constant gaussian ({med_c:.1f} km)"
        )

    def test_wins_majority_of_seeds(self, results):
        rate = float(np.mean(_final_errs(results, 'em') < _final_errs(results, 'const')))
        assert rate >= 0.55, (
            f"EM beats constant gaussian on {100*rate:.0f}% of seeds — expected ≥ 55%"
        )


# ---------------------------------------------------------------------------
# (c) Model-quality ranking
# ---------------------------------------------------------------------------

class TestModelQualityRanking:
    def test_full_ranking(self, results):
        med = {s: _med(results, s) for s in STRATEGIES}
        print("\nMedian errors at full budget (km):  "
              + "  ".join(f"{s}={med[s]:.0f}" for s in STRATEGIES))
        # adaptive model beats every fixed model and the dumb baseline
        assert med['em'] < med['const'] < med['sol']
        assert med['em'] < med['random']
        # comparable to the parameter-oracle (EM may even beat it in-sample)
        assert med['em'] < med['oracle'] * 1.3

    def test_misspecified_triangulation_loses_to_nearest_neighbour(self, results):
        """A wrong model is worse than no model: slope-1.0 triangulation on
        overhead-bearing RTTs loses to plain nearest-neighbour — the same
        phenomenon seen on real RIPE data with the pure-SOL model."""
        assert _med(results, 'random') < _med(results, 'sol')

    def test_oracle_beats_fixed_models(self, results):
        assert _med(results, 'oracle') < _med(results, 'const')
        assert _med(results, 'oracle') < _med(results, 'sol')


# ---------------------------------------------------------------------------
# Figure generation
# ---------------------------------------------------------------------------

class TestGenerateFigure:
    def test_error_over_measurements_adaptive_pdf(self):
        """Generate tests/error_over_measurements_adaptive.pdf."""
        from plot_error_adaptive_em import run_simulation, plot, OUT_PATH
        curves = run_simulation()
        plot(curves)
        assert os.path.exists(OUT_PATH), f"Figure not written to {OUT_PATH}"


# ---------------------------------------------------------------------------
# (d) Noise models under detour contamination
# ---------------------------------------------------------------------------

from probabilistic_helpers import (
    GAUSSIAN_NOISE, STUDENT_T_NOISE, ASYMMETRIC_NOISE,
)

DETOUR_PROB = 0.2          # fraction of measurements hit by a routing detour
DETOUR_MEAN_MS = 40.0      # Exp-distributed extra latency for detoured pings

NOISE_MODELS = (GAUSSIAN_NOISE, STUDENT_T_NOISE, ASYMMETRIC_NOISE)


def run_contaminated_seed(seed: int) -> dict[str, float]:
    """
    Same per-target (μ_t, σ_t) world, but 20% of measurements carry an
    Exp(40ms) routing detour — the one-sided heavy tail real RTTs have.
    Compares the three noise models on a fixed-slope gaussian estimator.
    """
    rng = np.random.default_rng(seed + 500_000)
    mu_t = float(rng.uniform(*MU_RANGE))
    sigma_t = float(rng.uniform(*SIGMA_RANGE))

    vp_locs = {n: ALL_LOCS[n] for n in VP_NAMES}
    target_loc = ALL_LOCS['_target']

    rtts = {}
    for n in VP_NAMES:
        rtt = (mu_t * get_distance(vp_locs[n], target_loc) / KM_PER_MS
               + float(rng.normal(0.0, sigma_t)))
        if rng.random() < DETOUR_PROB:
            rtt += float(rng.exponential(DETOUR_MEAN_MS))
        rtts[n] = rtt

    errors = {}
    with simulation_mode(ALL_LOCS):
        for nm in NOISE_MODELS:
            region = FeasibleRegion('t', mode=GAUSSIAN, noise_model=nm)
            for n in VP_NAMES:
                region.add_measurement(vp_locs[n], rtts[n])
            errors[nm] = get_distance(region.get_location(), target_loc)
    return errors


N_CONTAMINATED_SEEDS = 50

@pytest.fixture(scope='module')
def contaminated_results():
    return [run_contaminated_seed(s) for s in range(N_CONTAMINATED_SEEDS)]


class TestNoiseModelsUnderContamination:
    """
    Calibrated medians (60 seeds, 20% Exp(40ms) detours, full budget):
    gaussian ≈ 925 km, student_t ≈ 420 km, asymmetric ≈ 213 km.

    On CLEAN data the ranking flips mildly (gaussian ≈ student_t ≈ 168 km,
    asymmetric ≈ 195 km): student_t is a near-free robustness upgrade;
    asymmetric is the specialist for one-sided contamination at a ~16%
    clean-world cost.
    """

    def _med(self, rows, nm):
        return float(np.median([r[nm] for r in rows]))

    def test_robust_models_beat_gaussian_under_detours(self, contaminated_results):
        med_g = self._med(contaminated_results, GAUSSIAN_NOISE)
        med_t = self._med(contaminated_results, STUDENT_T_NOISE)
        med_a = self._med(contaminated_results, ASYMMETRIC_NOISE)
        print(f"\nContaminated medians (km): gaussian={med_g:.0f}  "
              f"student_t={med_t:.0f}  asymmetric={med_a:.0f}")
        assert med_t < med_g * 0.7, "student_t should clearly beat gaussian"
        assert med_a < med_g * 0.5, "asymmetric should roundly beat gaussian"

    def test_asymmetric_best_matches_onesided_contamination(self, contaminated_results):
        """Detours are one-sided, so the one-sided model should win."""
        assert self._med(contaminated_results, ASYMMETRIC_NOISE) < \
            self._med(contaminated_results, STUDENT_T_NOISE)


# ===========================================================================
# Multi-target budget allocation: the greedy enters the comparison
# ===========================================================================
#
# The project objective (mirrors assess_geolocators.run): given a TOTAL ping
# budget shared across many targets, minimise the AVERAGE geolocation error.
# A strategy must both estimate well and ALLOCATE pings across targets well.
#
# Scenario: 10 VPs (same cities), N_TARGETS targets at uniform-random
# locations in the European VP cluster, each with its own hidden (μ_t, σ_t).
#
# Honesty by construction: the data dict handed to strategies contains VP
# locations only — target coordinates live in the scenario dict and are used
# solely for scoring (and by the labelled oracle).

from iterative_greedy_geolocator import Iterative_Greedy_Geolocator
from perfect_geolocator import Perfect_Geolocator
from feasible_region_maintainer import HARD_CIRCLE, ADDITIVE

N_TARGETS = 5
TARGET_LAT_RANGE = (38.0, 58.0)   # roughly the European VP cluster
TARGET_LON_RANGE = (-8.0, 25.0)
TOTAL_BUDGET = 50                 # = all 10 VPs × 5 targets
MISSING_PENALTY_KM = 10_000.0     # matches assess_geolocators.run()

# Wider than the single-target MU_RANGE on purpose: with μ_t ~ U(1.01, 1.4)
# the constant-1.3 assumption is nearly right and geometry cancels the rest,
# so em ≈ gaussian. U(1.0, 2.0) makes per-target calibration actually matter
# (see TestEMEdgeVsModelMismatch for the systematic sweep).
MULTI_MU_RANGE = (1.0, 2.0)
MULTI_SIGMA_RANGE = SIGMA_RANGE

MULTI_STRATEGIES = ('random_nn', 'greedy_hard', 'greedy_gaussian',
                    'greedy_gaussian_105', 'greedy_em', 'greedy_additive',
                    'oracle')


def make_multi_scenario(seed: int, mu_range=MULTI_MU_RANGE,
                        sigma_range=MULTI_SIGMA_RANGE) -> dict:
    rng = np.random.default_rng(seed + 1_000_000)
    vp_locs = {n: ALL_LOCS[n] for n in VP_NAMES}

    targets: dict[str, dict] = {}
    for i in range(N_TARGETS):
        targets[f'target_{i}'] = {
            'loc': (float(rng.uniform(*TARGET_LAT_RANGE)),
                    float(rng.uniform(*TARGET_LON_RANGE))),
            'mu': float(rng.uniform(*mu_range)),
            'sigma': float(rng.uniform(*sigma_range)),
        }

    loc_loc_meas: dict[str, dict[str, list[float]]] = {}
    for vp in VP_NAMES:
        loc_loc_meas[vp] = {}
        for tid, t in targets.items():
            rtt = (t['mu'] * get_distance(vp_locs[vp], t['loc']) / KM_PER_MS
                   + float(rng.normal(0.0, t['sigma'])))
            loc_loc_meas[vp][tid] = [rtt]

    data = {'address_to_loc': dict(vp_locs), 'loc_loc_meas': loc_loc_meas}
    return {'vp_locs': vp_locs, 'targets': targets, 'data': data,
            'order_rng': np.random.default_rng(seed + 2_000_000)}


def _avg_error(estimates: dict, targets: dict) -> float:
    errs = []
    for tid, t in targets.items():
        if tid in estimates:
            errs.append(get_distance(estimates[tid], t['loc']))
        else:
            errs.append(MISSING_PENALTY_KM)
    return float(np.mean(errs))


def _snap(estimates: dict, sizes: dict) -> dict:
    return {tid: {'est': estimates.get(tid), 'size': sizes.get(tid)}
            for tid in set(estimates) | set(sizes)}


def _run_random_nn(sc: dict, snapshot_ks=()) -> tuple[list[float], dict]:
    pairs = [(vp, tid) for vp in VP_NAMES for tid in sc['targets']]
    sc['order_rng'].shuffle(pairs)
    best: dict[str, tuple[float, str]] = {}
    errs, snaps = [], {}
    for k, (vp, tid) in enumerate(pairs[:TOTAL_BUDGET], start=1):
        rtt = sc['data']['loc_loc_meas'][vp][tid][0]
        if tid not in best or rtt < best[tid][0]:
            best[tid] = (rtt, vp)
        estimates = {t: sc['vp_locs'][v] for t, (_, v) in best.items()}
        errs.append(_avg_error(estimates, sc['targets']))
        if k in snapshot_ks:
            snaps[k] = _snap(estimates, {})
    return errs, snaps


def _run_greedy(sc: dict, region_mode: str, snapshot_ks=(),
                region_slope: float = DEFAULT_SLOPE) -> tuple[list[float], dict]:
    ig = Iterative_Greedy_Geolocator(max_workers=1, region_mode=region_mode,
                                     region_slope=region_slope)
    ig.set_data(sc['data'])
    ig.solve()
    errs, snaps = [], {}
    try:
        for k in range(1, TOTAL_BUDGET + 1):
            ig.measurements(k)     # extends history incrementally (≤ k pings)
            estimates = ig.get_current_estimates()
            errs.append(_avg_error(estimates, sc['targets']))
            if k in snapshot_ks:
                sizes = {tid: r.get_region_size()
                         for tid, r in ig.target_regions.items() if r.constraints}
                snaps[k] = _snap(estimates, sizes)
        # The greedy stops spending once every region is under the
        # done-threshold — record how much budget it actually used.
        pings_used = len(ig.measurement_history)
    finally:
        ig.cleanup()
    return errs, snaps, pings_used


def _run_oracle(sc: dict, snapshot_ks=()) -> tuple[list[float], dict]:
    """Whole-system cheat: Perfect_Geolocator selection (error-guided
    greedy on ground truth — THE selection-oracle implementation, shared
    with assess_geolocators) + gaussian MAP estimation with the true
    (μ_t, σ_t)."""
    cheat_locs = dict(sc['vp_locs'])
    cheat_locs.update({tid: t['loc'] for tid, t in sc['targets'].items()})
    pg = Perfect_Geolocator()
    pg.set_data({'address_to_loc': cheat_locs,
                 'loc_loc_meas': sc['data']['loc_loc_meas']})
    order = pg.measurement_order

    regions = {tid: FeasibleRegion(tid, mode=GAUSSIAN, slope=t['mu'])
               for tid, t in sc['targets'].items()}
    errs, snaps = [], {}
    for k, (vp, tid) in enumerate(order[:TOTAL_BUDGET], start=1):
        rtt = sc['data']['loc_loc_meas'][vp][tid][0]
        regions[tid].add_measurement(sc['vp_locs'][vp], rtt,
                                     sigma_ms=sc['targets'][tid]['sigma'])
        estimates = {t: r.get_location()
                     for t, r in regions.items() if r.constraints}
        errs.append(_avg_error(estimates, sc['targets']))
        if k in snapshot_ks:
            sizes = {t: r.get_region_size()
                     for t, r in regions.items() if r.constraints}
            snaps[k] = _snap(estimates, sizes)
    return errs, snaps


def run_multi_seed(seed: int, snapshot_ks=(), mu_range=MULTI_MU_RANGE,
                   sigma_range=MULTI_SIGMA_RANGE) -> dict:
    sc = make_multi_scenario(seed, mu_range=mu_range, sigma_range=sigma_range)
    errors, snapshots, pings_used = {}, {}, {}
    runners = {
        'random_nn':       lambda: _run_random_nn(sc, snapshot_ks),
        'greedy_hard':     lambda: _run_greedy(sc, HARD_CIRCLE, snapshot_ks),
        'greedy_gaussian': lambda: _run_greedy(sc, GAUSSIAN, snapshot_ks),
        'greedy_gaussian_105': lambda: _run_greedy(sc, GAUSSIAN, snapshot_ks,
                                                   region_slope=1.05),
        'greedy_em':       lambda: _run_greedy(sc, EM_GAUSSIAN, snapshot_ks),
        'greedy_additive': lambda: _run_greedy(sc, ADDITIVE, snapshot_ks),
        'oracle':          lambda: _run_oracle(sc, snapshot_ks),
    }
    for name, run in runners.items():
        result = run()
        errors[name], snapshots[name] = result[0], result[1]
        pings_used[name] = result[2] if len(result) > 2 else TOTAL_BUDGET
    return {'scenario': sc, 'errors': errors, 'snapshots': snapshots,
            'pings_used': pings_used}


N_MULTI_SEEDS = 20

@pytest.fixture(scope='module')
def multi_results():
    return [run_multi_seed(seed) for seed in range(N_MULTI_SEEDS)]


def _multi_med(rows, strategy, k):
    return float(np.median([r['errors'][strategy][k - 1] for r in rows]))


class TestMultiTargetBudgetAllocation:
    """
    The project objective, measured: average error across 5 targets vs total
    pings spent. Per-target μ_t ~ U(1.0, 2.0) — wide enough that the fixed
    1.3-slope model is genuinely wrong for many targets (see
    TestEMEdgeVsModelMismatch for the systematic sweep). Calibrated 20-seed
    medians (deterministic seeds), with BASICALLY_GEOLOCATED acting as a
    deprioritisation rather than a hard stop:

        k=10: random=1214  g_hard=1025  g_gauss=1022  g_em=1042  g_add= 821  oracle=325
        k=25: random= 699  g_hard= 772  g_gauss= 657  g_em= 561  g_add= 508  oracle=211
        k=50: random= 462  g_hard= 392  g_gauss= 521  g_em= 222  g_add= 266  oracle=148

    greedy_additive (shared src/dst model) plays an AWAY game here — the
    world is multiplicative (per-target slope), which an additive offset
    cannot represent — yet it is the best non-oracle strategy at k=10 AND
    k=25, and still beats random at full budget; only the world-matched em
    estimator stays ahead late (see
    test_em_stays_ahead_of_additive_in_multiplicative_world).

    Findings pinned below:
    (a) greedy allocation wins the early budget regime;
    (b) greedy_em beats greedy_gaussian decisively (2.3× at full budget,
        every seed) — per-target μ calibration is what makes the greedy
        work when the fixed slope is wrong;
    (c) the fixed-slope gaussian greedy actually LOSES to random at full
        budget (521 vs 462) — a wrong model integrates more wrong
        information; em fixes exactly this;
    (d) greedy_em beats random at full budget (95% of seeds) and reaches
        random's full-budget accuracy by k=35;
    (e) estimation quality gates selection quality: greedy_hard's bad
        regions make its CHOICES worse than random spending at mid-budget.
    """

    def test_summary(self, multi_results):
        for k in (10, 25, 50):
            line = "  ".join(
                f"{s}={_multi_med(multi_results, s, k):7.1f}"
                for s in MULTI_STRATEGIES)
            print(f"\nk={k:2d}: {line}")

    def test_greedy_wins_early_budget(self, multi_results):
        for s in ('greedy_gaussian', 'greedy_em'):
            assert _multi_med(multi_results, s, 10) < \
                0.90 * _multi_med(multi_results, 'random_nn', 10)

    def test_em_greedy_beats_random_at_mid_budget(self, multi_results):
        assert _multi_med(multi_results, 'greedy_em', 25) < \
            0.85 * _multi_med(multi_results, 'random_nn', 25)

    def test_em_beats_constant_gaussian_under_greedy(self, multi_results):
        """The point of online calibration inside the greedy: with μ_t far
        from the prior, em is >1.5× better than the fixed slope at full
        budget and wins every seed."""
        assert _multi_med(multi_results, 'greedy_em', TOTAL_BUDGET) < \
            0.65 * _multi_med(multi_results, 'greedy_gaussian', TOTAL_BUDGET)
        wins = np.mean([
            r['errors']['greedy_em'][TOTAL_BUDGET - 1]
            < r['errors']['greedy_gaussian'][TOTAL_BUDGET - 1]
            for r in multi_results])
        assert wins >= 0.9

    def test_fixed_slope_gaussian_loses_to_random_at_full_budget(self, multi_results):
        """A wrong model integrates more wrong information: the constant-1.3
        greedy ends WORSE than dumb random+NN under wide μ mismatch."""
        assert _multi_med(multi_results, 'greedy_gaussian', TOTAL_BUDGET) > \
            _multi_med(multi_results, 'random_nn', TOTAL_BUDGET)

    def test_low_slope_greedy_diverges(self, multi_results):
        """slope=1.05 under μ_t ~ U(1.0, 2.0) is badly misspecified: its
        error stops improving (or worsens) with budget, ends >2× worse than
        random, and em beats it on every seed (medians ~999 → ~1018 from
        k=25 to k=50 vs em's 222)."""
        assert _multi_med(multi_results, 'greedy_gaussian_105', TOTAL_BUDGET) >= \
            0.95 * _multi_med(multi_results, 'greedy_gaussian_105', 25)
        assert _multi_med(multi_results, 'greedy_gaussian_105', TOTAL_BUDGET) > \
            2.0 * _multi_med(multi_results, 'random_nn', TOTAL_BUDGET)
        wins = np.mean([
            r['errors']['greedy_em'][TOTAL_BUDGET - 1]
            < r['errors']['greedy_gaussian_105'][TOTAL_BUDGET - 1]
            for r in multi_results])
        assert wins >= 0.9

    def test_budget_efficiency(self, multi_results):
        """greedy_em at 35 pings ≤ random+NN at all 50."""
        assert _multi_med(multi_results, 'greedy_em', 35) < \
            _multi_med(multi_results, 'random_nn', TOTAL_BUDGET)

    def test_em_greedy_beats_random_at_full_budget(self, multi_results):
        """The former TODOS-#3 headline ('greedy doesn't beat random'),
        resolved on synthetic data by budget-aware deprioritisation +
        online calibration."""
        assert _multi_med(multi_results, 'greedy_em', TOTAL_BUDGET) < \
            0.55 * _multi_med(multi_results, 'random_nn', TOTAL_BUDGET)
        wins = np.mean([
            r['errors']['greedy_em'][TOTAL_BUDGET - 1]
            < r['errors']['random_nn'][TOTAL_BUDGET - 1]
            for r in multi_results])
        assert wins >= 0.9

    def test_em_greedy_tracks_oracle(self, multi_results):
        assert _multi_med(multi_results, 'greedy_em', TOTAL_BUDGET) < \
            1.6 * _multi_med(multi_results, 'oracle', TOTAL_BUDGET)

    def test_estimation_quality_gates_selection(self, multi_results):
        """greedy_hard's misleading regions make its ping choices worse
        than random spending at mid-budget; the soft-region greedies with
        identical selection logic do fine."""
        assert _multi_med(multi_results, 'greedy_hard', 25) > \
            _multi_med(multi_results, 'random_nn', 25)
        assert _multi_med(multi_results, 'greedy_gaussian', 25) < \
            _multi_med(multi_results, 'random_nn', 25)

    def test_greedy_additive_wins_early_budget(self, multi_results):
        """Model misspecification matters little in the early regime, where
        allocation dominates: the additive greedy's trust-discounted utility
        is the best non-oracle allocator at k=10 even in a world its model
        class cannot represent (calibrated 821 vs random's 1214 and
        greedy_em's 1042)."""
        assert _multi_med(multi_results, 'greedy_additive', 10) < \
            0.85 * _multi_med(multi_results, 'random_nn', 10)
        assert _multi_med(multi_results, 'greedy_additive', 10) < \
            _multi_med(multi_results, 'greedy_em', 10)

    def test_greedy_additive_beats_random_at_full_budget(self, multi_results):
        assert _multi_med(multi_results, 'greedy_additive', TOTAL_BUDGET) < \
            0.75 * _multi_med(multi_results, 'random_nn', TOTAL_BUDGET)

    def test_em_stays_ahead_of_additive_in_multiplicative_world(self, multi_results):
        """Model class matches world: per-target slope em keeps the lead at
        full budget over the additive offset model (222 vs 266 calibrated).
        The mirror claim — additive wins its own world — is pinned in
        test_e2e_additive_em.py."""
        assert _multi_med(multi_results, 'greedy_em', TOTAL_BUDGET) < \
            _multi_med(multi_results, 'greedy_additive', TOTAL_BUDGET)

    def test_oracle_dominates(self, multi_results):
        for k in (10, 25, 50):
            for s in MULTI_STRATEGIES:
                if s == 'oracle':
                    continue
                assert _multi_med(multi_results, 'oracle', k) <= \
                    _multi_med(multi_results, s, k)


# ---------------------------------------------------------------------------
# EM's edge as a function of model mismatch
# ---------------------------------------------------------------------------

SWEEP_SEEDS = 8

# μ-range configs, ordered by mismatch with the fixed DEFAULT_SLOPE = 1.3
SWEEP_MU_CONFIGS = {
    'matched':  (1.25, 1.35),
    'moderate': (1.0, 1.6),
    'far':      (1.6, 2.0),
}
# σ-only variants, evaluated at matched μ
SWEEP_SIGMA_CONFIGS = {
    'narrow σ (1-2ms)': (1.0, 2.0),
    'wide σ (4-8ms)':   (4.0, 8.0),
}


def _em_vs_gaussian_ratio(mu_range, sigma_range=MULTI_SIGMA_RANGE) -> float:
    """Median over seeds of paired full-budget error ratios
    greedy_em / greedy_gaussian on identical scenarios (< 1 = em better)."""
    ratios = []
    for seed in range(SWEEP_SEEDS):
        sc = make_multi_scenario(seed, mu_range=mu_range,
                                 sigma_range=sigma_range)
        gauss_final = _run_greedy(sc, GAUSSIAN)[0][-1]
        sc = make_multi_scenario(seed, mu_range=mu_range,
                                 sigma_range=sigma_range)
        em_final = _run_greedy(sc, EM_GAUSSIAN)[0][-1]
        ratios.append(em_final / max(gauss_final, 1.0))
    return float(np.median(ratios))


def compute_mismatch_ratios() -> tuple[dict, dict]:
    """All sweep ratios in one pass — shared by the assertions and the
    figure (plot_em_edge_vs_mismatch.py)."""
    mu_ratios = {name: _em_vs_gaussian_ratio(rng)
                 for name, rng in SWEEP_MU_CONFIGS.items()}
    sigma_ratios = {name: _em_vs_gaussian_ratio(SWEEP_MU_CONFIGS['matched'],
                                                sigma_range=rng)
                    for name, rng in SWEEP_SIGMA_CONFIGS.items()}
    return mu_ratios, sigma_ratios


class TestEMEdgeVsModelMismatch:
    """
    How much online μ-calibration buys, as a function of how wrong the
    fixed DEFAULT_SLOPE = 1.3 assumption is. Calibrated ratios (median of
    paired em/gaussian full-budget errors; < 1 = em better):

        μ_t ~ U(1.25, 1.35)  (matched prior)   ratio ≈ 0.93
        μ_t ~ U(1.0, 1.6)    (moderate)        ratio ≈ 0.56
        μ_t ~ U(1.6, 2.0)    (far)             ratio ≈ 0.31

    σ mismatch alone creates NO em edge (ratios ≈ 0.84 / 0.99 for narrow /
    wide σ at matched μ): with one shared σ per target the MAP location is
    σ-invariant, so learning σ cannot move the estimate. The edge is all μ.
    """

    @pytest.fixture(scope='class')
    def ratios(self):
        return compute_mismatch_ratios()

    def test_edge_grows_with_mismatch(self, ratios):
        mu_ratios, _ = ratios
        print(f"\nem/gaussian ratios: {mu_ratios}")
        assert mu_ratios['far'] < mu_ratios['moderate'] < mu_ratios['matched']

    def test_no_free_lunch_when_prior_is_right(self, ratios):
        """With μ_t ≈ the prior there is nothing to learn: em ≈ gaussian
        (and must not be much worse — calibration shouldn't hurt)."""
        mu_ratios, _ = ratios
        assert 0.75 <= mu_ratios['matched'] <= 1.10

    def test_large_edge_when_prior_is_wrong(self, ratios):
        mu_ratios, _ = ratios
        assert mu_ratios['far'] < 0.5

    def test_sigma_mismatch_alone_creates_no_edge(self, ratios):
        """Shared-σ MAP is σ-invariant: wide vs narrow σ at matched μ
        leaves em ≈ gaussian either way."""
        _, sigma_ratios = ratios
        for label, ratio in sigma_ratios.items():
            assert 0.7 <= ratio <= 1.15, (
                f"{label}: ratio {ratio:.2f} — expected ≈ 1"
            )

    def test_generate_figure(self, ratios):
        """Renders tests/em_edge_vs_mismatch.pdf from the same ratios the
        assertions above checked."""
        from plot_em_edge_vs_mismatch import make_figure, OUT_PATH
        mu_ratios, sigma_ratios = ratios
        path = make_figure(mu_ratios, sigma_ratios, OUT_PATH)
        assert os.path.exists(path)
        assert os.path.getsize(path) > 5_000
