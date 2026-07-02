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

def run_seed(seed: int) -> dict:
    """
    Simulate one target with its own (μ_t, σ_t); feed the same measurements
    to every strategy incrementally.  Returns per-strategy error curves
    (index k-1 = error after k measurements) plus the EM parameter fits.
    """
    rng = np.random.default_rng(seed)
    mu_t = float(rng.uniform(*MU_RANGE))
    sigma_t = float(rng.uniform(*SIGMA_RANGE))

    # Resolve coordinates BEFORE the lock: VP locs are measurement metadata,
    # target loc is used only for error computation below.
    vp_locs: dict[str, LatLon] = {n: ALL_LOCS[n] for n in VP_NAMES}
    target_loc: LatLon = ALL_LOCS['_target']

    order = list(VP_NAMES)
    rng.shuffle(order)
    rtts = {
        n: mu_t * get_distance(vp_locs[n], target_loc) / KM_PER_MS
           + float(rng.normal(0.0, sigma_t))
        for n in order
    }

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
