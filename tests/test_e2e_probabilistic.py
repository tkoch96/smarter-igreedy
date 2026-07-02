"""
End-to-end integration test: exercises the real assess_geolocators pipeline
with synthetic data.

Data model
----------
    rtt_vp = DEFAULT_SLOPE × d(vp, target) / 100  +  N(0, sigma_vp²)

Ground truth carries realistic routing overhead (DEFAULT_SLOPE = 1.3× SOL —
pure SOL never happens on real fiber), and the estimators assume the same
slope, so the model is correctly specified and the Gaussian MAP is consistent.

Information boundary
--------------------
ALL_LOCS is a LockedLocationDict.  During the simulation block it is locked:
any accidental read of a ground-truth coordinate raises ValueError immediately.
VP locations are resolved once before the lock (legitimate measurement metadata).
Target location is only read after the lock is released, for error computation.

What is actually called
-----------------------
Estimation uses Geolocator_Comparator.convert_measurements_to_locations()
with different measurement_converter_mode settings — the same code path as
the real pipeline.  Measurement ordering uses Random_Geolocator.set_data /
measurements() — also the real code.

Oracle is the only exception: it uses the true per-VP sigma from the
data-generating model, which is never available in practice.

Claims tested
-------------
(a) gaussian works:               gaussian median error < 250 km
(b) both triangulate:             gaussian AND hard_circle both beat random median
(c) gaussian > random: gaussian wins majority of seeds vs random
(d) oracle  > gaussian:           oracle median < gaussian median

Note: without per-VP sigma calibration (which would require knowing VP distances),
Gaussian and hard_circle are roughly comparable.  The calibrated advantage of
Gaussian over hard_circle is tested separately via the oracle gap.
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import math
import numpy as np
import pytest

from assess_geolocators import Geolocator_Comparator
from random_geolocator import Random_Geolocator
from feasible_region_maintainer import FeasibleRegion, GAUSSIAN, DEFAULT_SLOPE
from probabilistic_helpers import KM_PER_MS
from utils import get_distance, LatLon, LockedLocationDict, simulation_mode

# The synthetic ground truth uses the same slope model as the estimators
# ("same model for now" — decouple later to study misspecification).
GROUND_TRUTH_SLOPE = DEFAULT_SLOPE


# ---------------------------------------------------------------------------
# Problem definition
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

VP_SIGMA_TRUE: dict[str, float] = {
    'london':    2.0,
    'paris':     1.5,
    'berlin':    2.5,
    'rome':      3.5,
    'madrid':    3.0,
    'amsterdam': 1.5,
    'warsaw':    2.0,
    'stockholm': 3.0,
    'new_york':  8.0,
    'istanbul':  4.0,
}


# ---------------------------------------------------------------------------
# Synthetic data generator — real pipeline format
# ---------------------------------------------------------------------------

def _rtt(src_loc: LatLon, dst_loc: LatLon, sigma_ms: float, rng: np.random.Generator) -> float:
    sol = get_distance(src_loc, dst_loc) / KM_PER_MS
    return GROUND_TRUTH_SLOPE * sol + float(rng.normal(0.0, sigma_ms))


def make_synthetic_data(rng: np.random.Generator) -> dict:
    """
    Returns {'address_to_loc': ..., 'loc_loc_meas': ...} in the real pipeline
    format.  Each VP pings the target once; RTTs drawn from the assumed model.
    Reads ALL_LOCS directly — must be called outside simulation_mode().
    """
    address_to_loc = dict(ALL_LOCS)   # includes _target and all VPs
    loc_loc_meas: dict[str, dict[str, list[float]]] = {}
    for vp in VP_NAMES:
        rtt = _rtt(ALL_LOCS[vp], ALL_LOCS['_target'], VP_SIGMA_TRUE[vp], rng)
        loc_loc_meas[vp] = {'_target': [rtt]}
    return {'address_to_loc': address_to_loc, 'loc_loc_meas': loc_loc_meas}


# ---------------------------------------------------------------------------
# Oracle (explicit upper bound — uses true sigma, not in the real pipeline)
# ---------------------------------------------------------------------------

def estimate_oracle(data: dict) -> LatLon:
    """Gaussian MAP with the true per-VP sigma. Reads VP locs from data."""
    address_to_loc = data['address_to_loc']
    region = FeasibleRegion('_target', mode=GAUSSIAN)
    for vp, dsts in data['loc_loc_meas'].items():
        rtt = min(dsts['_target'])
        region.add_measurement(address_to_loc[vp], rtt, sigma_ms=VP_SIGMA_TRUE[vp])
    return region.get_location()


# ---------------------------------------------------------------------------
# Multi-seed fixture
# ---------------------------------------------------------------------------

N_SEEDS = 80

@pytest.fixture(scope='module')
def results():
    # VP locations resolved once outside the lock — measurement metadata.
    vp_locs: dict[str, LatLon] = {vp: ALL_LOCS[vp] for vp in VP_NAMES}

    rows = []
    for seed in range(N_SEEDS):
        rng = np.random.default_rng(seed)

        # Generate synthetic data before locking (reads ALL_LOCS internally).
        data = make_synthetic_data(rng)

        # Build a comparator pointed at this seed's data.
        comparator = Geolocator_Comparator()
        comparator.target_data = data

        # Measurement ordering via the real Random_Geolocator.
        rng_geolocator = Random_Geolocator()
        rng_geolocator.set_data(data)
        meas = rng_geolocator.measurements(len(VP_NAMES))  # full budget

        # --- simulation phase: ALL_LOCS locked ---
        with simulation_mode(ALL_LOCS):
            comparator.measurement_converter_mode = 'nearest_neighbor'
            random_locs = comparator.convert_measurements_to_locations(meas)

            comparator.measurement_converter_mode = 'hard_circle'
            hc_locs = comparator.convert_measurements_to_locations(meas)

            comparator.measurement_converter_mode = 'gaussian'
            gauss_locs = comparator.convert_measurements_to_locations(meas)

            oracle_pt = estimate_oracle(data)

        # --- assessment phase: ALL_LOCS unlocked ---
        target_loc = ALL_LOCS['_target']
        def err(locs, key='_target'):
            return get_distance(locs[key], target_loc) if key in locs else 10_000.0

        rows.append({
            'random': err(random_locs),
            'hard_circle':       err(hc_locs),
            'gaussian':          err(gauss_locs),
            'oracle':            get_distance(oracle_pt, target_loc),
        })
    return rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _med(rows, key):  return float(np.median([r[key] for r in rows]))
def _mean(rows, key): return float(np.mean([r[key] for r in rows]))
def _wins(rows, a, b): return sum(1 for r in rows if r[a] < r[b]) / len(rows)


# ---------------------------------------------------------------------------
# (a) Gaussian works
# ---------------------------------------------------------------------------

class TestGaussianWorks:
    def test_median_error_under_250km(self, results):
        assert _med(results, 'gaussian') < 250.0, \
            f"Gaussian median {_med(results, 'gaussian'):.1f} km — expected < 250 km"

    def test_p90_under_600km(self, results):
        p90 = float(np.percentile([r['gaussian'] for r in results], 90))
        assert p90 < 600.0, f"Gaussian p90 {p90:.1f} km — expected < 600 km"

    def test_estimate_finite_and_in_bounds(self, results):
        rng  = np.random.default_rng(999)
        data = make_synthetic_data(rng)
        comparator = Geolocator_Comparator()
        comparator.target_data = data
        rg = Random_Geolocator()
        rg.set_data(data)
        meas = rg.measurements(len(VP_NAMES))
        with simulation_mode(ALL_LOCS):
            comparator.measurement_converter_mode = 'gaussian'
            locs = comparator.convert_measurements_to_locations(meas)
        lat, lon = locs['_target']
        assert math.isfinite(lat) and -90 <= lat <= 90
        assert math.isfinite(lon) and -180 <= lon <= 180


# ---------------------------------------------------------------------------
# (b) Gaussian beats hard_circle
# ---------------------------------------------------------------------------

class TestTriangulationBeatsNearestNeighbour:
    def test_gaussian_median_beats_nn(self, results):
        med_g = _med(results, 'gaussian')
        med_n = _med(results, 'random')
        assert med_g < med_n, \
            f"Gaussian ({med_g:.1f} km) should beat nearest-neighbour ({med_n:.1f} km)"

    def test_hard_circle_median_beats_nn(self, results):
        med_h = _med(results, 'hard_circle')
        med_n = _med(results, 'random')
        assert med_h < med_n, \
            f"Hard-circle ({med_h:.1f} km) should beat nearest-neighbour ({med_n:.1f} km)"

    def test_hard_circle_notably_worse_than_oracle(self, results):
        assert _med(results, 'hard_circle') > _med(results, 'oracle') * 1.3, \
            "hard-circle should be at least 1.3× worse than oracle (calibration gap)"


# ---------------------------------------------------------------------------
# (c) Gaussian beats random
# ---------------------------------------------------------------------------

class TestGaussianBeatsRandom:
    def test_gaussian_mean_beats_nn(self, results):
        assert _mean(results, 'gaussian') < _mean(results, 'random'), \
            f"Gaussian mean ({_mean(results, 'gaussian'):.1f} km) should beat " \
            f"nearest-neighbour ({_mean(results, 'random'):.1f} km)"

    def test_gaussian_wins_majority_vs_nn(self, results):
        rate = _wins(results, 'gaussian', 'random')
        assert rate >= 0.55, \
            f"Gaussian beats nearest-neighbour on {100*rate:.0f}% of seeds — expected ≥ 55%"


# ---------------------------------------------------------------------------
# (d) Oracle beats Gaussian
# ---------------------------------------------------------------------------

class TestOracleBetterThanGaussian:
    def test_oracle_median_beats_gaussian(self, results):
        assert _med(results, 'oracle') < _med(results, 'gaussian'), \
            f"Oracle ({_med(results, 'oracle'):.1f} km) should beat " \
            f"gaussian ({_med(results, 'gaussian'):.1f} km)"

    def test_oracle_wins_majority_vs_gaussian(self, results):
        rate = _wins(results, 'oracle', 'gaussian')
        assert rate >= 0.55, \
            f"Oracle beats gaussian on {100*rate:.0f}% of seeds — expected ≥ 55%"


# ---------------------------------------------------------------------------
# Full ranking
# ---------------------------------------------------------------------------

class TestFullRanking:
    def test_median_ranking(self, results):
        med = {k: _med(results, k) for k in ('oracle', 'gaussian', 'hard_circle', 'random')}
        print(f"\nMedian errors (km):  oracle={med['oracle']:.0f}  "
              f"gaussian={med['gaussian']:.0f}  "
              f"hard_circle={med['hard_circle']:.0f}  "
              f"random={med['random']:.0f}")
        # Oracle beats both triangulation methods
        assert med['oracle'] <= med['gaussian'], \
            f"oracle ({med['oracle']:.1f}) should be ≤ gaussian ({med['gaussian']:.1f})"
        assert med['oracle'] <= med['hard_circle'], \
            f"oracle ({med['oracle']:.1f}) should be ≤ hard_circle ({med['hard_circle']:.1f})"
        # Both triangulation methods beat nearest-neighbour
        assert med['gaussian']    < med['random'], \
            f"gaussian ({med['gaussian']:.1f}) should be < random ({med['random']:.1f})"
        assert med['hard_circle'] < med['random'], \
            f"hard_circle ({med['hard_circle']:.1f}) should be < random ({med['random']:.1f})"


# ---------------------------------------------------------------------------
# Figure generation
# ---------------------------------------------------------------------------

class TestGenerateFigure:
    def test_error_over_measurements_pdf(self):
        """Generate tests/error_over_measurements.pdf."""
        from plot_error_over_measurements import run_simulation, plot, OUT_PATH
        means, stds = run_simulation()
        plot(means, stds)
        assert os.path.exists(OUT_PATH), f"Figure not written to {OUT_PATH}"
