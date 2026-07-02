"""
Error-vs-measurements curve for the probabilistic geolocation comparison.

Run directly:
    cd ~/Documents/smarter-igreedy
    python tests/plot_error_over_measurements.py

Or included automatically when running pytest (see test_generate_figure in
test_e2e_probabilistic.py).

Saves:  tests/error_over_measurements.pdf
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from feasible_region_maintainer import FeasibleRegion, HARD_CIRCLE, GAUSSIAN
from probabilistic_helpers import KM_PER_MS, GLOBAL_SIGMA_MS
from utils import get_distance, LatLon, simulation_mode

from test_e2e_probabilistic import (
    ALL_LOCS, VP_NAMES, VP_SIGMA_TRUE,
    make_synthetic_data,
)


# ---------------------------------------------------------------------------
# Strategy implementations (budget-aware)
# ---------------------------------------------------------------------------

def _random_vp_order(rtts: dict[str, float], rng: np.random.Generator) -> list[str]:
    order = list(rtts.keys())
    rng.shuffle(order)
    return order


def _oracle_vp_order(vp_locs: dict[str, LatLon], target_loc: LatLon) -> list[str]:
    """VPs sorted by true distance to target (closest first). Call outside simulation_mode()."""
    return sorted(vp_locs, key=lambda n: get_distance(vp_locs[n], target_loc))


def error_random(rtts: dict[str, float], k: int, order: list[str],
                 vp_locs: dict[str, LatLon], target_loc: LatLon) -> float:
    """Nearest-neighbour: location of the lowest-RTT VP in the first k pings.
    vp_locs and target_loc resolved outside simulation_mode()."""
    subset = {n: rtts[n] for n in order[:k]}
    best = min(subset, key=subset.get)
    return get_distance(vp_locs[best], target_loc)


def error_hard_circle(rtts: dict[str, float], k: int, order: list[str],
                      vp_locs: dict[str, LatLon], target_loc: LatLon,
                      multiplier: float = 1.05) -> float:
    region = FeasibleRegion('t', mode=HARD_CIRCLE, radius_multiplier=multiplier)
    for name in order[:k]:
        region.add_measurement(vp_locs[name], max(0.0, rtts[name]))
    return get_distance(region.get_location(), target_loc)


def error_gaussian(rtts: dict[str, float], k: int, order: list[str],
                   vp_locs: dict[str, LatLon], target_loc: LatLon) -> float:
    region = FeasibleRegion('t', mode=GAUSSIAN)
    for name in order[:k]:
        region.add_measurement(vp_locs[name], rtts[name], sigma_ms=GLOBAL_SIGMA_MS)
    return get_distance(region.get_location(), target_loc)


def error_oracle(rtts: dict[str, float], k: int, oracle_order: list[str],
                 vp_locs: dict[str, LatLon], target_loc: LatLon) -> float:
    """Gaussian with true sigma and k closest VPs."""
    region = FeasibleRegion('t', mode=GAUSSIAN)
    for name in oracle_order[:k]:
        region.add_measurement(vp_locs[name], rtts[name], sigma_ms=VP_SIGMA_TRUE[name])
    return get_distance(region.get_location(), target_loc)


# ---------------------------------------------------------------------------
# Main simulation
# ---------------------------------------------------------------------------

N_SEEDS = 200
N_VPS   = len(VP_NAMES)
BUDGETS = list(range(1, N_VPS + 1))

STRATEGIES = ('random', 'hard_circle_1_3', 'hard_circle', 'gaussian', 'oracle')


def run_simulation() -> tuple[dict, dict]:
    """Returns (means, stds) dicts shaped {strategy: np.ndarray of length N_VPS}."""
    errors: dict[str, list[list[float]]] = {s: [[] for _ in BUDGETS] for s in STRATEGIES}

    # Resolve VP and target locations once — outside any lock.
    vp_locs: dict[str, LatLon] = {name: ALL_LOCS[name] for name in VP_NAMES}
    target_loc: LatLon = ALL_LOCS['_target']

    for seed in range(N_SEEDS):
        target_rng = np.random.default_rng(seed + 10_000)
        order_rng  = np.random.default_rng(seed + 30_000)

        data         = make_synthetic_data(target_rng)
        target_rtts  = {vp: min(data['loc_loc_meas'][vp]['_target']) for vp in VP_NAMES}
        order        = _random_vp_order(target_rtts, order_rng)
        oracle_order = _oracle_vp_order(vp_locs, target_loc)

        # Inference is inside the lock; error computation uses pre-resolved locs.
        with simulation_mode(ALL_LOCS):
            for i, k in enumerate(BUDGETS):
                errors['random'][i].append(
                    error_random(target_rtts, k, order, vp_locs, target_loc))
                errors['hard_circle_1_3'][i].append(
                    error_hard_circle(target_rtts, k, order, vp_locs, target_loc, multiplier=1.3))
                errors['hard_circle'][i].append(
                    error_hard_circle(target_rtts, k, order, vp_locs, target_loc, multiplier=1.05))
                errors['gaussian'][i].append(
                    error_gaussian(target_rtts, k, order, vp_locs, target_loc))
                errors['oracle'][i].append(
                    error_oracle(target_rtts, k, oracle_order, vp_locs, target_loc))

    means = {s: np.array([np.mean(errors[s][i]) for i in range(N_VPS)]) for s in STRATEGIES}
    stds  = {s: np.array([np.std( errors[s][i]) for i in range(N_VPS)]) for s in STRATEGIES}
    return means, stds


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

STYLE = {
    'random':          dict(color='#888888', ls='--',  lw=1.5, label='random (nearest-neighbour)'),
    'hard_circle_1_3': dict(color='#C44E2A', ls='-.',  lw=1.8, label='hard-circle (1.3×, original)'),
    'hard_circle':     dict(color='#E07B39', ls='-.',  lw=2.0, label='hard-circle (1.05×, tightened)'),
    'gaussian':        dict(color='#3A7FBF', ls='-',   lw=2.5, label='gaussian MAP (global σ, honest)'),
    'oracle':          dict(color='#2EAA5A', ls=':',   lw=2.0, label='oracle (true σ, best VPs)'),
}

OUT_PATH = os.path.join(os.path.dirname(__file__), 'error_over_measurements.pdf')


def plot(means: dict, stds: dict, out_path: str = OUT_PATH) -> None:
    fig, ax = plt.subplots(figsize=(7, 4.5))

    for s in STRATEGIES:
        kw = STYLE[s]
        ax.plot(BUDGETS, means[s], **kw)
        ax.fill_between(
            BUDGETS,
            means[s] - stds[s] / np.sqrt(N_SEEDS),
            means[s] + stds[s] / np.sqrt(N_SEEDS),
            color=kw['color'], alpha=0.15,
        )

    ax.set_xlabel('Number of VP measurements used', fontsize=11)
    ax.set_ylabel('Mean geolocation error (km)', fontsize=11)
    ax.set_title(
        f'Geolocation error vs measurement budget\n'
        f'Synthetic data, 10 VPs  ({N_SEEDS} seeds)',
        fontsize=11,
    )
    ax.set_xticks(BUDGETS)
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
    ax.grid(axis='y', ls='--', alpha=0.4)
    ax.grid(axis='y', which='minor', ls=':', alpha=0.2)
    ax.legend(fontsize=9, loc='upper right')
    ax.set_xlim(1, N_VPS)
    ax.set_ylim(bottom=0)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"Saved → {out_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    print(f"Running simulation: {N_SEEDS} seeds × {N_VPS} budgets …")
    means, stds = run_simulation()
    plot(means, stds)
