"""
Error-vs-measurements curves for the multi-target budget-allocation
comparison (test_e2e_adaptive_em.py :: TestMultiTargetBudgetAllocation).

The project objective, plotted: given a TOTAL ping budget shared across
N_TARGETS targets (each with its own hidden μ_t, σ_t), minimise the AVERAGE
geolocation error. Strategies are whole systems — selection + estimation:

    random_nn        random ordering + nearest-neighbour estimation
    greedy_hard      Iterative_Greedy_Geolocator, hard_circle regions
    greedy_gaussian  Iterative_Greedy_Geolocator, gaussian regions
    greedy_em        Iterative_Greedy_Geolocator, em_gaussian regions
    oracle           closest-VP-first per target + true (μ_t, σ_t)  (cheat)

Median stop-markers (▼) show where the greedy variants declare every target
geolocated and stop spending budget — the curves are flat after that point.

Run directly:
    cd ~/Documents/smarter-igreedy
    python tests/plot_error_adaptive_em.py

Or included automatically when running pytest (see TestGenerateFigure in
test_e2e_adaptive_em.py).

Saves:  tests/error_over_measurements_adaptive.pdf
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from test_e2e_adaptive_em import (
    run_multi_seed, MULTI_STRATEGIES, N_TARGETS, TOTAL_BUDGET,
    MULTI_MU_RANGE, MULTI_SIGMA_RANGE,
)

OUT_PATH = os.path.join(os.path.dirname(__file__), 'error_over_measurements_adaptive.pdf')

N_SEEDS = 20

LABELS = {
    'random_nn':           'random + nearest neighbour',
    'greedy_hard':         'greedy, hard_circle regions',
    'greedy_gaussian':     'greedy, gaussian slope=1.3',
    'greedy_gaussian_105': 'greedy, gaussian slope=1.05',
    'greedy_em':           'greedy, em_gaussian regions',
    'greedy_additive':     'greedy, additive regions (shared src/dst model)',
    'oracle':              'oracle (true locations & μ, σ)',
}
STYLES = {
    'random_nn':           dict(color='grey',       linestyle=':'),
    'greedy_hard':         dict(color='steelblue',  linestyle='--'),
    'greedy_gaussian':     dict(color='darkorange'),
    'greedy_gaussian_105': dict(color='mediumseagreen', linestyle='--'),
    'greedy_em':           dict(color='crimson',    linewidth=2.2),
    'greedy_additive':     dict(color='darkviolet', linewidth=2.0),
    'oracle':              dict(color='black',      linestyle='-.'),
}


def run_simulation() -> dict:
    """Returns {'curves': {strategy: (n_seeds, TOTAL_BUDGET) errors},
    'pings_used': {strategy: list}}."""
    curves = {s: [] for s in MULTI_STRATEGIES}
    pings_used = {s: [] for s in MULTI_STRATEGIES}
    for seed in range(N_SEEDS):
        row = run_multi_seed(seed)
        for s in MULTI_STRATEGIES:
            curves[s].append(row['errors'][s])
            pings_used[s].append(row['pings_used'][s])
    return {'curves': {s: np.array(v) for s, v in curves.items()},
            'pings_used': pings_used}


def plot(sim: dict) -> None:
    budgets = np.arange(1, TOTAL_BUDGET + 1)
    fig, ax = plt.subplots(figsize=(8.5, 5.2))

    for s in MULTI_STRATEGIES:
        med = np.median(sim['curves'][s], axis=0)
        ax.plot(budgets, med, label=LABELS[s], **STYLES[s])
        stop = float(np.median(sim['pings_used'][s]))
        if stop < TOTAL_BUDGET:
            k = int(round(stop))
            ax.plot([k], [med[k - 1]], marker='v', markersize=9,
                    color=STYLES[s]['color'], zorder=6)
            ax.annotate('stops', (k, med[k - 1]),
                        textcoords='offset points', xytext=(6, 8),
                        fontsize=8, color=STYLES[s]['color'])

    ax.set_xlabel('total measurements spent (across all targets)')
    ax.set_ylabel(f'median of avg geolocation error over {N_TARGETS} targets (km)')
    ax.set_title(
        f'{N_TARGETS} targets at random European locations, 10 VPs, '
        f'per-target μ ~ U{MULTI_MU_RANGE}, σ ~ U{MULTI_SIGMA_RANGE} ms — {N_SEEDS} seeds\n'
        f'unestimated targets incur the 10,000 km penalty '
        f'(dominates the early regime)',
        fontsize=10,
    )
    # Linear axis; cap the view so the early penalty-dominated regime
    # (~8000km while targets are still unmeasured) doesn't squash the
    # interesting 200-1500km range where the strategies separate.
    ax.set_ylim(0, 2000)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=9)
    fig.tight_layout()
    fig.savefig(OUT_PATH, bbox_inches='tight')
    plt.close(fig)


if __name__ == '__main__':
    plot(run_simulation())
    print(f'wrote {OUT_PATH}')
