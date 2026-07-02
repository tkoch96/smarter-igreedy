"""
Error-vs-measurements curves for the online-EM comparison
(test_e2e_adaptive_em.py — per-target unknown μ/σ data model).

Strategies: random+NN, gaussian slope=1.0 (SOL), gaussian slope=1.3
(constant), em_gaussian (online per-target μ/σ), oracle (true μ/σ).

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

from test_e2e_adaptive_em import run_seed, STRATEGIES, VP_NAMES, MU_RANGE, SIGMA_RANGE
from feasible_region_maintainer import DEFAULT_SLOPE

OUT_PATH = os.path.join(os.path.dirname(__file__), 'error_over_measurements_adaptive.pdf')

N_SEEDS = 120

LABELS = {
    'random': 'random + nearest neighbour',
    'sol':    'gaussian, slope = 1.0 (straight SOL)',
    'const':  f'gaussian, slope = {DEFAULT_SLOPE} (constant)',
    'em':     'em_gaussian (online per-target μ, σ)',
    'oracle': 'oracle (true per-target μ, σ)',
}
STYLES = {
    'random': dict(color='grey',      linestyle=':'),
    'sol':    dict(color='steelblue', linestyle='--'),
    'const':  dict(color='darkorange'),
    'em':     dict(color='crimson',   linewidth=2.2),
    'oracle': dict(color='black',     linestyle='-.'),
}


def run_simulation() -> dict[str, np.ndarray]:
    """Returns {strategy: (n_seeds, n_budgets) error matrix}."""
    per_strategy: dict[str, list[list[float]]] = {s: [] for s in STRATEGIES}
    for seed in range(N_SEEDS):
        row = run_seed(seed)
        for s in STRATEGIES:
            per_strategy[s].append(row['errors'][s])
    return {s: np.array(v) for s, v in per_strategy.items()}


def plot(curves: dict[str, np.ndarray]) -> None:
    budgets = np.arange(1, len(VP_NAMES) + 1)
    fig, ax = plt.subplots(figsize=(8, 5))

    for s in STRATEGIES:
        med = np.median(curves[s], axis=0)
        ax.plot(budgets, med, label=LABELS[s], **STYLES[s])

    ax.set_xlabel('number of measurements')
    ax.set_ylabel('median geolocation error (km)')
    ax.set_title(
        f'Per-target unknown μ ~ U{MU_RANGE}, σ ~ U{SIGMA_RANGE} ms — '
        f'{N_SEEDS} seeds',
        fontsize=10,
    )
    ax.set_xticks(budgets)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=9)
    fig.tight_layout()
    fig.savefig(OUT_PATH, bbox_inches='tight')
    plt.close(fig)


if __name__ == '__main__':
    plot(run_simulation())
    print(f'wrote {OUT_PATH}')
