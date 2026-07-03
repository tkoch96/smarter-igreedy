"""
Error-vs-measurements curves under the ADDITIVE two-way overhead world
(test_e2e_additive_em.py :: TestAdditiveBudgetSweep).

Ground truth: rtt = SOL + X_src + X_dst with per-node hidden (μ, σ), two
pathological destinations per scenario. Every strategy sees the SAME random
measurement order — the lines differ only in estimation — except
greedy_additive, which selects its own pings:

    random_nn        lowest-RTT VP seen
    const_gaussian   fixed slope 1.3
    per_target_em    per-target multiplicative (μ_t, σ_t)
    additive_em      per-source AND per-destination (μ, σ) — the only model
                     class that can represent this world
    greedy_additive  Iterative_Greedy_Geolocator + shared AdditiveLatencyModel,
                     σ̂_dst-discounted utility (selection + estimation system)
    oracle           whole-system cheat: Perfect_Geolocator selection
                     (error-guided greedy on ground truth) + true per-node
                     parameters — an upper bound at every budget

Plots the MEAN across seeds of the per-seed avg-over-targets error — the
same statistic assess_geolocators.run() reports.

Run directly:
    cd ~/Documents/smarter-igreedy
    python tests/plot_error_additive.py

Or included automatically when running pytest (see test_generate_figure in
TestAdditiveBudgetSweep).

Saves:  tests/error_over_measurements_additive.pdf
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

OUT_PATH = os.path.join(os.path.dirname(__file__), 'error_over_measurements_additive.pdf')

LABELS = {
    'random_nn':      'nearest neighbour',
    'const_gaussian': 'gaussian, slope = 1.3 (constant)',
    'per_target_em':  'per-target em (multiplicative μ_t, σ_t)',
    'additive_em':    'additive em (per-source AND per-dest μ, σ)',
    'greedy_additive': 'GREEDY selection + additive em (σ̂_dst in utility)',
    'greedy_additive_info': 'GREEDY info-gain selection (hypothesis disagreement)',
    'oracle':         'oracle (Perfect_Geolocator selection + true μ, σ)',
}
STYLES = {
    'random_nn':      dict(color='grey',       linestyle=':'),
    'const_gaussian': dict(color='darkorange'),
    'per_target_em':  dict(color='steelblue',  linestyle='--'),
    'additive_em':    dict(color='crimson',    linewidth=2.2),
    'greedy_additive': dict(color='darkviolet', linewidth=2.2),
    'greedy_additive_info': dict(color='deeppink', linewidth=2.2),
    'oracle':         dict(color='black',      linestyle='-.'),
}


def make_figure(rows=None, output_path: str = OUT_PATH) -> str:
    from test_e2e_additive_em import (
        run_additive_budget_seed, BUDGET_GRID, SWEEP_STRATEGIES,
        N_SWEEP_SEEDS, N_TARGETS, N_PATHOLOGICAL,
    )
    if rows is None:
        rows = [run_additive_budget_seed(seed) for seed in range(N_SWEEP_SEEDS)]

    fig, ax = plt.subplots(figsize=(8.5, 5.2))
    for s in SWEEP_STRATEGIES:
        mean = [float(np.mean([r['curves'][s][i] for r in rows]))
                for i in range(len(BUDGET_GRID))]
        ax.plot(BUDGET_GRID, mean, marker='.', label=LABELS[s], **STYLES[s])

    ax.set_xlabel('total measurements spent (across all targets)')
    ax.set_ylabel(f'mean avg geolocation error over {N_TARGETS} targets (km)')
    ax.set_title(
        f'Additive world: rtt = SOL + X_src + X_dst, per-node hidden (μ, σ), '
        f'{N_PATHOLOGICAL} pathological destinations —\n'
        f'{len(rows)} seeds, identical random order for every estimator '
        f'except greedy_additive, which selects its own pings',
        fontsize=10,
    )
    # High enough to show the early regime where selection separates from
    # the random-order estimators (means sit at 3500-4300 km at b=10).
    ax.set_ylim(0, 4600)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=9)
    fig.tight_layout()
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    return output_path


if __name__ == '__main__':
    print(f'wrote {make_figure()}')
