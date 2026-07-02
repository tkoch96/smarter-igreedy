"""
EM's edge vs model mismatch — figure for TestEMEdgeVsModelMismatch
(test_e2e_adaptive_em.py).

One point per μ-range config: the median paired full-budget error ratio
greedy_em / greedy_gaussian on identical multi-target scenarios (< 1 = em
better; the right-hand labels translate to "how many times better"). The
grey diamonds are the σ-only variants at matched μ: σ mismatch alone
creates no edge because the shared-σ MAP location is σ-invariant.

Run directly:
    cd ~/Documents/smarter-igreedy
    python tests/plot_em_edge_vs_mismatch.py

Or included automatically when running pytest (see test_generate_figure in
TestEMEdgeVsModelMismatch).

Saves:  tests/em_edge_vs_mismatch.pdf
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from feasible_region_maintainer import DEFAULT_SLOPE

OUT_PATH = os.path.join(os.path.dirname(__file__), 'em_edge_vs_mismatch.pdf')


def make_figure(mu_ratios: dict = None, sigma_ratios: dict = None,
                output_path: str = OUT_PATH) -> str:
    if mu_ratios is None or sigma_ratios is None:
        from test_e2e_adaptive_em import compute_mismatch_ratios
        mu_ratios, sigma_ratios = compute_mismatch_ratios()

    from test_e2e_adaptive_em import SWEEP_MU_CONFIGS, SWEEP_SEEDS

    names = list(SWEEP_MU_CONFIGS)          # matched, moderate, far
    xs = range(len(names))
    ys = [mu_ratios[n] for n in names]

    fig, ax = plt.subplots(figsize=(7.5, 5))
    ax.axhline(1.0, color='grey', linestyle='--', linewidth=1)
    ax.text(0.98, 1.02, 'no edge (em = gaussian)', fontsize=8, color='grey',
            ha='right', transform=ax.get_yaxis_transform())

    ax.plot(xs, ys, marker='o', markersize=9, color='crimson', linewidth=2,
            label='μ-range sweep (σ ~ U(1, 6) ms)')
    for x, name in zip(xs, names):
        r = mu_ratios[name]
        ax.annotate(f'{r:.2f}  (em ≈ {1 / r:.1f}× better)', (x, r),
                    textcoords='offset points', xytext=(10, -4), fontsize=9)

    # σ-only variants at matched μ
    for dx, (label, r) in zip((-0.12, 0.12), sigma_ratios.items()):
        ax.plot([0 + dx], [r], marker='D', markersize=7, color='dimgrey',
                zorder=5)
        ax.annotate(label, (0 + dx, r), textcoords='offset points',
                    xytext=(8, 6), fontsize=8, color='dimgrey')

    ax.set_xticks(list(xs))
    ax.set_xticklabels([
        f'{name}\nμ_t ~ U{SWEEP_MU_CONFIGS[name]}' for name in names
    ], fontsize=9)
    ax.set_ylim(0, 1.25)
    ax.set_ylabel('median paired error ratio  em / gaussian  (full budget)')
    ax.set_xlabel(f'mismatch between true per-target μ and the fixed '
                  f'slope = {DEFAULT_SLOPE}')
    ax.set_title(
        f'Online μ-calibration pays in proportion to model mismatch — '
        f'and σ mismatch alone buys nothing\n'
        f'(multi-target budget allocation, {SWEEP_SEEDS} paired seeds '
        f'per config)',
        fontsize=10,
    )
    ax.grid(alpha=0.3)
    ax.legend(fontsize=9, loc='lower left')
    fig.tight_layout()
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    return output_path


if __name__ == '__main__':
    print(f'wrote {make_figure()}')
