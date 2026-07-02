"""
Region-convergence filmstrip — the 1:1 spatial companion to
error_over_measurements_adaptive.pdf (same strategies, same multi-target
data model, one representative seed).

Each ROW is one strategy from the multi-target budget-allocation comparison
(test_e2e_adaptive_em.py :: run_multi_seed), in the same order and colours
as the error curves; each COLUMN is a total-measurement count k. A cell
shows all 5 targets at once: true location (★), the strategy's current
estimate (✗, joined to its truth by a grey line) and — for region-based
strategies — a dashed circle of radius get_region_size() around the
estimate (the strategy's own uncertainty claim).

Scenario: seed 15 of make_multi_scenario — chosen programmatically as the
seed whose error curves sit closest to the 20-seed medians while preserving
the headline orderings (greedy_em < random at k=10/25, random < greedy_em
at k=50, greedy_em stops early).

What to look for:
- greedy rows fill in ALL targets quickly (allocation), then keep
  tightening: once every region is under the 200km done-threshold, the
  deprioritised leftover budget flows to the least-certain targets
  (BASICALLY_GEOLOCATED deprioritises rather than hard-stops).
- random+NN covers targets slowly (penalty-dominated early) and improves
  much more slowly per ping.
- greedy_hard's oversized regions mislead its choices.

Run directly:
    cd ~/Documents/smarter-igreedy
    python tests/plot_region_convergence.py

Saves:  tests/region_convergence.pdf
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Circle

from utils import get_distance
from test_e2e_adaptive_em import run_multi_seed, MISSING_PENALTY_KM

OUT_PATH = os.path.join(os.path.dirname(__file__), 'region_convergence.pdf')

SEED = 15                       # see module docstring for how it was chosen
SNAPSHOT_KS = (5, 10, 20, 35, 50)

# Same order / colours as plot_error_adaptive_em.py STYLES
STRATEGY_ROWS = [
    ('random_nn',       'random + NN',              'grey'),
    ('greedy_hard',     'greedy, hard_circle',      'steelblue'),
    ('greedy_gaussian', 'greedy, gaussian',         'darkorange'),
    ('greedy_em',       'greedy, em_gaussian',      'crimson'),
    ('oracle',          'oracle (true locs, μ, σ)', 'black'),
]

# Map extent (identical for every panel; fixed graticule)
LAT_RANGE = (33.0, 63.0)
LON_RANGE = (-20.0, 34.0)
KM_PER_DEG_LAT = 111.0


def _draw_cell(ax, snap: dict, scenario: dict, pings_used: int, k: int) -> None:
    vp_locs = scenario['vp_locs']
    targets = scenario['targets']

    for vlat, vlon in vp_locs.values():
        if LON_RANGE[0] <= vlon <= LON_RANGE[1]:
            ax.plot(vlon, vlat, marker='^', color='black', markersize=4,
                    zorder=4)

    errs = []
    for tid, t in targets.items():
        tlat, tlon = t['loc']
        ax.plot(tlon, tlat, marker='*', color='goldenrod', markersize=11,
                markeredgecolor='black', zorder=6)
        info = snap.get(tid)
        if info is None or info.get('est') is None:
            errs.append(MISSING_PENALTY_KM)
            continue
        elat, elon = info['est']
        errs.append(get_distance((elat, elon), t['loc']))
        ax.plot([tlon, elon], [tlat, elat], color='grey', linewidth=0.7,
                zorder=5)
        ax.plot(elon, elat, marker='x', color='red', markersize=7,
                markeredgewidth=1.8, zorder=6)
        if info.get('size') is not None:
            # region's own uncertainty claim (radius in ~degrees of latitude)
            ax.add_patch(Circle((elon, elat),
                                info['size'] / KM_PER_DEG_LAT,
                                fill=False, linestyle='--', linewidth=0.8,
                                edgecolor='red', alpha=0.6, zorder=5))

    note = f"avg err = {np.mean(errs):.0f} km"
    if pings_used < k:
        note += f"  (stopped @ {pings_used})"
    ax.text(0.02, 0.03, note, transform=ax.transAxes, fontsize=7,
            bbox=dict(facecolor='white', alpha=0.8, edgecolor='none'))

    # Identical window in every panel; the fixed 10° graticule makes that
    # visually checkable when comparing cells left to right.
    ax.set_xlim(*LON_RANGE)
    ax.set_ylim(*LAT_RANGE)
    ax.set_xticks(range(-20, 35, 10))
    ax.set_yticks(range(40, 61, 10))
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    ax.tick_params(length=0)
    ax.grid(alpha=0.35, linewidth=0.5)


def make_figure(output_path: str = OUT_PATH) -> str:
    run = run_multi_seed(SEED, snapshot_ks=SNAPSHOT_KS)
    scenario = run['scenario']

    n_rows, n_cols = len(STRATEGY_ROWS), len(SNAPSHOT_KS)
    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(3.1 * n_cols, 2.6 * n_rows))

    for i, (strategy, label, color) in enumerate(STRATEGY_ROWS):
        for j, k in enumerate(SNAPSHOT_KS):
            snap = run['snapshots'][strategy].get(k, {})
            _draw_cell(axes[i, j], snap, scenario,
                       run['pings_used'][strategy], k)
            if i == 0:
                axes[0, j].set_title(f'after {k} total measurements',
                                     fontsize=9)
        axes[i, 0].set_ylabel(label, fontsize=9, labelpad=18, color=color)
        axes[i, 0].set_yticklabels([f'{d}°N' for d in range(40, 61, 10)],
                                   fontsize=6)
    for j in range(n_cols):
        axes[-1, j].set_xticklabels([f'{d}°E' for d in range(-20, 35, 10)],
                                    fontsize=6)

    fig.suptitle(
        f'Multi-target region convergence, seed {SEED} — companion to '
        f'error_over_measurements_adaptive.pdf: 5 targets (★), 10 VPs (▲), '
        f'dashed circles = each region\'s own uncertainty claim',
        fontsize=11,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    return output_path


if __name__ == '__main__':
    print(f'wrote {make_figure()}')
