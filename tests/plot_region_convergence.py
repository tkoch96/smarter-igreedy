"""
Region-convergence filmstrip: how each estimation method's region tightens
as measurements accumulate (test_feasible_region.py ::
TestGenerateConvergenceFigure).

Grid layout: one ROW per method, one COLUMN per measurement count.  A
routing detour (+70ms) is injected as measurement #4, so the columns before
and after it show each method's robustness in action:

  row 1  hard-circle      — circles + shaded feasible lens.  The detour's
                            huge circle is harmless (contains everything),
                            so the lens just stops shrinking.
  row 2  gaussian         — posterior heat.  The quadratic loss chases the
                            detour: watch the estimate jump at k=4.
  row 3  asymmetric       — posterior heat.  The linear detour tail barely
                            reacts; the region keeps converging.
  row 4  em_gaussian      — posterior heat under the FITTED slope μ̂
                            (annotated per panel): watch μ̂ move from the
                            1.3 prior toward the true 1.2.

Ground truth: target Prague, μ_true = 1.4 (above the assumed 1.3 slope so
hard circles stay valid), σ_true = 1.5ms, 10 VPs in a fixed order.
Deterministic (fixed RNG seed).

Display note: heat maps use σ=5ms for legibility; the plotted ESTIMATES
come from the real FeasibleRegion objects (MAP location is σ-independent
for a shared σ).

Run directly:
    cd ~/Documents/smarter-igreedy
    python tests/plot_region_convergence.py

Saves:  tests/region_convergence.pdf
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from feasible_region_maintainer import (
    FeasibleRegion, HARD_CIRCLE, GAUSSIAN, EM_GAUSSIAN, KM_PER_MS,
)
from probabilistic_helpers import (
    haversine_grid, GAUSSIAN_NOISE, ASYMMETRIC_NOISE, ASYM_FAST_SCALE,
)
from utils import get_distance

OUT_PATH = os.path.join(os.path.dirname(__file__), 'region_convergence.pdf')

# --- Scenario --------------------------------------------------------------

TARGET = (50.08, 14.44)     # Prague
VPS = {
    'london':    (51.50,  -0.10),
    'madrid':    (40.42,  -3.70),
    'stockholm': (59.33,  18.07),
    'istanbul':  (41.01,  28.97),
    'rome':      (41.90,  12.50),
    'amsterdam': (52.37,   4.90),
    'warsaw':    (52.23,  21.01),
    'paris':     (48.85,   2.35),
    'new_york':  (40.71, -74.01),
    'berlin':    (52.52,  13.41),
}
MEASUREMENT_ORDER = list(VPS)   # fixed order; detour hits measurement #4

# μ_true sits ABOVE the assumed 1.3 slope so the hard-circle model stays
# valid (radius = implied distance × 1.05 = 1.13 × d contains the truth).
# With μ_true below the slope, every clean circle slightly excludes the
# truth and the intersection empties out — that's TODOS #6, worth its own
# demo but not this one.
MU_TRUE = 1.4
# Noise-free except the detour: this is a mechanism demo, and additive
# noise can push close-VP hard circles below their true distance (a ±2ms
# swing beats Berlin's ~0.5ms validity margin), muddying the story.
SIGMA_TRUE_MS = 0.0
DETOUR_INDEX = 3        # 0-based: the 4th measurement (istanbul)
DETOUR_MS = 70.0
SNAPSHOT_KS = (1, 2, 4, 6, 10)

DISPLAY_SIGMA_MS = 5.0  # heat-map legibility only; MAP is σ-independent

# Map extent / resolution
LAT_RANGE = (33.0, 63.0)
LON_RANGE = (-20.0, 34.0)
GRID_STEP = 0.15

METHODS = [
    ('hard-circle',       dict(mode=HARD_CIRCLE)),
    ('gaussian',          dict(mode=GAUSSIAN)),
    ('asymmetric noise',  dict(mode=GAUSSIAN, noise_model=ASYMMETRIC_NOISE)),
    ('em_gaussian',       dict(mode=EM_GAUSSIAN)),
]


def measurements() -> list[tuple[str, float]]:
    """Deterministic (name, rtt) sequence with one injected detour."""
    rng = np.random.default_rng(4)
    seq = []
    for i, name in enumerate(MEASUREMENT_ORDER):
        rtt = (MU_TRUE * get_distance(VPS[name], TARGET) / KM_PER_MS
               + float(rng.normal(0.0, SIGMA_TRUE_MS)))
        if i == DETOUR_INDEX:
            rtt += DETOUR_MS
        seq.append((name, rtt))
    return seq


def run_method(kwargs: dict) -> dict[int, dict]:
    """Feed the sequence to a FeasibleRegion; snapshot state at SNAPSHOT_KS."""
    region = FeasibleRegion('t', **kwargs)
    snaps: dict[int, dict] = {}
    for k, (name, rtt) in enumerate(measurements(), start=1):
        region.add_measurement(VPS[name], rtt)
        if k in SNAPSHOT_KS:
            snaps[k] = {
                'location': region.get_location(),
                'size': region.get_region_size(),
                'slope': region.slope,
                'noise_model': region.noise_model,
                'mode': region.mode,
                'constraints': list(region.constraints),
                'vps_used': [VPS[n] for n, _ in measurements()[:k]],
                'rtts_used': [r for _, r in measurements()[:k]],
            }
    return snaps


# --- Rendering ---------------------------------------------------------------

def _grid():
    lats = np.arange(LAT_RANGE[0], LAT_RANGE[1], GRID_STEP)
    lons = np.arange(LON_RANGE[0], LON_RANGE[1], GRID_STEP)
    return np.meshgrid(lats, lons, indexing='ij')


def _nll_grid(dist_grids: list[np.ndarray], rtts: list[float],
              slope: float, noise_model: str) -> np.ndarray:
    total = np.zeros_like(dist_grids[0])
    s = DISPLAY_SIGMA_MS
    for dist, rtt in zip(dist_grids, rtts):
        r = rtt - slope * dist / KM_PER_MS
        if noise_model == ASYMMETRIC_NOISE:
            total += np.where(r >= 0.0, r / s,
                              (r * ASYM_FAST_SCALE) ** 2 / (2 * s ** 2))
        else:
            total += r ** 2 / (2 * s ** 2)
    return total


def _draw_cell(ax, snap: dict, dist_by_vp: dict) -> None:
    LATS, LONS = _grid.cache
    dists = [dist_by_vp[vp] for vp in snap['vps_used']]

    if snap['mode'] == HARD_CIRCLE:
        feasible = np.ones_like(LATS, dtype=bool)
        for (vp_loc, radius), dist in zip(snap['constraints'], dists):
            ax.contour(LONS, LATS, dist, levels=[radius],
                       colors='steelblue', linewidths=0.7)
            feasible &= dist <= radius
        if feasible.any():
            ax.contourf(LONS, LATS, feasible.astype(float), levels=[0.5, 1.5],
                        colors=['steelblue'], alpha=0.4)
    else:
        nll = _nll_grid(dists, snap['rtts_used'], snap['slope'],
                        snap['noise_model'])
        posterior = np.exp(-(nll - nll.min()))
        ax.contourf(LONS, LATS, posterior, levels=np.linspace(0.05, 1.0, 12),
                    cmap='Reds', alpha=0.9)

    for vp_loc in snap['vps_used']:
        if LON_RANGE[0] <= vp_loc[1] <= LON_RANGE[1]:
            ax.plot(vp_loc[1], vp_loc[0], marker='^', color='black',
                    markersize=4, zorder=5)
    ax.plot(TARGET[1], TARGET[0], marker='*', color='goldenrod',
            markersize=11, markeredgecolor='black', zorder=6)
    est = snap['location']
    ax.plot(est[1], est[0], marker='x', color='red', markersize=8,
            markeredgewidth=2.0, zorder=6)

    note = f"size={snap['size']:.0f}km  err={get_distance(est, TARGET):.0f}km"
    if snap['mode'] == EM_GAUSSIAN:
        note += f"  μ̂={snap['slope']:.2f}"
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
    LATS, LONS = _grid()
    _grid.cache = (LATS, LONS)
    dist_by_vp = {vp: haversine_grid(vp[0], vp[1], LATS, LONS)
                  for vp in VPS.values()}

    n_rows, n_cols = len(METHODS), len(SNAPSHOT_KS)
    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(3.1 * n_cols, 2.6 * n_rows))

    for i, (label, kwargs) in enumerate(METHODS):
        snaps = run_method(kwargs)
        for j, k in enumerate(SNAPSHOT_KS):
            _draw_cell(axes[i, j], snaps[k], dist_by_vp)
            if i == 0:
                title = f'after {k} measurement{"s" if k > 1 else ""}'
                if k >= DETOUR_INDEX + 1:
                    title += '  (incl. detour)'
                axes[0, j].set_title(title, fontsize=9)
        axes[i, 0].set_ylabel(label, fontsize=10, labelpad=18)
        axes[i, 0].set_yticklabels([f'{d}°N' for d in range(40, 61, 10)],
                                   fontsize=6)
    for j in range(n_cols):
        axes[-1, j].set_xticklabels([f'{d}°E' for d in range(-20, 35, 10)],
                                    fontsize=6)

    fig.suptitle(
        f'Region convergence over measurements — target Prague (★), '
        f'μ_true={MU_TRUE}, σ_true={SIGMA_TRUE_MS}ms; '
        f'measurement #{DETOUR_INDEX + 1} carries a +{DETOUR_MS:.0f}ms detour',
        fontsize=11,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    return output_path


if __name__ == '__main__':
    print(f'wrote {make_figure()}')
