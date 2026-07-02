"""
Map figure: three RTT models applied to the SAME mixed measurements
(TestGaussianVsHardCircle in test_feasible_region.py).

Scenario: target Paris; London, Berlin and Madrid observe realistic 2.0×SOL
RTTs, but Rome got a lucky near-SOL fiber path (1.0×SOL).  The modeler
assumes routing overhead is ~1.3×.

  (a) classic hard-overlap — radius = rtt × 100 (straight SOL conversion).
      Always valid (overhead only inflates RTT), but loose: a big RTT tells
      you almost nothing.

  (b) slacked hard-overlap — radius = rtt × 100 / 1.3.  Circles tighten,
      more information per ping... unless a measurement beats the assumed
      slope.  Rome's near-SOL RTT shrinks to 855 km < the true 1112 km:
      its circle EXCLUDES the truth, is disjoint from London's, and the
      feasible intersection is empty — geolocation fails outright.

  (c) gaussian — rtt ≈ 1.3 × SOL(d) + noise.  Same 1.3× slope, but soft:
      Rome's measurement is merely improbable, not impossible.  The other
      measurements keep their tight interpretation and the posterior stays
      concentrated near the truth.

Run directly:
    cd ~/Documents/smarter-igreedy
    python tests/plot_gaussian_vs_hard_circle.py

Or included automatically when running pytest (see
test_generate_map_figure in test_feasible_region.py).

Saves:  tests/gaussian_vs_hard_circle.pdf
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from feasible_region_maintainer import KM_PER_MS
from probabilistic_helpers import haversine_grid
from utils import get_distance

DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), 'gaussian_vs_hard_circle.pdf')

# --- Scenario (shared with the unit test asserting its properties) --------
TARGET = (48.9, 2.3)      # Paris
# name -> ((lat, lon), true routing overhead over SOL)
VPS = {
    'London': ((51.5, -0.1), 2.0),
    'Berlin': ((52.5, 13.4), 2.0),
    'Madrid': ((40.4, -3.7), 2.0),
    'Rome':   ((41.9, 12.5), 1.0),   # lucky near-SOL fiber path
}
ASSUMED_SLOPE = 1.3       # the modeler's overhead assumption
DEMO_SIGMA_MS = 5.0       # display sigma (MAP is sigma-independent)

# Map extent (degrees) and grid resolution
LAT_RANGE = (30.0, 64.0)
LON_RANGE = (-22.0, 32.0)
GRID_STEP = 0.1


def measurements() -> dict[str, float]:
    """name -> observed rtt (ms) = true_distance / 100 × true_overhead."""
    return {
        name: (get_distance(vp, TARGET) / KM_PER_MS) * overhead
        for name, (vp, overhead) in VPS.items()
    }


def grid():
    lats = np.arange(LAT_RANGE[0], LAT_RANGE[1], GRID_STEP)
    lons = np.arange(LON_RANGE[0], LON_RANGE[1], GRID_STEP)
    return np.meshgrid(lats, lons, indexing='ij')


def vp_distance_grids(LATS, LONS) -> dict[str, np.ndarray]:
    return {
        name: haversine_grid(vp[0], vp[1], LATS, LONS)
        for name, (vp, _) in VPS.items()
    }


def feasible_mask(dist_grids: dict, radii: dict) -> np.ndarray:
    mask = np.ones_like(next(iter(dist_grids.values())), dtype=bool)
    for name, dist in dist_grids.items():
        mask &= dist <= radii[name]
    return mask


def cell_areas_km2(LATS) -> np.ndarray:
    deg_km = GRID_STEP * 111.0
    return deg_km * deg_km * np.cos(np.radians(LATS))


def gaussian_nll_grid(dist_grids: dict, sigma_ms: float = DEMO_SIGMA_MS) -> np.ndarray:
    rtts = measurements()
    nll = np.zeros_like(next(iter(dist_grids.values())))
    for name, dist in dist_grids.items():
        expected = ASSUMED_SLOPE * dist / KM_PER_MS
        nll += (rtts[name] - expected) ** 2 / (2 * sigma_ms ** 2)
    return nll


def gaussian_map_estimate(LATS, LONS, nll: np.ndarray) -> tuple[float, float]:
    i, j = np.unravel_index(np.argmin(nll), nll.shape)
    return (float(LATS[i, j]), float(LONS[i, j]))


# --- Plotting --------------------------------------------------------------

def _draw_base(ax, title: str) -> None:
    for name, (vp, overhead) in VPS.items():
        vlat, vlon = vp
        ax.plot(vlon, vlat, marker='^', color='black', markersize=7, zorder=5)
        ax.annotate(f'{name} ({overhead}×SOL)', (vlon, vlat),
                    textcoords='offset points', xytext=(5, 5),
                    fontsize=8, zorder=5)
    ax.plot(TARGET[1], TARGET[0], marker='*', color='goldenrod',
            markersize=16, markeredgecolor='black', zorder=6,
            label='true target (Paris)')
    ax.set_title(title, fontsize=10)
    ax.set_xlim(*LON_RANGE)
    ax.set_ylim(*LAT_RANGE)
    ax.set_xlabel('longitude (°)')
    ax.grid(alpha=0.25, linewidth=0.5)
    ax.set_aspect(1.0 / np.cos(np.radians(TARGET[0])))  # roughly square km


def _note(ax, text: str, color: str = 'black') -> None:
    ax.text(0.02, 0.02, text, transform=ax.transAxes, fontsize=8,
            va='bottom', color=color,
            bbox=dict(facecolor='white', alpha=0.85, edgecolor='none'))


def _panel_hard(ax, LATS, LONS, dist_grids, divide_by: float, title: str) -> np.ndarray:
    rtts = measurements()
    radii = {n: r * KM_PER_MS / divide_by for n, r in rtts.items()}

    mask = feasible_mask(dist_grids, radii)
    for name, dist in dist_grids.items():
        excludes_truth = radii[name] < get_distance(VPS[name][0], TARGET)
        ax.contour(LONS, LATS, dist, levels=[radii[name]],
                   colors='crimson' if excludes_truth else 'steelblue',
                   linewidths=1.6 if excludes_truth else 1.2)
    if mask.any():
        ax.contourf(LONS, LATS, mask.astype(float), levels=[0.5, 1.5],
                    colors=['steelblue'], alpha=0.45)
        ax.contour(LONS, LATS, mask.astype(float), levels=[0.5],
                   colors='navy', linewidths=1.0)

    _draw_base(ax, title)
    return mask


def make_figure(output_path: str = DEFAULT_OUTPUT) -> str:
    LATS, LONS = grid()
    dist_grids = vp_distance_grids(LATS, LONS)
    areas = cell_areas_km2(LATS)
    rtts = measurements()

    fig, axes = plt.subplots(1, 3, figsize=(16, 5.5), sharey=True)

    # (a) classic: straight SOL conversion
    mask_a = _panel_hard(axes[0], LATS, LONS, dist_grids, divide_by=1.0,
                         title='(a) classic hard-overlap\nradius = rtt × 100 km (straight SOL)')
    _note(axes[0],
          f'valid — every circle contains the truth,\n'
          f'but loose: feasible ≈ {areas[mask_a].sum() / 1e3:.0f}k km²\n'
          f'(a 2×SOL RTT barely constrains anything)')

    # (b) slacked: divide by the assumed slope
    mask_b = _panel_hard(axes[1], LATS, LONS, dist_grids, divide_by=ASSUMED_SLOPE,
                         title=f'(b) slacked hard-overlap\nradius = rtt × 100 / {ASSUMED_SLOPE}')
    d_rome = get_distance(VPS['Rome'][0], TARGET)
    _note(axes[1],
          f'Rome is near-SOL: {rtts["Rome"]:.1f} ms / {ASSUMED_SLOPE} '
          f'→ {rtts["Rome"] * KM_PER_MS / ASSUMED_SLOPE:.0f} km\n'
          f'< true {d_rome:.0f} km — its circle (red) excludes the truth.\n'
          f'feasible = {areas[mask_b].sum():.0f} km² — EMPTY, geolocation fails',
          color='crimson')

    # (c) gaussian with the same slope, soft
    nll = gaussian_nll_grid(dist_grids)
    posterior = np.exp(-(nll - nll.min()))
    axes[2].contourf(LONS, LATS, posterior, levels=np.linspace(0.05, 1.0, 14),
                     cmap='Reds', alpha=0.85)
    shells = axes[2].contour(LONS, LATS, posterior,
                             levels=[np.exp(-2.0), np.exp(-0.5)],
                             colors='darkred', linewidths=1.2)
    axes[2].clabel(shells, fmt={np.exp(-2.0): '2σ', np.exp(-0.5): '1σ'},
                   fontsize=8, inline=True)
    # the (b)-style rings, for reference: gaussian keeps them as soft ridges
    for name, dist in dist_grids.items():
        axes[2].contour(LONS, LATS, dist,
                        levels=[rtts[name] * KM_PER_MS / ASSUMED_SLOPE],
                        colors='grey', linewidths=0.8, linestyles='dashed')

    map_est = gaussian_map_estimate(LATS, LONS, nll)
    axes[2].plot(map_est[1], map_est[0], marker='x', color='red',
                 markersize=10, markeredgewidth=2.5, zorder=6, label='MAP estimate')
    _draw_base(axes[2],
               f'(c) gaussian\nrtt ≈ {ASSUMED_SLOPE} × SOL(d) + noise (σ={DEMO_SIGMA_MS:.0f} ms)')
    _note(axes[2],
          f'same {ASSUMED_SLOPE}× slope, but soft: Rome is merely\n'
          f'unlikely, not impossible.  MAP error = '
          f'{get_distance(map_est, TARGET):.0f} km\n'
          f'dashed grey: the (b) rings this model keeps as soft ridges')

    axes[0].set_ylabel('latitude (°)')
    axes[2].legend(loc='upper left', fontsize=8)
    axes[0].legend(loc='upper left', fontsize=8)

    fig.suptitle(
        'Same 4 measurements — London/Berlin/Madrid at 2.0×SOL, Rome on a lucky near-SOL path — '
        'under three RTT models (assumed slope 1.3×)',
        fontsize=11,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(output_path, bbox_inches='tight')
    plt.close(fig)
    return output_path


if __name__ == '__main__':
    path = make_figure()
    print(f'wrote {path}')
