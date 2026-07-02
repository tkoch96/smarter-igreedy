"""
Probabilistic geolocation comparison on the real RIPE Atlas dataset.

Compares five estimation methods using identical random measurement ordering:

  nearest_neighbor   -- estimate = location of the lowest-RTT VP seen so far
  hard_circle_1_3    -- FeasibleRegion hard-circle with original 1.3× slack
  hard_circle_1_05   -- FeasibleRegion hard-circle with tightened 1.05× slack
  gaussian           -- FeasibleRegion Gaussian MAP, per-VP sigma from mesh,
                        per-VP mu correction for routing overhead
  oracle_sigma       -- Gaussian MAP with true sigma = mesh-fitted sigma
                        but NO mu correction (upper bound on Gaussian w/o mu)

Measurement ordering is from the Random_Geolocator (shuffled pairs), so all
estimation methods see exactly the same pings in the same order.  This isolates
estimation quality from VP-selection strategy.

Usage:
    cd ~/Documents/smarter-igreedy
    python assess_probabilistic.py

Output:
    figures/probabilistic_comparison.pdf
    figures/probabilistic_comparison_zoomed.pdf  (y-axis 0-2000km)
"""

import os, sys, time, pickle
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from utils import CACHE_DIR, FIG_DIR, get_distance, LatLon
from feasible_region_maintainer import FeasibleRegion, HARD_CIRCLE, GAUSSIAN
from probabilistic_helpers import (
    compute_per_vp_mu, compute_per_vp_sigma, GLOBAL_SIGMA_MS, KM_PER_MS,
)
from random_geolocator import Random_Geolocator
from pull_ripe_atlas_measurement_data import RipeAtlasPipeline


# ---------------------------------------------------------------------------
# Data loading (mirrors assess_geolocators.py)
# ---------------------------------------------------------------------------

def load_data():
    cache_fn = os.path.join(CACHE_DIR, 'cached_target_data.pkl')
    if not os.path.exists(cache_fn):
        rap = RipeAtlasPipeline(start_date="2026-02-24", end_date="2026-02-24")
        rap.execute()
        data = rap.load_parsed_target_data()
        pickle.dump(data, open(cache_fn, 'wb'))
    else:
        data = pickle.load(open(cache_fn, 'rb'))
        # Cache stores bare numpy.float64; wrap in list to match pipeline format
        for src in data['loc_loc_meas']:
            for dst in data['loc_loc_meas'][src]:
                data['loc_loc_meas'][src][dst] = [data['loc_loc_meas'][src][dst]]
    return data


def subsample(data, n=100, seed=31415):
    rng = np.random.default_rng(seed)
    all_srcs = list(data['loc_loc_meas'])
    rng.shuffle(all_srcs)
    keep = set(all_srcs[:n])
    new_meas = {s: {} for s in keep}
    for src in keep:
        for dst, rtts in data['loc_loc_meas'][src].items():
            if dst in keep and dst != src:
                new_meas[src][dst] = rtts
    return {
        'address_to_loc': data['address_to_loc'],
        'loc_loc_meas': new_meas,
    }


# ---------------------------------------------------------------------------
# Estimation methods
# ---------------------------------------------------------------------------

MeasData = dict[str, dict[str, list[float]]]

def convert_nearest_neighbor(measurements: MeasData, address_to_loc: dict) -> dict[str, LatLon]:
    dst_to_src_rtts: dict[str, dict[str, float]] = {}
    for src, dsts in measurements.items():
        for dst, rtts in dsts.items():
            if not rtts:
                continue
            dst_to_src_rtts.setdefault(dst, {})[src] = min(rtts)
    estimated = {}
    for dst, src_rtts in dst_to_src_rtts.items():
        best_src = min(src_rtts, key=src_rtts.get)
        if best_src in address_to_loc:
            estimated[dst] = address_to_loc[best_src]
    return estimated


def convert_hard_circle(
    measurements: MeasData,
    address_to_loc: dict,
    multiplier: float,
) -> dict[str, LatLon]:
    dst_to_src_rtts: dict[str, dict[str, float]] = {}
    for src, dsts in measurements.items():
        for dst, rtts in dsts.items():
            if rtts:
                dst_to_src_rtts.setdefault(dst, {})[src] = min(rtts)

    estimated = {}
    for dst, src_rtts in dst_to_src_rtts.items():
        region = FeasibleRegion(dst, mode=HARD_CIRCLE, radius_multiplier=multiplier)
        for src, rtt in src_rtts.items():
            if src in address_to_loc:
                region.add_measurement(address_to_loc[src], max(0.0, rtt))
        if region.constraints:
            estimated[dst] = region.get_location()
    return estimated


def convert_gaussian(
    measurements: MeasData,
    address_to_loc: dict,
    mu_map: dict[str, float],
    sigma_map: dict[str, float],
) -> dict[str, LatLon]:
    dst_to_src_rtts: dict[str, dict[str, float]] = {}
    for src, dsts in measurements.items():
        for dst, rtts in dsts.items():
            if rtts:
                dst_to_src_rtts.setdefault(dst, {})[src] = min(rtts)

    estimated = {}
    for dst, src_rtts in dst_to_src_rtts.items():
        region = FeasibleRegion(dst, mode=GAUSSIAN)
        for src, rtt in src_rtts.items():
            if src not in address_to_loc:
                continue
            mu    = mu_map.get(src, 0.0)
            sigma = sigma_map.get(src, GLOBAL_SIGMA_MS)
            corrected = max(0.01, rtt - mu)
            region.add_measurement(address_to_loc[src], corrected, sigma_ms=sigma)
        if region.constraints:
            estimated[dst] = region.get_location()
    return estimated


def mean_error(estimated: dict, address_to_loc: dict, all_targets: set) -> float:
    errors = []
    for dst in all_targets:
        if dst not in address_to_loc:
            continue
        if dst in estimated:
            errors.append(get_distance(estimated[dst], address_to_loc[dst]))
        else:
            errors.append(10_000.0)   # penalty for missing estimate
    return float(np.mean(errors)) if errors else float('nan')


# ---------------------------------------------------------------------------
# Profiling
# ---------------------------------------------------------------------------

def profile(data: dict, mu_map: dict, sigma_map: dict, n_targets: int = 10):
    """
    Time Gaussian FeasibleRegion on real targets with varying constraint counts.
    Prints convergence quality (error vs nearest-neighbour) for a sanity check.
    """
    address_to_loc = data['address_to_loc']
    loc_loc_meas   = data['loc_loc_meas']

    # Pick n_targets with the most VPs
    dst_srcs: dict[str, list] = {}
    for src, dsts in loc_loc_meas.items():
        for dst, rtts in dsts.items():
            if rtts:
                dst_srcs.setdefault(dst, []).append((src, min(rtts)))
    top_targets = sorted(dst_srcs, key=lambda d: len(dst_srcs[d]), reverse=True)[:n_targets]

    print(f"\n{'Target':<18} {'VPs':>4}  {'NN err':>8}  "
          f"{'HC1.05 err':>10}  {'Gauss err':>10}  {'t_gauss':>8}")
    print('-' * 70)

    for dst in top_targets:
        if dst not in address_to_loc:
            continue
        true_loc = address_to_loc[dst]
        src_rtts = dst_srcs[dst]

        # nearest-neighbour
        nn_src = min(src_rtts, key=lambda x: x[1])[0]
        nn_err = get_distance(address_to_loc[nn_src], true_loc) if nn_src in address_to_loc else 9999

        # hard circle 1.05×
        hc = FeasibleRegion(dst, mode=HARD_CIRCLE, radius_multiplier=1.05)
        for src, rtt in src_rtts:
            if src in address_to_loc:
                hc.add_measurement(address_to_loc[src], max(0.0, rtt))
        hc_err = get_distance(hc.get_location(), true_loc) if hc.constraints else 9999

        # gaussian
        t0 = time.perf_counter()
        gm = FeasibleRegion(dst, mode=GAUSSIAN)
        for src, rtt in src_rtts:
            if src not in address_to_loc:
                continue
            mu    = mu_map.get(src, 0.0)
            sigma = sigma_map.get(src, GLOBAL_SIGMA_MS)
            gm.add_measurement(address_to_loc[src], max(0.01, rtt - mu), sigma_ms=sigma)
        t_gauss = time.perf_counter() - t0
        gm_err = get_distance(gm.get_location(), true_loc) if gm.constraints else 9999

        n_vps = len([s for s, _ in src_rtts if s in address_to_loc])
        print(f"{dst:<18} {n_vps:>4}  {nn_err:>8.0f}  {hc_err:>10.0f}  {gm_err:>10.0f}  {t_gauss:>7.2f}s")


# ---------------------------------------------------------------------------
# Full sweep
# ---------------------------------------------------------------------------

METHODS = ('nearest_neighbor', 'hard_circle_1_3', 'hard_circle_1_05', 'gaussian')
MIN_BUDGET = 100
MAX_BUDGET = 2500
STEP       = 100


def run_sweep(data: dict, mu_map: dict, sigma_map: dict) -> dict:
    address_to_loc = data['address_to_loc']
    all_targets    = set(k for dsts in data['loc_loc_meas'].values() for k in dsts)
    print(f"\nTargets: {len(all_targets)}  |  nodes: {len(data['loc_loc_meas'])}")

    rng = Random_Geolocator()
    rng.set_data(data)
    rng.solve()

    results = {m: {'budgets': [], 'errors': []} for m in METHODS}

    budgets = list(range(MIN_BUDGET, MAX_BUDGET + 1, STEP))
    for i, budget in enumerate(budgets):
        t0 = time.perf_counter()
        meas = rng.measurements(budget)

        err_nn   = mean_error(convert_nearest_neighbor(meas, address_to_loc), address_to_loc, all_targets)
        err_hc13 = mean_error(convert_hard_circle(meas, address_to_loc, 1.3),  address_to_loc, all_targets)
        err_hc10 = mean_error(convert_hard_circle(meas, address_to_loc, 1.05), address_to_loc, all_targets)
        err_gm   = mean_error(convert_gaussian(meas, address_to_loc, mu_map, sigma_map), address_to_loc, all_targets)

        elapsed = time.perf_counter() - t0
        eta = elapsed * (len(budgets) - i - 1)

        for m, e in zip(METHODS, [err_nn, err_hc13, err_hc10, err_gm]):
            results[m]['budgets'].append(budget)
            results[m]['errors'].append(e)

        print(f"  budget={budget:4d}  NN={err_nn:6.0f}km  HC1.3={err_hc13:6.0f}km  "
              f"HC1.05={err_hc10:6.0f}km  Gauss={err_gm:6.0f}km  "
              f"({elapsed:.1f}s, ETA {eta/60:.1f}min)")

    return results


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

STYLE = {
    'nearest_neighbor': dict(color='#888888', ls='--',  lw=1.5, label='nearest-neighbour'),
    'hard_circle_1_3':  dict(color='#C44E2A', ls='-.',  lw=1.8, label='hard-circle (1.3×, original)'),
    'hard_circle_1_05': dict(color='#E07B39', ls='-.',  lw=2.0, label='hard-circle (1.05×, tightened)'),
    'gaussian':         dict(color='#3A7FBF', ls='-',   lw=2.5, label='gaussian MAP (μ+σ from mesh)'),
}


def plot(results: dict, out_path: str, ylim=None):
    fig, ax = plt.subplots(figsize=(8, 5))
    for m in METHODS:
        kw = STYLE[m]
        budgets = results[m]['budgets']
        errors  = results[m]['errors']
        ax.plot(budgets, errors, **kw)

    ax.set_xlabel('Total pings used (budget)', fontsize=11)
    ax.set_ylabel('Mean geolocation error (km)', fontsize=11)
    ax.set_title('Geolocation error vs measurement budget\nReal RIPE Atlas data (100-node subsample)', fontsize=11)
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
    ax.grid(axis='y', ls='--', alpha=0.4)
    ax.grid(axis='y', which='minor', ls=':', alpha=0.2)
    ax.legend(fontsize=9)
    ax.set_xlim(MIN_BUDGET, MAX_BUDGET)
    if ylim:
        ax.set_ylim(0, ylim)
    else:
        ax.set_ylim(bottom=0)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"Saved → {out_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    np.random.seed(31415)

    print("Loading data …")
    raw_data = load_data()
    print(f"Full dataset: {len(raw_data['loc_loc_meas'])} nodes")

    # Compute mu/sigma on the full mesh before subsampling (more peers = better estimates)
    print("Computing per-VP mu and sigma from full mesh …")
    t0 = time.perf_counter()
    mu_map    = compute_per_vp_mu(raw_data,    min_peers=10, global_fallback_ms=0.0)
    sigma_map = compute_per_vp_sigma(raw_data, min_peers=10, global_fallback_ms=GLOBAL_SIGMA_MS)
    print(f"  done in {time.perf_counter()-t0:.1f}s")
    sigmas = [v for v in sigma_map.values() if v < GLOBAL_SIGMA_MS]
    mus    = list(mu_map.values())
    print(f"  mu:    mean={np.mean(mus):.1f}ms  median={np.median(mus):.1f}ms  p95={np.percentile(mus,95):.1f}ms")
    print(f"  sigma: mean={np.mean(sigmas):.1f}ms  median={np.median(sigmas):.1f}ms  (fitted VPs only)")

    print("\nSubsampling to 100 nodes …")
    data = subsample(raw_data, n=100, seed=31415)

    # --- Profiling ---
    print("\n=== PROFILING (10 targets, all their VPs) ===")
    profile(data, mu_map, sigma_map, n_targets=10)

    # --- Full sweep ---
    print("\n=== FULL SWEEP ===")
    t_start = time.perf_counter()
    results = run_sweep(data, mu_map, sigma_map)
    print(f"\nTotal sweep time: {(time.perf_counter()-t_start)/60:.1f} min")

    cache_fn = os.path.join(CACHE_DIR, 'cached_results_probabilistic.pkl')
    pickle.dump(results, open(cache_fn, 'wb'))
    print(f"Results cached → {cache_fn}")

    plot(results, os.path.join(FIG_DIR, 'probabilistic_comparison.pdf'))
    plot(results, os.path.join(FIG_DIR, 'probabilistic_comparison_zoomed.pdf'), ylim=2000)
