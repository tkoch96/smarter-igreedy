"""
Cross-ESTIMATOR comparison on the real RIPE mesh at matched measurements.

Feeds the SAME random-ordered budgeted measurement sets (one
Random_Geolocator shuffle) through several converter modes of
Geolocator_Comparator, so the lines differ only in estimation:

    nearest_neighbor   lowest-RTT VP's location (the dumb strong baseline)
    em_gaussian        per-target online EM (μ_t, σ_t)
    em_asymmetric      per-target EM with the one-sided noise model
    additive_em        cross-target two-way model rtt = d/100 + X_src + X_dst

Reports MEAN and MEDIAN error per budget — the subsample contains isolated
probes (Guam / Cape Town / Dallas at n=20) whose ~10,000 km errors dominate
any mean.  Missing targets incur the 10,000 km penalty, matching
Geolocator_Comparator.run().

Reference numbers (seed-31415 20-probe subsample, from the handoff):
NN mean 2496 / median 604; per-target em mean 3913; em_asymmetric mean
2054 / median 594.  At n=100: NN@2000 mean ≈ 1620.

Usage:
    python assess_additive_real.py [n_subsample] [budget1,budget2,...]

Results are also pickled to cache/additive_real_results_n{N}.pkl.
"""

import os
import pickle
import random
import sys
import time

import numpy as np

from assess_geolocators import Geolocator_Comparator
from random_geolocator import Random_Geolocator
from utils import get_distance, CACHE_DIR

MODES = ('nearest_neighbor', 'em_gaussian', 'em_asymmetric', 'additive_em')
MISSING_PENALTY_KM = 10_000.0


def main(n_subsample: int, budgets: list[int]) -> None:
    np.random.seed(31415)
    random.seed(31415)

    gc = Geolocator_Comparator()
    gc.load_target_measurement_data()
    gc.get_random_subsample(n=n_subsample)

    address_to_loc = gc.target_data['address_to_loc']
    all_targets = set()
    n_pairs = 0
    for src, dsts in gc.target_data['loc_loc_meas'].items():
        all_targets.update(dsts.keys())
        n_pairs += len(dsts)
    all_targets = {t for t in all_targets if t in address_to_loc}
    print(f"subsample n={n_subsample}: {len(all_targets)} scored targets, "
          f"{n_pairs} measured pairs")

    rg = Random_Geolocator()
    rg.set_data(gc.target_data)
    rg.solve()

    results = {mode: {'budgets': [], 'mean': [], 'median': [], 'found': []}
               for mode in MODES}

    for budget in budgets:
        meas = rg.measurements(budget)
        for mode in MODES:
            gc.measurement_converter_mode = mode
            t0 = time.time()
            est = gc.convert_measurements_to_locations(meas)
            errors = [
                get_distance(est[dst], address_to_loc[dst])
                if dst in est else MISSING_PENALTY_KM
                for dst in all_targets
            ]
            results[mode]['budgets'].append(budget)
            results[mode]['mean'].append(float(np.mean(errors)))
            results[mode]['median'].append(float(np.median(errors)))
            results[mode]['found'].append(len(est))
            print(f"budget={budget:5d} {mode:17s} "
                  f"mean={np.mean(errors):8.1f}  median={np.median(errors):7.1f}  "
                  f"found={len(est):4d}/{len(all_targets)}  "
                  f"({time.time() - t0:.1f}s)", flush=True)
        print()

    out_fn = os.path.join(CACHE_DIR, f'additive_real_results_n{n_subsample}.pkl')
    pickle.dump(results, open(out_fn, 'wb'))
    print(f"wrote {out_fn}")

    print(f"\n=== summary (n={n_subsample}) ===")
    header = "budget  " + "".join(f"{m:>26s}" for m in MODES)
    print(header + "   (mean / median km)")
    for i, budget in enumerate(budgets):
        cells = "".join(
            f"{results[m]['mean'][i]:12.0f} /{results[m]['median'][i]:6.0f}     "
            for m in MODES)
        print(f"{budget:6d}  {cells}")


if __name__ == '__main__':
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    if len(sys.argv) > 2:
        budgets = [int(b) for b in sys.argv[2].split(',')]
    else:
        # n=20 has ~380 measured pairs at full coverage
        budgets = [50, 100, 200, 400] if n <= 20 else [200, 500, 1000, 1500, 2000, 2500]
    main(n, budgets)
