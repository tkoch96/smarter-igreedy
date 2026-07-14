"""Expected-vs-actual utility calibration report for any greedy run.

Reads a geolocator_run_*.pkl (needs utility_tracking + the world
snapshot for ground truth) and reports, per run-decile:

  - expected_util (the auction's promise for the chosen ping),
  - actual_util   (BELIEVED region-size reduction that resulted),
  - true_gain     (ground-truth error movement, est_before -> est_after),
  - explore fraction,

plus the mismatch quadrants the plateau debugging cares about:
  q_silent : expected < 50 km but true_gain > 500 km  (blind wins —
             the auction would never have chosen these on purpose)
  q_empty  : expected > 500 km but true_gain < 50 km  (bought promises
             that never paid — reliability/feedback question)

Usage:
  python analysis/utility_calibration.py \
      cache/geolocator_run_<shape>.pkl cache/world_<shape>.pkl [strategy]
"""
import sys, os, pickle
import numpy as np
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils import get_distance


def main(run_fn, world_fn, strategy=None):
    r = pickle.load(open(run_fn, 'rb'))
    addr = pickle.load(open(world_fn, 'rb'))['target_data']['address_to_loc']
    pd = r['plot_data']
    names = [s for s in pd if 'utility_tracking' in pd[s]]
    strategy = strategy or names[0]
    ut = pd[strategy]['utility_tracking']
    print(f"{strategy}: {len(ut)} pings tracked ({run_fn})")

    rows = []
    for row in ut:
        eb, ea, t = row['est_before'], row['est_after'], row['target']
        if ea is None or t not in addr:
            continue
        true_before = (get_distance(tuple(eb), addr[t]) if eb is not None
                       else None)
        true_after = get_distance(tuple(ea), addr[t])
        rows.append((row['ping_num'], t, bool(row['explore']),
                     float(row['expected_util'] or 0.0),
                     float(row['actual_util'] or 0.0),
                     (true_before - true_after) if true_before is not None
                     else float('nan')))
    n = len(rows)
    print(f"\n{'decile':>6s} {'explore':>8s} {'exp med':>9s} {'act med':>9s} "
          f"{'true med':>9s} {'q_silent':>9s} {'q_empty':>8s}")
    for i in range(10):
        seg = rows[i * n // 10:(i + 1) * n // 10]
        exp = np.array([x[3] for x in seg])
        act = np.array([x[4] for x in seg])
        tru = np.array([x[5] for x in seg])
        ok = np.isfinite(tru)
        silent = ((exp < 50) & ok & (tru > 500)).sum()
        empty = ((exp > 500) & ok & (np.abs(tru) < 50)).sum()
        print(f"{i:6d} {np.mean([x[2] for x in seg]):8.0%} "
              f"{np.median(exp):9.1f} {np.median(act):9.1f} "
              f"{np.median(tru[ok]) if ok.any() else float('nan'):9.1f} "
              f"{silent:9d} {empty:8d}")

    exp = np.array([x[3] for x in rows]); tru = np.array([x[5] for x in rows])
    ok = np.isfinite(tru)
    print(f"\ntotals: q_silent={( (exp<50)&ok&(tru>500) ).sum()} "
          f"q_empty={((exp>500)&ok&(np.abs(tru)<50)).sum()} of {ok.sum()} scored pings")
    # worst silent wins: the pings the auction was blind to
    idx = np.argsort(-(np.where((exp < 50) & ok, tru, -np.inf)))[:8]
    print("\nbiggest SILENT wins (expected<50km, big true gain):")
    for i in idx:
        p, t, ex, e, a, g = rows[i]
        if not np.isfinite(g) or g <= 500: break
        print(f"  ping {p:6d} tgt {t:<18s} explore={ex} expected={e:7.1f} "
              f"believed={a:7.1f} TRUE={g:7.0f} km")


if __name__ == '__main__':
    main(*sys.argv[1:])
