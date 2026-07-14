"""Probe audit on a small healthy world: at checkpoints during a greedy
run, score EVERY remaining candidate two ways —
  greedy: the exact evaluator the auction uses (_evaluate_vp_chunk_worker)
  truth : clone region + add the real measurement + reoptimize, measure
          the change in TRUE error (post-hoc ground truth; never fed back)
and compare rankings (top-5 lists, rank stats, gains among zero-scored).
"""
import sys, os, pickle
import numpy as np
sys.path.insert(0, os.path.expanduser('~/Documents/smarter-igreedy'))
os.chdir(os.path.expanduser('~/Documents/smarter-igreedy'))

def main():
    from iterative_greedy_geolocator import (Iterative_Greedy_Geolocator,
        _evaluate_vp_chunk_worker)
    from feasible_region_maintainer import ADDITIVE
    from probabilistic_helpers import GeodesicRtt
    from assess_geolocators import make_fiber_model
    from utils import get_distance

    ARM = os.environ.get('AUDIT_ARM', 'fiber')
    w = pickle.load(open('cache/world_100src_300dst_fibergeo.pkl','rb'))
    td = w['target_data']; addr = td['address_to_loc']; m = td['loc_loc_meas']
    n_pairs = sum(len(d) for d in m.values())
    model = (make_fiber_model(td, '_audit', slope=1.3) if ARM == 'fiber'
             else GeodesicRtt(slope=1.3))
    ig = Iterative_Greedy_Geolocator(region_mode=ADDITIVE, model_refit_every=25,
                                     selection='phased', max_workers=6,
                                     utility_dispatch='auto',
                                     polish_mode='incremental',
                                     rtt_model=model, name=f'audit_{ARM}')
    ig.set_data(td)
    ig.solve()

    checkpoints = [int(n_pairs*f) for f in (0.1, 0.3, 0.6, 0.9)]
    print(f"world 100x300, {n_pairs} pairs; arm={ARM}; checkpoints={checkpoints}", flush=True)

    for b in checkpoints:
        ig.measurements(b)
        rows = []
        for dst in ig.targets:
            avail = [s for s in ig.available_measurements[dst]
                     if s not in ig.measurements_used[dst]]
            if not avail:
                continue
            region = ig.target_regions[dst]
            cur_size = region.get_region_size()
            scored = _evaluate_vp_chunk_worker(
                avail, dst, region, [ig.vp_locations[s] for s in avail],
                cur_size, ig.utility_func, ig.rtt_func,
                [False]*len(avail))
            gscore = dict(scored)
            truth = addr[dst]
            est_before = (region.get_location() if region.constraints else None)
            err_before = (get_distance(est_before, truth) if est_before
                          else 10000.0)
            for s in avail:
                c = region.clone()
                c.add_measurement(ig.vp_locations[s], min(m[s][dst]), src=s,
                                  update_estimate=False)
                c.reoptimize()
                err_after = get_distance(c.get_location(), truth)
                rows.append((dst, s, float(gscore[s]),
                             err_before - err_after, err_before))
        g = np.array([r[2] for r in rows]); t = np.array([r[3] for r in rows])
        from scipy.stats import spearmanr
        rho = spearmanr(g, t).statistic if len(rows) > 2 else float('nan')
        # ranks
        order_t = np.argsort(-t); order_g = np.argsort(-g)
        grank = {i: k for k, i in enumerate(order_g)}
        print(f"\n=== checkpoint b={b} candidates={len(rows)} "
              f"spearman(greedy,true)={rho:.3f}")
        print(f"  true gains: med={np.median(t):.1f} p90={np.percentile(t,90):.1f} max={t.max():.1f} km")
        zero = t[g <= 0.0]
        print(f"  candidates greedy-scored <=0: {np.mean(g<=0.0):.0%}; their true gains med={np.median(zero) if len(zero) else float('nan'):.1f} p90={np.percentile(zero,90) if len(zero) else float('nan'):.1f} max={zero.max() if len(zero) else float('nan'):.1f}")
        print("  TOP-5 by TRUE gain (perfect knowledge):")
        for i in order_t[:5]:
            d,s,gs,tg,eb = rows[i]
            print(f"    {s:>16s} -> {d:<16s} true={tg:7.1f} km  greedy={gs:9.1f} (rank {grank[i]+1}/{len(rows)})  err_before={eb:6.0f}")
        print("  TOP-5 by GREEDY score:")
        for i in order_g[:5]:
            d,s,gs,tg,eb = rows[i]
            print(f"    {s:>16s} -> {d:<16s} greedy={gs:9.1f}  true={tg:7.1f} km  err_before={eb:6.0f}")
    ig.cleanup()

if __name__ == '__main__':
    main()
