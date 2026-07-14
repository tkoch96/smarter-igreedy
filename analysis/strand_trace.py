import sys, os, pickle
import numpy as np
sys.path.insert(0, os.path.expanduser('~/Documents/smarter-igreedy'))
os.chdir(os.path.expanduser('~/Documents/smarter-igreedy'))

def main():
    from iterative_greedy_geolocator import Iterative_Greedy_Geolocator
    from feasible_region_maintainer import ADDITIVE
    from assess_geolocators import make_fiber_model, evaluate_geolocator
    from utils import get_distance
    w = pickle.load(open('cache/world_100src_300dst_fibergeo.pkl','rb'))
    td = w['target_data']; addr = td['address_to_loc']
    model = make_fiber_model(td, '_strandtrace', slope=1.3)
    ig = Iterative_Greedy_Geolocator(region_mode=ADDITIVE, model_refit_every=25,
                                     selection='phased', max_workers=6,
                                     utility_dispatch='auto', polish_mode='incremental',
                                     rtt_model=model, name='strand_trace')
    pd = evaluate_geolocator(ig, td, 'nearest_neighbor', [2500])
    pickle.dump(pd, open('cache/strand_trace_run.pkl','wb'))
    errs = pd['per_target'][2500]
    stranded = sorted([t for t, e in errs.items() if e >= 5000])
    print(f"stranded n={len(stranded)}")
    ut = pd['utility_tracking']
    # per stranded target: trajectory of TRUE error at each of its pings
    n_dip = n_never = n_fewping = 0
    for t in stranded:
        rows = [r for r in ut if r['target'] == t]
        traj = []
        for r in rows:
            ea = r['est_after']
            if ea is not None:
                traj.append(get_distance(tuple(ea), addr[t]))
        final = errs[t]
        if len(rows) < 4:
            n_fewping += 1; tag = 'FEW-PINGS'
        elif traj and min(traj) < 0.5 * final:
            n_dip += 1; tag = f'DIPPED to {min(traj):.0f} then {final:.0f}'
        else:
            n_never += 1; tag = 'never below half of final'
        if len(rows) >= 4 and traj and min(traj) < 0.5*final:
            pass
    print(f"dipped-then-relost: {n_dip}   never-good: {n_never}   <4 pings: {n_fewping}")
    # detail for 5 worst
    worst = sorted(stranded, key=lambda t: -errs[t])[:5]
    for t in worst:
        rows = [r for r in ut if r['target'] == t]
        traj = [(r['ping_num'], get_distance(tuple(r['est_after']), addr[t]))
                for r in rows if r['est_after'] is not None]
        s = ' '.join(f"{p}:{e:.0f}" for p, e in traj)
        print(f"  {t} final={errs[t]:.0f} n_pings={len(rows)} traj: {s}")

if __name__ == '__main__':
    main()
