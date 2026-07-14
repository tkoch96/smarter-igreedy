"""
Benchmark + figure for the computation speedups (called by
tests/test_speedups.py; writes tests/speedups.pdf).

Four panels, each timing the OLD implementation against the NEW one on
identical synthetic inputs:

  A  utility-update latency vs candidate count — 'per_vp' (one executor
     job + one region pickle per VP, the historical path) vs 'chunk'
     (one job per worker) vs 'inline' (no executor; valid for the
     NM-free hypothesis utilities that phased/risk_gain use).
  B  checkpoint batch-polish latency vs accumulated pings —
     polish_mode='full' (fresh NN-anchored fit for every target) vs
     'incremental' (params always refit; locations re-optimised only
     for the ~dirty subset, warm-started).
  C  total polish cost of a budget grid — 18 linear checkpoints vs 10
     log-spaced ones (both to the same max budget), integrated from the
     measured t(pings) of panel B.
  D  policy-field dtype — float64 vs float32 bytes and disk-load time
     at the real graph's field length.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np

from probabilistic_helpers import additive_batch_em
from iterative_greedy_geolocator import Iterative_Greedy_Geolocator
from feasible_region_maintainer import ADDITIVE
from utils import get_distance

FIG_FN = os.path.join(os.path.dirname(__file__), 'speedups.pdf')


def make_world(n_vps=40, n_targets=60, seed=7):
    """Additive-model synthetic mesh in the real target_data format."""
    rng = np.random.default_rng(seed)
    addr = {}
    for i in range(n_vps):
        addr[f'vp{i}'] = (float(rng.uniform(-60, 65)),
                          float(rng.uniform(-180, 180)))
    for j in range(n_targets):
        addr[f't{j}'] = (float(rng.uniform(-60, 65)),
                         float(rng.uniform(-180, 180)))
    mu_s = {f'vp{i}': float(rng.uniform(2, 10)) for i in range(n_vps)}
    mu_t = {f't{j}': float(rng.uniform(2, 20)) for j in range(n_targets)}
    meas = {}
    for i in range(n_vps):
        s = f'vp{i}'
        for j in range(n_targets):
            t = f't{j}'
            d = get_distance(addr[s], addr[t])
            rtt = d / 100.0 + mu_s[s] + mu_t[t] + float(rng.normal(0, 3))
            meas.setdefault(s, {})[t] = [max(rtt, 0.1)]
    return {'address_to_loc': addr, 'loc_loc_meas': meas}


def make_greedy(selection='risk_gain', workers=4, **kw):
    return Iterative_Greedy_Geolocator(
        region_mode=ADDITIVE, model_refit_every=25, selection=selection,
        max_workers=workers, **kw)


def _median_time(fn, reps=5):
    ts = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t0)
    return float(np.median(ts))


def bench_utility_dispatch(vp_counts=(25, 50, 100, 200), n_pings=120,
                           seed=11):
    """Panel A data: {mode: [seconds per full cache sweep]} aligned with
    vp_counts.  A sweep re-scores every target's candidates (the same
    work solve() does at seeding and the per-ping updates amortise), so
    it covers the real mix of region states instead of one cherry-picked
    target."""
    out = {'per_vp': [], 'chunk': [], 'inline': []}
    for n_vps in vp_counts:
        world = make_world(n_vps=n_vps, n_targets=30, seed=seed)
        g = make_greedy()
        g.set_data(world)
        g.solve()
        g.measurements(n_pings)          # regions get real constraints
        # warm the executor so per_vp/chunk don't pay pool spawn in-loop
        g.utility_dispatch = 'chunk'
        g._update_best_vp_for_target(g.targets[0])
        for mode in out:
            g.utility_dispatch = mode
            out[mode].append(_median_time(
                lambda: [g._update_best_vp_for_target(d) for d in g.targets],
                reps=3))
        g.cleanup()
    return out


def bench_polish(ping_counts=(400, 800, 1600, 3200), dirty_frac=0.10,
                 seed=13):
    """Panel B data: full vs incremental batch polish at growing ping
    counts (incremental = params over everything, locations only for a
    dirty_frac of targets, warm-started from a previous full polish)."""
    world = make_world(n_vps=60, n_targets=80, seed=seed)
    addr = world['address_to_loc']
    rng = np.random.default_rng(seed)
    pairs = [(s, t) for s, d in world['loc_loc_meas'].items() for t in d]
    rng.shuffle(pairs)
    vp_locs = {k: v for k, v in addr.items() if k.startswith('vp')}

    t_full, t_incr = [], []
    for n in ping_counts:
        sub = {p: world['loc_loc_meas'][p[0]][p[1]] for p in pairs[:n]}
        prev, _, _, _, _ = additive_batch_em(sub, vp_locs)
        targets = sorted({t for _, t in sub})
        dirty = set(rng.choice(targets,
                               max(1, int(dirty_frac * len(targets))),
                               replace=False))
        t_full.append(_median_time(
            lambda: additive_batch_em(sub, vp_locs), reps=3))
        t_incr.append(_median_time(
            lambda: additive_batch_em(sub, vp_locs, n_iters=2,
                                      prev_estimates=prev,
                                      only_targets=dirty), reps=3))
    return t_full, t_incr


def bench_checkpoint_grids(ping_counts, t_full, t_incr, max_budget=27000):
    """Panel C data: total polish cost of a run's checkpoint grid,
    integrating the measured per-polish latencies (linear fit through
    the panel-B points) over each grid."""
    slope_full = float(np.polyfit(ping_counts, t_full, 1)[0])
    slope_incr = float(np.polyfit(ping_counts, t_incr, 1)[0])
    linear18 = np.arange(1500, max_budget + 1, 1500)
    log10 = sorted({int(round(b))
                    for b in np.geomspace(1500, max_budget, 10)})
    return {
        'linear-18 / full': slope_full * float(np.sum(linear18)),
        'log-10 / full': slope_full * float(np.sum(log10)),
        'linear-18 / incremental': slope_incr * float(np.sum(linear18)),
        'log-10 / incremental': slope_incr * float(np.sum(log10)),
    }


def bench_field_dtype(n_nodes=None, n_fields=24, tmp_dir=None):
    """Panel D data: bytes + disk-load time per field, float64 vs
    float32, at the real graph's field length when available."""
    import tempfile
    if n_nodes is None:
        try:
            from glob import glob
            gfn = sorted(glob(os.path.join(
                os.path.dirname(__file__), '..', 'internet_gmaps', 'data',
                'graph_*.npz')))[-1]
            n_nodes = len(np.load(gfn)['node_lat'])
        except Exception:
            n_nodes = 60000
    tmp_dir = tmp_dir or tempfile.mkdtemp(prefix='speedup_fields_')
    out = {}
    rng = np.random.default_rng(3)
    base = rng.uniform(1.0, 400.0, n_nodes)
    for dtype in (np.float64, np.float32):
        fns = []
        for i in range(n_fields):
            fn = os.path.join(tmp_dir, f'f_{np.dtype(dtype).name}_{i}.npy')
            np.save(fn, base.astype(dtype))
            fns.append(fn)
        t0 = time.perf_counter()
        for fn in fns:
            np.load(fn)
        out[np.dtype(dtype).name] = {
            'load_s': (time.perf_counter() - t0) / n_fields,
            'bytes': base.astype(dtype).nbytes,
        }
    return out, n_nodes


SIZE_FIG_FN = os.path.join(os.path.dirname(__file__), 'speedup_vs_size.pdf')


def bench_problem_size(sizes=((15, 30), (20, 60), (30, 120), (40, 180)),
                       budget_frac=0.3, seed=41, workers=2):
    """End-to-end greedy wall time, old config (per_vp dispatch + full
    polish) vs new (auto + incremental), across growing worlds.  Budget
    = budget_frac of the pair count, spent over two checkpoints so the
    polish path runs twice.  Returns (n_pairs list, t_old, t_new)."""
    import time
    pairs_n, t_old, t_new = [], [], []
    for n_vps, n_targets in sizes:
        w = make_world(n_vps=n_vps, n_targets=n_targets, seed=seed)
        n_pairs = sum(len(d) for d in w['loc_loc_meas'].values())
        budget = int(budget_frac * n_pairs)
        pairs_n.append(n_pairs)
        for label, kw, acc in (('old', dict(utility_dispatch='per_vp',
                                            polish_mode='full'), t_old),
                               ('new', dict(utility_dispatch='auto',
                                            polish_mode='incremental'), t_new)):
            np.random.seed(seed)
            g = make_greedy(selection='phased', workers=workers, **kw)
            g.set_data(w)
            g.solve()
            t0 = time.perf_counter()
            g.measurements(budget // 2)
            g.measurements(budget)
            acc.append(time.perf_counter() - t0)
            g.cleanup()
        print(f"{n_vps}x{n_targets} ({n_pairs} pairs, b={budget}): "
              f"old {t_old[-1]:.1f}s  new {t_new[-1]:.1f}s  "
              f"({t_old[-1] / t_new[-1]:.1f}x)", flush=True)
    return pairs_n, t_old, t_new


def generate_size_figure():
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    pairs_n, t_old, t_new = bench_problem_size()
    # power-law fits t = a * pairs^k, extrapolated to the production shape
    k_old, a_old = np.polyfit(np.log(pairs_n), np.log(t_old), 1)
    k_new, a_new = np.polyfit(np.log(pairs_n), np.log(t_new), 1)
    real_pairs = 38328   # the 300x2209 world
    proj = np.geomspace(pairs_n[0], real_pairs, 50)
    proj_old = np.exp(a_old) * proj ** k_old
    proj_new = np.exp(a_new) * proj ** k_new

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6))
    ax = axes[0]
    ax.loglog(pairs_n, t_old, 'o', color='tab:red', label='old (measured)')
    ax.loglog(pairs_n, t_new, 'o', color='tab:green', label='new (measured)')
    ax.loglog(proj, proj_old, '--', color='tab:red', alpha=0.6,
              label=f'old fit  t∝pairs^{k_old:.2f}')
    ax.loglog(proj, proj_new, '--', color='tab:green', alpha=0.6,
              label=f'new fit  t∝pairs^{k_new:.2f}')
    ax.axvline(real_pairs, color='gray', linestyle=':',
               label='300×2209 world')
    ax.set_xlabel('measured pairs in the world')
    ax.set_ylabel('greedy wall time (s), budget = 30% of pairs')
    ax.set_title('A. end-to-end greedy wall time vs problem size')
    ax.grid(True, which='both', linestyle='--', alpha=0.4)
    ax.legend(fontsize=8)

    ax = axes[1]
    speedup = np.array(t_old) / np.array(t_new)
    ax.semilogx(pairs_n, speedup, 'o-', color='tab:blue',
                label='measured speedup')
    ax.semilogx(proj, proj_old / proj_new, '--', color='tab:blue', alpha=0.6,
                label='fit extrapolation')
    at_real = float(np.exp(a_old) * real_pairs ** k_old
                    / (np.exp(a_new) * real_pairs ** k_new))
    ax.axvline(real_pairs, color='gray', linestyle=':')
    ax.annotate(f'projected {at_real:.0f}× at 300×2209',
                xy=(real_pairs, at_real), xytext=(-160, -12),
                textcoords='offset points', fontsize=9)
    ax.set_xlabel('measured pairs in the world')
    ax.set_ylabel('speedup (t_old / t_new)')
    ax.set_title('B. savings grow with problem size')
    ax.grid(True, which='both', linestyle='--', alpha=0.4)
    ax.legend(fontsize=8)

    fig.tight_layout()
    fig.savefig(SIZE_FIG_FN, dpi=200)
    plt.close(fig)
    print(f'wrote {SIZE_FIG_FN}')


def generate_figure():
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    vp_counts = (25, 50, 100, 200)
    disp = bench_utility_dispatch(vp_counts)
    ping_counts = (400, 800, 1600, 3200)
    t_full, t_incr = bench_polish(ping_counts)
    grids = bench_checkpoint_grids(ping_counts, t_full, t_incr)
    fields, n_nodes = bench_field_dtype()

    fig, axes = plt.subplots(2, 2, figsize=(13, 9))

    ax = axes[0, 0]
    for mode, style in (('per_vp', 'o-'), ('chunk', 's-'), ('inline', '^-')):
        ax.plot(vp_counts, disp[mode], style,
                label=f"{mode}{' (old)' if mode == 'per_vp' else ''}")
    speedup = disp['per_vp'][-1] / max(disp['inline'][-1], 1e-9)
    ax.set_title(f'A. selection re-scoring sweep, 30 targets '
                 f'(inline {speedup:.1f}× vs per_vp @ {vp_counts[-1]} VPs)')
    ax.set_xlabel('VPs in the mesh')
    ax.set_ylabel('seconds per sweep')
    ax.set_yscale('log')
    ax.grid(True, linestyle='--', alpha=0.5)
    ax.legend()

    ax = axes[0, 1]
    ax.plot(ping_counts, t_full, 'o-', label='full polish (old)')
    ax.plot(ping_counts, t_incr, '^-', label='incremental (10% dirty)')
    sp = t_full[-1] / max(t_incr[-1], 1e-9)
    ax.set_title(f'B. checkpoint batch polish ({sp:.1f}× @ '
                 f'{ping_counts[-1]} pings)')
    ax.set_xlabel('accumulated pings')
    ax.set_ylabel('seconds per polish')
    ax.grid(True, linestyle='--', alpha=0.5)
    ax.legend()

    ax = axes[1, 0]
    names = list(grids)
    vals = [grids[k] for k in names]
    colors = ['tab:red', 'tab:orange', 'tab:blue', 'tab:green']
    ax.barh(range(len(names)), vals, color=colors)
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=9)
    ax.invert_yaxis()
    sp = vals[0] / max(vals[-1], 1e-9)
    ax.set_title(f'C. run-total polish cost per checkpoint grid '
                 f'({sp:.0f}× end-to-end)')
    ax.set_xlabel('seconds (this synthetic world; scales with mesh size)')
    ax.grid(True, axis='x', linestyle='--', alpha=0.5)

    ax = axes[1, 1]
    labels = list(fields)
    x = np.arange(len(labels))
    mb = [fields[k]['bytes'] / 1e6 for k in labels]
    ax.bar(x, mb, width=0.5, color=['tab:red', 'tab:green'])
    for xi, v in zip(x, mb):
        ax.text(xi, v, f"{v:.2f} MB\n({int(1024 / v):,} fields/GB)",
                ha='center', va='bottom', fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylim(0, max(mb) * 1.35)
    ax.set_ylabel('MB per policy field')
    ax.set_title(f'D. field footprint at real graph size '
                 f'(n={n_nodes:,} nodes) — RAM, disk and LRU reach all 2×')

    fig.suptitle('Speedups: old vs new implementations on identical inputs',
                 fontsize=13)
    fig.tight_layout()
    fig.savefig(FIG_FN, dpi=200)
    plt.close(fig)
    print(f'wrote {FIG_FN}')
    return {'dispatch': disp, 'polish': (list(ping_counts), t_full, t_incr),
            'grids': grids, 'fields': fields}


if __name__ == '__main__':
    if '--size' in sys.argv:
        generate_size_figure()
    else:
        generate_figure()
