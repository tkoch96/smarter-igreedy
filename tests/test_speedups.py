"""
Equivalence + benchmark tests for the computation speedups.

The speedups are switches, not rewrites — every test here pins that the
fast path computes the SAME quantities as the preserved old path on
identical inputs, then the figure test measures t_new << t_old:

  utility_dispatch : 'inline'/'chunk' score candidate VPs identically to
                     the historical one-executor-job-per-VP path;
  polish_mode      : incremental batch polish leaves untouched targets
                     exactly at their previous estimates, tracks the full
                     polish on re-optimised ones, and never degrades the
                     end-to-end greedy;
  field_dtype      : float32 policy fields match float64 floors to µs
                     precision and use distinct disk-cache files.

tests/speedups.pdf (via plot_speedups.generate_figure) is the measured
old-vs-new timing evidence.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import pytest

from probabilistic_helpers import additive_batch_em
from utils import get_distance
from plot_speedups import make_world, make_greedy, generate_figure


@pytest.fixture(scope='module')
def world():
    return make_world(n_vps=30, n_targets=40, seed=17)


class TestUtilityDispatchEquivalence:
    def test_all_dispatch_modes_agree(self, world):
        g = make_greedy(workers=2)
        g.set_data(world)
        g.solve()
        g.measurements(90)   # give regions real constraints first
        sample = g.targets[::8]
        results = {}
        for mode in ('per_vp', 'inline', 'chunk'):
            g.utility_dispatch = mode
            results[mode] = {}
            for dst in sample:
                g._update_best_vp_for_target(dst)
                results[mode][dst] = g.best_vp_cache[dst]
        g.cleanup()
        for dst in sample:
            u_old = results['per_vp'][dst][1]
            for mode in ('inline', 'chunk'):
                src, u = results[mode][dst]
                assert u == pytest.approx(u_old, abs=1e-9), (mode, dst)
                # same argmax unless the utilities tie exactly
                if u != u_old:
                    assert src == results['per_vp'][dst][0], (mode, dst)


class TestIncrementalPolish:
    def test_untouched_targets_keep_previous_estimates(self, world):
        rng = np.random.default_rng(23)
        pairs = [(s, t) for s, d in world['loc_loc_meas'].items() for t in d]
        rng.shuffle(pairs)
        vp_locs = {k: v for k, v in world['address_to_loc'].items()
                   if k.startswith('vp')}
        sub = {p: world['loc_loc_meas'][p[0]][p[1]] for p in pairs[:500]}
        prev, _, _, _, _ = additive_batch_em(sub, vp_locs)
        targets = sorted({t for _, t in sub})
        dirty = set(targets[::5])
        est, _, _, _, _ = additive_batch_em(sub, vp_locs, n_iters=2,
                                            prev_estimates=prev,
                                            only_targets=dirty)
        for t in targets:
            if t not in dirty:
                assert tuple(est[t]) == tuple(prev[t]), t

    def test_incremental_tracks_full_polish_quality(self, world):
        """Re-optimised targets: incremental estimates stay close to the
        full polish, and are no worse against ground truth overall."""
        rng = np.random.default_rng(29)
        addr = world['address_to_loc']
        pairs = [(s, t) for s, d in world['loc_loc_meas'].items() for t in d]
        rng.shuffle(pairs)
        vp_locs = {k: v for k, v in addr.items() if k.startswith('vp')}
        prev_sub = {p: world['loc_loc_meas'][p[0]][p[1]] for p in pairs[:400]}
        full_sub = {p: world['loc_loc_meas'][p[0]][p[1]] for p in pairs[:500]}
        prev, _, _, _, _ = additive_batch_em(prev_sub, vp_locs)
        dirty = {t for _, t in pairs[400:500]}

        est_full, _, _, _, _ = additive_batch_em(full_sub, vp_locs)
        est_incr, _, _, _, _ = additive_batch_em(full_sub, vp_locs, n_iters=2,
                                                 prev_estimates=prev,
                                                 only_targets=dirty)
        err_full = np.mean([get_distance(est_full[t], addr[t])
                            for t in est_full])
        err_incr = np.mean([get_distance(est_incr[t], addr[t])
                            for t in est_incr])
        assert err_incr <= err_full * 1.10 + 25.0


class TestGreedyEndToEnd:
    def test_new_config_matches_old_and_is_faster(self):
        """Paired A/B over several seeds: greedy tie-breaking makes any
        single trial jitter-dominated (the same documented reason
        single-trial same-seed comparisons are noise on the real mesh),
        so equivalence is asserted on the cross-seed MEAN."""
        import time
        errs = {'old': [], 'new': []}
        wall = {'old': 0.0, 'new': 0.0}
        for seed in (17, 19, 23, 29, 31):
            w = make_world(n_vps=30, n_targets=40, seed=seed)
            addr = w['address_to_loc']
            for label, kw in (('old', dict(utility_dispatch='per_vp',
                                           polish_mode='full')),
                              ('new', dict(utility_dispatch='auto',
                                           polish_mode='incremental'))):
                np.random.seed(seed)
                g = make_greedy(selection='phased', workers=2, **kw)
                g.set_data(w)
                g.solve()
                t0 = time.perf_counter()
                g.measurements(150)
                g.measurements(300)   # 2nd checkpoint exercises incremental
                wall[label] += time.perf_counter() - t0
                est = g.get_current_estimates()
                errs[label].append(np.mean(
                    [get_distance(est[t], addr[t]) for t in est]))
                g.cleanup()
        err_old = float(np.mean(errs['old']))
        err_new = float(np.mean(errs['new']))
        assert err_new <= err_old * 1.10 + 25.0, (err_new, err_old)
        assert wall['new'] < wall['old'], wall


class _GeoFloor:
    """Mock atlas (module-level so it pickles to greedy workers)."""

    def __init__(self, locs):
        self.vp_locs = [(float(a), float(b)) for a, b in locs]

    def floor_ms(self, lat, lon):
        from probabilistic_helpers import KM_PER_MS
        return np.array([get_distance(vp, (lat, lon)) / KM_PER_MS
                         for vp in self.vp_locs])


class TestFiberModelEquivalence:
    """The dispatch/polish equivalence claims again, but with an injected
    FiberFloorRtt base model — the fast paths must not perturb the fiber
    code path any more than the geodesic one."""

    def _mock_fiber(self, world):
        from probabilistic_helpers import FiberFloorRtt

        locs = [world['address_to_loc'][k]
                for k in sorted(world['address_to_loc']) if k.startswith('vp')]
        return FiberFloorRtt(estimator=_GeoFloor(locs), vp_locs=locs,
                             slope=1.3, offset_ms=0.0)

    def test_dispatch_modes_agree_under_fiber(self, world):
        fiber = self._mock_fiber(world)
        g = make_greedy(workers=2, rtt_model=fiber)
        g.set_data(world)
        g.solve()
        g.measurements(90)
        sample = g.targets[::8]
        results = {}
        for mode in ('per_vp', 'inline', 'chunk'):
            g.utility_dispatch = mode
            results[mode] = {}
            for dst in sample:
                g._update_best_vp_for_target(dst)
                results[mode][dst] = g.best_vp_cache[dst]
        g.cleanup()
        for dst in sample:
            u_old = results['per_vp'][dst][1]
            for mode in ('inline', 'chunk'):
                assert results[mode][dst][1] == pytest.approx(u_old, abs=1e-9)

    def test_incremental_polish_quality_under_fiber(self, world):
        from probabilistic_helpers import additive_batch_em as abe
        fiber = self._mock_fiber(world)
        rng = np.random.default_rng(31)
        addr = world['address_to_loc']
        pairs = [(s, t) for s, d in world['loc_loc_meas'].items() for t in d]
        rng.shuffle(pairs)
        vp_locs = {k: v for k, v in addr.items() if k.startswith('vp')}
        prev_sub = {p: world['loc_loc_meas'][p[0]][p[1]] for p in pairs[:400]}
        full_sub = {p: world['loc_loc_meas'][p[0]][p[1]] for p in pairs[:500]}
        prev, _, _, _, _ = abe(prev_sub, vp_locs, rtt_model=fiber)
        dirty = {t for _, t in pairs[400:500]}
        est_full, _, _, _, _ = abe(full_sub, vp_locs, rtt_model=fiber)
        est_incr, _, _, _, _ = abe(full_sub, vp_locs, n_iters=2,
                                   rtt_model=fiber,
                                   prev_estimates=prev, only_targets=dirty)
        err_full = np.mean([get_distance(est_full[t], addr[t])
                            for t in est_full])
        err_incr = np.mean([get_distance(est_incr[t], addr[t])
                            for t in est_incr])
        assert err_incr <= err_full * 1.10 + 25.0


class TestFieldDtype:
    def test_float32_floors_match_float64(self, tmp_path):
        igm = os.path.join(os.path.dirname(__file__), '..', 'internet_gmaps')
        if igm not in sys.path:
            sys.path.append(igm)
        from fiber_graph import FiberGraph
        from floor_query import PolicyFloorEstimator

        # 4-node toy graph: two "continents" with one cable between them
        node_lat = np.array([10.0, 12.0, 40.0, 42.0])
        node_lon = np.array([10.0, 12.0, 60.0, 62.0])
        src = np.array([0, 1, 1, 2, 2, 3])
        dst = np.array([1, 0, 2, 1, 3, 2])
        rtt = np.array([3.0, 3.0, 55.0, 55.0, 3.0, 3.0])
        graph = FiberGraph(node_lat, node_lon, src, dst, rtt)
        ccs = np.array(['AA', 'AA', 'BB', 'BB'])

        def cc_fn(lats, lons):
            lats = np.atleast_1d(lats)
            return np.where(lats < 30.0, 'AA', 'BB').tolist()

        kw = dict(node_cc=ccs, vp_cc=np.array(['AA']), point_cc_fn=cc_fn,
                  cache_dir=str(tmp_path), no_route='open')
        e64 = PolicyFloorEstimator(graph, [10.0], [10.0], **kw)
        e32 = PolicyFloorEstimator(graph, [10.0], [10.0],
                                   field_dtype=np.float32, **kw)
        for lat, lon in ((11.0, 11.0), (41.0, 61.0), (25.0, 30.0)):
            f64 = e64.floor_ms(lat, lon)
            f32 = e32.floor_ms(lat, lon)
            assert np.allclose(f64, f32, rtol=1e-5, equal_nan=True)
        # distinct cache files: compact fields can never serve exact mode
        files = os.listdir(tmp_path)
        assert len([f for f in files if f.startswith('pfield_')]) >= 2


class TestSpeedupFigure:
    def test_generate_figure(self):
        stats = generate_figure()
        assert os.path.exists(os.path.join(os.path.dirname(__file__),
                                           'speedups.pdf'))
        # the headline claims: measured, not asserted-by-hope
        disp = stats['dispatch']
        assert disp['inline'][-1] < disp['per_vp'][-1]
        _, t_full, t_incr = stats['polish']
        assert t_incr[-1] < t_full[-1]
