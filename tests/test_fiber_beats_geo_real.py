"""Regression tests for the 2026-07-10 dense-target collapse.

Root cause (see "2026-07-10 dense collapse" in
.claude/FIBER_GEOLOCATOR_RESULTS.md): the
coverage-greedy source sampler broke its massive equal-gain ties
lexicographically by IP string, so the ~10 sources allowed to serve
every dense target (coverage_depth) were always the lowest-address
daily probes — a geographic cluster (AFRINIC 102.x / APNIC 103.x).
When the campaign-driven eligible pool shifted on 2026-07-09/10, the
sampled worlds started measuring every dense target exclusively from
Africa/Asia, and every strategy — the ground-truth oracle included —
collapsed on dense targets.  The fiber greedy amplified it worst
(clamped-offset additive model under a detour-heavy base), which
masqueraded as a fiber-model regression.

Three layers of defense:

  TestCoverageSamplerGeography  hermetic synthetic mesh: equal-gain
                                tie-breaks must not concentrate in low
                                address space, and sampling stays
                                deterministic per seed.
  TestFiberModelTokenFreshness  the estimator-registry staleness
                                footgun found during the same debug:
                                two worlds under one tag must never
                                serve each other's floors.
  TestFiberBeatsGeodesicReal    the missing end-to-end truth on real
                                data — fiber median error beats
                                geodesic on a healthy sampled world.
                                Slow (two greedy runs, ~25 min);
                                opt-in via GEOLOC_E2E_REAL=1.
"""

import os
import pickle
import sys

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import pytest

from utils import get_distance

REPO = os.path.join(os.path.dirname(__file__), '..')
GRAPH_GLOB = os.path.join(REPO, 'internet_gmaps', 'data')
MERGED_AVAILABLE = os.path.exists(
    os.path.join(REPO, 'internet_gmaps', 'mesh_campaign', 'data',
                 'state.sqlite'))


# ---------------------------------------------------------------------------
# 1. Sampler geography
# ---------------------------------------------------------------------------

# Probes tightly clustered in one region hold the lexicographically-
# smallest addresses in the mesh — the exact shape that reproduced the
# production failure (every daily probe ties on coverage gain; a
# lexicographic tie-break picks exactly the low-address cluster).
# Sized so the old tie-break fills ALL source slots from the cluster
# with high probability while a fair tie-break picks ~2 of them.
N_CLUSTER = 15
N_SPREAD = 55


def _synthetic_mesh(seed=0):
    rng = np.random.default_rng(seed)
    addr = {}
    for i in range(N_CLUSTER):  # sorts before '200.*'
        addr[f'102.0.0.{i}'] = (float(rng.uniform(-2, 2)),
                                float(rng.uniform(18, 22)))
    for i in range(N_SPREAD):
        addr[f'200.{i}.0.0'] = (float(rng.uniform(-55, 65)),
                                float(rng.uniform(-170, 170)))
    meas = {}
    for s, sloc in addr.items():
        meas[s] = {}
        for t, tloc in addr.items():
            if s == t:
                continue
            meas[s][t] = [1.5 * get_distance(sloc, tloc) / 100.0 + 5.0]
    return {'address_to_loc': dict(addr), 'loc_loc_meas': meas}


def _sample(n=10, n_targets=20, seed=1):
    from assess_geolocators import Geolocator_Comparator
    gc = Geolocator_Comparator(geolocators=[])
    gc.target_data = _synthetic_mesh()
    gc.get_random_subsample(n=n, n_targets=n_targets, seed=seed)
    return gc.target_data


class TestCoverageSamplerGeography:
    def test_tie_break_not_lexicographic(self):
        # Every probe covers every target, so ALL initial gains tie and
        # the sampler's tie-break alone decides the sources.  The old
        # (gain, addr) heap deterministically returned the ten 102.x
        # cluster probes; a geography-blind tie-break should pick a
        # near-random subset of the 60.
        counts = []
        for seed in (1, 2, 3):
            td = _sample(seed=seed)
            srcs = set(td['loc_loc_meas'])
            counts.append(sum(1 for s in srcs if s.startswith('102.')))
        # Fair tie-break: hypergeometric(70, 15, 10), mean ~2.1 per
        # draw.  The old lexicographic tie-break fills every slot from
        # the cluster (measured: 10/10 cluster sources per seed on the
        # pre-fix code).  5+ in every one of three draws is p < 1e-3
        # under the fix and certain under the bug.
        assert min(counts) <= 4, (
            f'cluster counts {counts}: source tie-break is still '
            f'concentrating in low address space')

    def test_sampling_deterministic_per_seed(self):
        a = _sample(seed=7)
        b = _sample(seed=7)
        assert sorted(a['loc_loc_meas']) == sorted(b['loc_loc_meas'])
        assert a['address_to_loc'] == b['address_to_loc']

    def test_targets_keep_all_chosen_sources(self):
        # coverage_depth caps what the greedy CREDITS, not what a
        # target keeps: in a complete mesh every chosen source's pair
        # survives into the world.
        td = _sample(n=12, n_targets=18, seed=3)
        vps_of = {}
        for s, dsts in td['loc_loc_meas'].items():
            for t in dsts:
                vps_of.setdefault(t, set()).add(s)
        n_srcs = len(td['loc_loc_meas'])
        assert n_srcs == 12
        assert all(len(v) >= n_srcs - 1 for v in vps_of.values())


# ---------------------------------------------------------------------------
# 2. Fiber-model registry freshness
# ---------------------------------------------------------------------------

def _mini_world(vp_locs):
    addr = {f'10.0.{i}.0': loc for i, loc in enumerate(vp_locs)}
    return {'address_to_loc': dict(addr),
            'loc_loc_meas': {a: {} for a in addr}}


@pytest.mark.skipif(
    not os.path.exists(os.path.join(REPO, 'internet_gmaps', 'data')),
    reason='fiber atlas graph not available')
class TestFiberModelTokenFreshness:
    def test_same_tag_different_world_gets_fresh_estimator(self):
        from assess_geolocators import make_fiber_model
        world_a = _mini_world([(51.5, -0.1), (40.7, -74.0), (35.7, 139.7)])
        world_b = _mini_world([(-33.9, 151.2), (48.9, 2.3), (37.8, -122.4)])

        model_a = make_fiber_model(world_a, '_tokentest')
        # Force estimator construction + registry insertion for A.
        idx_a = set(model_a.vp_idx)
        assert idx_a == {(51.5, -0.1), (40.7, -74.0), (35.7, 139.7)}

        # Same tag, different probes: with a tag-only cache token this
        # returned A's estimator (floors for the wrong VPs, silently).
        model_b = make_fiber_model(world_b, '_tokentest')
        assert model_b._token != model_a._token
        idx_b = set(model_b.vp_idx)
        assert idx_b == {(-33.9, 151.2), (48.9, 2.3), (37.8, -122.4)}

        # A keeps answering for A after B was built.
        assert set(model_a.vp_idx) == idx_a


# ---------------------------------------------------------------------------
# 3. Fiber beats geodesic on real data (the historical truth)
# ---------------------------------------------------------------------------

E2E_SNAPSHOT = os.path.join(REPO, 'cache', 'world_100src_300dst_fibergeo.pkl')
E2E_BUDGETS = [1000, 2500]


@pytest.mark.skipif(os.environ.get('GEOLOC_E2E_REAL') != '1',
                    reason='slow real-data e2e; set GEOLOC_E2E_REAL=1')
@pytest.mark.skipif(not MERGED_AVAILABLE,
                    reason='merged mesh (campaign DB) not available')
class TestFiberBeatsGeodesicReal:
    def _world(self):
        """Fixed world: reuse the snapshot when present, else sample one
        with the (fixed) coverage sampler and pin it."""
        if os.path.exists(E2E_SNAPSHOT):
            return pickle.load(open(E2E_SNAPSHOT, 'rb'))['target_data']
        from assess_geolocators import Geolocator_Comparator
        gc = Geolocator_Comparator(geolocators=[], data_source='merged')
        gc.load_target_measurement_data()
        gc.get_random_subsample(n=100, n_targets=300, seed=31415)
        tmp = E2E_SNAPSHOT + '.tmp'
        with open(tmp, 'wb') as fh:
            pickle.dump({'target_data': gc.target_data,
                         'meta': gc.experiment_meta}, fh)
        os.replace(tmp, E2E_SNAPSHOT)
        return gc.target_data

    def test_fiber_median_beats_geodesic(self):
        from assess_geolocators import (evaluate_geolocator,
                                        make_fiber_model)
        from iterative_greedy_geolocator import Iterative_Greedy_Geolocator
        from feasible_region_maintainer import ADDITIVE
        from probabilistic_helpers import GeodesicRtt

        td = self._world()
        results = {}
        for name, model in (
                ('greedy_phased_geo', GeodesicRtt(slope=1.3)),
                ('greedy_phased_fiber',
                 make_fiber_model(td, '_fibergeo', slope=1.3))):
            g = Iterative_Greedy_Geolocator(
                region_mode=ADDITIVE, model_refit_every=25,
                selection='phased', max_workers=6,
                utility_dispatch='auto', polish_mode='incremental',
                rtt_model=model, name=name)
            pd = evaluate_geolocator(g, td, 'nearest_neighbor',
                                     E2E_BUDGETS)
            results[name] = pd['per_target'][E2E_BUDGETS[-1]]

        med = {n: float(np.median(list(errs.values())))
               for n, errs in results.items()}
        mean = {n: float(np.mean(list(errs.values())))
                for n, errs in results.items()}
        print(f"\nfinal budget {E2E_BUDGETS[-1]}: "
              f"geo {mean['greedy_phased_geo']:.0f}/"
              f"{med['greedy_phased_geo']:.0f} km, "
              f"fiber {mean['greedy_phased_fiber']:.0f}/"
              f"{med['greedy_phased_fiber']:.0f} km (mean/median)")
        # The historical margin (2026-07-06/07) was 30-40% on medians —
        # far outside the documented 5-10% greedy run-to-run jitter.
        assert med['greedy_phased_fiber'] <= med['greedy_phased_geo'], (
            f"fiber median {med['greedy_phased_fiber']:.0f} km worse "
            f"than geodesic {med['greedy_phased_geo']:.0f} km")
