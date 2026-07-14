"""Unit tests for floor_query.PolicyFloorEstimator.

Three load-bearing guarantees (the handoff's contract for the
geolocation integration):
  1. exactness — agrees with policy_floor_matrix wherever both are
     defined (same masked-Dijkstra physics, arbitrary-point API);
  2. OPEN equivalence — under OPEN_POLICY it reproduces FloorEstimator
     bit-for-bit (the no-branching principle);
  3. inf semantics — a policy floor of inf falls back to the OPEN floor
     for exactly that (vp, point), never to bare geodesic.

All tests are hermetic: synthetic graphs, injected country codes, a fake
longitude-band geocoder (no reverse_geocoder dependency).
"""

import numpy as np
import pytest

import geo
from fiber_graph import GraphBuilder
from floor_query import FloorEstimator, NoRouteError, PolicyFloorEstimator
from test_transit_policy import masked_brute_floor
from transit_policy import (
    OPEN_POLICY,
    CountryRule,
    TransitPolicy,
    allowed_node_mask,
    no_transit,
    policy_floor_matrix,
)

EQ_DEG_KM = np.pi * geo.EARTH_RADIUS_KM / 180


def band_cc(lats, lons):
    """Fake geocoder: country from longitude band (AA / XX / BB)."""
    out = []
    for lon in np.atleast_1d(np.asarray(lons, dtype=float)):
        out.append("AA" if lon < 3.0 else ("XX" if lon < 6.0 else "BB"))
    return out


def chain_through_xx():
    """Equator chain lon 0..8; middle nodes belong to country XX
    (same construction as test_transit_policy)."""
    b = GraphBuilder(snap_tolerance_km=1.0)
    b.add_path([(0.0, float(lon)) for lon in range(0, 9)])
    g = b.build()
    node_cc = np.array(["AA", "AA", "AA", "XX", "XX", "XX", "BB", "BB", "BB"])
    return g, node_cc


MESH_LAT = np.zeros(3)
MESH_LON = np.array([0.5, 4.0, 7.5])  # in AA, XX, BB territory
MESH_CC = np.array(["AA", "XX", "BB"])
BLOCK_XX = TransitPolicy("t-block-xx", (no_transit("XX"),))


def make_pfe(g, node_cc, policy, **kw):
    return PolicyFloorEstimator(
        g, MESH_LAT, MESH_LON, node_cc=node_cc, vp_cc=MESH_CC,
        policy=policy, point_cc_fn=band_cc, **kw,
    )


class TestOpenEquivalence:
    def test_matches_floor_estimator_exactly(self):
        g, node_cc = chain_through_xx()
        pfe = make_pfe(g, node_cc, OPEN_POLICY)
        est = FloorEstimator(g, MESH_LAT, MESH_LON)
        rng = np.random.default_rng(0)
        queries = list(zip(MESH_LAT, MESH_LON)) + [
            (rng.uniform(-5, 5), rng.uniform(-3, 12)) for _ in range(15)
        ]
        for lat, lon in queries:
            want = est.floor_ms(lat, lon)
            np.testing.assert_array_equal(pfe.floor_ms(lat, lon), want)
            # no policy → raw and fallback floors coincide
            np.testing.assert_array_equal(pfe.policy_floor_ms(lat, lon), want)

    def test_floor_many_matches_floor(self):
        g, node_cc = chain_through_xx()
        pfe = make_pfe(g, node_cc, BLOCK_XX, no_route="open")
        lats = np.array([0.0, 1.0, -2.0])
        lons = np.array([0.2, 7.7, 4.4])
        many = pfe.floor_many_ms(lats, lons)
        assert many.shape == (3, 3)
        for row, lat, lon in zip(many, lats, lons):
            np.testing.assert_array_equal(row, pfe.floor_ms(lat, lon))


class TestMatrixEquality:
    def test_equals_policy_floor_matrix_on_mesh_locs(self):
        g, node_cc = chain_through_xx()
        mat = policy_floor_matrix(g, node_cc, MESH_LAT, MESH_LON, MESH_CC, BLOCK_XX)
        pfe = make_pfe(g, node_cc, BLOCK_XX)
        for t in range(3):
            got = pfe.policy_floor_ms(MESH_LAT[t], MESH_LON[t])
            # equality where finite AND agreement on where inf lives
            np.testing.assert_array_equal(got, mat[t, :])

    def test_equals_matrix_under_cable_distrust(self):
        # distrust factors scale edge weights before routing — the field
        # computation must apply the same scaled_base_data as the matrix
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0.0, float(lon)) for lon in range(0, 9)], feature="TG:flaky")
        g = b.build()
        node_cc = np.array(["AA"] * g.n_nodes)
        lat, lon = np.zeros(2), np.array([-0.3, 8.3])
        loc_cc = np.array(["AA", "BB"])
        distrust = TransitPolicy("d-flaky", (), cable_factors=(("TG:flaky", 2.0),))
        kw = dict(lastmile_km_max=100.0, direct_km_max=0.0)
        mat = policy_floor_matrix(g, node_cc, lat, lon, loc_cc, distrust, **kw)
        pfe = PolicyFloorEstimator(
            g, lat, lon, node_cc=node_cc, vp_cc=loc_cc, policy=distrust,
            point_cc_fn=band_cc, **kw,
        )
        for t in range(2):
            got = pfe.policy_floor_ms(lat[t], lon[t], cc=loc_cc[t])
            np.testing.assert_allclose(got, mat[t, :], rtol=1e-12)

    @pytest.mark.parametrize("seed", [0, 1, 2])
    def test_matches_masked_brute_force_at_arbitrary_points(self, seed):
        rng = np.random.default_rng(seed)
        b = GraphBuilder(snap_tolerance_km=1.0)
        n = 25
        lats, lons = rng.uniform(-20, 20, n), rng.uniform(-20, 20, n)
        ids = [b.node_id(la, lo) for la, lo in zip(lats, lons)]
        for _ in range(40):
            i, j = rng.integers(0, n, 2)
            if ids[i] != ids[j]:
                km = geo.haversine_km(lats[i], lons[i], lats[j], lons[j])
                b.add_edge(ids[i], ids[j], geo.rtt_ms(km * rng.uniform(1.0, 1.5)))
        g = b.build()
        node_cc = rng.choice(["AA", "BB", "XX", "YY"], size=g.n_nodes)
        policy = TransitPolicy("t-xxyy", (no_transit("XX", "YY"),))

        vp_lat = rng.uniform(-25, 25, 4)
        vp_lon = rng.uniform(-25, 25, 4)
        vp_cc = rng.choice(["AA", "XX", "CC"], size=4)
        pfe = PolicyFloorEstimator(
            g, vp_lat, vp_lon, node_cc=node_cc, vp_cc=vp_cc, policy=policy,
            point_cc_fn=band_cc,
        )
        for _ in range(10):
            t = (rng.uniform(-25, 25), rng.uniform(-25, 25))
            t_cc = rng.choice(["AA", "XX", "CC"])
            got = pfe.policy_floor_ms(t[0], t[1], cc=t_cc)
            for v in range(4):
                ok = allowed_node_mask(policy, node_cc, {vp_cc[v], t_cc})
                want = masked_brute_floor(
                    g, ok, (vp_lat[v], vp_lon[v]), t, 300.0, 300.0
                )
                assert got[v] == pytest.approx(want, rel=1e-9), (v, t, t_cc)


class TestNoRouteError:
    def test_strangled_route_raises_keyerror(self):
        # AA -> BB must cross XX; the policy bans XX, the OPEN graph
        # routes it: floor_ms must refuse loudly, not substitute a value
        g, node_cc = chain_through_xx()
        pfe = make_pfe(g, node_cc, BLOCK_XX, direct_km_max=0.0)
        with pytest.raises(NoRouteError, match="stranded"):
            pfe.floor_ms(0.0, 7.7)
        with pytest.raises(KeyError):  # NoRouteError IS a KeyError
            pfe.floor_ms(0.0, 7.7)
        # the raw validation API still exposes inf for the same query
        pf = pfe.policy_floor_ms(0.0, 7.7)
        assert np.isinf(pf[0]) and np.isfinite(pf[1]) and np.isfinite(pf[2])

    def test_subset_raises_only_when_a_blocked_vp_is_queried(self):
        g, node_cc = chain_through_xx()
        pfe = make_pfe(g, node_cc, BLOCK_XX, direct_km_max=0.0)
        got = pfe.floor_ms_subset(0.0, 7.7, [1, 2])  # exempt VPs: fine
        assert np.all(np.isfinite(got))
        with pytest.raises(NoRouteError):
            pfe.floor_ms_subset(0.0, 7.7, [0])

    def test_open_inf_does_not_raise(self):
        # off-grid target: even the open graph has no route — that is a
        # graph-coverage gap, not a policy bug, so no raise: stays inf
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0.0, 0.0), (0.0, 1.0)])
        g = b.build()
        pfe = PolicyFloorEstimator(
            g, [0.0], [0.2], node_cc=np.array(["AA", "AA"]), vp_cc=np.array(["AA"]),
            policy=BLOCK_XX, point_cc_fn=band_cc,
            direct_km_max=100.0, lastmile_km_max=100.0,
        )
        assert np.isinf(pfe.floor_ms(0.0, 90.0)[0])

    def test_invalid_no_route_mode_rejected(self):
        g, node_cc = chain_through_xx()
        with pytest.raises(ValueError):
            make_pfe(g, node_cc, BLOCK_XX, no_route="fallback")


class TestTerrestrialOnlyRules:
    def test_estimator_matches_matrix_with_terrestrial_ban(self):
        # ITU chain through XX + TG detour whose mid vertex is also XX:
        # a terrestrial-only ban must leave the submarine route usable,
        # identically in the matrix and the estimator
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0.0, float(lon)) for lon in range(0, 9)], feature="ITU")
        b.add_path([(0.0, 0.0), (6.0, 2.0), (6.0, 6.0), (0.0, 8.0)], feature="TG:around")
        g = b.build()
        node_cc = np.where((g.node_lon > 2.5) & (g.node_lon < 6.5), "XX", "AA")
        lat, lon = np.zeros(2), np.array([-0.3, 8.3])
        loc_cc = np.array(["AA", "BB"])
        pol = TransitPolicy(
            "t-terr",
            (CountryRule("t-terr-xx", lambda cc: cc == "XX", terrestrial_only=True),),
        )
        kw = dict(lastmile_km_max=100.0, direct_km_max=0.0)
        mat = policy_floor_matrix(g, node_cc, lat, lon, loc_cc, pol, **kw)
        pfe = PolicyFloorEstimator(
            g, lat, lon, node_cc=node_cc, vp_cc=loc_cc, policy=pol,
            point_cc_fn=band_cc, **kw,
        )
        for t in range(2):
            got = pfe.policy_floor_ms(lat[t], lon[t], cc=loc_cc[t])
            np.testing.assert_allclose(got, mat[t, :], rtol=1e-12)
        assert np.all(np.isfinite(mat))  # submarine keeps every pair routable


class TestInfFallback:
    def test_blocked_route_falls_back_to_open_floor(self):
        g, node_cc = chain_through_xx()
        pfe = make_pfe(g, node_cc, BLOCK_XX, direct_km_max=0.0, no_route="open")
        open_est = FloorEstimator(g, MESH_LAT, MESH_LON, direct_km_max=0.0)
        # target in BB territory: AA VP must cross XX -> policy inf
        lat, lon = 0.0, 7.7
        pf = pfe.policy_floor_ms(lat, lon)
        assert np.isinf(pf[0])          # AA -> BB blocked
        assert np.isfinite(pf[1])       # XX endpoint exemption
        assert np.isfinite(pf[2])       # BB itself
        fl = pfe.floor_ms(lat, lon)
        want_open = open_est.floor_ms(lat, lon)
        # inf entries take the OPEN floor, finite entries keep the policy floor
        assert fl[0] == want_open[0] and np.isfinite(fl[0])
        np.testing.assert_array_equal(fl[1:], pf[1:])

    def test_fallback_never_below_open_floor(self):
        g, node_cc = chain_through_xx()
        pfe = make_pfe(g, node_cc, BLOCK_XX, no_route="open")
        open_est = FloorEstimator(g, MESH_LAT, MESH_LON)
        rng = np.random.default_rng(7)
        for _ in range(20):
            lat, lon = rng.uniform(-5, 5), rng.uniform(-3, 12)
            assert np.all(pfe.floor_ms(lat, lon) >= open_est.floor_ms(lat, lon) - 1e-9)


class TestDiskCache:
    def test_cache_roundtrip_and_reuse(self, tmp_path):
        g, node_cc = chain_through_xx()
        pfe1 = make_pfe(g, node_cc, BLOCK_XX, cache_dir=tmp_path)
        want = [pfe1.policy_floor_ms(0.0, lon) for lon in (0.2, 4.4, 7.7)]
        n_files = len(list(tmp_path.glob("pfield_*.npy")))
        assert n_files > 0
        # a fresh instance must read the cached fields, not recompute
        pfe2 = make_pfe(g, node_cc, BLOCK_XX, cache_dir=tmp_path)
        got = [pfe2.policy_floor_ms(0.0, lon) for lon in (0.2, 4.4, 7.7)]
        for w, r in zip(want, got):
            np.testing.assert_array_equal(r, w)
        assert len(list(tmp_path.glob("pfield_*.npy"))) == n_files

    def test_policy_name_keys_the_cache(self, tmp_path):
        g, node_cc = chain_through_xx()
        make_pfe(g, node_cc, BLOCK_XX, cache_dir=tmp_path).policy_floor_ms(0.0, 7.7)
        n_before = len(list(tmp_path.glob("pfield_*.npy")))
        renamed = TransitPolicy("t-block-xx-v2", (no_transit("XX"),))
        make_pfe(g, node_cc, renamed, cache_dir=tmp_path).policy_floor_ms(0.0, 7.7)
        assert len(list(tmp_path.glob("pfield_*.npy"))) > n_before


class TestVpSubsets:
    def test_subset_matches_full_query(self):
        g, node_cc = chain_through_xx()
        pfe = make_pfe(g, node_cc, BLOCK_XX, no_route="open")
        for lat, lon in ((0.0, 0.2), (0.0, 4.4), (1.0, 7.7)):
            full = pfe.floor_ms(lat, lon)
            for subset in ([0], [2, 0], [1, 2]):
                got = pfe.floor_ms_subset(lat, lon, subset)
                np.testing.assert_array_equal(got, full[np.asarray(subset)])

    def test_subset_fallback_is_per_vp(self):
        # AA->BB blocked (inf -> open floor) while XX->BB stays finite:
        # a mixed subset must fall back only on the blocked rows
        g, node_cc = chain_through_xx()
        pfe = make_pfe(g, node_cc, BLOCK_XX, direct_km_max=0.0, no_route="open")
        got = pfe.floor_ms_subset(0.0, 7.7, [0, 1])
        open_ = FloorEstimator(g, MESH_LAT, MESH_LON, direct_km_max=0.0)
        assert got[0] == open_.floor_ms(0.0, 7.7)[0]
        assert got[1] == pfe.policy_floor_ms(0.0, 7.7)[1]

    def test_lru_eviction_keeps_results_exact(self, tmp_path):
        g, node_cc = chain_through_xx()
        ref = make_pfe(g, node_cc, BLOCK_XX, no_route="open")
        pfe = make_pfe(g, node_cc, BLOCK_XX, cache_dir=tmp_path,
                       max_cached_fields=2, no_route="open")
        for lon in (0.2, 4.4, 7.7, 0.2, 7.7):   # forces evictions + reloads
            np.testing.assert_array_equal(pfe.floor_ms(0.0, lon),
                                          ref.floor_ms(0.0, lon))
        assert len(pfe._fields) <= 2


class TestLazyClassFields:
    def test_one_dijkstra_per_vp_class(self):
        g, node_cc = chain_through_xx()
        pfe = make_pfe(g, node_cc, BLOCK_XX)
        calls = []
        original = pfe._compute_field

        def counting(v, cc):
            calls.append((v, cc))
            return original(v, cc)

        pfe._compute_field = counting
        # AA and BB targets share no signature with XX; querying many
        # points in one country must not recompute fields
        for lon in (7.2, 7.7, 8.3):
            pfe.policy_floor_ms(0.0, lon)   # BB class
        assert len(calls) == pfe.n_vps
        pfe.policy_floor_ms(0.0, 4.4)       # XX class -> one more round
        assert len(calls) == 2 * pfe.n_vps
