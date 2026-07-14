"""Unit tests for transit_policy.py: rule composition and the node-masked
floor computation, on synthetic graphs with injected country codes."""

import numpy as np
import pytest
from scipy.sparse.csgraph import dijkstra

import geo
from fiber_graph import GraphBuilder
from floor_query import FloorEstimator
from transit_policy import (
    AFRICA_CCS,
    DEFAULT_POLICY,
    OPEN_POLICY,
    RUSSIA_LAND_BORDERS,
    TransitPolicy,
    africa_containment,
    allowed_node_mask,
    no_transit,
    policy_floor_matrix,
    russia_borders,
    small_country,
    soviet_bloc,
)

EQ_DEG_KM = np.pi * geo.EARTH_RADIUS_KM / 180


class TestRules:
    def test_no_transit(self):
        rule = no_transit("ZA", "CN")
        assert rule.member("ZA") and rule.member("CN") and not rule.member("US")

    def test_small_country_population_proxy(self):
        rule = small_country(5.0)
        assert rule.member("MU")  # 1.3M
        assert not rule.member("HK")  # 7.5M — the user's dividing line
        assert not rule.member("SG")  # 5.9M major hub stays open
        assert rule.member("XQ")  # unknown code counts as small

    def test_russia_borders_legacy_rule(self):
        rule = russia_borders()
        for cc in ("KZ", "MN", "CN", "FI", "GE", "LT"):
            assert rule.member(cc)
        assert not rule.member("DE")
        assert len(RUSSIA_LAND_BORDERS) == 14

    def test_soviet_bloc_exempts_eu_members(self):
        rule = soviet_bloc()
        for cc in ("RU", "BY", "UA", "KZ", "UZ", "TM", "KG", "TJ", "GE", "AM", "AZ", "MD", "MN", "KP"):
            assert rule.member(cc), cc
        # the "finland-y" carve-outs: never-bloc neighbors + EU ex-bloc
        for cc in ("FI", "NO", "EE", "LV", "LT", "PL", "HU", "CZ", "RO", "BG", "DE"):
            assert not rule.member(cc), cc
        # without the carve-out the Baltics are back in
        assert soviet_bloc(exempt_eu=False).member("EE")

    def test_africa_containment_regions(self):
        rule = africa_containment()
        # NG banned for a US<->BR pair, allowed once an endpoint is African
        assert rule.banned("NG", frozenset({"US", "BR"}))
        assert not rule.banned("NG", frozenset({"US", "ZA"}))
        assert not rule.banned("NG", frozenset({"EG", "ZA"}))
        # Suez corridor exempt by default: EG never banned
        assert not rule.banned("EG", frozenset({"DE", "SG"}))
        assert africa_containment(exempt_suez=False).banned("EG", frozenset({"DE", "SG"}))
        assert "ZA" in AFRICA_CCS and "EG" in AFRICA_CCS

    def test_small_island_transit(self):
        from transit_policy import SMALL_ISLAND_NATIONS, small_island_transit

        rule = small_island_transit()
        # ZA<->IN may not ride SAFE through Mauritius
        assert rule.banned("MU", frozenset({"ZA", "IN"}))
        # any small-island endpoint unlocks the CLASS: MU<->DE may hop RE
        assert not rule.banned("MU", frozenset({"MU", "DE"}))
        assert not rule.banned("RE", frozenset({"MU", "DE"}))
        # major island hubs are not members; Pacific relays are exempt
        for cc in ("HK", "SG", "TW", "NZ", "IE", "FJ", "GU"):
            assert not rule.banned(cc, frozenset({"US", "DE"})), cc
        assert "IS" in SMALL_ISLAND_NATIONS  # island access chains: IS->FO->GB
        assert not rule.banned("FO", frozenset({"IS", "GB"}))
        # v3.7: BIG-island endpoints unlock too — DO/HT ride the Antilles
        assert not rule.banned("PR", frozenset({"DO", "US"}))
        assert rule.banned("PR", frozenset({"BR", "US"}))  # mainland pairs don't
        assert "BL" in SMALL_ISLAND_NATIONS  # v3.6 gap: stranded 137 pairs

    def test_group_terrestrial_factor_covers_cross_border(self):
        from transit_policy import OPEN_POLICY, policy_floor_matrix_parallel

        # one ITU chain whose middle crosses TM->UZ, one all-AA control
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0.0, float(lon)) for lon in range(0, 9)], feature="ITU")
        b.add_path([(5.0, float(lon)) for lon in range(0, 9)], feature="ITU")
        g = b.build()
        node_cc = np.array(
            ["TM" if lat < 2.5 and lon < 4 else ("UZ" if lat < 2.5 else "AA")
             for lat, lon in zip(g.node_lat, g.node_lon)]
        )
        lat = np.array([0.0, 0.0, 5.0, 5.0])
        lon = np.array([-0.3, 8.3, -0.3, 8.3])
        loc_cc = np.array(["XA", "XB", "XA", "XB"])
        kw = dict(n_workers=2, lastmile_km_max=100.0, direct_km_max=0.0)
        policy = TransitPolicy(
            "t-group", (), terrestrial_factors=((("TM", "UZ"), 2.0),)
        )
        open_ = policy_floor_matrix_parallel(g, node_cc, lat, lon, loc_cc, OPEN_POLICY, **kw)
        scaled = policy_floor_matrix_parallel(g, node_cc, lat, lon, loc_cc, policy, **kw)
        assert scaled[1, 0] > 1.5 * open_[1, 0]  # TM/UZ chain doubled incl. border edge
        assert scaled[3, 2] == pytest.approx(open_[3, 2], rel=1e-9)  # AA chain untouched

    def test_corridor_factor_scales_edges_in_box(self):
        from transit_policy import OPEN_POLICY, policy_floor_matrix_parallel

        # two featureless chains; only one passes through the corridor box
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0.0, float(lon)) for lon in range(0, 9)])
        b.add_path([(30.0, float(lon)) for lon in range(0, 9)])
        g = b.build()
        node_cc = np.array(["AA"] * g.n_nodes)
        lat = np.array([0.0, 0.0, 30.0, 30.0])
        lon = np.array([-0.3, 8.3, -0.3, 8.3])
        loc_cc = np.array(["XA", "XB", "XA", "XB"])
        kw = dict(n_workers=2, lastmile_km_max=200.0, direct_km_max=0.0)
        policy = TransitPolicy(
            "t-corr", (), corridor_factors=(("box", (-2.0, 2.0, 2.0, 6.0), 2.0),)
        )
        open_ = policy_floor_matrix_parallel(g, node_cc, lat, lon, loc_cc, OPEN_POLICY, **kw)
        scaled = policy_floor_matrix_parallel(g, node_cc, lat, lon, loc_cc, policy, **kw)
        assert scaled[1, 0] > 1.2 * open_[1, 0]  # equatorial chain crosses the box
        assert scaled[3, 2] == pytest.approx(open_[3, 2], rel=1e-9)  # lat-30 chain untouched

    def test_indian_ocean_containment(self):
        from transit_policy import (
            INDIAN_OCEAN_BOX,
            indian_ocean_containment,
        )

        rule = indian_ocean_containment()
        # mainland<->mainland may not cross the open Indian Ocean
        assert rule.banned("XI", frozenset({"ZA", "AU"}))
        assert rule.banned("XI", frozenset({"OM", "AU"}))
        # island endpoints keep their chains
        assert not rule.banned("XI", frozenset({"MU", "DE"}))
        assert not rule.banned("XI", frozenset({"MG", "US"}))
        # remap: box nodes become XI, others untouched; policies without
        # remaps are a no-op
        pol = DEFAULT_POLICY
        cc = pol.remap_node_cc(
            np.array(["AA", "AA", "AA"]),
            np.array([-20.0, -20.0, 30.0]),
            np.array([70.0, 120.0, 70.0]),
        )
        np.testing.assert_array_equal(cc, ["XI", "AA", "AA"])
        lat0, lat1, lon0, lon1 = INDIAN_OCEAN_BOX
        assert lat1 <= 5.0  # the Suez->India->Malacca mainline stays out

    def test_soviet_bloc_region_exemption(self):
        # v3.4: a bloc endpoint unlocks bloc fiber (KZ exits via RU) but a
        # non-bloc pair still can't cross it, and EU-integrated ex-bloc
        # endpoints don't unlock (EE is outside the exempting region)
        rule = soviet_bloc(region_exempt=True)
        assert rule.banned("RU", frozenset({"DE", "JP"}))
        assert not rule.banned("RU", frozenset({"KZ", "DE"}))
        assert not rule.banned("UZ", frozenset({"KZ", "DE"}))  # region-wide
        assert rule.banned("RU", frozenset({"EE", "JP"}))

    def test_relay_island_exemptions(self):
        # v3.4: Caribbean + Atlantic/Indian relay islands exempt from the
        # small-country ban (the Pacific lesson, two more oceans)
        from transit_policy import CABLE_RELAY_ISLANDS

        rule = small_country(5.0, exempt=CABLE_RELAY_ISLANDS)
        for cc in ("AG", "DM", "SX", "TT", "CV", "ST", "SC", "MU", "MV", "FJ"):
            assert not rule.member(cc), cc
        assert rule.member("BT")  # non-relay small states stay restricted

    def test_africa_terrestrial_only_split(self):
        # v3.3: the granular Africa rule bans terrestrial edges only —
        # banned_split routes it to the terrestrial set, node set empty
        rule = africa_containment(country_granular=True, terrestrial_only=True)
        assert rule.terrestrial_only
        assert rule.banned("NG", frozenset({"US", "BR"}))
        pol = TransitPolicy("t", (rule,))
        node_b, terr_b = pol.banned_split({"NG", "US"}, {"US", "BR"})
        assert node_b == frozenset() and terr_b == {"NG"}
        # a country hit by a node rule too lands in node_banned
        pol2 = TransitPolicy("t2", (rule, no_transit("NG")))
        node_b, terr_b = pol2.banned_split({"NG"}, {"US", "BR"})
        assert node_b == {"NG"} and terr_b == frozenset()
        # attribution view stays the union
        assert pol.banned_set({"NG"}, {"US", "BR"}) == {"NG"}

    def test_default_policy_composition(self):
        # restricted() = banned with no exempting endpoint (worst case).
        # v3: TW added; Africa is country-granular (ZA/KE/NG via that rule).
        for cc in ("ZA", "CN", "MN", "AF", "TW", "MU", "KZ", "BY", "UZ", "TM", "RU", "NG", "KE", "EE"):
            assert DEFAULT_POLICY.restricted(cc), cc
        # open: majors, hubs, the full Suez corridor, Finland-y carve-outs,
        # and Pacific relay waypoints (FJ/TK/...)
        for cc in ("US", "DE", "JP", "IN", "HK", "SG", "EG", "DJ", "LK", "AU", "FI", "NO", "PL", "FJ", "TK", "GU"):
            assert not DEFAULT_POLICY.restricted(cc), cc

    def test_endpoint_exemptions_differ_by_rule_kind(self):
        # country rule: only the exact country unlocks itself
        assert "MU" in DEFAULT_POLICY.banned_set({"MU"}, {"FJ", "US"})
        assert "MU" not in DEFAULT_POLICY.banned_set({"MU"}, {"MU", "US"})
        # v3.2/v3.3 granular Africa (frozen): an African endpoint unlocks
        # only its own country
        from transit_policy import V2_POLICY, V33_POLICY

        assert "KE" in V33_POLICY.banned_set({"KE"}, {"NG", "US"})
        assert "KE" not in V33_POLICY.banned_set({"KE"}, {"KE", "US"})
        # v3.5 (current): back to region granularity, terrestrial-only —
        # a landlocked African endpoint may cross neighbors overland
        assert "KE" not in DEFAULT_POLICY.banned_set({"KE"}, {"UG", "US"})
        assert "KE" in DEFAULT_POLICY.banned_set({"KE"}, {"DE", "US"})
        # the v2 node-level region rule keeps continent-level exemption
        assert "KE" not in V2_POLICY.banned_set({"KE"}, {"NG", "US"})
        assert "ZA" in V2_POLICY.banned_set({"ZA"}, {"NG", "US"})

    def test_cable_distrust_scales_floors(self):
        from transit_policy import policy_floor_matrix_parallel

        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0.0, float(lon)) for lon in range(0, 9)], feature="TG:flaky")
        g = b.build()
        node_cc = np.array(["AA"] * g.n_nodes)
        lat, lon = np.zeros(2), np.array([-0.3, 8.3])
        loc_cc = np.array(["AA", "AA"])
        # tight last-mile cap so the graph is the only route (no beelines)
        kw = dict(n_workers=2, lastmile_km_max=100.0, direct_km_max=0.0)
        open_ = policy_floor_matrix_parallel(
            g, node_cc, lat, lon, loc_cc, OPEN_POLICY, **kw
        )
        distrust = TransitPolicy("d", (), cable_factors=(("TG:flaky", 2.0),))
        scaled = policy_floor_matrix_parallel(
            g, node_cc, lat, lon, loc_cc, distrust, **kw
        )
        # graph legs doubled; last-mile legs unchanged, so strictly between
        # 1x and 2x the open floor, and well above it
        assert scaled[1, 0] > 1.5 * open_[1, 0]
        assert scaled[1, 0] < 2.0 * open_[1, 0]

    def test_open_policy_restricts_nothing(self):
        assert OPEN_POLICY.banned_set(["CN", "MU", "ZA"], frozenset()) == frozenset()

    def test_describe_names_rules(self):
        s = DEFAULT_POLICY.describe()
        assert "small-country" in s and "soviet-bloc" in s and "no-africa-transit" in s


class TestAllowedNodeMask:
    def test_endpoint_exemption(self):
        policy = TransitPolicy("t", (no_transit("XX"),))
        node_cc = np.array(["AA", "XX", "BB"])
        np.testing.assert_array_equal(
            allowed_node_mask(policy, node_cc, {"AA", "BB"}), [True, False, True]
        )
        np.testing.assert_array_equal(
            allowed_node_mask(policy, node_cc, {"AA", "XX"}), [True, True, True]
        )


def masked_brute_floor(graph, node_ok, vp, t, direct_km_max, lastmile_km_max):
    """Reference: floor on the node-masked graph, evaluated the slow way."""
    km_v = geo.haversine_km(vp[0], vp[1], graph.node_lat, graph.node_lon)
    km_t = geo.haversine_km(t[0], t[1], graph.node_lat, graph.node_lon)
    lm_v = np.where((km_v <= lastmile_km_max) & node_ok, geo.rtt_ms(km_v), np.inf)
    lm_t = np.where((km_t <= lastmile_km_max) & node_ok, geo.rtt_ms(km_t), np.inf)
    m = graph.csr.toarray()
    m[~node_ok, :] = 0
    m[:, ~node_ok] = 0
    d = dijkstra(m, directed=True)
    via = float(np.min(lm_v[:, None] + d + lm_t[None, :]))
    dd = float(geo.haversine_km(*vp, *t))
    return min(via, geo.rtt_ms(dd) if dd <= direct_km_max else np.inf)


class TestPolicyFloorMatrix:
    def _chain_through_xx(self):
        """Equator chain lon 0..8; middle nodes belong to country XX."""
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0.0, float(lon)) for lon in range(0, 9)])
        g = b.build()
        node_cc = np.array(["AA", "AA", "AA", "XX", "XX", "XX", "BB", "BB", "BB"])
        return g, node_cc

    def test_transit_blocked_unless_endpoint(self):
        g, node_cc = self._chain_through_xx()
        policy = TransitPolicy("t", (no_transit("XX"),))
        lat = np.zeros(3)
        lon = np.array([0.5, 4.0, 7.5])  # in AA, XX, BB territory
        loc_cc = np.array(["AA", "XX", "BB"])
        out = policy_floor_matrix(
            g, node_cc, lat, lon, loc_cc, policy, direct_km_max=0.0, lastmile_km_max=100.0
        )
        assert np.isinf(out[2, 0])  # AA -> BB must cross XX: blocked
        assert np.isfinite(out[1, 0])  # AA -> XX endpoint exemption
        assert np.isfinite(out[2, 1])  # XX -> BB endpoint exemption
        # symmetric
        assert np.isinf(out[0, 2])

    def test_open_policy_matches_floor_estimator(self):
        g, node_cc = self._chain_through_xx()
        lat = np.zeros(3)
        lon = np.array([0.5, 4.0, 7.5])
        loc_cc = np.array(["AA", "XX", "BB"])
        out = policy_floor_matrix(g, node_cc, lat, lon, loc_cc, OPEN_POLICY)
        est = FloorEstimator(g, lat, lon)
        np.testing.assert_allclose(out, est.floor_many_ms(lat, lon), rtol=1e-9)

    def test_policy_never_lowers_floors(self):
        g, node_cc = self._chain_through_xx()
        lat, lon = np.zeros(3), np.array([0.5, 4.0, 7.5])
        loc_cc = np.array(["AA", "XX", "BB"])
        policy = TransitPolicy("t", (no_transit("XX"),))
        restricted = policy_floor_matrix(g, node_cc, lat, lon, loc_cc, policy)
        open_ = policy_floor_matrix(g, node_cc, lat, lon, loc_cc, OPEN_POLICY)
        assert np.all(restricted >= open_ - 1e-9)

    def test_policy_paths_floors_and_attribution(self):
        from transit_policy import OPEN_POLICY, policy_paths_parallel

        g, node_cc = self._chain_through_xx()
        lat, lon = np.zeros(3), np.array([0.5, 4.0, 7.5])
        loc_cc = np.array(["AA", "XX", "BB"])
        pairs = [(0, 2), (0, 1), (1, 2)]
        # open policy: floors match FloorEstimator; AA->BB transits XX
        floors, transit, edges, cells = policy_paths_parallel(
            g, node_cc, lat, lon, loc_cc, pairs, OPEN_POLICY, n_workers=2, chunk=1
        )
        est = FloorEstimator(g, lat, lon)
        for k, (v, t) in enumerate(pairs):
            assert floors[k] == pytest.approx(est.floor_ms(lat[t], lon[t])[v], rel=1e-9)
        assert "XX" in transit[0] and len(edges[0]) > 0
        # restricted policy: XX is unroutable for AA->BB, and by construction
        # can never appear as a transit attribution
        policy = TransitPolicy("t", (no_transit("XX"),))
        floors_p, transit_p, *_ = policy_paths_parallel(
            g, node_cc, lat, lon, loc_cc, pairs, policy, n_workers=2, chunk=1,
            direct_km_max=0.0,
        )
        assert np.isinf(floors_p[0])  # AA->BB blocked
        assert np.isfinite(floors_p[1]) and np.isfinite(floors_p[2])  # endpoint exemptions
        assert all("XX" not in tr for tr in transit_p)

    def test_terrestrial_distrust_targets_internal_links_only(self):
        from transit_policy import OPEN_POLICY, policy_floor_matrix_parallel

        # two chains: an ITU link inside 'IQ' and a TG cable inside 'IQ'
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0.0, float(lon)) for lon in range(0, 9)], feature="ITU")
        b.add_path([(5.0, float(lon)) for lon in range(0, 9)], feature="TG:gulf")
        g = b.build()
        node_cc = np.array(["IQ"] * g.n_nodes)
        lat = np.array([0.0, 0.0, 5.0, 5.0])
        lon = np.array([-0.3, 8.3, -0.3, 8.3])
        loc_cc = np.array(["AA", "BB", "AA", "BB"])
        kw = dict(n_workers=2, lastmile_km_max=100.0, direct_km_max=0.0)
        policy = TransitPolicy("t", (), terrestrial_factors=(("IQ", 2.0),))
        open_ = policy_floor_matrix_parallel(g, node_cc, lat, lon, loc_cc, OPEN_POLICY, **kw)
        scaled = policy_floor_matrix_parallel(g, node_cc, lat, lon, loc_cc, policy, **kw)
        assert scaled[1, 0] > 1.5 * open_[1, 0]  # ITU-internal link doubled
        assert scaled[3, 2] == pytest.approx(open_[3, 2], rel=1e-9)  # cable untouched

    def test_terrestrial_only_ban_keeps_submarine_route(self):
        from transit_policy import policy_floor_matrix_parallel

        # two routes AA -> BB: a short ITU overland chain through XX and a
        # longer TG submarine detour whose mid vertex ALSO geocodes to XX
        # (ocean vertices attribute to the nearest coastal state) — the
        # Africa v3.3 scenario
        b = GraphBuilder(snap_tolerance_km=1.0)
        b.add_path([(0.0, float(lon)) for lon in range(0, 9)], feature="ITU")
        b.add_path([(0.0, 0.0), (6.0, 2.0), (6.0, 6.0), (0.0, 8.0)], feature="TG:around")
        g = b.build()
        node_cc = np.where((g.node_lon > 2.5) & (g.node_lon < 6.5), "XX", "AA")
        lat, lon = np.zeros(2), np.array([-0.3, 8.3])
        loc_cc = np.array(["AA", "BB"])
        kw = dict(lastmile_km_max=100.0, direct_km_max=0.0)

        from transit_policy import CountryRule

        open_ = policy_floor_matrix(g, node_cc, lat, lon, loc_cc, OPEN_POLICY, **kw)
        terr_pol = TransitPolicy(
            "t-terr",
            (CountryRule("t-terr-xx", lambda cc: cc == "XX", terrestrial_only=True),),
        )
        terr = policy_floor_matrix(g, node_cc, lat, lon, loc_cc, terr_pol, **kw)
        node_pol = TransitPolicy("t-node", (no_transit("XX"),))
        node = policy_floor_matrix(g, node_cc, lat, lon, loc_cc, node_pol, **kw)

        # node ban severs both routes (submarine mid vertex is XX): inf.
        assert np.isinf(node[1, 0])
        # terrestrial ban: overland blocked, submarine detour survives —
        # finite, exactly the floor of the graph with only the TG path
        b2 = GraphBuilder(snap_tolerance_km=1.0)
        b2.add_path([(0.0, 0.0), (6.0, 2.0), (6.0, 6.0), (0.0, 8.0)], feature="TG:around")
        est2 = FloorEstimator(b2.build(), lat, lon, **kw)
        want = est2.floor_ms(lat[1], lon[1])[0]
        assert np.isfinite(terr[1, 0])
        assert terr[1, 0] == pytest.approx(want, rel=1e-9)
        assert terr[1, 0] > open_[1, 0]  # detour is longer than overland
        # parallel path agrees with serial under terrestrial bans
        par = policy_floor_matrix_parallel(
            g, node_cc, lat, lon, loc_cc, terr_pol, n_workers=2, **kw
        )
        np.testing.assert_allclose(par, terr, rtol=1e-12)

    def test_parallel_matches_serial(self):
        from transit_policy import policy_floor_matrix_parallel

        g, node_cc = self._chain_through_xx()
        lat, lon = np.zeros(3), np.array([0.5, 4.0, 7.5])
        loc_cc = np.array(["AA", "XX", "BB"])
        policy = TransitPolicy("t", (no_transit("XX"),))
        serial = policy_floor_matrix(g, node_cc, lat, lon, loc_cc, policy)
        parallel = policy_floor_matrix_parallel(
            g, node_cc, lat, lon, loc_cc, policy, n_workers=2, chunk=2
        )
        np.testing.assert_allclose(parallel, serial, rtol=1e-12)

    @pytest.mark.parametrize("seed", [0, 1, 2])
    def test_matches_masked_brute_force_on_random_graphs(self, seed):
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
        policy = TransitPolicy("t", (no_transit("XX", "YY"),))

        loc_lat = rng.uniform(-25, 25, 5)
        loc_lon = rng.uniform(-25, 25, 5)
        loc_cc = rng.choice(["AA", "XX", "CC"], size=5)
        out = policy_floor_matrix(g, node_cc, loc_lat, loc_lon, loc_cc, policy)
        for v in range(5):
            for t in range(5):
                ok = allowed_node_mask(policy, node_cc, {loc_cc[v], loc_cc[t]})
                want = masked_brute_floor(
                    g, ok, (loc_lat[v], loc_lon[v]), (loc_lat[t], loc_lon[t]), 300.0, 300.0
                )
                assert out[t, v] == pytest.approx(want, rel=1e-9), (v, t)
