"""Unit tests for the mesh-campaign machinery (no network, no API key)."""

import numpy as np
import pytest

from mesh_campaign.anycast import is_anycast
from mesh_campaign.results import min_rtt_of, _haversine_km
from mesh_campaign.scheduler import DiversityScheduler
from mesh_campaign.state import CampaignState
from mesh_campaign.surrogates import candidate_surrogate, hops_from_traceroute


def synth_probes(n_cc=4, asns_per_cc=3, probes_per_group=2):
    probes, pid = [], 0
    for c in range(n_cc):
        for a in range(asns_per_cc):
            for _ in range(probes_per_group):
                probes.append(
                    {
                        "id": pid,
                        "ip": f"10.{c}.{a}.{pid % 250}",
                        "asn": 1000 * c + a,  # ASNs unique per (cc, a)
                        "cc": f"C{c}",
                        "lat": 10.0 * c,
                        "lon": 5.0 * a,
                    }
                )
                pid += 1
    return probes


class TestScheduler:
    def test_country_tier_first_and_complete(self):
        probes = synth_probes()
        sched = DiversityScheduler(probes, seed=1)
        plan = sched.plan(1000)
        n_cc_pairs = 4 * 3 // 2  # unordered, distinct countries
        t1 = [p for p in plan if p[2] == "T1-country"]
        assert len(t1) == n_cc_pairs
        # T1 comes first in the plan
        assert all(p[2] == "T1-country" for p in plan[:n_cc_pairs])
        covered, _ = sched.coverage()["country_pairs"]
        assert covered >= n_cc_pairs  # + same-country pairs from later tiers

    def test_no_duplicate_pairs_ever(self):
        sched = DiversityScheduler(synth_probes(), seed=2)
        plan = sched.plan(10_000)
        keys = {(min(s, d), max(s, d)) for s, d, _ in plan}
        assert len(keys) == len(plan)
        # 24 probes -> at most C(24,2) pairs
        assert len(plan) <= 24 * 23 // 2

    def test_respects_already_attempted(self):
        probes = synth_probes()
        sched0 = DiversityScheduler(probes, seed=3)
        done = {(min(s, d), max(s, d)) for s, d, _ in sched0.plan(50)}
        sched = DiversityScheduler(probes, attempted_pairs=done, seed=4)
        plan = sched.plan(10_000)
        assert all((min(s, d), max(s, d)) not in done for s, d, _ in plan)

    def test_benched_probes_excluded(self):
        probes = synth_probes()
        benched = {0, 1}
        sched = DiversityScheduler(probes, benched=benched, seed=5)
        plan = sched.plan(10_000)
        assert all(s not in benched and d not in benched for s, d, _ in plan)

    def test_budget_respected(self):
        sched = DiversityScheduler(synth_probes(), seed=6)
        assert len(sched.plan(7)) == 7

    def test_batches_share_dst_and_cap_sources(self):
        probes = synth_probes(n_cc=6, asns_per_cc=4, probes_per_group=3)
        sched = DiversityScheduler(probes, seed=7)
        batches = sched.plan_batches(500, max_src=10)
        assert all(b.n_pairs <= 10 for b in batches)
        assert all(b.dst_prb not in b.src_prbs for b in batches)
        total = sum(b.n_pairs for b in batches)
        assert total == 500
        # batching must not fabricate or drop pairs vs the flat plan
        assert all(b.credits == 3 * b.n_pairs for b in batches)

    def test_failed_attempts_block_repeats_but_not_coverage(self):
        probes = synth_probes()
        pa, pb = probes[0], probes[-1]  # different countries
        failed = {(min(pa["id"], pb["id"]), max(pa["id"], pb["id"]))}
        sched = DiversityScheduler(
            probes, attempted_pairs=failed, successful_pairs=set(), seed=9
        )
        plan = sched.plan(10_000)
        # the exact probe pair is never retried...
        assert all({s, d} != {pa["id"], pb["id"]} for s, d, _ in plan)
        # ...but the country pair is still scheduled via other probes
        cc_key = tuple(sorted((pa["cc"], pb["cc"])))
        planned_ccs = {
            tuple(sorted((sched.by_id[s]["cc"], sched.by_id[d]["cc"])))
            for s, d, _ in plan
        }
        assert cc_key in planned_ccs

    def test_probation_caps_unverified_destinations(self):
        # many countries so a T1 dst would normally collect ~n_cc-1 sources
        probes = synth_probes(n_cc=30, asns_per_cc=1, probes_per_group=1)
        sched = DiversityScheduler(probes, seed=8)
        t1 = [p for p in sched.plan(10_000) if p[2] == "T1-country"]
        from collections import Counter

        per_dst = Counter(d for _, d, _ in t1)
        assert max(per_dst.values()) <= 5  # nobody is verified yet
        # verified destinations get full batches again
        sched2 = DiversityScheduler(
            probes, verified_dsts={p["id"] for p in probes}, seed=8
        )
        t1v = [p for p in sched2.plan(10_000) if p[2] == "T1-country"]
        per_dst_v = Counter(d for _, d, _ in t1v)
        assert max(per_dst_v.values()) > 5

    def test_deterministic_under_seed(self):
        probes = synth_probes()
        p1 = DiversityScheduler(probes, seed=42).plan(100)
        p2 = DiversityScheduler(probes, seed=42).plan(100)
        assert p1 == p2

    def test_cc_asn_tier_drains_before_asn_tier(self):
        # cc-asn pair coverage is the primary objective: no T3-asn pair may
        # appear before the last T2-cc-asn pair
        probes = synth_probes(n_cc=6, asns_per_cc=4, probes_per_group=2)
        plan = DiversityScheduler(probes, seed=10).plan(10_000)
        tiers = [t for *_, t in plan]
        if "T3-asn" in tiers and "T2-cc-asn" in tiers:
            assert tiers.index("T3-asn") > len(tiers) - 1 - tiers[::-1].index("T2-cc-asn")
        # every cc-asn pair is an asn pair too: asn coverage rides along
        cov = DiversityScheduler(probes, seed=10).coverage()
        assert cov["asn_pairs"][1] <= cov["cc_asn_pairs"][1]

    def test_endpoint_rescue_schedules_orphans_as_sources(self):
        probes = synth_probes(n_cc=4, asns_per_cc=3, probes_per_group=2)
        # successful pairs touching every group EXCEPT (C0, asn 0) = probes 0/1
        ok = {(2, 4), (6, 8), (10, 12), (14, 16), (18, 20), (22, 3),
              (5, 7), (9, 11), (13, 15), (17, 19), (21, 23)}
        verified = {d for pair in ok for d in pair}
        sched = DiversityScheduler(
            probes,
            attempted_pairs=ok,
            successful_pairs=ok,
            verified_dsts=verified,
            seed=11,
        )
        plan = sched.plan(10_000)
        rescue = [p for p in plan if p[2] == "T1b-endpoint"]
        assert rescue, "orphan group must get a rescue pair"
        # the orphan probe is the SOURCE, aimed at a verified destination
        assert all(s in (0, 1) and d in verified for s, d, _ in rescue)

    def test_no_dst_probes_never_targeted(self):
        probes = synth_probes(n_cc=6, asns_per_cc=4, probes_per_group=2)
        no_dst = {p["id"] for p in probes[::3]}
        sched = DiversityScheduler(probes, no_dst=no_dst, seed=12)
        plan = sched.plan(10_000)
        assert all(d not in no_dst for _, d, _ in plan)
        # ...but they still appear as sources
        assert any(s in no_dst for s, _, _ in plan)


class TestSurrogates:
    def _traceroute(self, hops):
        return {
            "result": [
                {"hop": i + 1, "result": [{"from": ip, "rtt": rtt}]}
                for i, (ip, rtt) in enumerate(hops)
            ]
        }

    def test_hops_parsed_in_order(self):
        res = self._traceroute([("192.168.1.1", 0.5), ("10.9.9.9", 1.0), ("8.8.8.8", 1.5)])
        assert list(hops_from_traceroute(res)) == [
            ("192.168.1.1", 0.5), ("10.9.9.9", 1.0), ("8.8.8.8", 1.5)]

    def test_candidate_skips_private_and_respects_rtt_cap(self):
        class Resolver:
            def asns(self, ip):
                return [64500] if ip == "193.0.11.7" else []

        # NB: real-global addresses on purpose — RFC5737 documentation
        # ranges (203.0.113.x etc.) are correctly rejected by the
        # is_global screen, which an earlier version of this test learned
        # the hard way
        probe = {"id": 1, "ip": "193.0.10.9", "asn": 64500}
        hops = [
            ("192.168.1.1", 0.4),   # private: skipped
            ("193.0.10.1", 0.9),    # same /24 as probe: fast path, no ASN lookup
            ("193.0.11.7", 1.5),
        ]
        import mesh_campaign.surrogates as sur

        calls = {}

        def fake_get(path, **kw):
            calls["path"] = path
            return [self._traceroute(hops)]

        orig = sur.atlas_api.get
        sur.atlas_api.get = fake_get
        try:
            got = candidate_surrogate(probe, Resolver(), max_rtt_ms=2.0)
            assert got == {"ip": "193.0.10.1", "hop_rtt_ms": 0.9}
            # over-cap first in-net hop => None (hops only get farther)
            far = [("192.168.1.1", 0.4), ("193.0.10.1", 5.0)]
            sur.atlas_api.get = lambda path, **kw: [self._traceroute(far)]
            assert candidate_surrogate(probe, Resolver(), max_rtt_ms=2.0) is None
        finally:
            sur.atlas_api.get = orig

    def test_anycast_lookup(self):
        import ipaddress

        nets = {int(ipaddress.ip_address("1.1.1.0")) >> 8}
        assert is_anycast("1.1.1.53", nets)
        assert not is_anycast("1.1.2.53", nets)
        assert not is_anycast("not-an-ip", nets)

    def test_surrogate_state_roundtrip(self, tmp_path):
        st = CampaignState(tmp_path / "s.sqlite")
        st.record_surrogate(504, "196.203.250.1", 1.7, 88.0)
        assert st.surrogates() == {504: "196.203.250.1"}

    def test_surrogate_forgives_prior_failures_only(self, tmp_path):
        st = CampaignState(tmp_path / "s.sqlite")
        # pair failed against the dst's dead listed address...
        st.record_scheduled(111, dst_prb=9, dst_ip="10.0.0.9", src_prbs=[1, 2])
        st.record_result(1, 9, None)
        st.record_result(2, 9, 50.0)
        assert (1, 9) in st.attempted_pairs()
        # ...then a surrogate lands: the FAILED pair is retryable, the ok
        # pair and failures against other dsts stay
        st.record_surrogate(9, "196.0.0.1", 1.0, 90.0)
        assert (1, 9) not in st.attempted_pairs()
        assert (2, 9) in st.attempted_pairs()
        # re-scheduling the forgiven pair rebinds its row to the new msm
        st.record_scheduled(222, dst_prb=9, dst_ip="196.0.0.1", src_prbs=[1])
        assert (1, 9) in st.attempted_pairs()  # pending again = blocked
        assert st.db.execute(
            "SELECT msm_id, status FROM pairs WHERE src=1 AND dst=9"
        ).fetchone() == (222, "pending")
        # a NEW failure against the surrogate itself stays blocked
        st.record_result(1, 9, None)
        assert (1, 9) in st.attempted_pairs()


class TestState:
    def test_roundtrip(self, tmp_path):
        st = CampaignState(tmp_path / "s.sqlite")
        st.record_scheduled(111, dst_prb=5, dst_ip="10.0.0.5", src_prbs=[1, 2, 3])
        assert st.attempted_pairs() == {(1, 5), (2, 5), (3, 5)}
        assert st.open_measurements() == [(111, 5, "10.0.0.5", "coverage")]
        st.record_result(1, 5, 12.5)
        st.record_result(2, 5, None)
        st.close_measurement(111)  # pair (3,5) still pending -> failed
        assert st.open_measurements() == []
        assert st.results("ok") == [(1, 5, 12.5)]
        assert {tuple(r[:2]) for r in st.results("failed")} == {(2, 5), (3, 5)}

    def test_benching_rules(self, tmp_path):
        st = CampaignState(tmp_path / "s.sqlite")
        for _ in range(3):
            st.strike_src(7)
        st.strike_src(8)
        st.credit_ok(9)
        for _ in range(5):
            st.sol_violation(9)
        assert st.benched_probes() == {7, 9}
        # a probe with successes is forgiven strikes
        for _ in range(3):
            st.strike_src(10)
        st.credit_ok(10)
        assert 10 not in st.benched_probes()


class TestRemeasure:
    def test_history_keeps_last_n(self, tmp_path):
        from mesh_campaign.state import HISTORY_KEEP

        st = CampaignState(tmp_path / "s.sqlite")
        st.record_scheduled(111, dst_prb=5, dst_ip="10.0.0.5", src_prbs=[1])
        for i in range(HISTORY_KEEP + 2):
            st.record_result(1, 5, 10.0 + i, rtt2=10.1 + i, msm_id=111)
        n = st.db.execute(
            "SELECT COUNT(*) FROM pair_history WHERE src=1 AND dst=5"
        ).fetchone()[0]
        assert n == HISTORY_KEEP

    def test_candidates_trigger_rules(self, tmp_path):
        st = CampaignState(tmp_path / "s.sqlite")
        st.record_scheduled(111, dst_prb=5, dst_ip="10.0.0.5", src_prbs=[1, 2, 3, 4])
        st.record_result(1, 5, 10.0, rtt2=11.0, msm_id=111)  # corroborated
        st.record_result(2, 5, 10.0, rtt2=17.0, msm_id=111)  # gappy
        st.record_result(3, 5, 10.0, rtt2=None, msm_id=111)  # single packet
        st.record_result(4, 5, None)  # failed: never a candidate
        cands = st.remeasure_candidates(max_per_week=99)
        assert [(s, d) for s, d, _ in cands] == [(3, 5), (2, 5)]  # worst first

    def test_candidates_weekly_cap(self, tmp_path):
        from mesh_campaign.state import REMEASURE_MAX_PER_WEEK

        st = CampaignState(tmp_path / "s.sqlite")
        st.record_scheduled(111, dst_prb=5, dst_ip="10.0.0.5", src_prbs=[1])
        for _ in range(REMEASURE_MAX_PER_WEEK):
            st.record_result(1, 5, 10.0, rtt2=17.0, msm_id=111)
        assert st.remeasure_candidates() == []  # chronically noisy: accepted as-is

    def test_failed_remeasure_never_demotes_ok_pair(self, tmp_path):
        st = CampaignState(tmp_path / "s.sqlite")
        st.record_scheduled(111, dst_prb=5, dst_ip="10.0.0.5", src_prbs=[1])
        st.record_result(1, 5, 10.0, rtt2=17.0, msm_id=111)
        st.record_result(1, 5, None, protect_ok=True)
        assert st.results("ok") == [(1, 5, 10.0)]

    def test_min2_rtt_of(self):
        from mesh_campaign.results import min2_rtt_of

        res = {"result": [{"rtt": 9.0}, {"rtt": 5.0}, {"rtt": 5.2}]}
        assert min2_rtt_of(res) == (5.0, 5.2)
        assert min2_rtt_of({"result": [{"rtt": 5.0}]}) == (5.0, None)
        assert min2_rtt_of({"result": [{"x": 1}]}) == (None, None)

    def test_export_uses_windowed_min(self, tmp_path):
        from mesh_campaign.export import campaign_target_data

        probes = [dict(p, id=pid) for p, pid in zip(synth_probes(), [100, 101, 102, 103])]
        st = CampaignState(tmp_path / "s.sqlite")
        st.record_scheduled(1, dst_prb=101, dst_ip="10.0.0.101", src_prbs=[100])
        st.record_result(100, 101, 20.0, rtt2=20.1, msm_id=1)
        # re-measure lands higher: pairs.min_rtt is the latest, but the
        # exported value is the min over the history window
        st.record_result(100, 101, 25.0, rtt2=25.1, msm_id=2)
        ip = {p["id"]: p["ip"] for p in probes}
        td = campaign_target_data(state=st, probes=probes)
        assert td["loc_loc_meas"] == {ip[100]: {ip[101]: 20.0}}


class TestExportAndMerge:
    def _state_with_pairs(self, tmp_path):
        st = CampaignState(tmp_path / "s.sqlite")
        st.record_scheduled(1, dst_prb=101, dst_ip="10.0.0.101", src_prbs=[100, 102])
        st.record_result(100, 101, 20.0)
        st.record_result(102, 101, 30.0)
        # seeded pair (msm_id=0): must NOT be exported
        st.db.execute("INSERT OR IGNORE INTO pairs VALUES (100,102,'ok',5.0,0,0)")
        st.db.commit()
        return st

    def test_campaign_target_data_shape_and_filters(self, tmp_path):
        from mesh_campaign.export import campaign_target_data

        probes = synth_probes()
        # remap ids to match: use probes 0..; craft custom by patching ids
        probes = [dict(p, id=pid) for p, pid in zip(probes, [100, 101, 102, 103])]
        st = self._state_with_pairs(tmp_path)
        td = campaign_target_data(state=st, probes=probes)
        ip = {p["id"]: p["ip"] for p in probes}
        assert td["loc_loc_meas"] == {
            ip[100]: {ip[101]: 20.0},
            ip[102]: {ip[101]: 30.0},
        }
        assert set(td["address_to_loc"]) == {ip[100], ip[101], ip[102]}

    def test_sol_suspects_excluded(self, tmp_path):
        from mesh_campaign.export import campaign_target_data

        probes = [dict(p, id=pid) for p, pid in zip(synth_probes(), [100, 101, 102, 103])]
        st = self._state_with_pairs(tmp_path)
        for _ in range(5):
            st.sol_violation(102)
        td = campaign_target_data(state=st, probes=probes)
        ip = {p["id"]: p["ip"] for p in probes}
        assert ip[102] not in td["loc_loc_meas"]
        assert td["loc_loc_meas"] == {ip[100]: {ip[101]: 20.0}}

    def test_merge_min_wins_and_is_oblivious(self):
        from mesh_data import merge_target_data

        base = {
            "address_to_loc": {"a": (0.0, 0.0), "b": (1.0, 1.0)},
            "loc_loc_meas": {"a": {"b": 50.0}},
        }
        extra = {
            "address_to_loc": {"b": (1.0, 1.0), "c": (2.0, 2.0)},
            "loc_loc_meas": {"a": {"b": 40.0, "c": 90.0}, "c": {"a": 80.0}},
        }
        m = merge_target_data(base, extra)
        assert m["loc_loc_meas"]["a"]["b"] == 40.0  # min wins
        assert m["loc_loc_meas"]["a"]["c"] == 90.0
        assert m["loc_loc_meas"]["c"] == {"a": 80.0}
        assert set(m["address_to_loc"]) == {"a", "b", "c"}
        assert base["loc_loc_meas"]["a"]["b"] == 50.0  # inputs untouched


class TestResultParsing:
    def test_min_rtt_of(self):
        assert min_rtt_of({"result": [{"rtt": 30.2}, {"rtt": 28.9}, {"x": 1}], "min": 29.5}) == 28.9
        assert min_rtt_of({"result": [], "min": -1}) is None
        assert min_rtt_of({"min": 15.0}) == 15.0

    def test_haversine_sanity(self):
        assert _haversine_km(0, 0, 0, 1) == pytest.approx(111.19, rel=1e-3)
