"""Unit tests for the mesh-campaign machinery (no network, no API key)."""

import numpy as np
import pytest

from mesh_campaign.results import min_rtt_of, _haversine_km
from mesh_campaign.scheduler import DiversityScheduler
from mesh_campaign.state import CampaignState


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


class TestState:
    def test_roundtrip(self, tmp_path):
        st = CampaignState(tmp_path / "s.sqlite")
        st.record_scheduled(111, dst_prb=5, dst_ip="10.0.0.5", src_prbs=[1, 2, 3])
        assert st.attempted_pairs() == {(1, 5), (2, 5), (3, 5)}
        assert st.open_measurements() == [(111, 5, "10.0.0.5")]
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
