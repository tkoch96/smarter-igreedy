"""Diversity-first pair scheduler.

Full 10k x 10k mesh = ~10^8 pairs = years at the credit limit, so order of
measurement is everything. Coverage tiers, most valuable first (caveat 2):

  T1   new unordered country<->country pair
  T1b  endpoint rescue: a (country, ASN) group that has never appeared in
       a successful pair gets its probe scheduled as a SOURCE toward a
       verified destination (pilot: source-side success was 100% even for
       probes whose own listed address never answers)
  T2   new unordered (country, ASN)<->(country, ASN) pair — the primary
       coverage objective; every such pair is also an ASN pair, so ASN
       coverage grows incidentally
  T3   new unordered ASN<->ASN pair (mops up multi-country-ASN combos)
  T4   any new probe<->probe pair

The scheduler greedily drains tiers in order under a per-day pair budget.
Within T1 it also batches for execution efficiency: RIPE bills per result
but caps concurrent measurements, so pairs sharing a destination probe are
packed into one measurement of up to 100 sources — a single well-chosen
destination covers ~100 country pairs at once.

Probe choice within a group cycles through members (spreads load, and a
dead pick is replaced next round via health strikes rather than starving
the group).
"""

import itertools
import random
from collections import defaultdict
from dataclasses import dataclass, field

from .atlas_api import MAX_PROBES_PER_MEAS, PING_PACKETS


@dataclass
class Batch:
    dst_prb: int
    dst_ip: str
    src_prbs: list
    tier: str

    @property
    def n_pairs(self):
        return len(self.src_prbs)

    @property
    def credits(self):
        return self.n_pairs * PING_PACKETS


def _cycles(groups, rng):
    out = {}
    for key, members in groups.items():
        members = members[:]
        rng.shuffle(members)
        out[key] = itertools.cycle(members)
    return out


PROBATION_MAX_SRC = 5  # sources risked on a destination address that has
# never answered a mesh ping (pilot finding: ~1/3 of listed addresses are
# unpingable; a dead target should burn 5 pairs, not 100)


class DiversityScheduler:
    def __init__(
        self,
        probes,
        attempted_pairs=frozenset(),
        successful_pairs=None,
        benched=frozenset(),
        verified_dsts=frozenset(),
        seed=31415,
    ):
        """attempted_pairs are never re-measured; but only successful_pairs
        (default: same as attempted, for simulations) count as COVERAGE —
        a country-pair whose one attempt failed must be retried with other
        probes, not marked done."""
        self.verified_dsts = set(verified_dsts)
        self.rng = random.Random(seed)
        self.probes = [p for p in probes if p["id"] not in benched]
        self.by_id = {p["id"]: p for p in self.probes}
        by_cc, by_asn, by_ccasn = defaultdict(list), defaultdict(list), defaultdict(list)
        for p in self.probes:
            by_cc[p["cc"]].append(p["id"])
            by_asn[p["asn"]].append(p["id"])
            by_ccasn[(p["cc"], p["asn"])].append(p["id"])
        self.ccs = sorted(by_cc)
        self.asns = sorted(by_asn)
        self.ccasns = sorted(by_ccasn)
        self.cyc_cc = _cycles(by_cc, self.rng)
        self.cyc_asn = _cycles(by_asn, self.rng)
        self.cyc_ccasn = _cycles(by_ccasn, self.rng)

        self.grp_cc, self.grp_asn, self.grp_ccasn = dict(by_cc), dict(by_asn), dict(by_ccasn)

        self.done_pairs = set(attempted_pairs)
        covered = self.done_pairs if successful_pairs is None else set(successful_pairs)
        self.cov_cc, self.cov_asn, self.cov_ccasn = set(), set(), set()
        for a, b in covered:
            if a in self.by_id and b in self.by_id:
                self._mark_covered(a, b)

    # -- coverage bookkeeping ------------------------------------------------
    @staticmethod
    def _key(x, y):
        return (x, y) if x <= y else (y, x)

    def _mark_covered(self, prb_a, prb_b):
        pa, pb = self.by_id[prb_a], self.by_id[prb_b]
        self.cov_cc.add(self._key(pa["cc"], pb["cc"]))
        self.cov_asn.add(self._key(pa["asn"], pb["asn"]))
        self.cov_ccasn.add(self._key((pa["cc"], pa["asn"]), (pb["cc"], pb["asn"])))

    def _take_pair(self, prb_a, prb_b):
        """Register the pair if new; True on success."""
        if prb_a == prb_b:
            return False
        key = self._key(prb_a, prb_b)
        if key in self.done_pairs:
            return False
        self.done_pairs.add(key)
        self._mark_covered(prb_a, prb_b)
        return True

    # -- dst choice --------------------------------------------------------------
    def _pick_dst(self, group_key, cycles, groups):
        """Prefer a verified (known-pingable) destination within the group:
        verified dsts take full 100-source batches, so they both avoid
        wasted probation pairs and keep the measurement count down."""
        members = groups[group_key]
        verified = [m for m in members if m in self.verified_dsts]
        if verified:
            return self.rng.choice(verified)
        return next(cycles[group_key])

    def _dst_cap(self, dst):
        return MAX_PROBES_PER_MEAS if dst in self.verified_dsts else PROBATION_MAX_SRC

    # -- tiers ----------------------------------------------------------------
    def _tier_country(self, budget):
        """One probe pair per uncovered country pair, batched dst-side: the
        dst probe sits in one country, the sources each come from a country
        whose pair with it is uncovered."""
        pairs = []
        for dst_cc in self.ccs:
            wanted = [
                cc
                for cc in self.ccs
                if cc != dst_cc and self._key(cc, dst_cc) not in self.cov_cc
            ]
            if not wanted:
                continue
            dst = self._pick_dst(dst_cc, self.cyc_cc, self.grp_cc)
            # probation: uncovered pairs beyond the cap stay uncovered in
            # the bookkeeping and get rescheduled another day
            wanted = wanted[: self._dst_cap(dst)]
            for src_cc in wanted:
                if len(pairs) >= budget:
                    return pairs
                src = next(self.cyc_cc[src_cc])
                if self._take_pair(src, dst):
                    pairs.append((src, dst, "T1-country"))
        return pairs

    def _tier_endpoint_rescue(self, budget, n_dsts=3):
        """One pair per never-successful (country, ASN) group, with the
        group's probe as the SOURCE and a verified probe as the target.
        Sources sharing one of n_dsts targets keeps this to a few
        measurements while one bad target can't burn every orphan's try."""
        pairs = []
        touched = {g for key in self.cov_ccasn for g in key}
        orphans = [g for g in self.ccasns if g not in touched]
        verified = [d for d in self.verified_dsts if d in self.by_id]
        if not orphans or not verified:
            return pairs
        dsts = self.rng.sample(verified, min(n_dsts, len(verified)))
        for g in orphans:
            if len(pairs) >= budget:
                break
            src = next(self.cyc_ccasn[g])
            for dst in self.rng.sample(dsts, len(dsts)):
                if self._take_pair(src, dst):
                    pairs.append((src, dst, "T1b-endpoint"))
                    break
        return pairs

    def _tier_sampled(self, budget, keys, cycles, groups, covered, tier_name):
        """Sampled group-pair coverage, batched dst-side: walk dst groups in
        random order; for each, sample uncovered partner groups until the
        destination's batch is full. Near-uniform over uncovered group
        pairs while that space is large, but ~batch-size fewer
        measurements than independent pair sampling."""
        pairs = []
        dst_keys = keys[:]
        self.rng.shuffle(dst_keys)
        for dst_key in dst_keys:
            if len(pairs) >= budget:
                break
            dst = self._pick_dst(dst_key, cycles, groups)
            cap = self._dst_cap(dst)
            added, tries = 0, 0
            while added < cap and tries < 6 * cap and len(pairs) < budget:
                tries += 1
                src_key = self.rng.choice(keys)
                if src_key == dst_key or self._key(src_key, dst_key) in covered:
                    continue
                src = next(cycles[src_key])
                if self._take_pair(src, dst):
                    pairs.append((src, dst, tier_name))
                    added += 1
        return pairs

    def _tier_probe(self, budget, max_reject=20):
        pairs = []
        ids = [p["id"] for p in self.probes]
        dst_ids = ids[:]
        self.rng.shuffle(dst_ids)
        for dst in dst_ids:
            if len(pairs) >= budget:
                break
            cap = self._dst_cap(dst)
            added, tries = 0, 0
            while added < cap and tries < 6 * cap and len(pairs) < budget:
                tries += 1
                src = self.rng.choice(ids)
                if self._take_pair(src, dst):
                    pairs.append((src, dst, "T4-probe"))
                    added += 1
        return pairs

    # -- public API -------------------------------------------------------------
    def plan(self, n_pairs):
        """Ordered list of (src_prb, dst_prb, tier), tiers drained in order."""
        plan = self._tier_country(n_pairs)
        plan += self._tier_endpoint_rescue(n_pairs - len(plan))
        plan += self._tier_sampled(
            n_pairs - len(plan),
            self.ccasns,
            self.cyc_ccasn,
            self.grp_ccasn,
            self.cov_ccasn,
            "T2-cc-asn",
        )
        plan += self._tier_sampled(
            n_pairs - len(plan), self.asns, self.cyc_asn, self.grp_asn, self.cov_asn, "T3-asn"
        )
        plan += self._tier_probe(n_pairs - len(plan))
        return plan

    def plan_batches(self, n_pairs, max_src=MAX_PROBES_PER_MEAS):
        """Group a plan into executable measurements (one dst, <=100 srcs)."""
        by_dst = defaultdict(lambda: defaultdict(list))
        for src, dst, tier in self.plan(n_pairs):
            by_dst[dst][tier].append(src)
        batches = []
        for dst, tiers in by_dst.items():
            for tier, srcs in tiers.items():
                for i in range(0, len(srcs), max_src):
                    batches.append(
                        Batch(dst, self.by_id[dst]["ip"], srcs[i : i + max_src], tier)
                    )
        return batches

    def coverage(self):
        n_cc = len(self.ccs)
        n_asn = len(self.asns)
        n_ccasn = len(self.ccasns)
        n_prb = len(self.probes)
        return {
            "country_pairs": (len(self.cov_cc), n_cc * (n_cc + 1) // 2),
            "asn_pairs": (len(self.cov_asn), n_asn * (n_asn + 1) // 2),
            "cc_asn_pairs": (len(self.cov_ccasn), n_ccasn * (n_ccasn + 1) // 2),
            "probe_pairs": (len(self.done_pairs), n_prb * (n_prb - 1) // 2),
        }
