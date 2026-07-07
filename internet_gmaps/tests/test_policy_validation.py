"""Does the v1 geopolitical transit policy (transit_policy.DEFAULT_POLICY)
actually explain the residuals it was built from?

Recomputes the full mesh floor matrix with the policy applied and compares
against the open (unrestricted) model:

  figures/policy_validation.pdf
      left: residual CDFs, open vs policy model (same 1.3 inflation), plus
            the 1.3·geodesic baseline for reference
      right: residual CDFs restricted to pairs whose OPEN-model path
            transited a now-restricted country (the pairs the policy is
            supposed to fix), open vs policy — plus the untouched-pairs
            control, where the two models must coincide

The falsifier to watch: pairs where measured < RAW policy floor. The open
raw floor was ~0.1% violated; if the policy pushes raw floors above real
measurements, the restriction is wrong for those pairs (real traffic DOES
transit there) — that rate is printed per formerly-transited country.

Skips like the other mesh tests. Runtime ~2-4 min (one Dijkstra per
(VP, target-country-class); see transit_policy.policy_floor_matrix).
"""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pytest

import hashlib

import numpy as _np

from build_graph import build_from_snapshot
from floor_query import FloorEstimator
from test_mesh_validation import (
    FIG_DIR,
    GRAPH_NPZS,
    MESH_PKL,
    FIBER_SLOPE,
    MeshEval,
    ecdf,
)
from test_transit_analysis import TransitAnalysis, _rg
from transit_policy import (
    DEFAULT_POLICY,
    OPEN_POLICY,
    V1_POLICY,
    V2_POLICY,
    policy_floor_matrix_parallel,
)

pytestmark = pytest.mark.skipif(
    not (MESH_PKL.exists() and GRAPH_NPZS and _rg is not None),
    reason="needs the mesh cache, a built graph npz, and reverse_geocoder",
)


class PolicyEval:
    """Floors for every model stage, on the same sampled pairs:
    planned-cables-included open graph -> RFS-only open graph -> v1 policy
    (falsified rules) -> v2 policy (current DEFAULT_POLICY)."""

    MAX_LOCS = None  # full scale: floors are parallel across VPs + disk-cached

    def _cached_matrix(self, name, fn):
        """Floor matrices only change with the graph/mesh/policy — cache to
        disk keyed by their fingerprints so reruns are near-instant."""
        key = f"{name}|{len(self.ev.locs)}|{len(self.ev.meas)}|{self.ev.graph.n_edges}"
        h = hashlib.md5(key.encode()).hexdigest()[:12]
        path = FIG_DIR.parent / "data" / "cache" / f"floors_{name}_{h}.npy"
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            return _np.load(path)
        mat = fn()
        _np.save(path, mat)
        return mat

    def __init__(self):
        self.ev = MeshEval(max_locs=self.MAX_LOCS)
        # attribution of what the OPEN model transited (before/after needs
        # the unrestricted paths, unlike the rule-debugging analysis)
        self.ana = TransitAnalysis(self.ev, policy=OPEN_POLICY)
        self.policy = DEFAULT_POLICY
        s = self.ana.pair_idx
        pair = lambda mat: mat[self.ev.dst_idx[s], self.ev.src_idx[s]]

        # stage 0: the graph as first built, planned cables included
        snapshot = GRAPH_NPZS[-1].parent / "raw" / GRAPH_NPZS[-1].stem.split("graph_")[1]
        self.planned_floor = pair(
            self._cached_matrix(
                "planned",
                lambda: FloorEstimator(
                    build_from_snapshot(snapshot, include_planned=True),
                    self.ev.lat,
                    self.ev.lon,
                ).floor_many_ms(self.ev.lat, self.ev.lon),
            )
        )
        # stage 2: v1 policy on the RFS-only graph
        self.v1_floor = pair(
            self._cached_matrix(
                V1_POLICY.name,
                lambda: policy_floor_matrix_parallel(
                    self.ev.graph, self.ana.node_cc, self.ev.lat, self.ev.lon,
                    self.ana.loc_cc, V1_POLICY,
                ),
            )
        )
        # stage 3: v2 (frozen history)
        self.v2_floor = pair(
            self._cached_matrix(
                V2_POLICY.name,
                lambda: policy_floor_matrix_parallel(
                    self.ev.graph, self.ana.node_cc, self.ev.lat, self.ev.lon,
                    self.ana.loc_cc, V2_POLICY,
                ),
            )
        )
        # stage 4: current policy
        self.policy_mat = self._cached_matrix(
            self.policy.name,
            lambda: policy_floor_matrix_parallel(
                self.ev.graph, self.ana.node_cc, self.ev.lat, self.ev.lon,
                self.ana.loc_cc, self.policy,
            ),
        )
        self.policy_floor = pair(self.policy_mat)

        self.meas = self.ev.meas[s]
        self.open_floor = self.ev.fiber[s]  # stage 1: RFS-only, no policy
        self.res_planned = self.meas - FIBER_SLOPE * self.planned_floor
        self.res_open = self.meas - FIBER_SLOPE * self.open_floor
        self.res_v1 = self.meas - FIBER_SLOPE * self.v1_floor
        self.res_v2 = self.meas - FIBER_SLOPE * self.v2_floor
        self.res_policy = self.meas - FIBER_SLOPE * self.policy_floor
        self.res_base = (self.ev.meas - self.ev.geod_baseline)[s]
        # per pair: which of its open-path transit countries the policy bans
        self.banned_on_path = [
            self.policy.banned_set(t, {sc, dc})
            for t, sc, dc in zip(self.ana.transit, self.ana.src_cc, self.ana.dst_cc)
        ]
        self.formerly = np.array([len(b) > 0 for b in self.banned_on_path])


@pytest.fixture(scope="module")
def pe():
    return PolicyEval()


class TestPolicyValidation:
    def test_policy_never_lowers_floors(self, pe):
        finite = np.isfinite(pe.open_floor)
        assert np.all(pe.policy_floor[finite] >= pe.open_floor[finite] - 1e-6)

    def test_untouched_pairs_unchanged(self, pe):
        # pairs whose open-model path transited no restricted country must
        # keep (almost exactly) the same floor
        untouched = ~pe.formerly & np.isfinite(pe.policy_floor)
        same = np.abs(pe.policy_floor[untouched] - pe.open_floor[untouched]) < 0.01
        assert np.mean(same) > 0.95

    def test_figure_and_summary(self, pe):
        finite = np.isfinite(pe.policy_floor)
        n_inf = int((~finite).sum())

        stages = [
            ("1.3·geodesic baseline", pe.res_base, "0.6", "--"),
            ("1.3·fiber, planned cables included", pe.res_planned, "tab:purple", "-"),
            ("1.3·fiber, open (RFS-only graph)", pe.res_open, "tab:green", "-"),
            ("1.3·fiber, v1 policy (falsified)", pe.res_v1, "tab:orange", "-"),
            ("1.3·fiber, v2 policy", pe.res_v2, "tab:brown", "-"),
            (f"1.3·fiber, {pe.policy.name} (current)", pe.res_policy, "tab:red", "-"),
        ]
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.5, 5))
        for ax, mask, subtitle in (
            (ax1, finite, f"all sampled pairs (n={finite.sum():,})"),
            (
                ax2,
                finite & pe.formerly,
                f"pairs whose open path transited a restricted country "
                f"(n={(finite & pe.formerly).sum():,})",
            ),
        ):
            for label, res, color, ls in stages:
                r = res[mask]
                r = r[np.isfinite(r)]
                ecdf(
                    ax,
                    np.clip(r, -100, 300),
                    color=color,
                    ls=ls,
                    lw=1.6,
                    label=f"{label}  [med {np.median(r):.0f}]",
                )
            ax.axvline(0, color="k", lw=0.8, ls=":")
            ax.set_xlabel("measured − model (ms, clipped)")
            ax.set_ylabel("CDF over pairs")
            ax.set_title(subtitle, fontsize=10)
            ax.legend(fontsize=8, loc="lower right")
            ax.grid(alpha=0.3)
        fig.suptitle(
            f"Model progression — {pe.policy.describe()}", fontsize=10
        )
        fig.tight_layout()
        FIG_DIR.mkdir(exist_ok=True)
        fig.savefig(FIG_DIR / "policy_validation.pdf", bbox_inches="tight")
        plt.close(fig)

        f = finite
        print("\n=== policy validation summary (model progression) ===")
        print(pe.policy.describe())
        print(f"pairs: {f.sum():,} finite under policy, {n_inf:,} now inf (no allowed route)")
        for name, r, floor in (
            ("planned", pe.res_planned[f], pe.planned_floor[f]),
            ("open", pe.res_open[f], pe.open_floor[f]),
            ("v1", pe.res_v1[f], pe.v1_floor[f]),
            ("v2", pe.res_v2[f], pe.v2_floor[f]),
            ("v3", pe.res_policy[f], pe.policy_floor[f]),
        ):
            fin = np.isfinite(r)
            print(
                f"{name:>8}: median {np.median(r[fin]):6.1f}  "
                f"P90 {np.percentile(r[fin], 90):6.1f} ms | "
                f"overshoot(1.3x) {np.mean(r[fin] < 0):5.1%} | "
                f"raw-floor violations {np.mean(pe.meas[f][fin] < floor[fin]):5.1%}"
            )

        print("\nformerly-transiting pairs, median residual open -> policy "
              "(raw-violation rate under policy):")
        restricted_seen = sorted(set().union(*pe.banned_on_path))
        rows = []
        for cc in restricted_seen:
            m = f & np.array([cc in b for b in pe.banned_on_path])
            if m.sum() < 300:
                continue
            viol = np.mean(pe.meas[m] < pe.policy_floor[m])
            rows.append((float(np.median(pe.res_open[m]) - np.median(pe.res_policy[m])), cc, m, viol))
        for _, cc, m, viol in sorted(rows, reverse=True)[:12]:
            print(
                f"  {cc}: n={m.sum():6,}  {np.median(pe.res_open[m]):6.1f} -> "
                f"{np.median(pe.res_policy[m]):6.1f} ms   (viol {viol:.1%})"
            )

        assert np.median(pe.res_policy[f]) <= np.median(pe.res_open[f]) + 1e-9
        # the policy must not break the floor semantics wholesale
        assert np.mean(pe.meas[f] < pe.policy_floor[f]) < 0.10
