"""Which kinds of modeled paths don't match the measurements?

Hypothesis (geopolitical routing): the shortest-fiber model happily routes
through any country's fiber, but real routing does not — e.g. traffic that
neither originates nor terminates in China does not transit China. If so,
pairs whose MODELED path transits such a country should carry a
systematically larger residual (measured − 1.3·fiber floor).

Method: reverse-geocode every graph node and probe to a country; for each
sampled pair, recover the modeled shortest path from the per-VP Dijkstra
predecessor tree and record its transit countries (path countries minus
the two endpoint countries). Aggregate residuals three ways:

  figures/transit_country_residuals.pdf
      left: residual CDFs for pairs transiting each top-offender country
            vs all pairs
      right: per-country median residual, transit-only vs endpoint pairs —
            the direct test of the "can't use their fiber unless you
            terminate there" rule (transit >> endpoint supports it)
  figures/transit_residual_map.pdf
      5-degree grid: median residual of all pairs whose modeled path
      crosses the cell — lights up the regions where modeled paths
      accumulate unexplained latency

Same skip conditions as test_mesh_validation. Runtime ~2 min; sample size
via TRANSIT_SAMPLE env var (default 120k pairs).
"""

import os
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pytest
from matplotlib.collections import LineCollection
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra

import geo  # noqa: F401 (used indirectly by helpers below)
from test_mesh_validation import (
    FIG_DIR,
    GRAPH_NPZS,
    MESH_PKL,
    FIBER_SLOPE,  # noqa: F401 — re-exported for test_policy_validation
    MeshEval,
    ecdf,
)

N_SAMPLE = int(os.environ.get("TRANSIT_SAMPLE", "120000"))
N_CAND = 512  # snap candidates per target; truncation shows up as path/floor mismatch
MIN_TRANSIT_SUPPORT = 500
MIN_ENDPOINT_SUPPORT = 200
GRID_DEG = 5.0
MIN_CELL_SUPPORT = 50

pytestmark = pytest.mark.skipif(
    not (MESH_PKL.exists() and GRAPH_NPZS),
    reason="needs the mesh cache and a built graph npz",
)

try:
    import reverse_geocoder as _rg
except ImportError:
    _rg = None
    pytestmark = pytest.mark.skip(reason="needs reverse_geocoder for country attribution")


def _countries(lats, lons):
    res = _rg.search(list(zip(map(float, lats), map(float, lons))), mode=1, verbose=False)
    return np.array([r["cc"] for r in res])


class TransitAnalysis:
    """Per-pair transit-country attribution from modeled shortest paths,
    routed under a transit policy (default: the CURRENT policy).

    The debugging loop this serves: rules we already trust are applied to
    the routing, so banned countries cannot appear as transit; whatever
    transit-vs-endpoint structure REMAINS in the residuals points at the
    next rule to add. Pass OPEN_POLICY for the historical unrestricted
    attribution (used by the policy before/after comparison)."""

    def __init__(self, ev, policy=None, n_sample=N_SAMPLE, seed=31415):
        from transit_policy import DEFAULT_POLICY, OPEN_POLICY, policy_paths_parallel

        self.policy = DEFAULT_POLICY if policy is None else policy
        g = ev.graph
        self.ev = ev
        self.node_cc = _countries(g.node_lat, g.node_lon)
        self.loc_cc = _countries(ev.lat, ev.lon)

        rng = np.random.default_rng(seed)
        n_pairs = len(ev.meas)
        sample = rng.choice(n_pairs, size=min(n_sample, n_pairs), replace=False)
        sample = sample[np.argsort(ev.src_idx[sample], kind="stable")]
        self.pair_idx = sample

        pairs = list(zip(ev.src_idx[sample], ev.dst_idx[sample]))
        self.floors, self.transit, self.path_edges, self.path_cells = policy_paths_parallel(
            g,
            self.node_cc,
            ev.lat,
            ev.lon,
            self.loc_cc,
            pairs,
            policy=self.policy,
            direct_km_max=ev.est.direct_km_max,
            lastmile_km_max=ev.est.lastmile_km_max,
            grid_deg=GRID_DEG,
        )
        # residuals against the SAME policy's floors (with the same 1.3):
        # measured − 1.3·floor, so remaining structure is unexplained by
        # the rules already in force
        from test_mesh_validation import FIBER_SLOPE

        self.residual = ev.meas[sample] - FIBER_SLOPE * self.floors
        # consistency: under the open policy the recovered floors must match
        # the FloorEstimator exactly
        if not self.policy.rules:
            self.mismatch_frac = float(
                np.mean(np.abs(self.floors - ev.fiber[sample]) > 0.5)
            )
        else:
            self.mismatch_frac = 0.0
        self.geod_km = ev.geod_km[sample]
        self.src_cc = self.loc_cc[ev.src_idx[sample]]
        self.dst_cc = self.loc_cc[ev.dst_idx[sample]]

    def by_transit_country(self):
        out = defaultdict(list)
        for r, ccs in zip(self.residual, self.transit):
            for cc in ccs:
                out[cc].append(r)
        return {cc: np.asarray(v) for cc, v in out.items()}

    def by_endpoint_country(self):
        out = defaultdict(list)
        for r, s, d in zip(self.residual, self.src_cc, self.dst_cc):
            out[s].append(r)
            if d != s:
                out[d].append(r)
        return {cc: np.asarray(v) for cc, v in out.items()}


@pytest.fixture(scope="module")
def ana():
    return TransitAnalysis(MeshEval())


class TestTransitCountryEffect:
    def test_country_figure_and_table(self, ana):
        transit = ana.by_transit_country()
        endpoint = ana.by_endpoint_country()
        eligible = {cc: v for cc, v in transit.items() if len(v) >= MIN_TRANSIT_SUPPORT}
        ranked = sorted(eligible, key=lambda cc: -np.median(eligible[cc]))

        print(
            f"\n=== transit-country residuals under policy '{ana.policy.name}' "
            "(median measured − 1.3·floor, ms) ==="
        )
        print(f"pairs sampled: {len(ana.residual):,}  (path/floor mismatch {ana.mismatch_frac:.2%})")
        print(f"{'cc':>4} {'n_transit':>9} {'transit_med':>11} {'n_endpoint':>10} {'endpoint_med':>12}")
        for cc in ranked[:15]:
            e = endpoint.get(cc, np.array([]))
            e_med = f"{np.median(e):>12.1f}" if len(e) else f"{'-':>12}"
            print(
                f"{cc:>4} {len(eligible[cc]):>9,} {np.median(eligible[cc]):>11.1f} "
                f"{len(e):>10,} {e_med}"
            )

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.5, 5))

        ax1.set_prop_cycle(color=plt.cm.tab10.colors)
        ecdf(ax1, np.clip(ana.residual, -50, 300), color="k", lw=2.2, label="all sampled pairs")
        for cc in ranked[:6]:
            ecdf(
                ax1,
                np.clip(eligible[cc], -50, 300),
                lw=1.4,
                label=f"transits {cc} (n={len(eligible[cc]):,})",
            )
        ax1.axvline(0, color="k", lw=0.8, ls=":")
        ax1.set_xlabel("measured − 1.3·fiber floor (ms, clipped)")
        ax1.set_ylabel("CDF over pairs")
        ax1.set_title(
            f"Residual CDFs by transit country — routed under '{ana.policy.name}'\n"
            "(worst 6 by median, support ≥ 500; banned countries cannot appear)"
        )
        ax1.legend(fontsize=8, loc="lower right")
        ax1.grid(alpha=0.3)

        rows = [
            cc
            for cc in ranked
            if len(endpoint.get(cc, ())) >= MIN_ENDPOINT_SUPPORT
        ][:14]
        y = np.arange(len(rows))[::-1]
        t_med = [np.median(eligible[cc]) for cc in rows]
        e_med = [np.median(endpoint[cc]) for cc in rows]
        for yi, tm, em in zip(y, t_med, e_med):
            ax2.plot([em, tm], [yi, yi], "-", color="0.7", lw=1.2, zorder=1)
        ax2.scatter(t_med, y, s=45, color="tab:red", zorder=2, label="transit only (not endpoint)")
        ax2.scatter(e_med, y, s=45, facecolors="none", edgecolors="tab:blue", zorder=2, label="endpoint pairs")
        ax2.set_yticks(y)
        ax2.set_yticklabels(rows)
        ax2.axvline(np.median(ana.residual), color="k", lw=0.8, ls=":", label="global median")
        ax2.set_xlabel("median residual (ms)")
        ax2.set_title(
            "Transit vs endpoint effect — REMAINING under current policy\n"
            "transit ≫ endpoint ⇒ candidate for the next transit rule"
        )
        ax2.legend(fontsize=8)
        ax2.grid(alpha=0.3, axis="x")

        fig.tight_layout()
        FIG_DIR.mkdir(exist_ok=True)
        fig.savefig(FIG_DIR / "transit_country_residuals.pdf", bbox_inches="tight")
        plt.close(fig)

        assert len(ana.residual) > 50_000
        assert ana.mismatch_frac < 0.05  # path recovery must agree with FloorEstimator
        assert len(eligible) >= 3


class TestCableResidualCorrelation:
    def test_cable_offenders_figure(self, ana):
        """Which cables / link groups are correlated with unexplained
        latency? Aggregates residuals per SOURCE FEATURE: TeleGeography
        edges by cable name, ITU terrestrial edges grouped by the country
        pair they connect. A feature whose users carry huge residuals is
        infrastructure the model trusts but real routing barely uses (the
        central-Africa problem, caught at cable granularity).

        Produces figures/cable_residual_offenders.pdf: top offenders bar +
        world map of edges colored by median residual of paths using them.
        """
        g = ana.ev.graph
        assert g.edge_feature is not None, "rebuild the graph npz (features missing)"

        # per-edge residual lists from the sampled modeled paths
        edge_res = defaultdict(list)
        for r, edges in zip(ana.residual, ana.path_edges):
            for e in edges:
                edge_res[e].append(r)

        def edge_label(e):
            fi = g.edge_feature[e]
            name = g.feature_names[fi] if fi >= 0 else "unknown"
            if name == "ITU":
                a, b = ana.node_cc[g.edge_src[e]], ana.node_cc[g.edge_dst[e]]
                return f"ITU {'-'.join(sorted({a, b}))}"
            return name

        label_res = defaultdict(list)
        for e, v in edge_res.items():
            label_res[edge_label(e)].extend(v)
        stats = {
            lab: (np.median(v), len(v))
            for lab, v in label_res.items()
            if len(v) >= 300
        }
        top = sorted(stats, key=lambda lab: -stats[lab][0])[:15]

        print("\n=== features correlated with residual (median ms, n path-uses) ===")
        for lab in top:
            print(f"  {lab:<28s} {stats[lab][0]:7.1f}  (n={stats[lab][1]:,})")

        fig, (ax1, ax2) = plt.subplots(
            1, 2, figsize=(16.5, 5.5), gridspec_kw={"width_ratios": [1.1, 1.6]}
        )
        y = np.arange(len(top))[::-1]
        ax1.barh(y, [stats[lab][0] for lab in top], color="tab:red", alpha=0.8)
        ax1.set_yticks(y)
        # long TeleGeography ids squish the panel: truncate, keep full names
        # in the printed table above
        ax1.set_yticklabels(
            [lab if len(lab) <= 26 else lab[:25] + "…" for lab in top], fontsize=7
        )
        ax1.axvline(np.median(ana.residual), color="k", ls=":", lw=0.8, label="global median")
        ax1.set_xlabel("median residual of paths using feature (ms)")
        ax1.set_title("Top offender cables / link groups\n(support ≥ 300 path-uses)", fontsize=10)
        ax1.legend(fontsize=8)

        med = np.full(g.n_edges, np.nan)
        for e, v in edge_res.items():
            if len(v) >= 50:
                med[e] = np.median(v)
        segs, vals, bg = [], [], []
        for e in range(g.n_edges):
            s, d = g.edge_src[e], g.edge_dst[e]
            if abs(g.node_lon[s] - g.node_lon[d]) >= 180:
                continue
            seg = [(g.node_lon[s], g.node_lat[s]), (g.node_lon[d], g.node_lat[d])]
            if np.isfinite(med[e]):
                segs.append(seg)
                vals.append(med[e])
            else:
                bg.append(seg)
        ax2.add_collection(LineCollection(bg, colors="0.88", linewidths=0.25, rasterized=True))
        lc = LineCollection(segs, cmap="RdYlGn_r", norm=plt.Normalize(0, 120), linewidths=1.2)
        lc.set_array(np.array(vals))
        ax2.add_collection(lc)
        fig.colorbar(lc, ax=ax2, label="median residual of paths using edge (ms)")
        ax2.set_xlim(-180, 180)
        ax2.set_ylim(-60, 80)
        ax2.set_title(
            "Edges colored by median residual of the modeled paths using them\n"
            "(grey: unused or support < 50)",
            fontsize=10,
        )
        fig.tight_layout()
        FIG_DIR.mkdir(exist_ok=True)
        fig.savefig(FIG_DIR / "cable_residual_offenders.pdf", bbox_inches="tight", dpi=150)
        plt.close(fig)

        assert len(stats) >= 10
        assert len(segs) > 100


class TestTransitGridMap:
    def test_grid_heatmap(self, ana):
        cell_res = defaultdict(list)
        for r, cells in zip(ana.residual, ana.path_cells):
            for cell in cells:
                cell_res[cell].append(r)

        lat_bins = np.arange(-60, 85, GRID_DEG)
        lon_bins = np.arange(-180, 185, GRID_DEG)
        med = np.full((len(lat_bins) - 1, len(lon_bins) - 1), np.nan)
        for (ci, cj), v in cell_res.items():
            if len(v) < MIN_CELL_SUPPORT:
                continue
            lat0, lon0 = ci * GRID_DEG, cj * GRID_DEG
            i = int((lat0 - lat_bins[0]) / GRID_DEG)
            j = int((lon0 - lon_bins[0]) / GRID_DEG)
            if 0 <= i < med.shape[0] and 0 <= j < med.shape[1]:
                med[i, j] = np.median(v)

        g = ana.ev.graph
        fig, ax = plt.subplots(figsize=(13, 6.5))
        segs = [
            [(g.node_lon[s], g.node_lat[s]), (g.node_lon[d], g.node_lat[d])]
            for s, d in zip(g.edge_src, g.edge_dst)
            if abs(g.node_lon[s] - g.node_lon[d]) < 180
        ]
        ax.add_collection(LineCollection(segs, colors="0.75", linewidths=0.25, rasterized=True))
        pm = ax.pcolormesh(
            lon_bins,
            lat_bins,
            np.ma.masked_invalid(med),
            cmap="RdYlGn_r",
            vmin=0,
            vmax=120,
            alpha=0.75,
        )
        fig.colorbar(pm, ax=ax, label="median residual of paths crossing cell (ms)")
        ax.set_xlim(-180, 180)
        ax.set_ylim(-60, 80)
        ax.set_title(
            f"Where modeled paths accumulate unexplained latency — {GRID_DEG:.0f}° cells, "
            f"support ≥ {MIN_CELL_SUPPORT} pairs\n(median measured − 1.3·fiber floor over all "
            "pairs whose modeled shortest path crosses the cell)"
        )
        FIG_DIR.mkdir(exist_ok=True)
        fig.savefig(FIG_DIR / "transit_residual_map.pdf", bbox_inches="tight", dpi=150)
        plt.close(fig)

        n_cells = int(np.isfinite(med).sum())
        print(f"\ngrid map: {n_cells} cells with support >= {MIN_CELL_SUPPORT}")
        assert n_cells > 50
