"""Integration tests against the real RIPE Atlas mesh + the built fiber graph.

Produces the milestone-0/1 validation artifacts and a printed summary
(run pytest with -s to see it):

  figures/mesh_validation_cdfs.pdf  three CDFs on one axis:
      (a) 1.3×fiber floor − 1.3×geodesic  (how the two models differ)
      (b) measured − 1.3×geodesic         (baseline tightness)
      (c) measured − 1.3×fiber floor      (our tightness)
  Both models carry the same 1.3 inflation, so the comparison isolates
  the path geometry. NB: measured < 1.3×fiber is a model overshoot, not
  impossible physics — only the RAW floor (FIBER_SLOPE = 1) has the hard
  violation semantics, and the admissibility test below uses the raw floor.
  figures/mesh_case_studies.pdf     world-map case studies:
      (a) all three agree              (b) fiber wins over 1.3×geodesic
      (c) both models far from measured (d) 1.3×geodesic wins over fiber

Skipped unless both inputs exist:
  ../cache/cached_target_data.pkl   (the mesh; see parent CLAUDE.md)
  data/graph_*.npz                  (run build_graph.py first)

Runtime ~1 minute: probes are clustered to 0.25° metros (MeshEval), so
the per-VP fields scale with metro count (~3.2k at the 2026-07-09 mesh,
10.7k probes / ~388k directed pairs), not raw probe count.
"""

import pickle
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pytest
from matplotlib.collections import LineCollection

import geo
from fiber_graph import FiberGraph
from floor_query import FloorEstimator, floor_path_ms

PKG_DIR = Path(__file__).parent.parent
MESH_PKL = PKG_DIR.parent / "cache" / "cached_target_data.pkl"
GRAPH_NPZS = sorted((PKG_DIR / "data").glob("graph_*.npz"))
FIG_DIR = PKG_DIR / "figures"

GEODESIC_SLOPE = 1.3  # the standard cheap baseline: rtt = 1.3 * geodesic_km / 100
FIBER_SLOPE = 1.3  # same inflation on the fiber floor, isolating path geometry
GOOD_MS = 10.0  # |model - measured| below this: "good agreement"
BAD_MS = 40.0  # above this: "bad agreement"

pytestmark = pytest.mark.skipif(
    not (MESH_PKL.exists() and GRAPH_NPZS),
    reason="needs the mesh cache and a built graph npz",
)


class MeshEval:
    """Everything computed once: unique probe locations, fiber floor matrix,
    geodesic matrix, and per-directed-pair aligned arrays."""

    def __init__(self, include_campaign=True, max_locs=None, seed=31415, cluster_deg=0.25):
        from mesh_data import load_target_data

        d = load_target_data(MESH_PKL, include_campaign=include_campaign)
        addr_loc = d["address_to_loc"]
        raw_locs = sorted({tuple(map(float, v)) for v in addr_loc.values()})
        # Cluster co-located probes (~cluster_deg deg grid): floors from two
        # points r km apart differ by <= 2r at fiber speed (~0.6 ms at 28 km)
        # — below measurement noise — so a cluster shares one field. Within a
        # cluster pair the MIN measured RTT wins: co-located probes' best
        # measurement is the tightest ground-truth bound for the site pair
        # (and healthy neighbors absorb sick-probe outliers).
        if cluster_deg:
            cells = {}
            for l in raw_locs:
                cells.setdefault(
                    (round(l[0] / cluster_deg), round(l[1] / cluster_deg)), []
                ).append(l)
            rep_of = {}
            reps = []
            for members in cells.values():
                rep = (
                    float(np.mean([m[0] for m in members])),
                    float(np.mean([m[1] for m in members])),
                )
                reps.append(rep)
                for m in members:
                    rep_of[m] = rep
            self.locs = sorted(reps)
        else:
            self.locs = raw_locs
            rep_of = {l: l for l in raw_locs}
        if max_locs is not None and len(self.locs) > max_locs:
            # analyses whose cost scales with VP count (per-VP Dijkstras)
            # run on a seeded random subset of locations
            rng = np.random.default_rng(seed)
            keep = rng.choice(len(self.locs), max_locs, replace=False)
            self.locs = [self.locs[i] for i in sorted(keep)]
        self._rep_of = rep_of
        loc_id = {loc: k for k, loc in enumerate(self.locs)}
        self.lat = np.array([l[0] for l in self.locs])
        self.lon = np.array([l[1] for l in self.locs])

        npz = np.load(GRAPH_NPZS[-1])
        self.graph = FiberGraph(
            npz["node_lat"],
            npz["node_lon"],
            npz["edge_src"],
            npz["edge_dst"],
            npz["edge_rtt_ms"],
            edge_feature=npz["edge_feature"] if "edge_feature" in npz else None,
            feature_names=tuple(npz["feature_names"]) if "feature_names" in npz else (),
        )
        self.est = FloorEstimator(self.graph, self.lat, self.lon)
        self.fiber_mat = self.est.floor_many_ms(self.lat, self.lon)  # (L, L)
        self.geod_mat = geo.haversine_km(
            self.lat[:, None], self.lon[:, None], self.lat[None, :], self.lon[None, :]
        )

        best = {}
        for src, dsts in d["loc_loc_meas"].items():
            i = loc_id.get(self._rep_of.get(tuple(map(float, addr_loc[src]))))
            if i is None:  # location subsampled out
                continue
            for dst, rtt in dsts.items():
                j = loc_id.get(self._rep_of.get(tuple(map(float, addr_loc[dst]))))
                if j is None or i == j:
                    continue
                rtt = float(rtt)
                if best.get((i, j), np.inf) > rtt:
                    best[(i, j)] = rtt
        self.src_idx = np.array([k[0] for k in best])
        self.dst_idx = np.array([k[1] for k in best])
        self.meas = np.array(list(best.values()))
        self.fiber = self.fiber_mat[self.dst_idx, self.src_idx]  # (n_targets, n_vps) indexing
        self.fiber13 = FIBER_SLOPE * self.fiber  # the model under evaluation
        self.geod_km = self.geod_mat[self.src_idx, self.dst_idx]
        self.geod_baseline = GEODESIC_SLOPE * geo.rtt_ms(self.geod_km)


@pytest.fixture(scope="module")
def ev():
    return MeshEval()


def ecdf(ax, values, **kw):
    x = np.sort(values)
    ax.plot(x, np.arange(1, len(x) + 1) / len(x), **kw)


def pct(v, q):
    return np.percentile(v, q)


class TestMeshCdfs:
    def test_admissibility_on_real_mesh(self, ev):
        # The fiber floor may not meaningfully undercut the pure geodesic
        # floor. It CAN undercut it slightly: edge lengths come from raw
        # polyline vertices while node positions are snapped (<= 5 km
        # tolerance), so a many-hop path accumulates small negative error
        # (measured worst on this graph: -0.12 ms over 18 hops).
        finite = np.isfinite(ev.fiber)
        assert np.all(ev.fiber[finite] >= geo.rtt_ms(ev.geod_km[finite]) - 1.0)

    def test_cdf_figure_and_summary(self, ev):
        finite = np.isfinite(ev.fiber)
        n_inf = int((~finite).sum())
        fiber, meas, base = ev.fiber13[finite], ev.meas[finite], ev.geod_baseline[finite]

        a = fiber - base  # 1.3·fiber vs the 1.3·geodesic baseline
        b = meas - base  # baseline tightness (vs measured)
        c = meas - fiber  # our tightness (vs measured); < 0 = model overshoot

        fig, ax = plt.subplots(figsize=(7, 4.5))
        ecdf(ax, np.clip(a, -100, 250), label="(a) 1.3·fiber floor − 1.3·geodesic", color="tab:blue")
        ecdf(ax, np.clip(b, -100, 250), label="(b) measured − 1.3·geodesic", color="tab:orange")
        ecdf(ax, np.clip(c, -100, 250), label="(c) measured − 1.3·fiber floor", color="tab:green")
        ax.axvline(0, color="k", lw=0.8, ls=":")
        ax.set_xlabel("difference (ms, clipped to [-100, 250])")
        ax.set_ylabel("CDF over mesh pairs")
        ax.set_title(
            f"Mesh validation: {len(fiber):,} directed pairs, {len(ev.locs)} probe locations\n"
            f"(fiber graph: {GRAPH_NPZS[-1].name}; speed = {geo.KM_PER_MS:.0f} km/ms RTT)"
        )
        ax.legend(loc="lower right", fontsize=9)
        ax.grid(alpha=0.3)
        FIG_DIR.mkdir(exist_ok=True)
        fig.savefig(FIG_DIR / "mesh_validation_cdfs.pdf", bbox_inches="tight")
        plt.close(fig)

        overshoot_fiber = float(np.mean(c < 0))
        overshoot_base = float(np.mean(b < 0))
        viol_raw = float(np.mean(ev.meas[finite] < ev.fiber[finite]))
        physics = float(np.mean(meas < geo.rtt_ms(ev.geod_km[finite])))
        print("\n=== mesh validation summary ===")
        print(f"pairs: {len(fiber):,} finite, {n_inf:,} inf floor (endpoint >300km from graph)")
        for name, v in [
            ("(a) 1.3fiber−1.3geo", a),
            ("(b) meas−1.3geo", b),
            ("(c) meas−1.3fiber", c),
        ]:
            print(
                f"{name}: median {np.median(v):7.1f}  P10 {pct(v, 10):7.1f}  P90 {pct(v, 90):7.1f} ms"
            )
        print(f"overshoot rate (meas < 1.3*fiber floor):    {overshoot_fiber:6.1%}")
        print(f"overshoot rate (meas < 1.3*geodesic):       {overshoot_base:6.1%}")
        print(f"violation rate (meas < RAW fiber floor):    {viol_raw:6.1%}  <- hard floor semantics")
        print(f"physics violations (meas < 1.0*geodesic):   {physics:6.1%}  <- probe geoloc errors")

        assert len(fiber) > 100_000
        # loose canaries only — the numbers themselves are the deliverable
        assert viol_raw < 0.05  # the raw floor must stay a near-valid bound
        assert overshoot_fiber < 0.5


try:
    import reverse_geocoder as _rg

    def place_name(lat, lon):
        """Nearest GeoNames city, e.g. 'Toledo, US' (offline lookup)."""
        r = _rg.search([(float(lat), float(lon))], mode=1, verbose=False)[0]
        return f"{r['name']}, {r['cc']}"

except ImportError:  # optional dep; labels degrade to coordinates

    def place_name(lat, lon):
        return f"({lat:.1f}, {lon:.1f})"


class TestPerProbeCdfs:
    def test_per_probe_median_cdf_and_bad_apples(self, ev):
        """Are the disagreements driven by a few bad-apple probes or spread
        uniformly? For each probe: the median difference over all directed
        pairs it participates in (either role). If a handful of probes were
        ruining it, the per-probe median CDFs would hug zero with a short
        extreme tail, far from the all-pairs CDFs; if they track the
        all-pairs CDFs, the looseness is uniform."""
        finite = np.isfinite(ev.fiber)
        metrics = {
            "(a) 1.3·fiber floor − 1.3·geodesic": (ev.fiber13 - ev.geod_baseline)[finite],
            "(b) measured − 1.3·geodesic": (ev.meas - ev.geod_baseline)[finite],
            "(c) measured − 1.3·fiber floor": (ev.meas - ev.fiber13)[finite],
        }
        # group by probe: each pair contributes to both endpoints
        idx2 = np.concatenate([ev.src_idx[finite], ev.dst_idx[finite]])
        order = np.argsort(idx2, kind="stable")
        bounds = np.searchsorted(idx2[order], np.arange(len(ev.locs) + 1))
        has_pairs = np.flatnonzero(np.diff(bounds) > 0)

        colors = {"(a)": "tab:blue", "(b)": "tab:orange", "(c)": "tab:green"}
        fig, ax = plt.subplots(figsize=(7, 4.5))
        per_probe = {}
        for name, v in metrics.items():
            v2 = np.concatenate([v, v])[order]
            med = np.array([np.median(v2[bounds[l] : bounds[l + 1]]) for l in has_pairs])
            per_probe[name] = med
            color = colors[name[:3]]
            ecdf(ax, np.clip(med, -100, 250), label=f"{name} (per-probe median)", color=color)
            ecdf(
                ax,
                np.clip(v, -100, 250),
                color=color,
                ls="--",
                lw=0.9,
                alpha=0.45,
                label=f"{name[:3]} all pairs",
            )
        ax.axvline(0, color="k", lw=0.8, ls=":")
        ax.set_xlabel("difference (ms, clipped to [-100, 250])")
        ax.set_ylabel("CDF over probes (solid) / pairs (dashed)")
        ax.set_title(
            f"Per-probe medians vs all pairs — {len(has_pairs)} probes\n"
            "solid ≈ dashed ⇒ disagreement is uniform, not a few bad apples"
        )
        ax.legend(loc="lower right", fontsize=8)
        ax.grid(alpha=0.3)
        FIG_DIR.mkdir(exist_ok=True)
        fig.savefig(FIG_DIR / "mesh_validation_cdfs_per_probe.pdf", bbox_inches="tight")
        plt.close(fig)

        med_c = per_probe["(c) measured − 1.3·fiber floor"]
        print("\n=== per-probe medians of (c) measured − 1.3·fiber floor ===")
        print(
            f"median {np.median(med_c):.1f}  P10 {pct(med_c, 10):.1f}  "
            f"P90 {pct(med_c, 90):.1f} ms | probes with median > 100 ms: "
            f"{int((med_c > 100).sum())} / {len(med_c)}"
        )
        print("worst 8 probes (highest median slack over their pairs):")
        for r in np.argsort(med_c)[-8:][::-1]:
            l = has_pairs[r]
            print(
                f"  {place_name(ev.lat[l], ev.lon[l]):28s} "
                f"({ev.lat[l]:7.2f},{ev.lon[l]:8.2f})  median (c) {med_c[r]:6.1f} ms"
            )

        assert len(med_c) > 800
        # bad-apple check: even after dropping the worst 5% of probes, the
        # typical slack must remain the same order — i.e. looseness is not
        # attributable to a few probes (update if a future graph changes this)
        trimmed = np.sort(med_c)[: int(0.95 * len(med_c))]
        assert np.median(trimmed) > 0.5 * np.median(med_c)


def split_antimeridian(lonlat):
    """Split a (n,2) lon/lat polyline where it crosses the date line."""
    lonlat = np.asarray(lonlat)
    if len(lonlat) < 2:
        return [lonlat]
    breaks = np.flatnonzero(np.abs(np.diff(lonlat[:, 0])) > 180) + 1
    return np.split(lonlat, breaks)


def geodesic_points(a, b, n=64):
    """Points along the great circle from a to b (lat, lon in degrees)."""
    p, q = geo.unit_xyz(*a), geo.unit_xyz(*b)
    omega = np.arccos(np.clip(np.dot(p, q), -1, 1))
    if omega < 1e-12:
        return np.array([[a[1], a[0]], [b[1], b[0]]])
    t = np.linspace(0, 1, n)[:, None]
    xyz = (np.sin((1 - t) * omega) * p + np.sin(t * omega) * q) / np.sin(omega)
    lat = np.degrees(np.arcsin(np.clip(xyz[:, 2], -1, 1)))
    lon = np.degrees(np.arctan2(xyz[:, 1], xyz[:, 0]))
    return np.column_stack([lon, lat])


class TestCaseStudyMaps:
    def _draw_panel(self, ax, ev, k, title):
        src = (ev.lat[ev.src_idx[k]], ev.lon[ev.src_idx[k]])
        dst = (ev.lat[ev.dst_idx[k]], ev.lon[ev.dst_idx[k]])
        # basemap: the fiber graph itself
        g = ev.graph
        segs = [
            [(g.node_lon[s], g.node_lat[s]), (g.node_lon[d], g.node_lat[d])]
            for s, d in zip(g.edge_src, g.edge_dst)
            if abs(g.node_lon[s] - g.node_lon[d]) < 180
        ]
        ax.add_collection(LineCollection(segs, colors="0.8", linewidths=0.3, rasterized=True))
        ax.scatter(
            ev.lon, ev.lat, s=1.0, c="k", alpha=0.25, lw=0, zorder=2,
            rasterized=True,
        )
        for seg in split_antimeridian(geodesic_points(src, dst)):
            ax.plot(seg[:, 0], seg[:, 1], "--", color="tab:orange", lw=1.6)
        rtt, path = floor_path_ms(ev.graph, src, dst)
        if path:
            ll = np.array([(lon, lat) for lat, lon in path])
            for seg in split_antimeridian(ll):
                ax.plot(seg[:, 0], seg[:, 1], "-", color="tab:blue", lw=1.8)
        ax.plot(*src[::-1], "o", color="tab:red", ms=6, zorder=5)
        ax.plot(*dst[::-1], "s", color="tab:red", ms=6, zorder=5)
        ax.set_xlim(-180, 180)
        ax.set_ylim(-60, 80)
        ax.set_xticks([])
        ax.set_yticks([])
        meas, base, fiber = ev.meas[k], ev.geod_baseline[k], ev.fiber13[k]
        ax.set_title(
            f"{title}\n{place_name(*src)}  →  {place_name(*dst)}\n"
            f"measured {meas:.0f} ms | 1.3·geodesic {base:.0f} ms | "
            f"1.3·fiber floor {fiber:.0f} ms",
            fontsize=9,
        )

    def test_case_study_maps(self, ev):
        finite = np.isfinite(ev.fiber)
        err_base = np.abs(ev.meas - ev.geod_baseline)
        err_fiber = np.abs(ev.meas - ev.fiber13)
        long_haul = ev.geod_km > 1000  # short pairs agree trivially

        cases = {
            "(a) all agree": np.where(
                finite & long_haul & (err_base <= GOOD_MS) & (err_fiber <= GOOD_MS),
                ev.geod_km,  # most interesting = longest
                -np.inf,
            ),
            "(b) fiber wins": np.where(
                finite & long_haul & (err_base >= BAD_MS) & (err_fiber <= GOOD_MS),
                err_base - err_fiber,
                -np.inf,
            ),
            "(c) both bad": np.where(
                finite & long_haul & (err_base >= BAD_MS) & (err_fiber >= BAD_MS),
                np.minimum(err_base, err_fiber),
                -np.inf,
            ),
            "(d) fiber loses": np.where(
                finite & long_haul & (err_base <= GOOD_MS) & (err_fiber >= BAD_MS),
                err_fiber - err_base,
                -np.inf,
            ),
        }

        fig, axes = plt.subplots(2, 2, figsize=(14, 8))
        print("\n=== case studies ===")
        for ax, (title, score) in zip(axes.flat, cases.items()):
            n_in_cat = int(np.sum(np.isfinite(score)))
            if n_in_cat == 0:
                ax.text(0.5, 0.5, f"{title}\n(no pairs in category)", ha="center", va="center")
                ax.set_xticks([])
                ax.set_yticks([])
                print(f"{title}: 0 pairs")
                continue
            k = int(np.argmax(score))
            self._draw_panel(ax, ev, k, f"{title}  [{n_in_cat:,} pairs]")
            src_name = place_name(ev.lat[ev.src_idx[k]], ev.lon[ev.src_idx[k]])
            dst_name = place_name(ev.lat[ev.dst_idx[k]], ev.lon[ev.dst_idx[k]])
            print(
                f"{title}: {n_in_cat:,} pairs; shown: {src_name} -> {dst_name}  "
                f"meas {ev.meas[k]:.0f} / base {ev.geod_baseline[k]:.0f} / "
                f"1.3fiber {ev.fiber13[k]:.0f} ms"
            )
        fig.suptitle(
            "Fiber-floor case studies — orange dashed: great circle; blue: fiber path; "
            "grey: fiber graph; black dots: probe locations",
            fontsize=11,
        )
        FIG_DIR.mkdir(exist_ok=True)
        fig.savefig(FIG_DIR / "mesh_case_studies.pdf", bbox_inches="tight", dpi=150)
        plt.close(fig)

        assert any(np.any(np.isfinite(s)) for s in cases.values())
