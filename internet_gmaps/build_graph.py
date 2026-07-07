"""Build the fiber graph from a data/raw/<date>/ snapshot (see fetch_public_data.py).

Floor discipline: every edge weight must be defensible as physically
unavoidable. Submarine cables get a per-cable slack factor (published
length / drawn cartographic length, >= 1) because the published length is
real glass. ITU terrestrial links stay at their drawn (city-to-city
straight line) length by default — a detour factor would tighten the
floor but risks manufacturing violations; it is a knob, default off.

Run directly for a build + component report:
    python build_graph.py [snapshot_dir]
"""

import json
import re
import sys
from pathlib import Path

import numpy as np

import geo
from fiber_graph import GraphBuilder

DEFAULT_SNAP_KM = 5.0
MAX_SLACK = 3.0  # guard against bad drawn geometry making published/drawn absurd

ITU_DEFAULT_TYPES = ("Fibre Operational",)


def parse_length_km(text):
    """'45,000 km' -> 45000.0; returns None if unparseable."""
    if not text:
        return None
    m = re.search(r"([\d,\.]+)\s*km", text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _feature_lines(feature):
    """Yield each LineString's coordinate list; handles MultiLineString and null geometry."""
    g = feature.get("geometry")
    if g is None:
        return
    if g["type"] == "LineString":
        yield g["coordinates"]
    elif g["type"] == "MultiLineString":
        yield from g["coordinates"]


def _drawn_km(lines):
    total = 0.0
    for line in lines:
        c = np.asarray(line, dtype=float)  # GeoJSON order: (lon, lat)
        total += float(np.sum(geo.haversine_km(c[:-1, 1], c[:-1, 0], c[1:, 1], c[1:, 0])))
    return total


def load_telegeography(
    builder, cable_geo, cable_details=None, max_slack=MAX_SLACK, include_planned=False
):
    """Add submarine cable geometries; returns {cable_id: slack} for reporting.

    Planned / not-yet-RFS cables are excluded by default: a latency FLOOR
    may only use infrastructure that exists (the Umoja lesson — a planned
    trans-Africa cable was the single worst residual offender)."""
    cable_details = cable_details or {}
    slacks = {}
    for feature in cable_geo["features"]:
        cable_id = feature["properties"].get("id")
        if not include_planned and cable_details.get(cable_id, {}).get("is_planned"):
            continue
        lines = list(_feature_lines(feature))
        if not lines:
            continue
        slack = 1.0
        published = parse_length_km(cable_details.get(cable_id, {}).get("length"))
        drawn = _drawn_km(lines)
        if published and drawn > 0:
            slack = min(max(published / drawn, 1.0), max_slack)
        slacks[cable_id] = slack
        for line in lines:
            builder.add_path(
                [(lat, lon) for lon, lat in line], slack=slack, feature=f"TG:{cable_id}"
            )
    return slacks


def load_landing_points(builder, landing_geo):
    """Register landing points as nodes; within snap tolerance of a cable
    end they merge onto it, stitching cables that share a landing site."""
    n = 0
    for feature in landing_geo["features"]:
        g = feature.get("geometry")
        if g is None:
            continue
        lon, lat = g["coordinates"][:2]
        builder.node_id(lat, lon)
        n += 1
    return n


def load_itu(builder, itu_geo, include_types=ITU_DEFAULT_TYPES, slack=1.0):
    """Add ITU terrestrial links, filtered by type_inf (default: operational fibre only;
    microwave is a different propagation medium, planned links aren't a floor)."""
    n = 0
    for feature in itu_geo["features"]:
        if feature["properties"].get("type_inf") not in include_types:
            continue
        for line in _feature_lines(feature):
            builder.add_path([(lat, lon) for lon, lat in line], slack=slack, feature="ITU")
            n += 1
    return n


def build_from_snapshot(
    snapshot_dir, snap_tolerance_km=DEFAULT_SNAP_KM, with_itu=True, include_planned=False
):
    snapshot_dir = Path(snapshot_dir)
    load = lambda name: json.loads((snapshot_dir / name).read_text())
    builder = GraphBuilder(snap_tolerance_km=snap_tolerance_km)
    load_telegeography(
        builder,
        load("tg_cable_geo.json"),
        load("tg_cable_details.json"),
        include_planned=include_planned,
    )
    load_landing_points(builder, load("tg_landing_point_geo.json"))
    if with_itu:
        load_itu(builder, load("itu_trx_geocatalogue.json"))
    return builder.build()


def component_report(graph):
    labels = graph.component_labels
    sizes = np.sort(np.bincount(labels))[::-1]
    return {
        "n_nodes": graph.n_nodes,
        "n_edges": graph.n_edges,
        "n_components": len(sizes),
        "giant_component_frac": float(sizes[0] / graph.n_nodes) if graph.n_nodes else 0.0,
        "component_sizes_top10": sizes[:10].tolist(),
    }


def main():
    raw = Path(__file__).parent / "data" / "raw"
    snapshot = Path(sys.argv[1]) if len(sys.argv) > 1 else sorted(raw.iterdir())[-1]
    print(f"Building from {snapshot}")
    graph = build_from_snapshot(snapshot)
    for k, v in component_report(graph).items():
        print(f"  {k}: {v}")
    out = Path(__file__).parent / "data" / f"graph_{snapshot.name}.npz"
    np.savez_compressed(
        out,
        node_lat=graph.node_lat,
        node_lon=graph.node_lon,
        edge_src=graph.edge_src,
        edge_dst=graph.edge_dst,
        edge_rtt_ms=graph.edge_rtt_ms,
        edge_feature=graph.edge_feature,
        feature_names=np.array(graph.feature_names),
    )
    print(f"Saved {out}")


if __name__ == "__main__":
    main()
