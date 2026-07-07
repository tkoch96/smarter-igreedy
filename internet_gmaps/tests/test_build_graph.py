"""Unit tests for build_graph.py loaders, on synthetic GeoJSON fixtures."""

import numpy as np
import pytest

import geo
from build_graph import (
    load_itu,
    load_landing_points,
    load_telegeography,
    parse_length_km,
)
from fiber_graph import GraphBuilder

EQ_DEG_KM = np.pi * geo.EARTH_RADIUS_KM / 180  # km per equator degree, derived — never hardcode


def cable_feature(cable_id, lines):
    return {
        "type": "Feature",
        "properties": {"id": cable_id},
        "geometry": {"type": "MultiLineString", "coordinates": lines},
    }


class TestParseLength:
    def test_typical(self):
        assert parse_length_km("45,000 km") == 45000.0
        assert parse_length_km("600 km") == 600.0
        assert parse_length_km("6,600km") == 6600.0

    def test_unparseable(self):
        assert parse_length_km(None) is None
        assert parse_length_km("") is None
        assert parse_length_km("n.a.") is None


class TestLoadTelegeography:
    def test_slack_from_published_length(self):
        # drawn: 10 equator degrees ~ 1111.9 km; published double that
        drawn_km = 10 * EQ_DEG_KM
        fc = {"features": [cable_feature("c1", [[[0, 0], [10, 0]]])]}
        details = {"c1": {"length": f"{2 * drawn_km:,.0f} km"}}
        b = GraphBuilder(snap_tolerance_km=1.0)
        slacks = load_telegeography(b, fc, details)
        g = b.build()
        assert slacks["c1"] == pytest.approx(2.0, rel=1e-3)
        assert g.edge_rtt_ms[0] == pytest.approx(geo.rtt_ms(2 * drawn_km), rel=1e-3)

    def test_slack_never_below_one_and_capped(self):
        fc = {
            "features": [
                cable_feature("short", [[[0, 0], [10, 0]]]),
                cable_feature("absurd", [[[0, 20], [10, 20]]]),
            ]
        }
        details = {
            "short": {"length": "100 km"},  # published < drawn -> clamp to 1.0
            "absurd": {"length": "1,000,000 km"},  # drawn tiny vs published -> cap
        }
        b = GraphBuilder(snap_tolerance_km=1.0)
        slacks = load_telegeography(b, fc, details, max_slack=3.0)
        assert slacks["short"] == 1.0
        assert slacks["absurd"] == 3.0

    def test_missing_details_mean_unit_slack(self):
        fc = {"features": [cable_feature("c1", [[[0, 0], [10, 0]]])]}
        b = GraphBuilder(snap_tolerance_km=1.0)
        slacks = load_telegeography(b, fc, cable_details=None)
        assert slacks["c1"] == 1.0

    def test_planned_cables_excluded_by_default(self):
        fc = {
            "features": [
                cable_feature("real", [[[0, 0], [10, 0]]]),
                cable_feature("future", [[[0, 20], [10, 20]]]),
            ]
        }
        details = {"future": {"is_planned": True, "rfs": "2028"}}
        b = GraphBuilder(snap_tolerance_km=1.0)
        slacks = load_telegeography(b, fc, details)
        assert "real" in slacks and "future" not in slacks
        b2 = GraphBuilder(snap_tolerance_km=1.0)
        assert "future" in load_telegeography(b2, fc, details, include_planned=True)

    def test_null_geometry_skipped(self):
        fc = {"features": [{"type": "Feature", "properties": {"id": "x"}, "geometry": None}]}
        b = GraphBuilder()
        load_telegeography(b, fc)
        assert b.n_nodes == 0


class TestLoadLandingPoints:
    def test_landing_point_merges_onto_cable_end(self):
        fc = {"features": [cable_feature("c1", [[[0, 0], [10, 0]]])]}
        b = GraphBuilder(snap_tolerance_km=5.0)
        load_telegeography(b, fc)
        lp = {
            "features": [
                {  # 0.01 deg (~1.1 km) off the cable end: must merge
                    "type": "Feature",
                    "properties": {"id": "lp1"},
                    "geometry": {"type": "Point", "coordinates": [0.01, 0.0]},
                },
                {  # far away: becomes its own (isolated) node
                    "type": "Feature",
                    "properties": {"id": "lp2"},
                    "geometry": {"type": "Point", "coordinates": [50.0, 30.0]},
                },
            ]
        }
        n_before = b.n_nodes
        assert load_landing_points(b, lp) == 2
        assert b.n_nodes == n_before + 1


class TestLoadItu:
    def _fc(self):
        def link(type_inf, coords):
            return {
                "type": "Feature",
                "properties": {"type_inf": type_inf},
                "geometry": None
                if coords is None
                else {"type": "LineString", "coordinates": coords},
            }

        return {
            "features": [
                link("Fibre Operational", [[0, 10], [1, 10]]),
                link("Fibre Planned", [[0, 20], [1, 20]]),
                link("Microwave Operational", [[0, 30], [1, 30]]),
                link("Fibre Operational", None),  # 1 real feature has null geometry
            ]
        }

    def test_default_filter_keeps_operational_fibre_only(self):
        b = GraphBuilder(snap_tolerance_km=1.0)
        assert load_itu(b, self._fc()) == 1
        assert b.build().n_edges == 1

    def test_include_types_widens_filter(self):
        b = GraphBuilder(snap_tolerance_km=1.0)
        n = load_itu(b, self._fc(), include_types=("Fibre Operational", "Fibre Planned"))
        assert n == 2
        assert b.build().n_edges == 2

    def test_terrestrial_slack_applies(self):
        b = GraphBuilder(snap_tolerance_km=1.0)
        fc = {
            "features": [
                {
                    "type": "Feature",
                    "properties": {"type_inf": "Fibre Operational"},
                    "geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 0]]},
                }
            ]
        }
        load_itu(b, fc, slack=1.3)
        g = b.build()
        assert g.edge_rtt_ms[0] == pytest.approx(geo.rtt_ms(1.3 * EQ_DEG_KM), rel=1e-6)
