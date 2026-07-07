"""Unit tests for geo.py: distances, unit conventions, sphere/chord conversions."""

import numpy as np
import pytest

import geo

EQ_DEG_KM = np.pi * geo.EARTH_RADIUS_KM / 180  # km per equator degree, derived — never hardcode


class TestHaversine:
    def test_zero_distance(self):
        assert geo.haversine_km(12.3, 45.6, 12.3, 45.6) == pytest.approx(0.0, abs=1e-9)

    def test_quarter_circumference(self):
        expected = np.pi / 2 * geo.EARTH_RADIUS_KM
        assert geo.haversine_km(0, 0, 0, 90) == pytest.approx(expected, rel=1e-9)
        assert geo.haversine_km(0, 0, 90, 0) == pytest.approx(expected, rel=1e-9)

    def test_known_city_pair_lhr_jfk(self):
        # LHR (51.4700, -0.4543) to JFK (40.6413, -73.7781) ~ 5540 km great circle
        d = geo.haversine_km(51.4700, -0.4543, 40.6413, -73.7781)
        assert d == pytest.approx(5540, rel=0.005)

    def test_symmetry(self):
        assert geo.haversine_km(10, 20, -30, 140) == pytest.approx(
            geo.haversine_km(-30, 140, 10, 20), rel=1e-12
        )

    def test_antimeridian(self):
        # 2 degrees of equator across the date line, not 358
        assert geo.haversine_km(0, 179, 0, -179) == pytest.approx(2 * EQ_DEG_KM, rel=1e-6)

    def test_vectorized(self):
        lats = np.array([0.0, 0.0, 0.0])
        d = geo.haversine_km(0.0, 0.0, lats, np.array([1.0, 2.0, 3.0]))
        assert d.shape == (3,)
        np.testing.assert_allclose(d, [EQ_DEG_KM, 2 * EQ_DEG_KM, 3 * EQ_DEG_KM], rtol=1e-6)


class TestRttConvention:
    def test_100km_is_1ms(self):
        assert geo.rtt_ms(100.0) == pytest.approx(1.0)

    def test_matches_smarter_igreedy_constant(self):
        assert geo.KM_PER_MS == 100.0

    def test_array_input(self):
        np.testing.assert_allclose(geo.rtt_ms(np.array([0.0, 250.0])), [0.0, 2.5])


class TestUnitXyz:
    def test_unit_norm(self):
        rng = np.random.default_rng(31415)
        lat = rng.uniform(-90, 90, 50)
        lon = rng.uniform(-180, 180, 50)
        xyz = geo.unit_xyz(lat, lon)
        assert xyz.shape == (50, 3)
        np.testing.assert_allclose(np.linalg.norm(xyz, axis=1), 1.0, rtol=1e-12)

    def test_reference_points(self):
        np.testing.assert_allclose(geo.unit_xyz(0, 0), [1, 0, 0], atol=1e-12)
        np.testing.assert_allclose(geo.unit_xyz(90, 123), [0, 0, 1], atol=1e-9)

    def test_chord_matches_geodesic(self):
        # chord distance between xyz points must round-trip to the haversine distance
        a, b = (10.0, 20.0), (-35.0, 150.0)
        chord = np.linalg.norm(geo.unit_xyz(*a) - geo.unit_xyz(*b))
        assert geo.chord_to_km(chord) == pytest.approx(geo.haversine_km(*a, *b), rel=1e-9)


class TestParentProjectConsistency:
    """The atlas must speak the same units as its downstream consumer
    (smarter-igreedy): same speed constant, same Earth radius, same
    distances. geo.py imports the constants; these tests pin the
    independent implementations against each other."""

    def test_haversine_matches_utils_fast_haversine(self):
        from utils import fast_haversine

        rng = np.random.default_rng(31415)
        for _ in range(100):
            lat1, lat2 = rng.uniform(-90, 90, 2)
            lon1, lon2 = rng.uniform(-180, 180, 2)
            assert geo.haversine_km(lat1, lon1, lat2, lon2) == pytest.approx(
                fast_haversine(lat1, lon1, lat2, lon2), rel=1e-12, abs=1e-9
            )

    def test_km_per_ms_is_the_parent_constant(self):
        import probabilistic_helpers

        assert geo.KM_PER_MS is probabilistic_helpers.KM_PER_MS

    def test_constants_match_all_parent_copies(self):
        import feasible_region_maintainer

        assert geo.KM_PER_MS == feasible_region_maintainer.KM_PER_MS
        assert geo.EARTH_RADIUS_KM == feasible_region_maintainer.EARTH_RADIUS_KM

    def test_get_distance_reexport(self):
        assert geo.get_distance((0.0, 0.0), (0.0, 1.0)) == pytest.approx(EQ_DEG_KM, rel=1e-9)


class TestChordConversions:
    def test_roundtrip(self):
        for km in [0.0, 1.0, 500.0, 10000.0, np.pi * geo.EARTH_RADIUS_KM * 0.99]:
            assert geo.chord_to_km(geo.km_to_chord(km)) == pytest.approx(km, rel=1e-9, abs=1e-9)

    def test_small_angle(self):
        assert geo.km_to_chord(1.0) == pytest.approx(1.0 / geo.EARTH_RADIUS_KM, rel=1e-6)
