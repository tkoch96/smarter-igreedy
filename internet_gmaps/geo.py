"""Geodesic helpers for the fiber atlas — thin layer over the parent
smarter-igreedy project's conventions.

Reused from the parent (single source of truth, keeps the atlas consistent
with its downstream consumer):
  - KM_PER_MS (probabilistic_helpers): RTT_ms = path_km / KM_PER_MS, the
    "1 ms RTT per 100 km" rule (~c/1.468 in fiber, one way ~204,000 km/s).
  - fast_haversine / get_distance / LatLon (utils): scalar single-pair use.
  - Earth radius 6371.0 km, matching utils.fast_haversine exactly.

Added here (no parent equivalent): broadcastable-array haversine — the
parent's haversine_grid is scalar-VP-to-grid only — and unit-sphere /
chord conversions for KD-tree work. tests/test_geo.py pins the scalar
implementations against each other.
"""

import sys
from pathlib import Path

import numpy as np

_PARENT = str(Path(__file__).resolve().parent.parent)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

from probabilistic_helpers import KM_PER_MS  # noqa: E402
from utils import LatLon, fast_haversine, get_distance  # noqa: E402, F401

EARTH_RADIUS_KM = 6371.0  # matches utils.fast_haversine


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km; degrees in, scalars or broadcastable
    arrays. Array counterpart of utils.fast_haversine (same radius, same
    atan2 formulation)."""
    lat1, lon1, lat2, lon2 = (
        np.radians(np.asarray(x, dtype=float)) for x in (lat1, lon1, lat2, lon2)
    )
    a = (
        np.sin((lat2 - lat1) / 2) ** 2
        + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    )
    a = np.clip(a, 0.0, 1.0)
    return 2 * EARTH_RADIUS_KM * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


def rtt_ms(path_km):
    """Fiber-floor RTT for a path of the given length in km."""
    return path_km / KM_PER_MS


def unit_xyz(lat, lon):
    """Unit-sphere Cartesian coordinates, shape (..., 3). For KD-trees:
    Euclidean (chord) distance is monotone in geodesic distance, so
    nearest-neighbor order is preserved exactly."""
    lat = np.radians(np.asarray(lat, dtype=float))
    lon = np.radians(np.asarray(lon, dtype=float))
    return np.stack(
        [np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)], axis=-1
    )


def km_to_chord(km):
    """Unit-sphere chord length subtended by a geodesic distance in km."""
    ang = np.minimum(np.asarray(km, dtype=float) / EARTH_RADIUS_KM, np.pi)
    return 2 * np.sin(ang / 2)


def chord_to_km(chord):
    """Inverse of km_to_chord."""
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.clip(np.asarray(chord, dtype=float) / 2, 0.0, 1.0))
