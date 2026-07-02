import contextlib
import geopy.distance
import math
from typing import Tuple

DATA_DIR = "data"
FIG_DIR = "figures"
CACHE_DIR = "cache"

LatLon = Tuple[float, float]

# Global cache dictionary for distances
_DISTANCE_CACHE: dict[Tuple[float, float, float, float], float] = {}


def fast_haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Extremely fast, low-overhead distance calculation in km."""
    R = 6371.0 # Earth radius in kilometers

    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def get_distance(loc1: LatLon, loc2: LatLon) -> float:
	return fast_haversine(loc1[0], loc1[1], loc2[0], loc2[1])


class LockedLocationDict(dict):
    """
    A dict whose values are inaccessible during simulation_mode().

    Use this for any ground-truth location mapping (VP locs, target locs)
    to enforce that inference code never reads geographic coordinates.
    Accessing a key while locked raises ValueError immediately.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._locked = False

    def __getitem__(self, key):
        if self._locked:
            raise ValueError(
                f"Location lookup for '{key}' is forbidden during simulation. "
                "Inference must not access ground-truth coordinates."
            )
        return super().__getitem__(key)

    def get(self, key, default=None):
        if self._locked:
            raise ValueError(
                f"Location lookup for '{key}' is forbidden during simulation."
            )
        return super().get(key, default)


@contextlib.contextmanager
def simulation_mode(*dicts: 'LockedLocationDict'):
    """
    Context manager that locks one or more LockedLocationDicts.

    Any access to a locked dict inside the block raises ValueError,
    catching accidental use of ground-truth locations in inference code.

    Usage:
        locs = LockedLocationDict({'a': (0, 0), 'b': (1, 1)})
        with simulation_mode(locs):
            run_inference(...)   # cannot call locs[key] here
        error = get_distance(locs['a'], estimate)  # fine outside block
    """
    for d in dicts:
        d._locked = True
    try:
        yield
    finally:
        for d in dicts:
            d._locked = False


def convert_32_to_24(slash_32: str) -> str:
	return ".".join(slash_32.split('.')[0:3]) + ".0"