"""
Standalone helper functions for the probabilistic geolocation model.

These are pure functions with no dependency on FeasibleRegion or any geolocator
class. They can be imported independently and unit-tested in isolation.

Model summary
-------------
For a VP at known location `vp` and observed RTT `r` (ms), the log-likelihood
of the target being at location `x` is:

    log P(r | x, vp) = -( r - slope * d(x, vp) / 100 )^2 / (2 * sigma_vp^2)

where d(x, vp) is great-circle distance in km, 100 km/ms is the SOL floor,
`slope` is the assumed routing-overhead factor over SOL (1.0 = pure SOL;
fiber paths typically run 1.1-1.3x), and sigma_vp is per-VP noise in ms.

The combined negative log-posterior (the quantity to minimise) is:

    nll(x) = sum_i  ( r_i - slope * d(x, vp_i) / 100 )^2 / (2 * sigma_i^2)

`Constraint` in the new model is (vp_loc, sigma_ms, rtt_ms).

The helpers here default to slope=1.0 (pure SOL) so they stay minimal pure
functions; FeasibleRegion passes its configured slope explicitly.
"""

from __future__ import annotations

import math
import os
import numpy as np
from typing import Any

from utils import LatLon, get_distance, _normalize_latlon

# (vp_location, sigma_ms, rtt_ms)
ProbConstraint = tuple[LatLon, float, float]

KM_PER_MS = 100.0          # speed-of-light floor: 1 ms RTT ~ 100 km one-way
GLOBAL_SIGMA_MS = 15.0     # fallback when a VP has fewer than min_peers peers
MIN_PEERS_FOR_SIGMA = 10   # minimum mesh pairs needed to fit a per-VP sigma

# ---------------------------------------------------------------------------
# Noise models: per-residual negative log-likelihood shapes
# ---------------------------------------------------------------------------
# RTT-vs-model residuals are not really gaussian: overhead is one-sided
# (SOL is a hard floor — a measurement can beat the model only slightly,
# but can exceed it wildly via detours) and heavy-tailed (occasional long
# routes that a quadratic loss chases catastrophically).

GAUSSIAN_NOISE   = 'gaussian'     # r²/(2σ²) — thin-tailed, symmetric
STUDENT_T_NOISE  = 'student_t'    # heavy-tailed, symmetric: outlier-robust
ASYMMETRIC_NOISE = 'asymmetric'   # steep wall below the model, linear above

STUDENT_T_DOF = 3.0        # ν: lower = heavier tails
ASYM_FAST_SCALE = 3.0      # how much steeper the faster-than-model side is


def residual_nll(residual_ms: float, sigma_ms: float,
                 noise_model: str = GAUSSIAN_NOISE) -> float:
    """
    Negative log-likelihood contribution of one RTT residual
    (residual = observed rtt − model-predicted rtt, in ms).

    gaussian    : r² / (2σ²)
    student_t   : ((ν+1)/2) · log(1 + r²/(νσ²)) — grows ~logarithmically for
                  |r| ≫ σ, so a single detour can't drag the estimate far.
    asymmetric  : slower than model (r ≥ 0): |r|/σ — a forgiving Laplace
                  tail for detours; faster than model (r < 0): quadratic
                  with a σ/ASYM_FAST_SCALE scale — beating the model is
                  nearly impossible physically, so it costs steeply.
    """
    r = residual_ms
    if noise_model == GAUSSIAN_NOISE:
        return (r ** 2) / (2.0 * sigma_ms ** 2)
    if noise_model == STUDENT_T_NOISE:
        return ((STUDENT_T_DOF + 1.0) / 2.0) * math.log1p(
            (r ** 2) / (STUDENT_T_DOF * sigma_ms ** 2))
    if noise_model == ASYMMETRIC_NOISE:
        if r >= 0.0:
            return r / sigma_ms
        return ((r * ASYM_FAST_SCALE) ** 2) / (2.0 * sigma_ms ** 2)
    raise ValueError(f"noise_model {noise_model!r} not understood")


# ---------------------------------------------------------------------------
# Core probabilistic primitives
# ---------------------------------------------------------------------------

def gaussian_nll(point: LatLon, constraints: list[ProbConstraint],
                 slope: float = 1.0,
                 noise_model: str = GAUSSIAN_NOISE,
                 rtt_model: 'RttModel' = None) -> float:
    """
    Negative log-posterior for `point` given `constraints`.

    This is the objective that Nelder-Mead minimises to find the MAP estimate.
    A lower value means the point is more consistent with the observed RTTs.
    (Historical name: with noise_model='gaussian' — the default — this is
    the sum of squared normalised residuals; other noise models swap the
    per-residual loss shape, see `residual_nll`.)

    Parameters
    ----------
    point       : (lat, lon) candidate target location
    constraints : list of (vp_loc, sigma_ms, rtt_ms)
    slope       : assumed routing-overhead factor, expected rtt = slope * d/100
    noise_model : GAUSSIAN_NOISE | STUDENT_T_NOISE | ASYMMETRIC_NOISE
    rtt_model   : optional injected base model replacing the d/100 term
                  (expected rtt = slope * rtt_model.base_ms(vp, point))

    Returns
    -------
    float -- total negative log-likelihood (non-negative)
    """
    if not constraints:
        return 0.0

    lat, lon = point
    total = 0.0
    for (vp_lat, vp_lon), sigma_ms, rtt_ms in constraints:
        if rtt_model is None:
            dist_km = get_distance((lat, lon), (vp_lat, vp_lon))
            expected_rtt = slope * dist_km / KM_PER_MS
        else:
            expected_rtt = slope * rtt_model.base_ms((vp_lat, vp_lon), (lat, lon))
        total += residual_nll(rtt_ms - expected_rtt, sigma_ms, noise_model)
    return total


def mean_absolute_residual(point: LatLon, constraints: list[ProbConstraint],
                           slope: float = 1.0,
                           rtt_model: 'RttModel' = None) -> float:
    """
    Mean absolute RTT residual at `point` across all constraints.

    Replaces `get_region_size()` as the uncertainty proxy: lower = more
    confident. Returns `inf` when there are no constraints.

    Parameters
    ----------
    point       : (lat, lon) candidate target location
    constraints : list of (vp_loc, sigma_ms, rtt_ms)
    slope       : assumed routing-overhead factor, expected rtt = slope * d/100

    Returns
    -------
    float -- mean |rtt - slope * d/100| in ms
    """
    if not constraints:
        return float('inf')

    lat, lon = point
    residuals = []
    for (vp_lat, vp_lon), _sigma, rtt_ms in constraints:
        if rtt_model is None:
            dist_km = get_distance((lat, lon), (vp_lat, vp_lon))
            expected_rtt = slope * dist_km / KM_PER_MS
        else:
            expected_rtt = slope * rtt_model.base_ms((vp_lat, vp_lon), (lat, lon))
        residuals.append(abs(rtt_ms - expected_rtt))
    return sum(residuals) / len(residuals)


# ---------------------------------------------------------------------------
# Per-VP routing statistics: mean overhead (mu) and noise (sigma)
# ---------------------------------------------------------------------------

def _collect_vp_residuals(
    target_data: dict[str, Any],
) -> dict[str, list[float]]:
    """
    For each VP (as source), collect residuals  rtt - d(vp, peer)/100
    over all mesh peers whose locations are known.
    """
    address_to_loc: dict[str, LatLon] = target_data.get('address_to_loc', {})
    loc_loc_meas = target_data.get('loc_loc_meas', {})

    residuals_by_vp: dict[str, list[float]] = {}
    for src, dsts in loc_loc_meas.items():
        src_loc = address_to_loc.get(src)
        if src_loc is None:
            continue
        for dst, rtts in dsts.items():
            dst_loc = address_to_loc.get(dst)
            if dst_loc is None or not rtts:
                continue
            min_rtt = min(rtts)
            true_dist_km = get_distance(src_loc, dst_loc)
            residual = min_rtt - true_dist_km / KM_PER_MS
            residuals_by_vp.setdefault(src, []).append(residual)

    return residuals_by_vp


def compute_per_vp_mu(
    target_data: dict[str, Any],
    min_peers: int = MIN_PEERS_FOR_SIGMA,
    global_fallback_ms: float = 0.0,
) -> dict[str, float]:
    """
    Estimate per-VP mean routing overhead (mu) from the dense mesh.

    For each VP, compute the mean residual  rtt - d(vp, peer)/100  across
    all peers with known locations.  VPs with fewer than `min_peers` pairs
    fall back to `global_fallback_ms` (default 0 — assume SOL-exact).

    Use the returned mu to correct observed RTTs before passing them to the
    Gaussian NLL:  corrected_rtt = rtt - mu_vp.

    Parameters
    ----------
    target_data         : dict with 'address_to_loc' and 'loc_loc_meas'
    min_peers           : minimum peer count needed to trust the fitted mu
    global_fallback_ms  : mu used when a VP has too few peers

    Returns
    -------
    dict[str, float] -- VP subnet/24 → mu_ms
    """
    residuals_by_vp = _collect_vp_residuals(target_data)
    address_to_loc: dict[str, LatLon] = target_data.get('address_to_loc', {})

    mu_map: dict[str, float] = {}
    for vp in address_to_loc:
        resids = residuals_by_vp.get(vp, [])
        if len(resids) >= min_peers:
            mu_map[vp] = float(np.mean(resids))
        else:
            mu_map[vp] = global_fallback_ms

    return mu_map


def compute_per_vp_sigma(
    target_data: dict[str, Any],
    min_peers: int = MIN_PEERS_FOR_SIGMA,
    global_fallback_ms: float = GLOBAL_SIGMA_MS,
) -> dict[str, float]:
    """
    Estimate per-VP routing noise (sigma) from the dense mesh.

    For each VP, collect residuals  rtt - d(vp, peer)/100  over all mesh
    peers whose locations are known, then take the standard deviation.
    VPs with fewer than `min_peers` neighbours fall back to `global_fallback_ms`.

    Parameters
    ----------
    target_data         : dict with 'address_to_loc' and 'loc_loc_meas'
    min_peers           : minimum number of peers to trust a VP's fitted sigma
    global_fallback_ms  : sigma used when a VP has too few peers

    Returns
    -------
    dict[str, float] -- VP subnet/24 → sigma_ms
    """
    residuals_by_vp = _collect_vp_residuals(target_data)
    address_to_loc: dict[str, LatLon] = target_data.get('address_to_loc', {})

    sigma_map: dict[str, float] = {}
    all_vps = set(address_to_loc.keys())

    for vp in all_vps:
        resids = residuals_by_vp.get(vp, [])
        if len(resids) >= min_peers:
            sigma = float(np.std(resids, ddof=1))
            # Clamp: never let sigma collapse to near-zero (numerical safety)
            sigma_map[vp] = max(sigma, 0.5)
        else:
            sigma_map[vp] = global_fallback_ms

    return sigma_map


# ---------------------------------------------------------------------------
# Vectorised haversine for grid integration (Path 2 building block)
# ---------------------------------------------------------------------------

def haversine_grid(vp_lat: float, vp_lon: float, lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
    """
    Great-circle distance (km) from one VP to every cell in a lat/lon grid.

    Parameters
    ----------
    vp_lat, vp_lon : VP coordinates in degrees
    lats, lons     : 2-D numpy arrays of the same shape (e.g. from np.meshgrid)

    Returns
    -------
    np.ndarray of the same shape as `lats`, distances in km
    """
    R = 6371.0
    phi1 = math.radians(vp_lat)
    phi2 = np.radians(lats)
    dphi = np.radians(lats - vp_lat)
    dlam = np.radians(lons - vp_lon)
    a = np.sin(dphi / 2) ** 2 + math.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2) ** 2
    return 2.0 * R * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))


def posterior_mean_grid(
    constraints: list[ProbConstraint],
    lat_resolution: float = 1.0,
    lon_resolution: float = 1.0,
    rtt_model: 'RttModel' = None,
) -> LatLon:
    """
    Posterior mean location via grid integration (Path 2).

    Discretises Earth onto a lat/lon grid, evaluates the Gaussian log-posterior
    at each cell using the vectorised haversine, softmaxes to weights, and
    returns the weighted centroid.

    The weighted mean is guaranteed to stay within [-90,90] x [-180,180] and
    never collapses to a single wrong peak — multimodal posteriors spread the
    estimate between modes rather than picking one arbitrarily.

    Parameters
    ----------
    constraints      : list of (vp_loc, sigma_ms, rtt_ms)
    lat_resolution   : grid spacing in degrees (default 1° → 180 rows)
    lon_resolution   : grid spacing in degrees (default 1° → 360 cols)

    Returns
    -------
    (lat, lon) posterior mean
    """
    lat_grid = np.arange(-90.0, 90.0 + lat_resolution, lat_resolution)
    lon_grid = np.arange(-180.0, 180.0 + lon_resolution, lon_resolution)
    LATS, LONS = np.meshgrid(lat_grid, lon_grid, indexing='ij')

    log_p = np.zeros_like(LATS)
    for (vp_lat, vp_lon), sigma_ms, rtt_ms in constraints:
        if rtt_model is None:
            dist_km = haversine_grid(vp_lat, vp_lon, LATS, LONS)
            expected_rtt = dist_km / KM_PER_MS
        else:
            expected_rtt = rtt_model.base_ms_grid((vp_lat, vp_lon), LATS, LONS)
        log_p += -((rtt_ms - expected_rtt) ** 2) / (2.0 * sigma_ms ** 2)

    # Numerical stability: subtract max before exp
    log_p -= log_p.max()
    weights = np.exp(log_p)
    weights /= weights.sum()

    lat_est = float(np.sum(weights * LATS))
    lon_est = float(np.sum(weights * LONS))
    return lat_est, lon_est


# ---------------------------------------------------------------------------
# Injectable base RTT models:  what replaces the d / KM_PER_MS term
# ---------------------------------------------------------------------------
# Every estimator in this project converts VP->candidate geometry into an
# expected baseline RTT.  Historically that conversion was hardwired as
# geodesic-at-fiber-speed (d / KM_PER_MS, scaled by a mode-level slope or
# added to per-node offsets).  RttModel makes the conversion injectable so
# the fiber-atlas floor (internet_gmaps) can replace the geodesic term:
# the floor is a function of the two COORDINATES, not of their distance —
# a fiber isochrone is not a circle.
#
# Contract: base_ms(vp_loc, loc) returns the SOL-equivalent baseline in ms,
# i.e. exactly the quantity d / KM_PER_MS used to be.  Mode-level slopes
# (gaussian / em_gaussian) multiply it; additive offsets add to it.  Every
# call site keeps a `rtt_model=None` default that preserves the original
# geodesic expression bit-for-bit, so existing behavior and tests are
# untouched unless a model is passed.

class RttModel:
    """Base predictive RTT between two points (the d/KM_PER_MS term)."""

    def base_ms(self, vp_loc: LatLon, loc: LatLon) -> float:
        raise NotImplementedError

    def base_ms_many(self, vp_loc: LatLon, locs: list[LatLon]) -> list[float]:
        return [self.base_ms(vp_loc, loc) for loc in locs]

    def base_ms_rows(self, vp_locs: list[LatLon], loc: LatLon) -> list[float]:
        """base_ms for many VPs at one point (subclasses may batch)."""
        return [self.base_ms(vp_loc, loc) for vp_loc in vp_locs]

    def base_ms_grid(self, vp_loc: LatLon, lats: np.ndarray,
                     lons: np.ndarray) -> np.ndarray:
        out = np.empty_like(lats, dtype=float)
        it = np.nditer(lats, flags=['multi_index'])
        for _ in it:
            idx = it.multi_index
            out[idx] = self.base_ms(vp_loc, (float(lats[idx]), float(lons[idx])))
        return out


class GeodesicRtt(RttModel):
    """Today's behavior as an object: slope × geodesic_km / KM_PER_MS."""

    def __init__(self, slope: float = 1.0) -> None:
        self.slope = slope

    def base_ms(self, vp_loc: LatLon, loc: LatLon) -> float:
        return self.slope * get_distance(vp_loc, loc) / KM_PER_MS

    def base_ms_grid(self, vp_loc: LatLon, lats: np.ndarray,
                     lons: np.ndarray) -> np.ndarray:
        return self.slope * haversine_grid(vp_loc[0], vp_loc[1], lats, lons) / KM_PER_MS


# Process-global estimator registry: fiber floor estimators hold tens of MB
# of per-VP distance fields, which must never ride along when a region (and
# its model) is pickled to a utility-evaluation worker.  A FiberFloorRtt
# built with `estimator_factory=` pickles as (factory, token) only; each
# process builds the estimator once, on first use, and shares it here.
_FIBER_ESTIMATORS: dict[str, Any] = {}

DEFAULT_FIBER_SLOPE = 1.3   # validated inflation over the raw floor (atlas)


class FiberFloorRtt(RttModel):
    """slope × fiber_floor(vp, loc) + offset_ms, floors from an
    internet_gmaps FloorEstimator / PolicyFloorEstimator.

    `vp_locs` must be the (lat, lon) list aligned with the estimator's VP
    rows — base_ms looks its vp_loc argument up in that list.  Floors are
    memoized per exact query point (keys are exact so a cached neighbor
    can never stand in for the queried point).  When the estimator
    supports VP-subset queries (PolicyFloorEstimator.floor_ms_subset),
    only the VPs actually asked about are computed — with hundreds of
    VPs, a region's MAP loop touches ~20 of them; base_ms_rows batches
    one subset lookup per optimizer point.

    inf handling: a PolicyFloorEstimator raises NoRouteError where the
    policy allows no route (a policy bug — build the estimator with
    no_route="open" for the legacy OPEN-floor fallback).  If the OPEN
    floor itself is inf (the point is beyond lastmile_km_max of all
    mapped infrastructure), base_ms falls back to the geodesic at fiber
    speed — the only finite admissible bound left (floor ≥ geodesic
    always); the atlas simply has nothing to say there.

    Pickling: pass `estimator_factory` (a picklable zero-arg callable
    rebuilding the estimator) and the instance pickles without the
    estimator or caches; workers rebuild once per process (keyed by
    `cache_token`).  Without a factory the estimator itself is pickled —
    fine for small graphs and tests only.
    """

    def __init__(self, estimator=None, vp_locs: list[LatLon] = (),
                 slope: float = DEFAULT_FIBER_SLOPE, offset_ms: float = 0.0,
                 estimator_factory=None, cache_token: str = None,
                 prior_mu_ms: float = None) -> None:
        if estimator is None and estimator_factory is None:
            raise ValueError("need an estimator or an estimator_factory")
        self.slope = slope
        self.offset_ms = offset_ms
        # Additive-model prior mean to use over this base (None = global
        # default).  Production fiber models pass 0.0: the sloped floor
        # already covers typical overhead, so the honest baseline is "no
        # model correction needed" and a large fitted offset must be
        # EARNED by many measurements, not granted by 3.
        self.prior_mu_ms = prior_mu_ms
        # () = derive from the estimator (vp_lat/vp_lon or vp_locs attrs)
        # on first use, so pickles carry no per-VP payload
        self.vp_locs = [(float(a), float(b)) for a, b in vp_locs]
        self._factory = estimator_factory
        self._token = cache_token or (repr(estimator_factory)
                                      if estimator_factory else None)
        self._estimator = estimator
        self._vp_idx: dict = None
        self._floor_cache: dict[tuple, dict[int, float]] = {}

    _FLOOR_CACHE_MAX = 200_000

    @property
    def estimator(self):
        if self._estimator is None:
            est = _FIBER_ESTIMATORS.get(self._token)
            if est is None:
                est = self._factory()
                _FIBER_ESTIMATORS[self._token] = est
            self._estimator = est
        return self._estimator

    @property
    def vp_idx(self):
        if self._vp_idx is None:
            locs = self.vp_locs
            if not locs:
                est = self.estimator
                if hasattr(est, 'vp_lat'):
                    locs = list(zip(map(float, est.vp_lat),
                                    map(float, est.vp_lon)))
                else:
                    locs = [(float(a), float(b)) for a, b in est.vp_locs]
            self._vp_idx = {(round(a, 6), round(b, 6)): i
                            for i, (a, b) in enumerate(locs)}
        return self._vp_idx

    def __getstate__(self):
        state = self.__dict__.copy()
        state['_floor_cache'] = {}
        state['_vp_idx'] = None
        state.pop('_node_rows', None)
        if self._factory is not None:
            state['_estimator'] = None
        return state

    def _floors_at(self, loc: LatLon, vs: list[int]) -> dict[int, float]:
        """Floors for the given VP rows at loc, via the per-point memo.
        {v: floor_ms}; missing rows are fetched in one subset call when
        the estimator supports it, else via one full-vector call."""
        key = (float(loc[0]), float(loc[1]))
        entry = self._floor_cache.get(key)
        if entry is None:
            if len(self._floor_cache) >= self._FLOOR_CACHE_MAX:
                self._floor_cache.clear()
            entry = self._floor_cache[key] = {}
        missing = [v for v in vs if v not in entry]
        if missing:
            est = self.estimator
            if hasattr(est, 'floor_ms_subset'):
                vals = est.floor_ms_subset(loc[0], loc[1], missing)
                for v, f in zip(missing, vals):
                    entry[v] = float(f)
            else:
                for v, f in enumerate(np.asarray(est.floor_ms(loc[0], loc[1]))):
                    entry[v] = float(f)
        return entry

    def _vp_row(self, vp_loc: LatLon) -> int:
        return self.vp_idx[(round(vp_loc[0], 6), round(vp_loc[1], 6))]

    # -- graph-node search support (map-matching to the fiber atlas) -----
    # The estimator's per-VP fields already hold every VP's floor to every
    # graph node, so the MAP location step can score ALL nodes exactly
    # (vectorized) instead of letting Nelder-Mead wander into open ocean,
    # where off-infrastructure points fall back to smooth geodesic
    # predictions the data can never falsify.

    @property
    def supports_node_search(self) -> bool:
        est = self.estimator
        return (hasattr(est, 'graph')
                and (hasattr(est, '_field') or hasattr(est, '_fields')))

    def node_grid(self):
        g = self.estimator.graph
        return g.node_lat, g.node_lon

    def _node_floor_row(self, v: int) -> np.ndarray:
        """Per-VP OPEN floor to every graph node (candidate-generation
        quality: open ≤ policy floors; final candidates are rescored with
        the full policy base_ms).  Cached per VP per process."""
        rows = getattr(self, '_node_rows', None)
        if rows is None:
            rows = self._node_rows = {}
        f = rows.get(v)
        if f is None:
            est = self.estimator
            if hasattr(est, '_field'):        # PolicyFloorEstimator
                f = np.asarray(est._field(v, est._OPEN_SIG, None),
                               dtype=np.float32)
            else:                             # FloorEstimator
                f = np.asarray(est._fields[v], dtype=np.float32)
            rows[v] = f
        return f

    def node_bases(self, vp_locs: list[LatLon]) -> np.ndarray:
        """(len(vp_locs), n_nodes) matrix of base_ms at every node."""
        rows = np.vstack([self._node_floor_row(self._vp_row(vp))
                          for vp in vp_locs])
        return self.slope * rows + self.offset_ms

    def on_infrastructure(self, loc: LatLon) -> bool:
        """Within last-mile reach of any mapped fiber node — the model's
        domain.  Beyond it every prediction is the geodesic fallback."""
        nlat, nlon = self.node_grid()
        km = haversine_grid(loc[0], loc[1], nlat, nlon)
        return bool(km.min() <= getattr(self.estimator,
                                        'lastmile_km_max', 300.0))

    def base_ms(self, vp_loc: LatLon, loc: LatLon) -> float:
        v = self._vp_row(vp_loc)
        f = self._floors_at(loc, [v])[v]
        if not math.isfinite(f):
            f = get_distance(vp_loc, loc) / KM_PER_MS
        return self.slope * f + self.offset_ms

    def base_ms_rows(self, vp_locs: list[LatLon], loc: LatLon) -> list[float]:
        """base_ms for many VPs at one point — one subset lookup total."""
        vs = [self._vp_row(vp) for vp in vp_locs]
        floors = self._floors_at(loc, sorted(set(vs)))
        out = []
        for vp_loc, v in zip(vp_locs, vs):
            f = floors[v]
            if not math.isfinite(f):
                f = get_distance(vp_loc, loc) / KM_PER_MS
            out.append(self.slope * f + self.offset_ms)
        return out


# ---------------------------------------------------------------------------
# Additive two-way overhead model:  rtt = SOL + X_src + X_dst
# ---------------------------------------------------------------------------
# X_src ~ N(mu_src, sigma_src²), X_dst ~ N(mu_dst, sigma_dst²) — per-node
# overhead means AND per-node noise. The sigma_dst of a destination with
# pathological routing grows large, which is the honest "stop spending pings
# here" signal a selection algorithm can consume.
#
# Identifiability note: only the sums mu_src + mu_dst are observable (gauge
# freedom: add c to every source, subtract from every destination). The
# symmetric priors anchor the split; consumers should rely on predictions
# and CENTERED offsets, which are gauge-invariant.

# Prior per-node overhead mean.  5 ms dates from the bare-geodesic era,
# where offsets legitimately absorbed real path overhead; with a good
# base model (fiber ×1.3) the honest prior is ~0 — "the atlas needs no
# correction until measurements prove otherwise" (GEOLOC_PRIOR_MU_MS=0
# for such runs; pair with a stronger GEOLOC_PRIOR_STRENGTH so a few
# residuals can't claim a large model fix).
ADDITIVE_PRIOR_MU_MS = float(os.environ.get('GEOLOC_PRIOR_MU_MS', '5.0'))


def _prior_mu_for(rtt_model) -> float:
    """Per-base-model prior mean: an RttModel may carry its own
    `prior_mu_ms` (production fiber models set 0.0 — "the atlas needs no
    correction until measurements prove otherwise"); None defers to the
    global constant (bare geodesic keeps 5 ms — offsets there legitimately
    absorb real overhead)."""
    pm = getattr(rtt_model, 'prior_mu_ms', None)
    return ADDITIVE_PRIOR_MU_MS if pm is None else float(pm)
ADDITIVE_PRIOR_VAR_MS2 = 25.0     # prior per-node noise variance (5ms)²
# Pseudo-observations anchoring each node's (μ, σ²) at the prior — the
# L2/ridge strength of the additive fit.  At 2.0 a node with ~15 pairs
# outvotes its prior 7:1, which lets a mislocated target launder its
# position error into a huge fitted offset (measured: err>5000 km
# targets carry median μ̂_dst 27 ms vs 0.5 ms for well-located ones) and
# stop bidding for pings.  GEOLOC_PRIOR_STRENGTH overrides for A/B runs
# (read at import; inherited by spawned workers).
ADDITIVE_PRIOR_STRENGTH = float(os.environ.get('GEOLOC_PRIOR_STRENGTH', '2.0'))
ADDITIVE_VAR_FLOOR_MS2 = 0.04     # (0.2ms)² — don't let variances collapse

# Floor for the fitted per-node mean offsets (ms).  0.0 is the historical
# hard lower-bound assumption: the base term is a physical floor, so
# overhead can only be nonnegative.  A SMALL NEGATIVE value gives the fit
# a soft landing when the base model can OVER-predict — e.g. a sloped
# fiber floor whose ×1.3 slack exceeds the real detour on some paths:
# modest over-prediction is then absorbed by the offset instead of being
# forced into the location estimate (measured failure mode: 47% of long
# dense constraints "impossible" → wholesale MAP displacement).
# Run-level A/B knob: GEOLOC_MU_FLOOR_MS (read at import; inherited by
# spawned worker processes).
ADDITIVE_MU_FLOOR_MS = float(os.environ.get('GEOLOC_MU_FLOOR_MS', '0.0'))

# Graph-node search (map-matching to the fiber atlas) in the additive MAP
# location step — DEFAULT ON for base models that expose per-node floors
# (real atlas estimators; test mocks don't, keeping plumbing pins
# bit-exact).  GEOLOC_NODE_SEARCH=0 restores free Nelder-Mead only.
NODE_SEARCH_DEFAULT = os.environ.get('GEOLOC_NODE_SEARCH', '1') == '1'
NODE_SEARCH_TOP_K = 3


LEARN_ALL = (True, True, True, True)  # (mu_src, var_src, mu_dst, var_dst)


def fit_additive_params(residuals_by_pair: dict, n_iters: int = 8,
                        learn: tuple = LEARN_ALL,
                        mu_floor_ms: float = None,
                        prior_mu_ms: float = None):
    """
    Fit the two-way additive overhead model from SOL residuals.

    residuals_by_pair: {(src, dst): [residual_ms, ...]} where
                       residual = observed rtt − d(src, dst_estimate)/100.
                       (Distances come from ESTIMATED destination locations —
                       honest; no ground truth needed.)

    Returns (mu_src, var_src, mu_dst, var_dst) — dicts keyed by node name.
    Means via alternating shrunk averages (two-way ANOVA style); variances
    via moment matching on the de-meaned residuals, split alternately
    between the source and destination of each pair.

    `learn` = (mu_src, var_src, mu_dst, var_dst) ablation mask: a frozen
    family stays at its prior constant (exactly the value unknown nodes
    fall back to), so the other families' fits absorb what they can.

    `mu_floor_ms` clamps the fitted means (None → ADDITIVE_MU_FLOOR_MS,
    default 0.0 = the historical nonnegative-overhead assumption; see the
    constant's comment for when a small negative floor is warranted).
    """
    if mu_floor_ms is None:
        mu_floor_ms = ADDITIVE_MU_FLOOR_MS
    if prior_mu_ms is None:
        prior_mu_ms = ADDITIVE_PRIOR_MU_MS
    learn_mu_s, learn_var_s, learn_mu_t, learn_var_t = learn
    srcs = sorted({s for s, _ in residuals_by_pair})
    dsts = sorted({t for _, t in residuals_by_pair})
    mu_s = {s: prior_mu_ms for s in srcs}
    mu_t = {t: prior_mu_ms for t in dsts}

    by_src = {s: [(t, rs) for (s2, t), rs in residuals_by_pair.items() if s2 == s]
              for s in srcs}
    by_dst = {t: [(s, rs) for (s, t2), rs in residuals_by_pair.items() if t2 == t]
              for t in dsts}

    for _ in range(n_iters):
        if learn_mu_t:
            for t in dsts:
                num = ADDITIVE_PRIOR_STRENGTH * prior_mu_ms
                den = ADDITIVE_PRIOR_STRENGTH
                for s, rs in by_dst[t]:
                    for r in rs:
                        num += r - mu_s[s]
                        den += 1.0
                mu_t[t] = max(mu_floor_ms, num / den)
        if learn_mu_s:
            for s in srcs:
                num = ADDITIVE_PRIOR_STRENGTH * prior_mu_ms
                den = ADDITIVE_PRIOR_STRENGTH
                for t, rs in by_src[s]:
                    for r in rs:
                        num += r - mu_t[t]
                        den += 1.0
                mu_s[s] = max(mu_floor_ms, num / den)
        if not (learn_mu_s or learn_mu_t):
            break

    # Per-pair excess variance of the de-meaned residuals
    pair_var = {}
    for (s, t), rs in residuals_by_pair.items():
        e = [r - mu_s[s] - mu_t[t] for r in rs]
        pair_var[(s, t)] = sum(x * x for x in e) / len(e)

    var_s = {s: ADDITIVE_PRIOR_VAR_MS2 for s in srcs}
    var_t = {t: ADDITIVE_PRIOR_VAR_MS2 for t in dsts}
    for _ in range(n_iters):
        if learn_var_t:
            for t in dsts:
                num = ADDITIVE_PRIOR_STRENGTH * ADDITIVE_PRIOR_VAR_MS2
                den = ADDITIVE_PRIOR_STRENGTH
                for s, _ in by_dst[t]:
                    num += max(0.0, pair_var[(s, t)] - var_s[s])
                    den += 1.0
                var_t[t] = max(ADDITIVE_VAR_FLOOR_MS2, num / den)
        if learn_var_s:
            for s in srcs:
                num = ADDITIVE_PRIOR_STRENGTH * ADDITIVE_PRIOR_VAR_MS2
                den = ADDITIVE_PRIOR_STRENGTH
                for t, _ in by_src[s]:
                    num += max(0.0, pair_var[(s, t)] - var_t[t])
                    den += 1.0
                var_s[s] = max(ADDITIVE_VAR_FLOOR_MS2, num / den)
        if not (learn_var_s or learn_var_t):
            break

    return mu_s, var_s, mu_t, var_t


def additive_map_location(constraint_rows: list, starts: list[LatLon],
                          rtt_model: 'RttModel' = None) -> LatLon:
    """
    MAP location under the additive model.  constraint_rows are
    (vp_loc, rtt_ms, mean_offset_ms, var_sum_ms2) — expected rtt =
    base + mean_offset, per-measurement weight 1/var_sum, where base is
    d/100 or the injected rtt_model's floor term.

    Nelder-Mead is local, so several starts are tried and the best kept:
    always pass the previous estimate AND the NN anchor (lowest-RTT VP) —
    an early wrong fixed point must not trap later refits.

    Fiber-atlas models additionally get GRAPH-NODE SEARCH (default; kill
    switch GEOLOC_NODE_SEARCH=0): the same objective is evaluated at
    every graph node in one vectorized pass (map-matching — the answer
    must live near mapped fiber), the best node seeds an extra NM start,
    the top nodes compete as candidates under the full policy base, and
    an off-infrastructure winner (where base_ms is the unfalsifiable
    geodesic fallback — how estimates used to end up mid-Pacific) is
    rejected in favor of the best on-infrastructure candidate.
    """
    from scipy.optimize import minimize

    if rtt_model is None:
        def nll(x):
            total = 0.0
            for vp_loc, rtt, mean_off, var_sum in constraint_rows:
                r = rtt - get_distance((x[0], x[1]), vp_loc) / KM_PER_MS - mean_off
                total += r * r / (2.0 * var_sum)
            return total
    else:
        row_vps = [row[0] for row in constraint_rows]

        def nll(x):
            bases = rtt_model.base_ms_rows(row_vps, (x[0], x[1]))
            total = 0.0
            for (vp_loc, rtt, mean_off, var_sum), base in zip(constraint_rows, bases):
                r = rtt - base - mean_off
                total += r * r / (2.0 * var_sum)
            return total

    node_candidates: list[LatLon] = []
    use_nodes = (rtt_model is not None
                 and NODE_SEARCH_DEFAULT
                 and getattr(rtt_model, 'supports_node_search', False)
                 and constraint_rows)
    if use_nodes:
        # Vectorized global scan of the SAME objective over every graph
        # node (open floors — candidate generation; candidates are then
        # rescored by nll(), i.e. the full policy base).
        B = rtt_model.node_bases([row[0] for row in constraint_rows])
        rtts = np.array([row[1] for row in constraint_rows])[:, None]
        offs = np.array([row[2] for row in constraint_rows])[:, None]
        vars_ = np.array([row[3] for row in constraint_rows])[:, None]
        R = rtts - B - offs
        cost = np.where(np.isfinite(R), R * R / (2.0 * vars_), np.inf).sum(axis=0)
        nlat, nlon = rtt_model.node_grid()
        k = min(NODE_SEARCH_TOP_K, len(cost))
        for i in np.argpartition(cost, k - 1)[:k]:
            if np.isfinite(cost[i]):
                node_candidates.append((float(nlat[i]), float(nlon[i])))

    best, best_val = None, float('inf')
    best_oninfra, best_oninfra_val = None, float('inf')
    for start in list(starts) + node_candidates[:1]:
        res = minimize(nll, np.array(start), method='Nelder-Mead',
                       tol=1e-4, options={'maxiter': 500})
        pt, val = res.x, float(res.fun)
        if val < best_val:
            best, best_val = pt, val
        if use_nodes and val < best_oninfra_val:
            p = _normalize_latlon(float(pt[0]), float(pt[1]))
            if rtt_model.on_infrastructure(p):
                best_oninfra, best_oninfra_val = pt, val
    # raw node candidates compete directly, rescored under the policy base
    for p in node_candidates:
        val = float(nll(np.array(p)))
        if val < best_val:
            best, best_val = np.array(p), val
        if val < best_oninfra_val:      # nodes are on-infra by construction
            best_oninfra, best_oninfra_val = np.array(p), val

    if use_nodes and best_oninfra is not None:
        chosen = _normalize_latlon(float(best[0]), float(best[1]))
        if not rtt_model.on_infrastructure(chosen):
            # the free-search winner lives in the geodesic-fallback zone
            # (unfalsifiable terrain) — take the best answer the model can
            # actually speak for
            best = best_oninfra
    return _normalize_latlon(float(best[0]), float(best[1]))


def additive_batch_em(rtts_by_pair: dict, vp_locs: dict,
                      n_iters: int = 4, rtt_model: 'RttModel' = None,
                      learn: tuple = LEARN_ALL,
                      prev_estimates: dict = None,
                      only_targets: set = None,
                      extra_starts: dict = None):
    """
    Fresh batch fit of the additive model: params-first alternation from
    NN-anchored location inits (both paid-for pitfalls baked in — never
    warm-start params, never let a location step run before the parameter
    step has seen residuals against the anchors).

    This is THE estimation path for the additive model.  Incremental
    per-ping location updates ratchet: early MAP steps under prior offsets
    absorb a pathological target's offset into distance, and later refits
    can't win it back (measured: patho μ̂_t 16 vs true 35 after a full
    greedy run whose final batch polish then recovers it).

    rtts_by_pair: {(src, dst): [rtt_ms, ...]}
    vp_locs:      {src: (lat, lon)} — pairs with unknown srcs are ignored.
    rtt_model:    optional injected base model — residuals and the MAP are
                  computed against its floor term instead of d/100 (the
                  per-node offsets then learn slack over the floor).
    learn:        (mu_src, var_src, mu_dst, var_dst) ablation mask, threaded
                  to fit_additive_params — frozen families stay at priors
                  throughout the alternation, so the location steps never
                  see learned values for them.
    prev_estimates: optional {dst: (lat, lon)} from a previous polish —
                  used as location inits (an EXTRA Nelder-Mead start; the
                  NN anchor stays, and params are still fit fresh, per the
                  never-warm-start-params rule).  Multi-start keeps the
                  best NLL, so a stale previous estimate cannot make the
                  fit worse than the fresh path's own starts.
    only_targets: optional subset of targets whose LOCATION is
                  re-optimised (the incremental-polish hot set: dirty or
                  offset-moved targets).  Params are still fit over ALL
                  pairs; other targets keep prev_estimates (or the NN
                  anchor when absent).
    extra_starts: optional {dst: (lat, lon)} of ADDITIONAL Nelder-Mead
                  starts (e.g. the regions' live per-ping estimates).
                  Without it, a target whose incremental estimate escaped
                  an offset-position ridge gets polished only from the NN
                  anchor / previous-polish starts — both possibly in the
                  laundered basin — and the polish OVERWRITES the escape
                  (measured: ping-time error 2,977 km → polished 13,827).
                  Multi-start keeps the best NLL, so extra starts can
                  only help.

    Returns (estimates, mu_s, var_s, mu_t, var_t).
    """
    pairs = {(s, t): rs for (s, t), rs in rtts_by_pair.items()
             if s in vp_locs and rs}
    if not pairs:
        return {}, {}, {}, {}, {}
    prior_mu = _prior_mu_for(rtt_model)

    best: dict[str, tuple[float, str]] = {}
    for (s, t), rs in pairs.items():
        r = min(rs)
        if t not in best or r < best[t][0]:
            best[t] = (r, s)
    nn_est = {t: vp_locs[s] for t, (_, s) in best.items()}
    estimates = dict(nn_est)
    if prev_estimates:
        for t in estimates:
            if t in prev_estimates:
                estimates[t] = tuple(prev_estimates[t])
    reopt = (set(estimates) if only_targets is None
             else set(only_targets) & set(estimates))

    def base(s, t):
        if rtt_model is None:
            return get_distance(vp_locs[s], estimates[t]) / KM_PER_MS
        return rtt_model.base_ms(vp_locs[s], estimates[t])

    mu_s = var_s = mu_t = var_t = {}
    for _ in range(n_iters):
        residuals = {
            (s, t): [r - base(s, t) for r in rs]
            for (s, t), rs in pairs.items()
        }
        mu_s, var_s, mu_t, var_t = fit_additive_params(residuals, learn=learn,
                                                       prior_mu_ms=prior_mu)
        for t in estimates:
            if t not in reopt:
                continue
            rows = [(vp_locs[s], r, mu_s[s] + mu_t[t], var_s[s] + var_t[t])
                    for (s, t2), rs in pairs.items() if t2 == t
                    for r in rs]
            starts = [estimates[t], nn_est[t]]
            if extra_starts and t in extra_starts:
                starts.append(tuple(extra_starts[t]))
            estimates[t] = additive_map_location(rows, starts,
                                                 rtt_model=rtt_model)

    if extra_starts:
        # Basin arbitration.  The alternation is params-first: offsets are
        # fitted against the INITIAL estimates, so a position hypothesis
        # from another basin (a region's live estimate that escaped an
        # offset-position ridge) can never win the location step — the
        # judging parameters belong to the incumbent basin.  Give each
        # hypothesis its own self-consistent offset (closed-form μ_t refit
        # with everything else fixed) and charge the prior's
        # pseudo-observation cost, so a laundered 27 ms offset finally
        # pays for its size; keep the better penalized NLL per target.
        def penalized_nll(t, loc):
            rows = [(s, r) for (s, t2), rs in pairs.items() if t2 == t
                    for r in rs]
            if not rows:
                return float('inf'), prior_mu
            num = ADDITIVE_PRIOR_STRENGTH * prior_mu
            den = ADDITIVE_PRIOR_STRENGTH
            res = []
            for s, r in rows:
                b = (get_distance(vp_locs[s], loc) / KM_PER_MS
                     if rtt_model is None else rtt_model.base_ms(vp_locs[s], loc))
                res.append((s, r - b))
                num += r - b - mu_s[s]
                den += 1.0
            mu = max(ADDITIVE_MU_FLOOR_MS, num / den)
            nll = sum((e - mu_s[s] - mu) ** 2 / (2.0 * (var_s[s] + var_t[t]))
                      for s, e in res)
            nll += (ADDITIVE_PRIOR_STRENGTH * (mu - prior_mu) ** 2
                    / (2.0 * var_t[t]))
            return nll, mu
        for t in reopt:
            if t not in extra_starts:
                continue
            live = tuple(extra_starts[t])
            nll_pol, _ = penalized_nll(t, estimates[t])
            nll_live, mu_live = penalized_nll(t, live)
            if nll_live < nll_pol:
                estimates[t] = live
                mu_t[t] = mu_live

    return estimates, mu_s, var_s, mu_t, var_t


class AdditiveLatencyModel:
    """
    Shared cross-target state for the additive two-way model — the
    "LatencyModel" object the greedy needs because X_src is pooled across
    ALL targets while FeasibleRegions are per-target.

    Owns the accumulated raw measurements and the fitted per-node
    (μ, σ²).  `refit` recomputes SOL residuals against the callers'
    CURRENT location estimates (honest: no ground truth) and re-runs
    `fit_additive_params` from its internal prior inits — deliberately
    NOT warm-started: carrying early-budget fixed points forward degraded
    full-budget error ~2× in the budget sweep.

    Consumers:
      predict(src, dst, dist_km) → (expected_rtt_ms, var_ms2)  with
          expected rtt = d/100 + μ̂_s + μ̂_t and var = σ̂_s² + σ̂_t².
          Unknown nodes fall back to the priors.
      predict_at(src, dst, src_loc, loc) → same, but the base term comes
          from the injected rtt_model when one is set (coordinates, not
          distance — a fiber floor is not a function of d).
      sigma_dst(dst) → σ̂_t, the "stop sinking budget here" signal.
    """

    def __init__(self, rtt_model: 'RttModel' = None,
                 learn: tuple = LEARN_ALL) -> None:
        self.rtt_model = rtt_model
        # Per-base-model prior mean (production fiber = 0: trust the atlas)
        self.prior_mu_ms = _prior_mu_for(rtt_model)
        # (mu_src, var_src, mu_dst, var_dst) ablation mask — frozen
        # families stay at their prior constants across refits.
        self.learn = learn
        self.rtts_by_pair: dict[tuple[str, str], list[float]] = {}
        self.mu_s: dict[str, float] = {}
        self.var_s: dict[str, float] = {}
        self.mu_t: dict[str, float] = {}
        self.var_t: dict[str, float] = {}

    def record(self, src: str, dst: str, rtts: list[float]) -> None:
        self.rtts_by_pair.setdefault((src, dst), []).extend(rtts)

    def base_ms(self, src_loc: LatLon, loc: LatLon) -> float:
        """The model's baseline RTT term between two points (d/100, or the
        injected rtt_model's floor)."""
        if self.rtt_model is None:
            return get_distance(src_loc, loc) / KM_PER_MS
        return self.rtt_model.base_ms(src_loc, loc)

    def refit(self, vp_locs: dict[str, LatLon],
              estimates: dict[str, LatLon]) -> None:
        residuals = {
            (s, t): [r - self.base_ms(vp_locs[s], estimates[t]) for r in rs]
            for (s, t), rs in self.rtts_by_pair.items()
            if t in estimates and s in vp_locs
        }
        if residuals:
            self.mu_s, self.var_s, self.mu_t, self.var_t = \
                fit_additive_params(residuals, learn=self.learn,
                                    prior_mu_ms=self.prior_mu_ms)

    def mean_offset(self, src: str, dst: str) -> float:
        return (self.mu_s.get(src, self.prior_mu_ms)
                + self.mu_t.get(dst, self.prior_mu_ms))

    def var_sum(self, src: str, dst: str) -> float:
        return (self.var_s.get(src, ADDITIVE_PRIOR_VAR_MS2)
                + self.var_t.get(dst, ADDITIVE_PRIOR_VAR_MS2))

    def predict(self, src: str, dst: str, dist_km: float) -> tuple[float, float]:
        return (dist_km / KM_PER_MS + self.mean_offset(src, dst),
                self.var_sum(src, dst))

    def predict_at(self, src: str, dst: str, src_loc: LatLon,
                   loc: LatLon) -> tuple[float, float]:
        return (self.base_ms(src_loc, loc) + self.mean_offset(src, dst),
                self.var_sum(src, dst))

    def sigma_dst(self, dst: str) -> float:
        return math.sqrt(self.var_t.get(dst, ADDITIVE_PRIOR_VAR_MS2))
