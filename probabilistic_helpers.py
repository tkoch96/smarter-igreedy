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

    inf handling: a PolicyFloorEstimator already falls back to the OPEN
    floor where the policy allows no route.  If the OPEN floor itself is
    inf (the point is beyond lastmile_km_max of all mapped infrastructure),
    base_ms falls back to the geodesic at fiber speed — the only finite
    admissible bound left (floor ≥ geodesic always); the atlas simply has
    nothing to say there.

    Pickling: pass `estimator_factory` (a picklable zero-arg callable
    rebuilding the estimator) and the instance pickles without the
    estimator or caches; workers rebuild once per process (keyed by
    `cache_token`).  Without a factory the estimator itself is pickled —
    fine for small graphs and tests only.
    """

    def __init__(self, estimator=None, vp_locs: list[LatLon] = (),
                 slope: float = DEFAULT_FIBER_SLOPE, offset_ms: float = 0.0,
                 estimator_factory=None, cache_token: str = None) -> None:
        if estimator is None and estimator_factory is None:
            raise ValueError("need an estimator or an estimator_factory")
        self.slope = slope
        self.offset_ms = offset_ms
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

ADDITIVE_PRIOR_MU_MS = 5.0        # prior per-node overhead mean
ADDITIVE_PRIOR_VAR_MS2 = 25.0     # prior per-node noise variance (5ms)²
ADDITIVE_PRIOR_STRENGTH = 2.0     # pseudo-observations anchoring each node
ADDITIVE_VAR_FLOOR_MS2 = 0.04     # (0.2ms)² — don't let variances collapse


def fit_additive_params(residuals_by_pair: dict, n_iters: int = 8):
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
    """
    srcs = sorted({s for s, _ in residuals_by_pair})
    dsts = sorted({t for _, t in residuals_by_pair})
    mu_s = {s: ADDITIVE_PRIOR_MU_MS for s in srcs}
    mu_t = {t: ADDITIVE_PRIOR_MU_MS for t in dsts}

    by_src = {s: [(t, rs) for (s2, t), rs in residuals_by_pair.items() if s2 == s]
              for s in srcs}
    by_dst = {t: [(s, rs) for (s, t2), rs in residuals_by_pair.items() if t2 == t]
              for t in dsts}

    for _ in range(n_iters):
        for t in dsts:
            num = ADDITIVE_PRIOR_STRENGTH * ADDITIVE_PRIOR_MU_MS
            den = ADDITIVE_PRIOR_STRENGTH
            for s, rs in by_dst[t]:
                for r in rs:
                    num += r - mu_s[s]
                    den += 1.0
            mu_t[t] = max(0.0, num / den)
        for s in srcs:
            num = ADDITIVE_PRIOR_STRENGTH * ADDITIVE_PRIOR_MU_MS
            den = ADDITIVE_PRIOR_STRENGTH
            for t, rs in by_src[s]:
                for r in rs:
                    num += r - mu_t[t]
                    den += 1.0
            mu_s[s] = max(0.0, num / den)

    # Per-pair excess variance of the de-meaned residuals
    pair_var = {}
    for (s, t), rs in residuals_by_pair.items():
        e = [r - mu_s[s] - mu_t[t] for r in rs]
        pair_var[(s, t)] = sum(x * x for x in e) / len(e)

    var_s = {s: ADDITIVE_PRIOR_VAR_MS2 for s in srcs}
    var_t = {t: ADDITIVE_PRIOR_VAR_MS2 for t in dsts}
    for _ in range(n_iters):
        for t in dsts:
            num = ADDITIVE_PRIOR_STRENGTH * ADDITIVE_PRIOR_VAR_MS2
            den = ADDITIVE_PRIOR_STRENGTH
            for s, _ in by_dst[t]:
                num += max(0.0, pair_var[(s, t)] - var_s[s])
                den += 1.0
            var_t[t] = max(ADDITIVE_VAR_FLOOR_MS2, num / den)
        for s in srcs:
            num = ADDITIVE_PRIOR_STRENGTH * ADDITIVE_PRIOR_VAR_MS2
            den = ADDITIVE_PRIOR_STRENGTH
            for t, _ in by_src[s]:
                num += max(0.0, pair_var[(s, t)] - var_t[t])
                den += 1.0
            var_s[s] = max(ADDITIVE_VAR_FLOOR_MS2, num / den)

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

    best, best_val = None, float('inf')
    for start in starts:
        res = minimize(nll, np.array(start), method='Nelder-Mead',
                       tol=1e-4, options={'maxiter': 500})
        if res.fun < best_val:
            best, best_val = res.x, res.fun
    return _normalize_latlon(float(best[0]), float(best[1]))


def additive_batch_em(rtts_by_pair: dict, vp_locs: dict,
                      n_iters: int = 4, rtt_model: 'RttModel' = None):
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

    Returns (estimates, mu_s, var_s, mu_t, var_t).
    """
    pairs = {(s, t): rs for (s, t), rs in rtts_by_pair.items()
             if s in vp_locs and rs}
    if not pairs:
        return {}, {}, {}, {}, {}

    best: dict[str, tuple[float, str]] = {}
    for (s, t), rs in pairs.items():
        r = min(rs)
        if t not in best or r < best[t][0]:
            best[t] = (r, s)
    nn_est = {t: vp_locs[s] for t, (_, s) in best.items()}
    estimates = dict(nn_est)

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
        mu_s, var_s, mu_t, var_t = fit_additive_params(residuals)
        for t in estimates:
            rows = [(vp_locs[s], r, mu_s[s] + mu_t[t], var_s[s] + var_t[t])
                    for (s, t2), rs in pairs.items() if t2 == t
                    for r in rs]
            estimates[t] = additive_map_location(rows, [estimates[t], nn_est[t]],
                                                 rtt_model=rtt_model)

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

    def __init__(self, rtt_model: 'RttModel' = None) -> None:
        self.rtt_model = rtt_model
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
                fit_additive_params(residuals)

    def mean_offset(self, src: str, dst: str) -> float:
        return (self.mu_s.get(src, ADDITIVE_PRIOR_MU_MS)
                + self.mu_t.get(dst, ADDITIVE_PRIOR_MU_MS))

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
