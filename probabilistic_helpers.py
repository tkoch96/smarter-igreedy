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

from utils import LatLon, get_distance

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
                 noise_model: str = GAUSSIAN_NOISE) -> float:
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

    Returns
    -------
    float -- total negative log-likelihood (non-negative)
    """
    if not constraints:
        return 0.0

    lat, lon = point
    total = 0.0
    for (vp_lat, vp_lon), sigma_ms, rtt_ms in constraints:
        dist_km = get_distance((lat, lon), (vp_lat, vp_lon))
        expected_rtt = slope * dist_km / KM_PER_MS
        total += residual_nll(rtt_ms - expected_rtt, sigma_ms, noise_model)
    return total


def mean_absolute_residual(point: LatLon, constraints: list[ProbConstraint],
                           slope: float = 1.0) -> float:
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
        dist_km = get_distance((lat, lon), (vp_lat, vp_lon))
        expected_rtt = slope * dist_km / KM_PER_MS
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
        dist_km = haversine_grid(vp_lat, vp_lon, LATS, LONS)
        expected_rtt = dist_km / KM_PER_MS
        log_p += -((rtt_ms - expected_rtt) ** 2) / (2.0 * sigma_ms ** 2)

    # Numerical stability: subtract max before exp
    log_p -= log_p.max()
    weights = np.exp(log_p)
    weights /= weights.sum()

    lat_est = float(np.sum(weights * LATS))
    lon_est = float(np.sum(weights * LONS))
    return lat_est, lon_est
