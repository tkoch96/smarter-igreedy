"""
Offline analysis of the RTT vs geographic distance relationship in the
real RIPE Atlas mesh.

Fits multiple models, diagnoses per-VP heterogeneity, and recommends
the best generative model for the probabilistic geolocator.

Saves figures to figures/analysis_*.pdf
"""

import os, pickle, sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from scipy import stats
from scipy.optimize import curve_fit

from utils import CACHE_DIR, FIG_DIR, get_distance, LatLon

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

def load_pairs(n_sample=None, seed=0):
    """
    Returns arrays: distances (km), rtts (ms), src_ids, dst_ids.
    If n_sample is set, draws that many pairs uniformly at random.
    """
    cache_fn = os.path.join(CACHE_DIR, 'cached_target_data.pkl')
    data = pickle.load(open(cache_fn, 'rb'))
    loc  = data['address_to_loc']
    meas = data['loc_loc_meas']

    dists, rtts, srcs = [], [], []
    for src, dsts in meas.items():
        if src not in loc:
            continue
        for dst, rtt_val in dsts.items():
            if dst not in loc:
                continue
            # cache stores bare float; pipeline wraps in list — handle both
            if isinstance(rtt_val, list):
                rtt = min(rtt_val)
            else:
                rtt = float(rtt_val)
            if rtt <= 0:
                continue
            d = get_distance(loc[src], loc[dst])
            if d < 1:          # same-city pairs: skip (distance noise dominates)
                continue
            dists.append(d)
            rtts.append(rtt)
            srcs.append(src)

    dists = np.array(dists)
    rtts  = np.array(rtts)
    srcs  = np.array(srcs)

    if n_sample is not None and n_sample < len(dists):
        rng = np.random.default_rng(seed)
        idx = rng.choice(len(dists), n_sample, replace=False)
        dists, rtts, srcs = dists[idx], rtts[idx], srcs[idx]

    print(f"Loaded {len(dists):,} pairs  "
          f"(dist: {dists.min():.0f}–{dists.max():.0f} km  "
          f"rtt: {rtts.min():.1f}–{np.percentile(rtts,99):.1f} ms p99)")
    return dists, rtts, srcs, data['address_to_loc'], data['loc_loc_meas']


# ---------------------------------------------------------------------------
# Model definitions
# ---------------------------------------------------------------------------

SOL = 100.0   # km / ms  (speed-of-light floor in fiber)

def model_proportional(d, k):
    """rtt = k * d / 100  (k is routing overhead factor, k≥1)"""
    return k * d / SOL

def model_affine(d, a, b):
    """rtt = a * d + b  (slope + intercept)"""
    return a * d + b

def model_power(d, c, alpha):
    """rtt = c * d^alpha"""
    return c * np.power(d, alpha)

def model_affine_log(log_d, a, b):
    """log(rtt) = a * log(d) + b  — fit in log space"""
    return a * log_d + b


def fit_models(dists, rtts):
    results = {}

    # --- 1. Proportional (force through origin, slope = k/100) ---
    popt, _ = curve_fit(model_proportional, dists, rtts, p0=[2.0])
    pred = model_proportional(dists, *popt)
    resid = rtts - pred
    results['proportional'] = dict(
        label=f'proportional: rtt = {popt[0]:.2f}·d/100',
        params={'k': popt[0]},
        pred=pred, resid=resid,
        rmse=np.sqrt(np.mean(resid**2)),
        mae=np.mean(np.abs(resid)),
    )

    # --- 2. Affine (slope + intercept) ---
    popt2, _ = curve_fit(model_affine, dists, rtts, p0=[1/SOL, 20.0])
    pred2 = model_affine(dists, *popt2)
    resid2 = rtts - pred2
    results['affine'] = dict(
        label=f'affine: rtt = {popt2[0]*1000:.2f}e-3·d + {popt2[1]:.1f}',
        params={'a': popt2[0], 'b': popt2[1]},
        pred=pred2, resid=resid2,
        rmse=np.sqrt(np.mean(resid2**2)),
        mae=np.mean(np.abs(resid2)),
    )

    # --- 3. Power law (fit in log–log space) ---
    mask = (dists > 0) & (rtts > 0)
    log_d = np.log(dists[mask])
    log_r = np.log(rtts[mask])
    popt3, _ = curve_fit(model_affine_log, log_d, log_r, p0=[0.5, 2.0])
    c = np.exp(popt3[1])
    alpha = popt3[0]
    pred3 = model_power(dists, c, alpha)
    resid3 = rtts - pred3
    results['power'] = dict(
        label=f'power law: rtt = {c:.3f}·d^{alpha:.3f}',
        params={'c': c, 'alpha': alpha},
        pred=pred3, resid=resid3,
        rmse=np.sqrt(np.mean(resid3**2)),
        mae=np.mean(np.abs(resid3)),
    )

    # --- 4. SOL floor: rtt = d/100 + overhead, model overhead distribution ---
    overhead = rtts - dists / SOL
    results['overhead_dist'] = dict(
        overhead=overhead,
        mean=overhead.mean(), median=np.median(overhead),
        std=overhead.std(),
        skew=stats.skew(overhead),
        p5=np.percentile(overhead, 5),
        p25=np.percentile(overhead, 25),
        p75=np.percentile(overhead, 75),
        p95=np.percentile(overhead, 95),
        frac_negative=np.mean(overhead < 0),
    )

    return results


def per_vp_stats(dists, rtts, srcs):
    """Per-VP mean overhead, sigma, and fitted slope."""
    vps = np.unique(srcs)
    stats_out = {}
    for vp in vps:
        mask = srcs == vp
        d_vp = dists[mask]
        r_vp = rtts[mask]
        if len(d_vp) < 5:
            continue
        overhead = r_vp - d_vp / SOL
        # Per-VP linear fit: rtt = a_vp * d + b_vp
        try:
            slope, intercept, rval, _, _ = stats.linregress(d_vp, r_vp)
        except Exception:
            slope, intercept, rval = np.nan, np.nan, np.nan
        stats_out[vp] = dict(
            n=len(d_vp),
            mu=overhead.mean(),
            sigma=overhead.std(ddof=1),
            slope=slope,
            intercept=intercept,
            r=rval,
        )
    return stats_out


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def fig1_scatter_and_models(dists, rtts, model_results):
    """RTT vs distance scatter with model fits overlaid."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # subsample for scatter (plotting 800k points is slow)
    rng = np.random.default_rng(0)
    idx = rng.choice(len(dists), min(20_000, len(dists)), replace=False)
    d_s, r_s = dists[idx], rtts[idx]

    for ax, ylim, title_sfx in zip(axes, [None, np.percentile(rtts, 95)], ['full range', 'zoomed (p95)']):
        ax.scatter(d_s, r_s, alpha=0.04, s=4, color='steelblue', rasterized=True, label='data')

        d_line = np.linspace(dists.min(), dists.max(), 400)
        ax.plot(d_line, d_line / SOL, 'r--', lw=1.5, label='SOL floor (100 km/ms)')

        colors = ['#E07B39', '#2EAA5A', '#9B59B6']
        for (key, color) in zip(('proportional', 'affine', 'power'), colors):
            m = model_results[key]
            if key == 'proportional':
                y = model_proportional(d_line, model_results[key]['params']['k'])
            elif key == 'affine':
                p = model_results[key]['params']
                y = model_affine(d_line, p['a'], p['b'])
            else:
                p = model_results[key]['params']
                y = model_power(d_line, p['c'], p['alpha'])
            ax.plot(d_line, y, color=color, lw=2,
                    label=f"{m['label']}  (MAE={m['mae']:.0f}ms)")

        ax.set_xlabel('Geographic distance (km)')
        ax.set_ylabel('Min RTT (ms)')
        ax.set_title(f'RTT vs distance — {title_sfx}')
        ax.legend(fontsize=8, loc='upper left')
        ax.set_xlim(0, dists.max())
        if ylim:
            ax.set_ylim(0, ylim)
        ax.grid(alpha=0.3)

    fig.tight_layout()
    out = os.path.join(FIG_DIR, 'analysis_scatter_models.pdf')
    fig.savefig(out, dpi=150, bbox_inches='tight')
    print(f'Saved {out}')


def fig2_residuals(model_results):
    """Residual distributions for each model."""
    keys = ['proportional', 'affine', 'power']
    fig, axes = plt.subplots(2, 3, figsize=(14, 8))

    for col, key in enumerate(keys):
        m   = model_results[key]
        res = m['resid']
        clip = np.percentile(np.abs(res), 99)
        res_c = res[np.abs(res) < clip]

        # histogram
        ax = axes[0, col]
        ax.hist(res_c, bins=80, color='steelblue', alpha=0.7, density=True)
        x = np.linspace(res_c.min(), res_c.max(), 300)
        mu_r, std_r = res_c.mean(), res_c.std()
        ax.plot(x, stats.norm.pdf(x, mu_r, std_r), 'r-', lw=2, label='Gaussian fit')
        # laplace
        loc_l, scale_l = stats.laplace.fit(res_c)
        ax.plot(x, stats.laplace.pdf(x, loc_l, scale_l), 'g--', lw=2, label='Laplace fit')
        ax.set_title(f'{key}\nRMSE={m["rmse"]:.0f}ms  MAE={m["mae"]:.0f}ms')
        ax.set_xlabel('Residual (ms)')
        ax.legend(fontsize=7)
        ax.grid(alpha=0.3)

        # Q-Q plot
        ax2 = axes[1, col]
        stats.probplot(res_c, dist='norm', plot=ax2)
        ax2.set_title(f'{key} — Normal Q-Q')
        ax2.grid(alpha=0.3)

    fig.suptitle('Residual analysis by model', fontsize=13, y=1.01)
    fig.tight_layout()
    out = os.path.join(FIG_DIR, 'analysis_residuals.pdf')
    fig.savefig(out, dpi=150, bbox_inches='tight')
    print(f'Saved {out}')


def fig3_overhead_distribution(model_results):
    """Distribution of raw overhead = rtt - d/100 and log-overhead."""
    od = model_results['overhead_dist']
    overhead = od['overhead']
    pos = overhead[overhead > 0]

    fig, axes = plt.subplots(1, 3, figsize=(15, 4))

    # raw overhead
    ax = axes[0]
    clip = np.percentile(overhead, 99)
    ax.hist(overhead[overhead < clip], bins=100, color='steelblue', alpha=0.7, density=True)
    ax.axvline(od['mean'], color='r', lw=2, label=f"mean={od['mean']:.0f}ms")
    ax.axvline(od['median'], color='g', lw=2, linestyle='--', label=f"median={od['median']:.0f}ms")
    ax.set_xlabel('Overhead = rtt − d/100 (ms)')
    ax.set_title(f'Raw overhead  (frac<0: {od["frac_negative"]:.1%})')
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)

    # log overhead (positive only)
    ax = axes[1]
    log_pos = np.log(pos)
    ax.hist(log_pos, bins=80, color='#E07B39', alpha=0.7, density=True)
    mu_log, std_log = log_pos.mean(), log_pos.std()
    x = np.linspace(log_pos.min(), log_pos.max(), 300)
    ax.plot(x, stats.norm.pdf(x, mu_log, std_log), 'r-', lw=2,
            label=f'Normal(μ={mu_log:.2f}, σ={std_log:.2f})')
    ax.set_xlabel('log(overhead)  [positive overhead only]')
    ax.set_title('Log-overhead: is it lognormal?')
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)

    # overhead vs distance (binned)
    ax = axes[2]
    all_d = np.linspace(0, overhead.max(), 1)  # dummy; recompute below
    # we need distances here -- return them from load_pairs
    ax.text(0.5, 0.5, '(see fig4 for overhead vs distance)', ha='center', va='center',
            transform=ax.transAxes, fontsize=11)
    ax.set_title('See fig4')

    fig.tight_layout()
    out = os.path.join(FIG_DIR, 'analysis_overhead_dist.pdf')
    fig.savefig(out, dpi=150, bbox_inches='tight')
    print(f'Saved {out}')
    return mu_log, std_log


def fig4_overhead_vs_distance(dists, rtts):
    """Is overhead a function of distance?"""
    overhead = rtts - dists / SOL

    # Bin by distance
    edges = np.percentile(dists, np.linspace(0, 100, 21))
    edges = np.unique(edges)
    bin_centres, bin_mean, bin_median, bin_p25, bin_p75 = [], [], [], [], []
    for lo, hi in zip(edges[:-1], edges[1:]):
        mask = (dists >= lo) & (dists < hi) & (overhead > 0)
        if mask.sum() < 10:
            continue
        oh = overhead[mask]
        bin_centres.append((lo + hi) / 2)
        bin_mean.append(oh.mean())
        bin_median.append(np.median(oh))
        bin_p25.append(np.percentile(oh, 25))
        bin_p75.append(np.percentile(oh, 75))

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))

    ax = axes[0]
    rng = np.random.default_rng(1)
    idx = rng.choice(len(dists), min(15_000, len(dists)), replace=False)
    oh_clip = np.clip(overhead[idx], 0, np.percentile(overhead, 97))
    ax.scatter(dists[idx], oh_clip, alpha=0.05, s=4, color='steelblue', rasterized=True)
    ax.plot(bin_centres, bin_mean,   'r-',  lw=2.5, label='bin mean')
    ax.plot(bin_centres, bin_median, 'g--', lw=2,   label='bin median')
    ax.fill_between(bin_centres, bin_p25, bin_p75, alpha=0.2, color='green', label='IQR')
    ax.set_xlabel('Geographic distance (km)')
    ax.set_ylabel('Overhead = rtt − d/100 (ms, clipped at p97)')
    ax.set_title('Routing overhead vs distance')
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)

    # Log overhead vs log distance
    ax2 = axes[1]
    pos = (dists > 0) & (overhead > 0)
    log_d = np.log10(dists[pos])
    log_oh = np.log10(overhead[pos])
    idx2 = rng.choice(len(log_d), min(15_000, len(log_d)), replace=False)
    ax2.scatter(log_d[idx2], log_oh[idx2], alpha=0.05, s=4, color='#E07B39', rasterized=True)
    slope, intercept, r, _, _ = stats.linregress(log_d, log_oh)
    x_fit = np.linspace(log_d.min(), log_d.max(), 200)
    ax2.plot(x_fit, slope * x_fit + intercept, 'k-', lw=2,
             label=f'OLS: slope={slope:.2f}, r={r:.2f}')
    ax2.set_xlabel('log10(distance km)')
    ax2.set_ylabel('log10(overhead ms)')
    ax2.set_title('Log–log: does overhead scale with distance?')
    ax2.legend(fontsize=9)
    ax2.grid(alpha=0.3)
    print(f"\nOverhead vs distance (log-log): slope={slope:.3f}, r={r:.3f}")
    if abs(slope) < 0.2:
        print("  → overhead is roughly INDEPENDENT of distance (intercept/additive model)")
    elif slope > 0.5:
        print("  → overhead scales strongly with distance (multiplicative model)")
    else:
        print("  → weak distance dependence (mixed model)")

    fig.tight_layout()
    out = os.path.join(FIG_DIR, 'analysis_overhead_vs_distance.pdf')
    fig.savefig(out, dpi=150, bbox_inches='tight')
    print(f'Saved {out}')


def fig5_per_vp(vp_stats):
    """Distribution of per-VP mu, sigma, slope, and mu-sigma correlation."""
    mus    = np.array([v['mu']    for v in vp_stats.values()])
    sigmas = np.array([v['sigma'] for v in vp_stats.values()])
    slopes = np.array([v['slope'] for v in vp_stats.values()])
    intercepts = np.array([v['intercept'] for v in vp_stats.values()])
    rs     = np.array([v['r']     for v in vp_stats.values()])

    # clip extreme outliers for display
    mu_c  = np.clip(mus,   *np.percentile(mus,   [1, 99]))
    sig_c = np.clip(sigmas,*np.percentile(sigmas,[1, 99]))
    sl_c  = np.clip(slopes,*np.percentile(slopes,[1, 99]))

    fig, axes = plt.subplots(2, 3, figsize=(15, 8))

    def hist_with_stats(ax, data, label, color):
        ax.hist(data, bins=60, color=color, alpha=0.7, density=True)
        ax.axvline(np.median(data), color='k', lw=2, linestyle='--',
                   label=f'median={np.median(data):.1f}')
        ax.axvline(np.mean(data),   color='r', lw=2,
                   label=f'mean={np.mean(data):.1f}')
        ax.set_xlabel(label)
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)

    hist_with_stats(axes[0,0], mu_c,  'Per-VP mean overhead μ_v (ms)', 'steelblue')
    axes[0,0].set_title('VP routing overhead μ_v')

    hist_with_stats(axes[0,1], sig_c, 'Per-VP overhead std σ_v (ms)',  '#E07B39')
    axes[0,1].set_title('VP routing noise σ_v')

    hist_with_stats(axes[0,2], sl_c,  'Per-VP rtt-vs-dist slope (ms/km)', '#2EAA5A')
    axes[0,2].set_title('Per-VP slope (routing speed)')

    # mu vs sigma scatter
    ax = axes[1,0]
    ax.scatter(mu_c, sig_c, alpha=0.4, s=20, color='steelblue')
    r_ms, _ = stats.pearsonr(mu_c, sig_c)
    ax.set_xlabel('μ_v (ms)')
    ax.set_ylabel('σ_v (ms)')
    ax.set_title(f'μ vs σ per VP  (r={r_ms:.2f})')
    ax.grid(alpha=0.3)
    # fit line
    m_fit, b_fit = np.polyfit(mu_c, sig_c, 1)
    x_fit = np.linspace(mu_c.min(), mu_c.max(), 100)
    ax.plot(x_fit, m_fit*x_fit + b_fit, 'r-', lw=2,
            label=f'σ≈{m_fit:.2f}μ+{b_fit:.1f}')
    ax.legend(fontsize=8)

    # slope vs intercept scatter
    ax = axes[1,1]
    int_c = np.clip(intercepts, *np.percentile(intercepts, [1,99]))
    ax.scatter(sl_c, int_c, alpha=0.4, s=20, color='#9B59B6')
    r_si, _ = stats.pearsonr(sl_c, int_c)
    ax.set_xlabel('slope (ms/km)')
    ax.set_ylabel('intercept (ms)')
    ax.set_title(f'VP slope vs intercept  (r={r_si:.2f})')
    ax.grid(alpha=0.3)

    # R² distribution
    ax = axes[1,2]
    r2 = rs**2
    ax.hist(r2, bins=40, color='#E07B39', alpha=0.7)
    ax.axvline(np.median(r2), color='k', lw=2, linestyle='--',
               label=f'median R²={np.median(r2):.3f}')
    ax.set_xlabel('R² of per-VP linear fit')
    ax.set_title('How linear is rtt vs dist per VP?')
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)

    print(f"\nPer-VP statistics ({len(vp_stats)} VPs):")
    print(f"  μ_v:    mean={mus.mean():.1f}  median={np.median(mus):.1f}  std={mus.std():.1f}  range=[{mus.min():.0f},{np.percentile(mus,99):.0f}]ms")
    print(f"  σ_v:    mean={sigmas.mean():.1f}  median={np.median(sigmas):.1f}  range=[{sigmas.min():.1f},{np.percentile(sigmas,99):.0f}]ms")
    print(f"  slope:  mean={slopes.mean()*1000:.2f}  median={np.median(slopes)*1000:.2f} ms/1000km")
    print(f"  R²:     mean={np.mean(r2):.3f}  median={np.median(r2):.3f}")
    print(f"  corr(μ,σ)={r_ms:.3f}  corr(slope,intercept)={r_si:.3f}")

    fig.suptitle('Per-VP routing heterogeneity', fontsize=13)
    fig.tight_layout()
    out = os.path.join(FIG_DIR, 'analysis_per_vp.pdf')
    fig.savefig(out, dpi=150, bbox_inches='tight')
    print(f'Saved {out}')


def print_model_summary(model_results):
    print("\n" + "="*65)
    print("MODEL FIT SUMMARY")
    print("="*65)
    print(f"{'Model':<20} {'RMSE (ms)':>12} {'MAE (ms)':>12}")
    print("-"*46)
    for key in ('proportional', 'affine', 'power'):
        m = model_results[key]
        print(f"{key:<20} {m['rmse']:>12.1f} {m['mae']:>12.1f}")

    od = model_results['overhead_dist']
    print(f"\nOverhead = rtt - d/100:")
    print(f"  mean={od['mean']:.1f}ms  median={od['median']:.1f}ms  std={od['std']:.1f}ms")
    print(f"  skew={od['skew']:.2f}  frac<0={od['frac_negative']:.1%}")
    print(f"  IQR=[{od['p25']:.0f}, {od['p75']:.0f}]ms  p5={od['p5']:.0f}ms  p95={od['p95']:.0f}ms")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    print("Loading data …")
    dists, rtts, srcs, address_to_loc, loc_loc_meas = load_pairs()

    print("\nFitting models …")
    model_results = fit_models(dists, rtts)
    print_model_summary(model_results)

    print("\nComputing per-VP statistics …")
    vp_stats = per_vp_stats(dists, rtts, srcs)

    print("\nGenerating figures …")
    fig1_scatter_and_models(dists, rtts, model_results)
    fig2_residuals(model_results)
    fig3_overhead_distribution(model_results)
    fig4_overhead_vs_distance(dists, rtts)
    fig5_per_vp(vp_stats)
    print("\nDone.")
