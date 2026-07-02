# Simulation Environment & Research Problem

## The research problem

You have a set of **vantage points** (VPs — RIPE Atlas probes) and a set of
**targets** (unknown IP addresses). VPs can ping targets to produce RTT
measurements. You have a limited total budget of pings. The question is:

> Given a budget of N pings, which VP→target pairs should you measure, and
> in what order, to minimise mean geolocation error across all targets?

Two distinct sub-problems:

1. **Estimation**: given the RTT measurements you've made so far, what is your
   best estimate of each target's location?
2. **Selection**: which VP should you ping next to reduce uncertainty the most?

Most of the current code addresses estimation. The `Iterative_Greedy_Geolocator`
addresses selection.

---

## The simulation environment

The data is a **dense all-pairs mesh** of RIPE Atlas probes pinging each other
(`cache/cached_target_data.pkl`). The mesh has ~909 nodes; each node pings
~80%+ of the others.

**Key point**: the mesh is used to simulate the scenario — not as a calibration
source. We treat each probe as an "unknown target" and use the other probes as
VPs trying to geolocate it. Ground truth (the probe's real location) is used
only to evaluate accuracy after estimation is done.

---

## What information is allowed during inference

| Information | Allowed? | Reason |
|---|---|---|
| RTT from VP to target | ✅ yes | This is the measurement |
| VP's own location | ✅ yes | Needed to place constraint rings |
| Target's true location | ❌ no | This is what we're trying to find |
| VP-to-VP distances (for calibration) | ❌ no | Requires knowing all VP locations — not available in production |

VP locations are known in practice (RIPE Atlas publishes probe coordinates).
They are passed explicitly to the estimator as part of the measurement interface.

VP-to-VP distances look tempting for calibrating per-VP sigma and mu
(routing overhead), but this is cheating: in production you wouldn't have
distance-labelled VP-to-VP pairs available. The honest baseline uses a fixed
global sigma with no per-VP calibration.

---

## The LockedLocationDict enforcement

`utils.py` provides `LockedLocationDict` and `simulation_mode()` to enforce
this boundary at runtime:

```python
ALL_LOCS = LockedLocationDict({
    '_target': (50.08, 14.44),
    'london':  (51.50, -0.10),
    ...
})

# VP locations resolved BEFORE the lock (legitimate measurement metadata)
vp_locs = {name: ALL_LOCS[name] for name in VP_NAMES}

with simulation_mode(ALL_LOCS):
    # ALL_LOCS is locked — any ALL_LOCS[key] here raises ValueError
    estimate = run_inference(rtts, vp_locs)   # receives pre-resolved VP locs

# Assessment: ALL_LOCS unlocked, target location accessible
error = get_distance(estimate, ALL_LOCS['_target'])
```

Any accidental location lookup inside the simulation block raises immediately,
making it impossible to silently cheat.

---

## How the methods work

### Random (`Random_Geolocator`)

Shuffles all (VP, target) pairs randomly. Returns the first `budget` pairs when
asked for measurements. **Does not do any estimation** — estimation is handled
by `Geolocator_Comparator.convert_measurements_to_locations()` separately.

`random.shuffle(self.measurement_order)` is the only logic.

Combined with `nearest_neighbor` estimation in the comparator, this is the
baseline: random ordering + report location of lowest-RTT VP seen.

### Nearest-neighbour (estimation mode, not a geolocator)

`measurement_converter_mode = 'nearest_neighbor'` in `Geolocator_Comparator`.

For each target, pick the VP with the lowest observed RTT. Report that VP's
location as the estimate. Gets better as budget grows because more pings means
a better chance of catching a physically close VP.

This is always paired with `Random_Geolocator` as the baseline.

### Hard-circle (`measurement_converter_mode = 'hard_circle'`)

For each target, create a `FeasibleRegion(mode='hard_circle')`. Each RTT
measurement adds a circle: target must be within `rtt × 100km × multiplier`
of the VP. Nelder-Mead finds the intersection centroid.

Problem: the loss landscape is nearly flat inside all circles. Nelder-Mead
barely moves from its starting point. Multiplier 1.3 is too loose (Null Island
issue); 1.05 is better but still flat.

### Gaussian MAP (`measurement_converter_mode = 'gaussian'`)

For each target, create a `FeasibleRegion(mode='gaussian')`. Each RTT
contributes a term to the negative log-posterior:

```
NLL(x) = Σ_v  (rtt_v - d(x, v) / 100)² / (2 σ²)
```

Nelder-Mead minimises NLL — always has a gradient (proper bowl), never flat.

`σ = GLOBAL_SIGMA_MS = 15ms` — a fixed constant, no per-VP calibration.
With constant σ, NLL reduces to sum of squared RTT residuals (equal weighting).

**Without calibration, Gaussian and hard-circle are roughly comparable** at full
budget on correctly-specified synthetic data (oracle=152km, hard-circle=212km,
Gaussian=223km, random=281km). The Gaussian advantage comes from per-VP sigma
weighting — which requires calibration that we don't allow.

### Oracle (explicit upper bound)

Uses the **true** per-VP sigma from the data-generating model. Not available
in practice. Used to quantify the gap between honest inference and the best
possible inference with perfect noise knowledge.

In the plot script, oracle additionally uses the optimal VP ordering (closest
VPs first by true distance). Both of these are explicit cheats; the oracle is
labelled as such.

---

## Why nearest-neighbour is a strong baseline

At budget=2500 (almost all 909 VPs), nearest-neighbour has seen enough pings
that at least one VP is physically near almost every target. It picks that VP's
location directly. This is hard to beat because model-based methods (Gaussian,
hard-circle) with misspecified parameters can produce wrong estimates for some
targets, dragging up the mean — while nearest-neighbour's errors are bounded by
the VP network density.

At low budget (few VPs seen), triangulation methods have more room to win
because they extract more information from each RTT by reasoning about geometry.

---

## Real-data limitations

The RIPE Atlas data has mean routing overhead ~67ms (median 53ms). The assumed
model `rtt ≈ d/100` is badly misspecified: true RTTs are `d/100 + overhead`
where overhead is large, VP-dependent, and scales with distance (`∝ d^0.67`).

The Gaussian MAP with global sigma and no overhead correction places every
ring ~6700km too far from each VP. This causes catastrophic failures for some
targets (e.g. 10,129km error vs 3km for nearest-neighbour).

A better model would be per-VP affine: `rtt ≈ a_v × d + b_v` where `a_v`
(slope) and `b_v` (intercept) are estimated per-VP. But estimating these
requires the mesh — which is cheating under our rules. The EM approach (refine
per-VP parameters from accumulating target estimates online) resolves this but
is not yet implemented.
