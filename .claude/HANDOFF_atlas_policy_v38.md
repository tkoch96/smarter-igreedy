# Handoff: fiber-atlas transit policy v3.8 + validation tooling

> Written 2026-07-11. State of the atlas after the 07-09..07-11 policy
> iteration campaign. Read `internet_gmaps/README.md` for the module
> map, `internet_gmaps/TRANSIT_POLICY.md` for the rule-by-rule evidence
> (v1 through v3.8 with every falsified round preserved), and
> `.claude/HANDOFF_routing_realism.md` for the research agenda.

## Current state (all verified 2026-07-11)

- `DEFAULT_POLICY` is **v3.8-geopolitical**. All suites green: 48 unit
  tests + 4 policy validation tests (incl. `test_no_policy_stranded_pairs`,
  the routability contract: zero pairs the open graph can route that the
  policy cannot).
- Mesh at last run: 926,730 ok pairs / ~12.2k probes (campaign db grows
  ~90k pairs/day when the daily run fires; note the count you ran
  against: `SELECT COUNT(*) FROM pairs WHERE status='ok'` on
  `internet_gmaps/mesh_campaign/data/state.sqlite`).
- Headline numbers (120k-pair sample): median residual 12.5 ms (open:
  19.6), raw-floor violations 2.4%, per-probe median error against
  policy floors 17.6 ms vs 23.1 open (probes >100 ms: 25, was 113).
- Everything is UNCOMMITTED in the repo. A commit checkpoint is overdue.

## What the policy machinery can express now (transit_policy.py)

- `CountryRule` / `RegionRule` node bans with endpoint exemptions
  (region rules can exempt a SUPERSET via `exempt_region` — e.g. any
  island-nation endpoint unlocks the small-island class).
- `terrestrial_only=True` on either rule kind: ban applies to overland
  (ITU) edges only; submarine cables through the country's waters keep
  routing. This resolved three falsified rounds where node bans severed
  ocean trunks (ocean vertices geocode to the nearest coastal state).
- `cable_factors` (name x multiplier), `terrestrial_factors` (single cc
  = internal links; tuple of ccs = any overland link touching the
  group), `corridor_factors` (lat/lon box x multiplier — all edges with
  a vertex inside).
- `node_cc_remaps`: geographic pseudo-countries. Nodes in a box get a
  synthetic code before rules run (v3.8: "XI" = open Indian Ocean), so
  ocean regions are first-class rule subjects with zero new machinery.
- Penalties vs deletion: measured equivalent at x2 (see
  `analysis/penalty_vs_deletion.py`); penalties can't strand, prefer them.

## The floor contract (floor_query.py)

- `PolicyFloorEstimator.floor_ms` RAISES `NoRouteError` (a KeyError)
  when the policy strands a pair the open graph can route. Construct
  with `no_route="open"` for the legacy silent open-floor fallback
  (the geolocator's factory does this — cold-start queries hit banned
  regions legitimately). Off-graph points return inf under both modes.
- Policy floors are ROUTING-REALISM estimates, not physical bounds:
  distrust multipliers deliberately push floors above pure physics.
  The physical lower bound is the OPEN floor. Anything that needs a
  hard bound must use open floors or accept the ~2% violation rate.
- The `data/cache/policy_fields/` disk cache is keyed by policy NAME —
  bump the name on any rule change. `field_dtype=np.float32` halves it
  (max observed float32 error: 7.6e-6 ms).

## Validation loop (how to iterate a policy)

1. Edit rules in `transit_policy.py`: freeze the old DEFAULT as
   `V<NN>_POLICY` (its floor matrix is cached under its name and the
   progression figure uses it), define the new DEFAULT with a bumped
   name, add the new frozen stage to `tests/test_policy_validation.py`.
2. `python -m pytest tests/test_transit_policy.py tests/test_policy_floor_estimator.py -q`
   (seconds).
3. `python -m pytest tests/test_policy_validation.py -s` (~7 min: one
   uncached floor matrix per new policy at ~3.3k metro clusters; all
   frozen stages come from `data/cache/floors_*.npy`). Read three
   falsifiers: stranded count (must be 0), raw violations (~2%; a jump
   means floors above reality somewhere — check the per-country table),
   median residual (tightness).
4. `python -m pytest tests/test_transit_analysis.py -s` (~2 min)
   regenerates the four cable figures under the new policy.
5. If stranding: `analysis/diagnose_stranded.py` names who and which rule.
   If a red region on the map: `analysis/region_cable_attribution.py`
   names the cables.

All runs from `internet_gmaps/` with `~/Documents/venv312/bin/python`.
Suites scale with 0.25-degree metro clusters (~3.3k), not raw probes.

GOTCHA 1: any ad-hoc script that triggers the parallel routing MUST have
an `if __name__ == "__main__":` guard — macOS spawns workers by
re-importing the main module; without the guard every worker re-runs
the whole script and the machine wedges.

GOTCHA 2 (reproducibility): the mesh is NOT a pure function of the data
files. `mesh_campaign/export.py::campaign_target_data` calls
`fetch_inventory()` — a live RIPE Atlas API request — so probe churn
shifts the merged mesh slightly between days even with an unchanged
results db (observed: -4 clusters / -1,256 pairs overnight). Two
consequences: (a) the suites' floor-matrix cache keys (mesh-size-based)
go stale daily — rerun the policy suite once before using
analysis/ scripts that read the cache (they exit with instructions if
stale); (b) same-day figures are reproducible, cross-day ones are not
bit-identical. Proper fix if it matters: snapshot the inventory at
export time instead of fetching live.

## Figures (internet_gmaps/figures/, all regenerable)

- Suite-generated: mesh_validation_cdfs(.., _per_probe), mesh_case_studies
  (mesh suite); transit_country_residuals, cable_residual_offenders
  (error map, width = traffic volume), cable_usage_map (volume view),
  cable_fix_impact (volume x error = which cable to fix next — THE
  prioritization figure; top item: Iran overland at ~9% of total error
  mass), transit_residual_map (transit suite); policy_validation
  (progression CDFs, policy suite).
- Script-generated: noise_audit, cable_penalty_vs_deletion,
  mesh_validation_cdfs_per_probe_policy (see analysis/ headers);
  mesh_campaign_coverage via
  `python -c "from mesh_campaign.report import simulate_coverage; simulate_coverage()"`.
- mesh_campaign_day1_cdf.pdf is a historical one-off; no generator.

## Open threads, in priority order

1. **Geolocator floor-validity dispute** (memory:
   `fiber-floor-validity-regression`): the geolocation project reported
   floors above measurements on 24-41% of dense pairs post-07-09. The
   atlas side CANNOT reproduce it — four independent floor paths agree
   to microseconds and pass the <=5% validity check
   (`analysis/estimator_repro.py`, `analysis/dense_pair_diagnostic.py`).
   Ball is with the geolocator session; likeliest cause: their check
   compares against the 1.3x-slope model value instead of the raw floor.
2. **EU<->South-Asia overland belt**: Iran/Turkey/FALCON/BBG hold ~20%+
   of total error mass (see cable_fix_impact). The single biggest
   remaining dial-mover; a distrust experiment is one policy iteration.
3. **Island-rule waters artifact**: TT/SH/CV show 7-16% raw violations —
   cables PASSING those islands' waters get caught by the node ban.
   Designed fix: scope island bans to near-shore nodes (<= ~75 km of
   the geocoded settlement).
4. **African noise contamination**: NA/MG/TD/ZM/MW/CD offender rows are
   8-15% contaminated by noisy measurements (analysis/noise_power_check.py)
   — don't let those rows alone justify rules. Cheap fix: re-ping the
   ~6.4k noisy pairs in a campaign day (~19k credits); the pipeline's
   min-merge heals transient noise automatically.
5. **Unfixable-by-rules residue**: Gulf/Japan endpoint slack and the
   diffuse 5-14 ms background on the big trunks need a regional-slack /
   endpoint-offset model term, not routing rules
   (`.claude/HANDOFF_routing_realism.md` has the ranked mechanisms).
