# Routing realism — what we learned, what's unresolved, how to improve it

> Handoff for a fresh agent. Written 2026-07-06 after three
> falsification rounds (policy v1 -> v3.2) against ~1M measured mesh
> pairs. Companion to FIBER_GEOLOCATOR_RESULTS.md (the consumer) and
> internet_gmaps/TRANSIT_POLICY.md (rule-by-rule evidence + literature).
> This one is the research agenda: which routing the shortest-fiber
> model gets wrong, why, and the ranked list of mechanisms that would
> improve it. The loop that produced all of this is one command:
> `pytest tests/test_transit_analysis.py tests/test_policy_validation.py -s`
> (~5 min, internet_gmaps/, venv312) — change a rule, rerun, read the
> residual/violation tables.

## The central taxonomy (biggest transferable learning)

Every "problem country" decomposes by its transit-vs-endpoint residual
signature, and each signature wants a DIFFERENT mechanism. Applying the
wrong one provably fails:

| Signature | Meaning | Mechanism | Examples found |
|---|---|---|---|
| transit >> endpoint | their fiber exists but through-traffic can't use it | country ban / containment | CN (82 vs n/a), KZ (87/30), TW (26/-3), MZ (23/-7) |
| flat, both high | region-wide routing overhead (tromboning) | per-region SLACK in the predictive layer — NOT a routing rule | OM (67/67), the Gulf generally |
| inverted (endpoint worse) | access-network overhead at the endpoint | nothing routing-side; per-VP offsets absorb it | JO (27/60), KH (25/40), PK (20/45) |
| high on a feature, diluted at country level | a specific crossing is fiction, the country's other infra is fine | edge/feature-level distrust | Iraq: ITU IQ links 93 ms while IQ "transit" pairs on Gulf coastal cables are healthy |
| appears after penalizing a sibling | corridor degradation, not a bad cable | corridor-wide distrust | SEACOM x2 -> IMEWE inherited 110 ms (same trench) |

Corollary: a ban is only justified by the first signature AND a
literature story AND surviving the violation falsifier (measured < raw
floor = impossible physics = the rule is wrong). Rules that would move
nothing (IL at +5 ms) are unfalsifiable config debt — don't add them.

## Validated REAL (resist the temptation to ban)

- Egypt/Suez: tolled chokepoint, 17-30% of intercontinental traffic;
  cost is the constraint, not physics.
- Iran (EPEG, Frankfurt->Oman via RU/AZ/IR, operational since 2012) and
  the Türkiye/Balkans overland corridor: the model's two heaviest
  remaining transits (20-27k pairs each) at only 17-22 ms residual.
- Relay islands in three oceans: Pacific (FJ/TK/KI/WS/AS/NF/TO...,
  88-99% violations when banned), Caribbean (SR 41%, GD 49%), West
  Africa Atlantic (CV/MR ~15%), Suez-corridor small states (DJ/ER).
  Small population != no transit; often transit is the entire role.
- Finland, Norway, EU-integrated ex-bloc (banning FI produced 79%
  violations — the single strongest falsification we recorded).

## Validated FICTION (current rules, keep)

- CN/MN transit; soviet-bloc-minus-EU overland (Central Asia corridor);
  AF; TW (carriers interconnect in HK/Tokyo, not Taipei — APNIC);
  intra-Africa transit at country granularity (boomerang routing);
  Iraq internal overland; Red Sea cut-series systems at x2 (real cables,
  chronically severed 2024-25, months-long repair queues).

## Open puzzles, ranked by expected value

1. **Japan paradox**: ban-like signature (transit 22 vs endpoint -1.5,
   n~1.8-2.9k across ITU JP + JIH + RJCN + APG) yet Tokyo is a
   documented open hub. Hypothesis: the model switches cable systems at
   the WRONG Japanese nodes (rural landing stations vs Tokyo metro
   facilities). Test: dump the actual nodes those paths traverse
   (policy_paths_parallel returns edge ids -> node ids) and check
   against PeeringDB facility locations. If confirmed, this is the
   smoking gun for mechanism #1 below — worth doing FIRST because it
   converts many hand-curated rules into one principled one.
2. **APG at 107.6 ms** (top remaining cable offender): East Asia
   consortium system; overlaps the JP/TW story. Same node-level dump.
3. **Nepal (ITU NP / IN-NP, ~63 ms)**: Himalayan India<->China overland
   — genuinely used (there IS an operational Kathmandu-Lhasa link) or
   fiction? Small n; needs the transit-vs-endpoint split + one search.
4. **Sri Lanka**: 116 ms transit under open model -> ~29 under v3.1
   (mostly absorbed by the Red Sea corridor distrust). Verify it stays
   resolved; if not, it's an island-hop variant.
5. **IL triplet rule**: documented (Arab networks won't transit Israel)
   but currently unfalsifiable (+5 ms, n=714). Trigger: mesh grows
   Middle East pairs, or Blue-Raman/TEAS RFS (which would make Israeli
   transit REAL — the rule may be obsolete before it's justified).
6. **inf-floor growth**: 15-18% of pairs have no allowed route under
   v3.2 (mostly granular-Africa). Is that over-restriction? Check
   violation rates of the pairs whose floors went inf-adjacent, and
   define the fallback contract (the geolocation integration uses
   open-floor fallback — see FIBER_GEOLOCATOR_RESULTS.md).

## Proposed mechanisms, ranked

1. **Interconnection-evidence rule** (replaces hand-curated lists):
   allow switching BETWEEN cable systems only at nodes that host >=2
   distinct features AND sit within ~50 km of a PeeringDB facility/IXP
   (data already local: edge features in the graph npz; PeeringDB is a
   documented free API — see DATA_SOURCES.md). This would derive the
   Pacific/Caribbean/Suez relay exemptions, likely resolve the JP/TW/APG
   cluster, and kill mid-desert ITU chaining without naming countries.
   Implementation note: switching constraints are edge-pair state —
   either line-graph Dijkstra (edge-as-node, ~2x size) or approximate by
   deleting non-facility nodes' inter-feature adjacency.
2. **Per-region slack layer** (for the flat-high class): floor stays as
   is; the PREDICTIVE model gets region-pair offsets (Gulf +X ms, East
   Asia +Y ms), fitted on held-out pairs. The additive per-VP offset
   machinery in this repo is 80% of it.
3. **Held-out validation**: all exemptions so far were carved on the
   same mesh they're scored on. Pull a fresh day of dumps
   (pull_ripe_atlas_measurement_data.py) or a fresh campaign tranche and
   re-score v3.2 untouched. Cheap, publishable, overdue.
4. **Attribution fixes**: ocean-vertex nearest-city geocoding charges
   coastal states for cables passing offshore (MA/DZ/TN ~5% violations,
   CV/MR partly artifact). Fix: attribute a vertex to a country only if
   within ~25 km of land, else "international waters" (never banned).
   Would clean the Mediterranean/Atlantic friction and sharpen every
   country table.
5. **Triplet rules** (src-region, transit, dst-region): framework slot
   exists (class-signature grouping); first real candidates: IL (above),
   and "US<->anywhere avoids RU" style pairs if the mesh ever shows them.
6. **Traceroute grounding**: replace inference-by-residual with
   observation — map real paths to cables via Nautilus (open source,
   SIGMETRICS'24) or CAIDA ark for the specific corridors in dispute.
   The gold standard; also the only way to separate "policy-avoided"
   from "capacity-avoided".
7. **Learned per-edge openness**: regress edge usage against residuals
   to fit a continuous trust coefficient per edge (the cable-offenders
   table is already the diagnostic). Data-driven endgame, but keep the
   named-rule layer — the interpretability is what made every
   falsification above possible.

## Methodology pitfalls (learned the hard way — don't repeat)

- Attribute transit from paths routed UNDER the current policy, not the
  open model, or you re-litigate solved countries forever (MN/AF
  dominated every table until this was fixed).
- Country-level medians can dilute real signals (Iraq) and manufacture
  fake ones (coastal attribution). Always cross-read the feature-level
  table.
- Penalizing one cable displaces paths to its trench-siblings; check the
  next run for inherited residuals before declaring victory.
- Campaign RTTs are single-visit min-of-3 (dump mesh is min-over-a-day):
  looser bounds, occasional sick-probe artifacts (a Riga probe with
  993 ms medians; a 2,784 ms Riga->Kawaguchi pair). The 0.25-degree
  location clustering with min-wins absorbs most of it; a probe-QC rule
  (bench absurd-median probes) is still open.
- Rules were tuned at FIBER_SLOPE=1.3 evaluation; raw-floor violation
  rates are the slope-free ground truth — always report both.
- Cache keys include the policy NAME: bump it on every rule change
  (v3 -> v3.1 -> v3.2 pattern) or you read stale floors.

## Assets

internet_gmaps/: transit_policy.py (rules + parallel floors + paths),
tests/test_transit_analysis.py (the attribution loop + figures),
tests/test_policy_validation.py (progression + per-rule violation
tables), TRANSIT_POLICY.md (evidence + all literature links),
figures/{transit_country_residuals, cable_residual_offenders,
policy_validation, transit_residual_map}.pdf. Mesh:
mesh_data.load_target_data() (dumps + campaign merged). Campaign can buy
targeted pairs for any hypothesis above at 3 credits/pair, 100k
results/day (mesh_campaign/, scheduler takes a targeted tier easily).

## Definition of a good next iteration

Pick puzzle #1 (Japan node-level dump). If it confirms wrong-landing
switching, implement mechanism #1 behind a policy flag, rerun the loop,
and check three numbers: JP/TW/APG residuals collapse, the relay-island
exemption lists become removable without violation spikes, and the
overall raw violation rate stays <= 4.2%. That single change, if it
works, retires more hand-curated config than any rule added so far.
