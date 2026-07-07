# Transit policy — when can a country's fiber carry through-traffic?

> Companion to `transit_policy.py`. Status 2026-07-06: v2 policy
> (soviet-bloc-minus-EU, AF, Africa containment w/ Suez exemption,
> Pacific-relay exemption) validated at 3.7% raw violations vs 0.3% open;
> remaining friction is Caribbean/West-Africa relays and coastal-vertex
> attribution — see "Empirical verdict".

## Why

The mesh validation (`tests/test_transit_analysis.py`) showed the open
shortest-fiber model's residuals concentrate on paths routed through
countries real traffic avoids: the model sent 12% of pairs through China
(85 ms median residual, zero Chinese endpoints in the mesh), and the
transit-vs-endpoint split showed the signature everywhere (Kazakhstan
87 ms as transit vs 30 ms as endpoint, etc.). Fiber that exists is not
fiber you may use.

## Mechanism

`TransitPolicy` = named set of rules, each a predicate `cc -> bool`.
A node in restricted country X is removed from the graph for pair
(src, dst) unless X is an endpoint country. `policy_floor_matrix()`
computes exact floors under a policy by grouping targets into
country-classes (one Dijkstra per VP × class).

Current rule builders: `no_transit(*ccs)`, `small_country(min_pop)`,
`russia_borders()`. Adding a rule = one predicate + rebuild the policy.
Future endpoint-conditional rules ("US→anywhere may not transit the
Middle East") slot in as a `(src_cc, transit_cc, dst_cc)` rule family;
the per-pair grouping already supports pair-dependent masks.

Known v1 semantic caveats (deliberate, revisit later):
1. Node countries via nearest-city reverse geocoding → mid-ocean cable
   vertices attribute to the nearest coastal state; passing NEAR a
   restricted island counts as landing there (over-restrictive).
2. Same-cable express pass-through at a landing station is blocked too;
   distinguishing "lands and interconnects" from "lands and continues"
   needs per-edge cable ids.

## Empirical verdict — v2 policy (mesh, 120k sampled pairs)

History: v1 ((a)-(d) as first stated) scored 7.7% raw violations, with
FI-transit at 79% violations and Pacific islands at 60-91% — that run
falsified "borders Russia" (too broad in Europe) and small-country as
applied to cable-relay islands. v2 responds: soviet-bloc replaces
russia-borders (EU ex-bloc + FI/NO carved out), AF added, Africa
containment added (Suez corridor exempt), Pacific relay islands exempted
from small-country.

v2 numbers: median residual 13.0 → 7.8 ms, P90 85 → 45 ms; raw-floor
violations 0.3% → **3.7%**; ~8% of pairs get an inf floor (no allowed
route — the caller must fall back to the open floor or geodesic there).

| Rule | Verdict | Evidence |
|---|---|---|
| no CN/MN transit | **Confirmed at scale.** | CN: 12,829 pairs, 82 → 17 ms, 2.5% viol. MN: 146 → 35 ms, 0.2% viol. |
| soviet-bloc-minus-EU | Holds. | BY: 81 → 15 ms, 1.9% viol; Central Asia clean; no FI-style blowups. |
| no AF transit | Holds. | (was 133 ms median as transit under open model) |
| Africa containment | **Coastal friction.** | MA/DZ/TN ~5% viol (Mediterranean cables' ocean vertices attributed to coastal states); CV/MR/EH ~15% (the Europe↔South-America Atlantic route hugs West Africa — partly attribution artifact, partly CV being a real relay). |
| small-country | **Two more relay regions found.** | SR (Suriname) 41%, GD (Grenada) 49% viol — Caribbean island-hop systems are real transit, same phenomenon as the Pacific. Needs a Caribbean/Atlantic relay exemption or the cable-degree rule. |

The pattern across three oceans is now unmistakable: **small-island
"transit" is real wherever long-haul cables land and interconnect**.
The durable fix is a cable-degree rule (allow transit at nodes where ≥2
distinct cable systems meet, computable from edge features + landing
points) replacing hand-curated exemption lists.

## Cable-level attribution (figures/cable_residual_offenders.pdf)

Aggregating residuals per source feature (TG cable name / ITU link
country-pair) names the over-trusted infrastructure directly. Top
offenders: TATA TGN-Eurasia (155 ms), the Mongolia/Russia and
Afghanistan ITU link groups (~140 ms), India-Asia-Xpress (134 ms),
**SAFE (114 ms at n=30k path-uses — the 2002-era South-Africa–Asia cable
the model loves as an Indian-Ocean shortcut)**, PEACE, TGN-Intra-Asia,
Russia-Japan RJCN (106 ms). This figure also caught a genuine loader bug:
the previous #1 offender was **Umoja (171 ms) — a cable with RFS 2028**.
The graph was including 89 planned cables; `load_telegeography` now
excludes `is_planned` systems by default (a floor may only use
infrastructure that exists).

## Rules of thumb from the literature (what "practically usable" means)

- **China is a special case even as endpoint**: the ["Great Bottleneck"
  study (SIGMETRICS 2020)](https://dl.acm.org/doi/10.1145/3379479) found
  79% of transnational pairs into/out of China throttled below 1 Mbps for
  5+ h/day, with the bottleneck *inside* China. Transit through China for
  third parties is essentially nonexistent; even endpoint traffic behaves
  anomalously ([APNIC summary](https://blog.apnic.net/2020/08/19/characterizing-transnational-internet-performance-in-china/)).
- **Intra-Africa traffic hairpins through Europe**: [75% of inter-NREN
  African traffic takes intercontinental
  detours](https://link.springer.com/chapter/10.1007/978-3-319-16886-9_7)
  ("boomerang routing", [Springer 2018](https://link.springer.com/chapter/10.1007/978-3-319-98878-8_1));
  see also [AFRINIC's country-level latency work](https://afrinic.net/ast/pdf/research/insight-latency-africa.pdf)
  and the [African IXP congestion study (IMC 2017)](https://dl.acm.org/doi/10.1145/3131365.3131394).
  Implication: modeled *terrestrial* transit across Africa is largely
  fiction today, while coastal submarine relays (CV, DJ) are real.
- **Egypt is a tolled chokepoint, not a ban**: ~17–30% (up to 90% of
  Asia–Europe) of intercontinental traffic crosses Egypt; Telecom Egypt
  holds a de-facto monopoly charging ~50% above comparable routes
  ([CSIS case study](https://www.csis.org/analysis/strategic-future-subsea-cables-egypt-case-study),
  [DCD](https://www.datacenterdynamics.com/en/analysis/egypts-submarine-cable-stranglehold/)).
  For a latency floor Egypt transit is REAL and must stay allowed — cost,
  not physics, is the constraint. Alternatives (Syria/Iraq/Iran overland,
  e.g. EPEG via Iran) exist but are avoided for stability/sanctions.
- **Nation-state avoidance is measurable and asymmetric**:
  [Edmundson et al.](https://arxiv.org/abs/1605.07685) measured
  country-level paths and built [RAN](https://ransom.cs.princeton.edu/)
  to route around specified states; their
  [COMPASS 2018 paper](https://ensa.fi/papers/nationstate_compass18.pdf)
  quantifies which countries are unavoidable (US/GB/DE hyper-transit) vs
  avoidable. Their "unavoidable core" list is a good allow-list prior; the
  inverse (rarely-transited states despite geography) is our restrict-list.
- **General pattern**: submarine relays are permissive (landing + branch
  fees), overland transit is restrictive (licensing, single state-owned
  carriers, interception law). When in doubt: trust cables, distrust
  terrestrial shortcuts through non-market states.

## Files

- `transit_policy.py` — rules, DEFAULT_POLICY, `policy_floor_matrix()`
- `tests/test_transit_policy.py` — unit tests (synthetic countries)
- `tests/test_policy_validation.py` — mesh before/after + per-rule
  violation table; figure `figures/policy_validation.pdf`

## v3 / v3.1 (2026-07-06, post-campaign 10x mesh + literature round)

Changes: `no_transit("TW")` (APNIC: carriers interconnect in HK/Tokyo, not
Taipei); Africa containment goes country-granular (intra-African coastal
transit chains like the Mozambique hop are also fiction; subsumes the ZA
rule); NEW mechanism `cable_factors` — RTT multipliers for real-but-
chronically-degraded infrastructure. v3 distrusted SEACOM/TGN-Eurasia x2
and the model displaced onto IMEWE (residual 112.6 -> 110.0 ms, same
trench): the unit of distrust is the CORRIDOR. v3.1 applies x2 to the
whole documented Red Sea cut series (SEACOM/TGN, IMEWE, EIG, SMW4, AAE-1;
Feb 2024 + Sept 2025 incidents, months-long Yemeni-permit repair delays).
Result: corridor gone from the offender list, violations HELD at 4.2% —
the distrust survives its falsifier.

Deliberate non-rules from the research round: OM/JO/KH (endpoint-side
residuals — Gulf tromboning per RIPE NCC Gulf report / access overhead —
need a regional-slack mechanism, not routing rules); IL (documented Arab
avoidance, but Blue-Raman/TEAS are making Israeli transit real; +5 ms
signal too weak to validate a rule); JP (ban-like signature but Tokyo is
a documented open hub — suspect wrong-landing-site switching, needs
node-level look).

Next-iteration agenda from the v3.1 offender table: ITU Iraq links
(92.7 ms — conflict-zone overland, Europe<->Gulf shortcut fiction);
ITU Nepal links (62.8 — Himalayan India<->China crossing); APG (107.6);
watch inf-pair growth (18k pairs, 15%) from granular Africa — the
geolocation integration needs an explicit inf-fallback convention.
