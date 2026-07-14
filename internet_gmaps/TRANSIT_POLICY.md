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

## v3.2 (2026-07-06, evening round)

NEW mechanism `terrestrial_factors` — RTT multipliers on ITU links
INTERNAL to a country (the Iraq lesson: ITU IQ at 92.7 ms while IQ
'transit' pairs riding Gulf coastal cables are fine; a node ban would
have severed those submarine chains). v3.2 = v3.1 + distrust-itu[IQ x2].

## v3.3 (2026-07-09, expanded mesh 835k pairs)

The granular Africa ban goes **terrestrial-only** (new `CountryRule
terrestrial_only=True`): only ITU overland edges in African countries are
banned for pairs without the matching African endpoint; nodes stay, so
the east/west coastal submarine systems keep routing — sea cables around
Africa ARE how traffic transits the continent, and the node-level ban
was severing them because ocean vertices reverse-geocode to the nearest
coastal state. Evidence: the v3.2 falsifier table itself (MR/EH/CV at
15-16%, MA/DZ/TN at 7-9% raw violations with negative medians — floors
pushed ABOVE real measurements on west-coast routes), plus ~14% of
sampled pairs stranded outright (no allowed route).

Paired contract change in the query layer: `PolicyFloorEstimator.
floor_ms` now RAISES `NoRouteError` (a KeyError) when the policy strands
a pair the open graph can route, instead of silently substituting the
OPEN floor (`no_route="open"` restores the old fallback). Points off the
graph entirely (open floor inf) still return inf — that is graph
coverage, not policy. `test_policy_validation.py::
test_no_policy_stranded_pairs` pins stranded == 0 under the current
policy; the "inf-fallback convention" agenda item above is thereby
closed.

## v3.4 (2026-07-09, FALSIFIED same day — kept for the progression)

From the v3.3 stranded-pair diagnosis (12,180 sampled pairs open-routable
but policy-unroutable): soviet-bloc gains region-level endpoint exemption
(the per-country exemption left every landlocked bloc state — KZ 3.7k
stranded pairs, KG/TJ/MN — with no exit, while reality exits via Moscow),
and the Pacific relay-island exemption was extended by enumeration to the
Caribbean (Antilles chain; DO at 11M population was stranded by its
neighbors) and Atlantic/Indian oceans (CV/ST/GQ, MU/RE/SC/KM/MV).
Falsifier verdict: enumeration is whack-a-mole. The west-Africa trunk
also crosses the waters of small NON-island states (GM/GW/GA/NA/EH/MR —
Mauritania at 4.9M sits just under the 5M threshold), so formerly-
stranded west-African pairs got detour floors ABOVE their measurements
(GM 72%, GW 78%, NG 80% raw violations) and 2,256 landlocked-African
pairs stayed stranded.

## v3.5 (2026-07-09, current)

The v3.3 terrestrial-only insight generalized — every intuition in the
policy was about OVERLAND fiber:

- `small-country-terrestrial[<5M]`: the small-state ban applies to ITU
  edges only. Node semantics kept severing submarine trunks wherever
  ocean vertices geocode to a small coastal state (Pacific v1 88-99%,
  Antilles 28-39%, west Africa 50-80% — three oceans, same artifact).
- `no-africa-transit-terrestrial-except-suez`: back to REGION
  granularity. Landlocked Africa (UG/ZW/ZM/MW/BF/TD/SS/RW/CD/LS) must
  cross neighbors overland to reach the coast — geography, not fiction;
  the coastal-chain fiction the granular rule targeted is submarine and
  the terrestrial-only mechanism already governs it. Non-African pairs
  still cannot cross Africa overland.
- soviet-bloc-minus-EU-region kept from v3.4 (validated: KZ 69.9 ->
  27.7 ms median at 0.7% violations, MN 126.7 -> 23.7 at 0.9%).

Result (120k sampled pairs, 835k-pair mesh): **0 policy-stranded pairs**
(the 108 remaining inf are open-graph coverage gaps), raw-floor
violations 3.9% -> **1.2%** — the most admissible floor of the series —
at the cost of explanatory tightness (median residual 10.1 -> 16.7 ms vs
open 19.2). For the geolocator the trade is correct: a floor's hard
requirement is admissibility (never above reality); tightness is
secondary. The falsified v3.1-agenda items now read: every formerly-
transiting country sits at positive median residual with <9% violations
(worst: RU 8.6%). test_no_policy_stranded_pairs pins stranded == 0;
NoRouteError (floor_query) makes any future regression loud.

Open question carried forward: the v3.2/v3.3 tightness (median ~10 ms)
came partly from bans the falsifiers now reject. Recovering it without
violating admissibility needs finer mechanisms — the interconnection-
evidence rule (HANDOFF_routing_realism.md) is the ranked candidate.

## v3.6 (2026-07-09, FALSIFIED same day — kept for the progression)

Three rules against the routes the v3.5 relaxations exposed:
(1) `no-small-island-transit` — small-population island nations never
carry non-island traffic (node-level: kills ZA<->IN riding SAFE through
Mauritius); (2) RJCN/KJCN distrust x2 (land at Nakhodka, carriers
interconnect in Tokyo/HK — the Taiwan lesson at cable level) + NEW group
form of terrestrial_factors covering cross-border overland corridors
(AZ-KG-TJ-TM-UZ x2: 96-115 ms residuals even for bloc-endpoint pairs);
(3) NEW corridor_factors mechanism — geographic distrust box over
Yemen waters / Bab-el-Mandeb x1.5 (future trench siblings inherit the
war-zone risk automatically) + PEACE by name x2 (it absorbed the
displaced model traffic under v3.5: 165 ms, n=5,023).
Falsifier verdict: rules (2)/(3) validated (tightness recovered 16.8 ->
13.1 ms median, violations 2.1%, no EU<->Asia blast radius from the
corridor factor). Rule (1) scoped its exemption wrong: only SMALL-island
endpoints could unlock the class, stranding Hispaniola — DO (11M) and HT
are big islands served by the small-island Antilles chain — 1,085
sampled pairs, plus BL/MF missing from the class (137 more).

## v3.7 (2026-07-09, current)

v3.6 with the island rule's exemption widened to ANY island-nation
endpoint (`ISLAND_NATIONS` = small + big islands + Pacific relays;
RegionRule gains `exempt_region` ⊇ region) and BL/MF added to the class.
Island traffic rides island chains; mainland<->mainland still cannot
touch small islands.

Result (120k sampled pairs): **0 policy-stranded pairs** (108 inf are
open-graph coverage gaps), median residual 13.5 ms (vs open 19.2, v3.5
16.7), raw violations 2.0%, overshoot 20.2%. The three v3.6 mechanisms
survive their falsifiers under v3.7: MN 22.3 ms med / 1.3% viol, BY
18.8 / 2.2%, CN 22.3 / 2.4%, Maghreb positive medians.

Known remaining over-restriction (the next refinement, deliberately not
patched by enumeration): waters-attribution on the island ban — TT 15.5%
/ SH 11.3% / CV 6.8% raw violations with negative medians are cables
PASSING those islands' waters (ocean vertices geocode to the nearest
island), not island infrastructure. Candidate mechanism: scope node bans
to near-shore nodes (distance to the geocoded settlement <= ~75 km),
which would also sharpen every other node rule's caveat about mid-ocean
vertices.
