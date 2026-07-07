# Fiber Atlas — data availability survey

> Written 2026-07-04. Companion to `../.claude/HANDOFF_fiber_atlas.md`.
> Everything marked **[verified live]** was actually fetched today from this
> machine, not just found in a paper.

## TL;DR

The two load-bearing datasets are free and live **right now**:

1. **TeleGeography submarine cable map** — public GeoJSON API, 715 cable
   geometries + landing points, updated 2026-07-03. **[verified live]**
2. **ITU BBmaps terrestrial transmission map** — open GeoServer **WFS**
   endpoint serving **40,358 terrestrial link LineStrings** (fibre +
   microwave, with operational status) as GeoJSON, `Fees: NONE,
   AccessConstraints: NONE`. **[verified live]** This is the piece the
   handoff assumed was "mostly proprietary" — it isn't, at the
   node-to-node level.

That is enough to build handoff milestones 0–2 without paying anyone or
emailing anyone. The genuinely unavailable stuff (true rights-of-way
geometry, commercial metro fiber) only matters for tightening the floor
later, and there are defensible proxies (OSM corridors, detour factors).

---

## Tier 1 — free, verified live today

### TeleGeography Submarine Cable Map API v3 **[verified live]**

- `https://www.submarinecablemap.com/api/v3/cable/cable-geo.json` —
  715 cables as MultiLineStrings (last-modified 2026-07-03).
- `https://www.submarinecablemap.com/api/v3/landing-point/landing-point-geo.json`
  — landing points as Points with city names.
- `https://www.submarinecablemap.com/api/v3/cable/{id}.json` — per-cable
  metadata: **published total length** (e.g. 2Africa = "45,000 km";
  625 of 695 cables have one), landing-point list, RFS year, owners.
- Caveats:
  - Geometries are **cartographic**, not surveyed routes — stylized for
    display, offset so parallel cables don't overlap. Segment length
    computed from the polyline ≠ true cable length. The published
    per-cable `length` field lets us calibrate a per-cable **slack
    factor** (true km / drawn km), typically ~1.1–1.5.
  - The old `telegeography/www.submarinecablemap.com` GitHub repo is dead
    (forks like `delusan/…` froze years ago; a crawl-based mirror exists at
    [lintaojlu/submarine_cable_information](https://github.com/lintaojlu/submarine_cable_information)
    with historical snapshots). The live API is the right source.
  - License: the API is public but the data is TeleGeography's; research
    use is community-standard (Nautilus, iGDB etc. all build on it), but
    **don't redistribute raw dumps in a public repo** without checking —
    they sell a [geocoded-data license](https://www2.telegeography.com/license-geocoded-map-data)
    (Tier 3 below) and are known to be friendly to academics who ask.

### ITU BBmaps terrestrial transmission map (WFS) **[verified live]**

- Portal: https://bbmaps.itu.int/app ; catalogue record: [ITU Transmission
  Networks](https://bbmaps.itu.int/geonetwork/srv/api/records/f9af598b-da16-4a7a-a757-6cffc02e9565).
- Open WFS (GeoServer):
  `https://bbmaps.itu.int/geoserver/itu-geocatalogue/ows?service=WFS&version=2.0.0&request=GetFeature&typeNames=itu-geocatalogue:trx_geocatalogue&outputFormat=application/json`
  → 40,358 LineString features: 35,212 "Fibre Operational", 2,805
  "Microwave Operational", ~2,200 planned/under-construction fibre.
  Capabilities advertise `Fees: NONE, AccessConstraints: NONE`.
- Sourced by ITU from operator RFIs + public operator maps; strongest in
  Africa / Asia-Pacific / CIS / LatAm (their mapping projects), which is
  exactly where our island/isolated failure class lives.
- Caveats:
  - Many features are **two-vertex city-to-city straight lines** —
    node-to-node abstraction, not rights-of-way. Fine for a floor graph
    (apply a terrestrial detour factor); do not read physical routes off it.
  - Coverage in US/EU is thinner (ITU focuses on developing regions);
    cover those with NREN topologies + OSM + InterTubes instead.
  - No formal license text found on the record page. For a paper we should
    email `fns@itu.int` (contact in the WFS capabilities) to confirm
    citation/redistribution terms — but access itself is unrestricted.

### PeeringDB

- Public REST API (https://www.peeringdb.com/apidocs/), full dump of
  IXPs + interconnection facilities **with lat/lon** (`fac` objects), also
  published as KMZ. Free; AUP forbids commercial resale, research is fine.
- Role: node set for the terrestrial v0 graph ("connect IXP cities"),
  snapping anchors, and city importance weighting.

### Internet Topology Zoo (NREN + operator topologies)

- ~250 operator topologies (GÉANT, Internet2, AARNet, national NRENs…) in
  GraphML/GML **with per-node lat/lon**. Original site died April 2024;
  long-term archive: [mroughan/InternetTopologyZoo](https://github.com/mroughan/InternetTopologyZoo).
- Caveat: snapshots are 2010–2012 vintage. Backbone links are durable
  (rights-of-way don't move) but treat as prior, not current truth.
  Current GÉANT/Internet2/ESnet maps are published on their own sites.

### OpenStreetMap / Open Infrastructure Map

- OSM `telecom:medium=fibre` + related tags, rendered at
  https://openinframap.org (code: [openinframap/openinframap](https://github.com/openinframap/openinframap));
  bulk extracts via osmium on planet files or paid convenience exports
  ([Infrageomatics](https://www.infrageomatics.com/products/osm-export)).
- Actual mapped fiber is **very patchy** (France excellent, most of the
  world absent). The bigger OSM win for us is **rights-of-way priors**:
  motorway + rail geometries to route corridor edges where no fiber data
  exists, per the handoff's v0 plan.

### iGDB — Internet Geographic Database (IMC 2022)

- [Paper](https://dl.acm.org/doi/10.1145/3517745.3561443) +
  [code/data, GPL-3.0](https://github.com/standerson4/iGDB). Anderson,
  Salamatian, Bischof, Dainotti, Barford.
- Already does a chunk of our part-1/part-2: merges submarine cables,
  PoPs/colos/IXPs, and **approximates terrestrial links along shortest
  rights-of-way (highways/railways)**, shipped as processed CSVs ready to
  load. ~2022 vintage, low maintenance (6 commits).
- Decision to make early: **fork/ingest iGDB vs. build our own graph and
  use iGDB as cross-check + bibliography.** Worth an afternoon of reading
  their schema before writing our own part 2.

## Tier 2 — free validation / latency data (for the loop, not the graph)

- **Our RIPE Atlas mesh** — `../cache/cached_target_data.pkl`, ~800k
  directed pair constraints `floor ≤ observed`. Primary validation set.
- **WonderNetwork global pings** — https://wondernetwork.com/pings,
  ~240 city mesh, hourly, min-of-30 style; day-dumps downloadable from
  their blog. **Independent** second validation mesh with server
  locations known by construction (their own POPs) — nice because it
  can't share RIPE's probe-geolocation errors.
- **CAIDA Ark** topology/RTT data — free with academic agreement
  (ask-nicely-lite); useful later for path-level validation.
- **Nautilus** ([SIGMETRICS'24](https://arxiv.org/abs/2302.14201), open
  source) — maps 3M IP links to specific submarine cables with confidence
  scores. Not needed for the floor itself, but a ready-made tool when we
  want to ask "does traffic for this pair actually take the cable my
  shortest path chose?" Successor to watch: Calypso
  ([SIGCOMM 2025](https://dl.acm.org/doi/10.1145/3718958.3750512)).

## Tier 3 — data at a cost

| Product | What you get | Cost signal |
|---|---|---|
| [TeleGeography geocoded map data license](https://www2.telegeography.com/license-geocoded-map-data) | The same GeoJSON we can already fetch, plus contractual right to redistribute + updates | Annual license, price on request (institutional $k's). Only needed if we publish a derived dataset. |
| TeleGeography Transport Networks / GlobalComms research | Capacity, pricing, ownership detail | Expensive; irrelevant to a latency floor. |
| [Infrapedia](https://www.infrapedia.com/) paid tiers | Global subsea + terrestrial + colo atlas, crowdsourced/near-real-time | Free to view w/ login, no bulk download; they broker data-provider intros. Research access: Tier 4. |
| FiberLocator / GeoTel / Wired Real Estate | US commercial metro + long-haul fiber maps | $k–$10k's; overkill for a floor model. |

Assessment: **nothing in this tier is needed** for milestones 0–4. The
only plausible future purchase is the TeleGeography license, and only for
redistribution rights at publication time.

## Tier 4 — email and ask nicely

- **InterTubes** (US long-haul conduits, [SIGCOMM 2015](https://conferences.sigcomm.org/sigcomm/2015/pdf/papers/p565.pdf)):
  hosted on the [IMPACT portal](https://www.impactcybertrust.org/dataset_view?idDataset=521)
  (DS-0521, free account, listed "unrestricted / commercial use allowed").
  IMPACT's operational status is shaky post-2022 — if registration fails,
  email **Ram Durairajan (U. Oregon)** or **Paul Barford (UW-Madison)**;
  both are responsive to student data requests, and Barford co-authored
  iGDB (which already embeds InterTubes-style US conduits).
- **Internet Atlas / Network Atlas** (the UW predecessor of Infrapedia):
  [IMPACT DS-1145](https://www.impactcybertrust.org/dataset_view?idDataset=1145)
  says researchers granted access get **one year of downloadable updates
  from live.infrapedia.com** — the documented ask-nicely path into
  Infrapedia's global terrestrial layer. Worth one email once we know
  what ITU+OSM coverage is missing.
- **ITU** (`fns@itu.int`): confirm license wording for the WFS layer, and
  ask whether the richer attribute set (operator, capacity, link length)
  behind the BBmaps app is sharable for research.
- **NREN NOCs** (GÉANT operations, Internet2, ESnet): current topology
  files with site coordinates on request; ESnet and Internet2 publish
  much of it openly already.
- **TeleGeography research team**: academics report getting blessing (and
  occasionally richer data) just by describing the project.

## What is genuinely NOT available (and the workaround)

1. **True terrestrial rights-of-way geometry, globally.** Commercial
   long-haul routes (Zayo, Lumen, euNetworks…) are proprietary; no amount
   of asking gets a global vector layer. Workaround (handoff already has
   it): corridor edges = OSM motorway/rail shortest paths × detour
   factor, calibrated per-region against the mesh's lower envelope.
2. **Surveyed submarine cable routes.** Cartographic polylines + published
   total lengths is as good as it gets publicly; calibrate slack factors.
   (KIS-ORCA / admiralty charts exist for fishing safety but are
   license-encumbered and barely better for us.)
3. **Last-mile / metro detail.** Irrelevant for a floor — charge last-mile
   at fiber speed over the geodesic from the snap point, as the handoff
   prescribes.
4. **Live cable status / outages.** Not needed for a floor; Nautilus-style
   failure analysis is a different product.

## Prior art — has anyone built the "Google Maps for packets"?

Short answer (surveyed 2026-07-04): **no complete, citable version exists.**
One hobbyist tool covers the submarine half; academia has built every
component separately, always in service of a different question.

- **[GeoCables](https://www.geocables.com/)** — the closest thing. Solo
  developer ("Evgeny K."), active 2026. Dijkstra over TeleGeography
  cables + landing points, speed-of-light latency model, RIPE Atlas pings
  for spot validation, API for registered users. **Submarine only** —
  land segments are a flat distance multiplier, no terrestrial graph, no
  floor semantics, no published accuracy evaluation. Useful as a sanity
  cross-check for our milestone-1 numbers, and its existence is demand
  validation, not preemption.
- **["Towards a Speed of Light Internet"](https://arxiv.org/pdf/1505.03449) /
  "Why is the Internet so slow?!"** (Singla et al. 2014, Bozkurt et al.
  PAM 2017) — measured the 3–4× inflation of RTT over c-latency and
  named infrastructure inflation "the next frontier". Uses **geodesic**
  c-latency as the baseline throughout — i.e., they quantified the
  problem our floor model fixes, and stopped there.
- **[cISP](https://www.usenix.org/conference/nsdi22/presentation/bhattacherjee)**
  (NSDI '22) — built exactly our kind of graph (rights-of-way, towers,
  length/speed edge weights) but to *design a new* near-c network, not to
  model the floor of the existing one. Their methodology transfers;
  their artifact doesn't answer `floor(src, dst)`.
- **[iGDB](https://dl.acm.org/doi/10.1145/3517745.3561443)** (IMC '22) —
  the cross-layer *database* (conduits along rights-of-way + cables +
  PoPs) without a latency-floor query engine or mesh validation on top.
  The closest academic substrate; not the product.
- **[Nautilus](https://arxiv.org/abs/2302.14201)** (SIGMETRICS '24),
  **[Calypso](https://dl.acm.org/doi/10.1145/3718958.3750512)**
  (SIGCOMM '25), Xaminer, and ISOC Pulse's 2026 submarine-performance
  work — all solve the **inverse** problem: given measured paths, which
  cable did they use / how did it perform. None offers point-to-point
  minimum-RTT queries.
- **Latency prediction systems** (iPlane, structural latency prediction,
  [GLIDS](https://arxiv.org/pdf/2405.04319) 2024) — measurement-driven
  *estimates* of actual latency, not physical lower bounds; wrong
  semantics for constraint-based inference (an estimate can be violated,
  a floor cannot).

Why the gap persists, best guess: a floor model is infrastructure-paper
material (hard to publish standalone, so fragments ship inside bigger
papers); the actors with surveyed route data (operators, TeleGeography,
HFT route planners) monetize it privately and certainly have internal
equivalents; and validating a *floor* needs a dense measured mesh plus
the `floor ≤ observed` discipline — which we happen to have.

Differentiators worth defending in a paper: (1) submarine + terrestrial
in one queryable graph, (2) explicit floor semantics with
violation-driven refinement against a mesh, (3) a downstream customer
(geolocation) demonstrating end-to-end value.

## Suggested immediate next steps

1. `python fetch_public_data.py` (this dir) — snapshot TeleGeography
   (cable-geo, landing points, per-cable metadata) + ITU WFS dump into
   `data/raw/<date>/`. Re-run any time; sources are living documents.
2. Read iGDB's schema (`standerson4/iGDB`) before designing ours —
   decide ingest-vs-crosscheck.
3. Milestone 0 from the handoff: mesh + geodesic baseline tightness
   report, so the submarine-only graph has a before/after.
4. Defer all Tier-3/Tier-4 contact until the v1 violation report tells us
   *which* missing infrastructure actually hurts.
