"""Transit policies: whose fiber can carry through-traffic.

The validation runs showed the shortest-fiber model's residuals concentrate
on paths the model routes through countries that real traffic avoids (see
TRANSIT_POLICY.md for the evidence and literature). A TransitPolicy is a
named set of rules deciding, per country code, whether that country's
infrastructure may be used by traffic that neither originates nor
terminates there.

Semantics (v1, node-based): a node in restricted country X is removed from
the graph for pair (src, dst) unless X ∈ {cc(src), cc(dst)}. Removing a
node removes all its incident edges. Refinement (v3.3, edge-based):
a rule with terrestrial_only=True bans only ITU overland edges touching
X — nodes stay, so submarine systems whose ocean vertices geocode to X
keep routing (sea cables along a coast ARE transit; see the Africa rule).
Caveats, documented so we can refine:
  - Node countries come from nearest-city reverse geocoding, so mid-ocean
    cable vertices are attributed to the nearest coastal state; a cable
    merely passing NEAR a restricted island is treated as landing there
    (conservative — restricts too much, never too little). For NODE-based
    rules only; terrestrial-only rules are immune by construction.
  - Same-cable pass-through at a landing station (express wavelengths) is
    also blocked; distinguishing "lands and interconnects" from "lands and
    continues" needs per-edge cable ids — a future refinement.

Extending over time: add rules below and rebuild DEFAULT_POLICY. For
future endpoint-conditional rules ("US-anywhere pairs may not transit the
Middle East"), add a second rule family with signature
(src_cc, transit_cc, dst_cc) -> bool and fold it into allowed_node_mask —
the per-pair grouping in policy_floor_matrix already supports
pair-dependent masks, only the class key needs to grow.
"""

from dataclasses import dataclass
from typing import Callable, Tuple

import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra

import geo
from floor_query import DEFAULT_DIRECT_KM_MAX, DEFAULT_LASTMILE_KM_MAX

# Rough 2024 populations in millions, for the small-country rule. Rough is
# fine (the rule is a heuristic); update freely. Missing codes are treated
# as 0 (restricted) — the long tail of microstates and territories.
POP_MILLIONS = {
    "AF": 42, "AL": 2.8, "DZ": 45, "AO": 36, "AR": 46, "AM": 2.8, "AU": 26,
    "AT": 9.1, "AZ": 10.2, "BA": 3.2, "BD": 173, "BE": 11.7, "BF": 23,
    "BG": 6.4, "BH": 1.5, "BI": 13, "BJ": 14, "BO": 12, "BR": 216,
    "BT": 0.8, "BW": 2.7, "BY": 9.2, "CA": 40, "CD": 102, "CF": 5.7,
    "CG": 6.1, "CH": 8.9, "CI": 29, "CL": 20, "CM": 28, "CN": 1411,
    "CO": 52, "CR": 5.2, "CU": 11, "CY": 1.3, "CZ": 10.9, "DE": 84,
    "DJ": 1.1, "DK": 5.9, "DO": 11.3, "EC": 18, "EE": 1.4, "EG": 106,
    "ER": 3.7, "ES": 48, "ET": 127, "FI": 5.6, "FJ": 0.9, "FR": 68,
    "GA": 2.4, "GB": 68, "GE": 3.7, "GH": 34, "GM": 2.7, "GN": 14,
    "GQ": 1.7, "GR": 10.4, "GT": 18, "GU": 0.17, "GW": 2.1, "GY": 0.8,
    "HK": 7.5, "HN": 10.6, "HR": 3.9, "HT": 11.7, "HU": 9.6, "ID": 277,
    "IE": 5.3, "IL": 9.8, "IN": 1429, "IQ": 45, "IR": 89, "IS": 0.39,
    "IT": 59, "JM": 2.8, "JO": 11.4, "JP": 124, "KE": 55, "KG": 7.0,
    "KH": 17, "KM": 0.9, "KP": 26, "KR": 52, "KW": 4.3, "KZ": 20,
    "LA": 7.6, "LB": 5.5, "LK": 22, "LR": 5.4, "LS": 2.3, "LT": 2.9,
    "LU": 0.66, "LV": 1.9, "LY": 6.9, "MA": 37, "MD": 2.5, "ME": 0.62,
    "MG": 30, "MK": 1.8, "ML": 23, "MM": 54, "MN": 3.4, "MO": 0.7,
    "MR": 4.9, "MT": 0.54, "MU": 1.3, "MV": 0.52, "MW": 21, "MX": 128,
    "MY": 34, "MZ": 33, "NA": 2.6, "NC": 0.27, "NE": 27, "NG": 224,
    "NI": 7.0, "NL": 18, "NO": 5.5, "NP": 31, "NZ": 5.2, "OM": 5.1,
    "PA": 4.5, "PE": 34, "PF": 0.31, "PG": 10.3, "PH": 117, "PK": 240,
    "PL": 37, "PR": 3.2, "PT": 10.3, "PY": 6.9, "QA": 2.7, "RE": 0.87,
    "RO": 19, "RS": 6.6, "RU": 144, "RW": 14, "SA": 36, "SB": 0.74,
    "SC": 0.12, "SD": 48, "SE": 10.5, "SG": 5.9, "SI": 2.1, "SK": 5.4,
    "SL": 8.8, "SN": 18, "SO": 18, "SR": 0.62, "SS": 11, "SV": 6.3,
    "SY": 23, "SZ": 1.2, "TD": 18, "TG": 9.0, "TH": 72, "TJ": 10.1,
    "TL": 1.4, "TM": 6.5, "TN": 12, "TR": 85, "TT": 1.5, "TW": 23,
    "TZ": 67, "UA": 37, "UG": 48, "US": 335, "UY": 3.4, "UZ": 36,
    "VE": 28, "VN": 99, "VU": 0.33, "XK": 1.7, "YE": 34, "ZA": 60,
    "ZM": 20, "ZW": 16,
}

# Land borders with Russia (incl. Kaliningrad neighbors PL, LT).
RUSSIA_LAND_BORDERS = frozenset(
    {"NO", "FI", "EE", "LV", "LT", "PL", "BY", "UA", "GE", "AZ", "KZ", "MN", "CN", "KP"}
)

# Soviet bloc: USSR republics + Warsaw-Pact-and-adjacent satellites.
USSR = frozenset(
    {"RU", "UA", "BY", "MD", "GE", "AM", "AZ", "KZ", "UZ", "TM", "KG", "TJ", "EE", "LV", "LT"}
)
SOVIET_SATELLITES = frozenset({"PL", "HU", "CZ", "SK", "RO", "BG", "AL", "MN", "KP"})
SOVIET_BLOC = USSR | SOVIET_SATELLITES
# The "nice" carve-out: ex-bloc states integrated into the EU (2004/2007
# accessions). Finland/Norway were never bloc members so need no carve-out.
EU_INTEGRATED_EX_BLOC = frozenset({"EE", "LV", "LT", "PL", "HU", "CZ", "SK", "RO", "BG"})

AFRICA_CCS = frozenset(
    """DZ AO BJ BW BF BI CM CV CF TD KM CG CD CI DJ EG GQ ER ET GA GM GH GN GW KE
    LS LR LY MA MG ML MR MU MW MZ NA NE NG RE RW SC SD SH SL SN SO SS ST SZ TG
    TN TZ UG YT ZA ZM ZW EH""".split()
)
# The Red Sea / Suez corridor: the one African transit that is emphatically
# real (17-30% of intercontinental traffic; see TRANSIT_POLICY.md). Cable
# polylines hug the African Red Sea coast, so these codes must stay open or
# the model bans Asia<->Europe entirely.
SUEZ_CORRIDOR = frozenset({"EG", "SD", "ER", "DJ"})

# Pacific cable-relay islands: waypoints of the operational trans-Pacific
# systems (Southern Cross, Hawaiki, ...). Banning them under the
# small-country rule produced 88-99% raw-floor violations in validation —
# transit is these states' entire role on the network.
PACIFIC_RELAY_ISLANDS = frozenset(
    "FJ NC PF CK WS AS TO TK KI TV NU NF VU SB GU MP MH FM PW NR WF".split()
)

# The same lesson in the other two oceans (v3.4). Caribbean: the Antilles
# chain (ECFS, Southern Caribbean Fiber, ARCOS ring) — banning the island
# waypoints stranded even the Dominican Republic (11M) and produced 28-39%
# raw-floor violations on AG/DM/SX/MS/SR/TT pairs. SR/GY ride the same
# coastal chain.
CARIBBEAN_RELAY_ISLANDS = frozenset(
    "AG AI AW BB BM BQ BS CW DM GD GP GY JM KN KY LC MQ MS PR SR SX TC TT VC VG VI".split()
)
# Atlantic / Indian Ocean: island waypoints of the Africa coastal systems
# (WACS/SAT-3/Equiano pass CV/ST/GQ waters; SAFE/METISS/LION hop
# MU/RE/SC/KM/YT; SEA-ME-WE branches hop MV). Node-banning them under the
# small-country rule severed the very submarine chains the v3.3 Africa
# rule re-opened, stranding NG/AO/CM/MU endpoints.
ATLANTIC_INDIAN_RELAY_ISLANDS = frozenset(
    "CV GQ KM MV MU RE SC SH ST YT".split()
)
CABLE_RELAY_ISLANDS = (
    PACIFIC_RELAY_ISLANDS | CARIBBEAN_RELAY_ISLANDS | ATLANTIC_INDIAN_RELAY_ISLANDS
)

# Small-population ISLAND nations/territories (v3.6): access networks, never
# transit for non-island traffic. Membership = own cc + island + <5M people;
# major island hubs are outside by population (HK 7.5M, SG 5.9M, TW, NZ, IE)
# and mainland small states (Lesotho, Bhutan, ...) are handled by the
# terrestrial small-country rule instead. MT/CY/BH included on the same
# population logic — watch their falsifier rows; Azores/Madeira/Canaries
# cannot be expressed (they carry PT/ES codes). Pacific relays are exempted
# in the rule itself (falsifier: 88-99% violations when banned, v1).
SMALL_ISLAND_NATIONS = frozenset(
    """AG AI AW BB BL BM BQ BS CW DM GD GP JM KN KY LC MF MQ MS PR SX TC TT VC VG VI
    CV ST SH IS FO FK GL
    KM MU MV RE SC YT
    MT CY BH""".split()
)
# Island nations of ANY size (v3.7): may UNLOCK small-island transit —
# island traffic is served by island chains (DO/HT out of Hispaniola ride
# the Antilles; 1,085 sampled pairs stranded when only small-island
# endpoints could unlock). Mainland<->mainland pairs still cannot touch
# the small islands.
BIG_ISLAND_NATIONS = frozenset(
    "CU DO HT LK MG IE GB JP NZ PH ID TW SG HK BN PG TL".split()
)
ISLAND_NATIONS = SMALL_ISLAND_NATIONS | BIG_ISLAND_NATIONS | PACIFIC_RELAY_ISLANDS


@dataclass(frozen=True)
class CountryRule:
    """Node in a restricted country X is banned for a pair unless X is one
    of the pair's endpoint countries (per-country exemption).

    terrestrial_only: the ban applies to ITU overland edges only — nodes
    stay in the graph, so submarine systems whose vertices geocode to X
    (coastal waypoints, landing stations) keep routing through. This is
    how the model expresses "X's overland fiber is not a through-route,
    but cables passing along its coast are" (the Africa v3.3 lesson)."""

    name: str
    member: Callable[[str], bool]
    terrestrial_only: bool = False

    def banned(self, cc: str, endpoint_ccs) -> bool:
        return self.member(cc) and cc not in endpoint_ccs

    def endpoint_signature(self, cc: str):
        # targets with equal signatures induce identical bans for this rule
        return cc if self.member(cc) else None


@dataclass(frozen=True)
class RegionRule:
    """Region containment: nodes in the region are banned unless ANY
    endpoint is in the exempting region (continent-level exemption).
    exempt_region: who may unlock the ban — defaults to the banned region
    itself; may be a superset (v3.7 island rule: SMALL islands are banned,
    but ANY island endpoint unlocks them — big-island states like DO/HT
    are served by the small-island chains).
    terrestrial_only: as in CountryRule — ITU edges only, nodes stay."""

    name: str
    region: frozenset
    terrestrial_only: bool = False
    exempt_region: frozenset = None

    def _exempt(self):
        return self.region if self.exempt_region is None else self.exempt_region

    def banned(self, cc: str, endpoint_ccs) -> bool:
        return cc in self.region and not any(e in self._exempt() for e in endpoint_ccs)

    def endpoint_signature(self, cc: str):
        return cc in self._exempt()


def no_transit(*ccs):
    """These countries' fiber never carries third-party traffic."""
    s = frozenset(ccs)
    return CountryRule(f"no-transit[{','.join(sorted(s))}]", lambda cc: cc in s)


def small_country(min_population_m=5.0, exempt=frozenset(), terrestrial_only=False):
    """Small states' fiber is access infrastructure, not transit (population
    proxy; unknown codes count as small). KNOWN over-restrictive for
    cable-relay islands — see TRANSIT_POLICY.md. `exempt` carves out small
    states that are established relays (e.g. the Suez corridor's DJ/ER).

    terrestrial_only (v3.5): ban only ITU overland links. As a node ban
    the rule kept severing submarine trunks wherever their ocean vertices
    geocode to a small coastal state (Pacific relays in v1, the Antilles,
    and finally GM/GW/GA/NA/EH/MR on the west-Africa trunk — 50-80% raw
    violations in the v3.4 falsifier). The access-not-transit intuition
    is about overland fiber; the ocean artifact is not evidence."""
    exempt = frozenset(exempt)
    kind = "-terrestrial" if terrestrial_only else ""
    return CountryRule(
        f"small-country{kind}[<{min_population_m}M]",
        lambda cc: POP_MILLIONS.get(cc, 0.0) < min_population_m and cc not in exempt,
        terrestrial_only=terrestrial_only,
    )


def russia_borders():
    """v1 rule, kept for comparison runs; superseded by soviet_bloc()."""
    return CountryRule("russia-borders", lambda cc: cc in RUSSIA_LAND_BORDERS)


def soviet_bloc(exempt_eu=True, region_exempt=False):
    """Soviet-bloc overland corridors are not practical through-routes —
    except the ex-bloc EU members (Baltics, Poland, etc.), which are
    ordinary European transit fabric (the v1 Finland lesson, generalized).

    region_exempt (v3.4): bloc fiber may carry traffic with a bloc
    endpoint. The per-country exemption stranded every landlocked bloc
    state (KZ/KG/TJ/MN had NO allowed route to the West — all their
    neighbors are bloc), while reality is that their traffic exits via
    Moscow. EU↔Asia THROUGH-transit across Russia stays banned: neither
    endpoint is in the bloc. The exempting region is the banned set
    itself, so an EU-integrated ex-bloc endpoint (EE, PL, ...) does not
    unlock Russian overland shortcuts."""
    banned = SOVIET_BLOC - (EU_INTEGRATED_EX_BLOC if exempt_eu else frozenset())
    tag = "-minus-EU" if exempt_eu else ""
    if region_exempt:
        return RegionRule(f"soviet-bloc{tag}-region", frozenset(banned))
    return CountryRule(f"soviet-bloc{tag}", lambda cc: cc in banned)


def small_island_transit(exempt=PACIFIC_RELAY_ISLANDS, exempt_region=ISLAND_NATIONS):
    """Small-population island nations are access networks, never transit:
    if neither endpoint is an island, paths may not touch a small one.
    The exemption is ISLAND-level (any island endpoint — big islands like
    DO/HT included — unlocks the class), so island -> island -> mainland
    access chains stay routable: the per-country variant stranded the
    Antilles in v3.2, and the small-island-only exemption stranded
    Hispaniola in v3.6. Node-level ON PURPOSE, unlike the terrestrial-only
    rules: the point is to stop submarine through-routing (ZA<->IN riding
    SAFE through Mauritius), which an ITU-edge ban cannot express."""
    region = SMALL_ISLAND_NATIONS - frozenset(exempt)
    return RegionRule(
        "no-small-island-transit", region, exempt_region=frozenset(exempt_region)
    )


# Open Indian Ocean (v3.8): south of the Suez->India->Malacca mainline
# (lat cap 4N keeps the northern rim and Sri Lanka out), east of the
# African coastal corridor (lon >= 50), west of Sumatra / the Perth<->
# Singapore corridor (lon <= 95). Pure ocean + islands by construction.
INDIAN_OCEAN_BOX = (-45.0, 4.0, 50.0, 95.0)
INDIAN_OCEAN_ISLANDS = frozenset("MG MU RE SC KM MV YT".split())


def indian_ocean_containment():
    """Practically nothing transits the open Indian Ocean: ZA<->AU or
    Gulf<->AU crossings are fiction (reality routes via Europe/Suez or the
    Pacific-Asian corridor). Island-endpoint traffic (MG/MU/RE/SC/KM/MV/YT)
    keeps its chains. Nodes in the box carry pseudo-cc "XI" (see
    TransitPolicy.node_cc_remaps), so this is an ordinary RegionRule."""
    return RegionRule(
        "no-indian-ocean-transit",
        frozenset({"XI"}),
        exempt_region=frozenset({"XI"}) | INDIAN_OCEAN_ISLANDS,
    )


def africa_containment(exempt_suez=True, country_granular=False, terrestrial_only=False):
    """African infrastructure only carries traffic with an African endpoint
    (the boomerang-routing reality). The Suez/Red-Sea corridor is exempt by
    default — banning it would ban the planet's main Asia<->Europe route.

    country_granular (v3): even African pairs only get their own two
    countries' fiber — the mesh showed intra-African transit chains
    (e.g. the Mozambique coastal hop) are also fiction; real intra-Africa
    traffic trombones via Europe.

    terrestrial_only (v3.3): the ban covers ITU overland links only. The
    east/west coastal submarine systems ARE how traffic rounds Africa, and
    node-level banning severed them because their ocean vertices geocode
    to the nearest coastal state (the west-coast falsifiers: MR/EH/CV at
    15-16% raw violations under v3.2)."""
    region = AFRICA_CCS - (SUEZ_CORRIDOR if exempt_suez else frozenset())
    tag = "-except-suez" if exempt_suez else ""
    kind = "-terrestrial" if terrestrial_only else ""
    if country_granular:
        return CountryRule(
            f"no-africa-transit{kind}-granular{tag}",
            lambda cc: cc in region,
            terrestrial_only=terrestrial_only,
        )
    return RegionRule(
        f"no-africa-transit{kind}{tag}", region, terrestrial_only=terrestrial_only
    )


@dataclass(frozen=True)
class TransitPolicy:
    name: str
    rules: Tuple
    # cable-level distrust: RTT multipliers on named source features, for
    # infrastructure that exists but is chronically degraded (e.g. the
    # 2024-25 Red Sea cut series). Pair-independent — applied to edge
    # weights before routing, so class signatures are unaffected.
    cable_factors: Tuple = ()
    # terrestrial distrust: RTT multipliers on ITU links. A single cc
    # string covers links INTERNAL to that country (both edge endpoints
    # there — the Iraq lesson: ITU IQ links at 93 ms while IQ 'transit'
    # pairs on Gulf cables are fine). A TUPLE of ccs covers any ITU link
    # touching the group (cross-border corridors: the Central-Asia
    # crossings TM-UZ / KG-UZ / IR-TM showed 96-109 ms residuals even for
    # bloc-endpoint pairs — reality trombones via Moscow).
    terrestrial_factors: Tuple = ()
    # corridor distrust: RTT multipliers on ALL edges with a vertex inside
    # a (lat_min, lat_max, lon_min, lon_max) box — chronic-degradation
    # zones where the evidence attaches to the waters, not to cable names
    # (Yemen: war-zone repair permits; new trench siblings inherit the
    # distrust automatically instead of absorbing displaced model traffic
    # like PEACE did). Pair-independent, applied via max() with the other
    # factors.
    corridor_factors: Tuple = ()
    # geographic pseudo-countries: nodes inside a box are re-attributed to
    # a synthetic cc BEFORE any rule runs, so ocean regions can be first-
    # class rule subjects ("XI" = open Indian Ocean, v3.8) without new
    # mask machinery. Tuple of (pseudo_cc, (lat0, lat1, lon0, lon1)).
    node_cc_remaps: Tuple = ()

    def remap_node_cc(self, node_cc, node_lat, node_lon):
        """Apply the policy's geographic pseudo-country remaps."""
        if not self.node_cc_remaps:
            return np.asarray(node_cc)
        node_cc = np.asarray(node_cc).copy()
        node_lat = np.asarray(node_lat)
        node_lon = np.asarray(node_lon)
        for cc, (lat0, lat1, lon0, lon1) in self.node_cc_remaps:
            m = (
                (node_lat >= lat0) & (node_lat <= lat1)
                & (node_lon >= lon0) & (node_lon <= lon1)
            )
            node_cc[m] = cc
        return node_cc

    def banned_set(self, ccs, endpoint_ccs) -> frozenset:
        """Countries banned for a pair with the given endpoint countries
        (union over rule kinds — the attribution view)."""
        endpoint_ccs = frozenset(endpoint_ccs)
        return frozenset(
            cc for cc in set(ccs) if any(r.banned(cc, endpoint_ccs) for r in self.rules)
        )

    def banned_split(self, ccs, endpoint_ccs):
        """(node_banned, terrestrial_banned) for a pair: full node removals
        vs bans that apply to ITU overland edges only (submarine systems
        keep routing through terrestrial-only-banned countries). A country
        hit by both rule kinds lands in node_banned."""
        endpoint_ccs = frozenset(endpoint_ccs)
        node, terr = set(), set()
        for cc in set(ccs):
            for r in self.rules:
                if r.banned(cc, endpoint_ccs):
                    if getattr(r, "terrestrial_only", False):
                        terr.add(cc)
                    else:
                        node.add(cc)
        return frozenset(node), frozenset(terr - node)

    def restricted(self, cc: str) -> bool:
        """Banned for a pair with no exempting endpoints (worst case)."""
        return any(r.banned(cc, frozenset()) for r in self.rules)

    def class_signature(self, cc: str) -> tuple:
        """Targets with equal signatures induce identical node masks (for a
        fixed VP) — the grouping key for policy_floor_matrix."""
        return tuple(r.endpoint_signature(cc) for r in self.rules)

    def describe(self) -> str:
        parts = [r.name for r in self.rules]
        parts += [f"distrust[{name} x{f}]" for name, f in self.cable_factors]
        parts += [
            f"distrust-itu[{cc if isinstance(cc, str) else '-'.join(sorted(cc))} x{f}]"
            for cc, f in self.terrestrial_factors
        ]
        parts += [f"distrust-corridor[{name} x{f}]" for name, _box, f in self.corridor_factors]
        return f"{self.name}: " + " | ".join(parts)


def scaled_base_data(policy, graph, base_coo, node_cc=None):
    """Edge weights with the policy's cable-, terrestrial- and corridor-
    distrust factors applied (max wins on overlap). base_coo entries map
    back to undirected edges via (min,max) lookup."""
    corridor = policy.corridor_factors
    has_features = graph.edge_feature is not None
    if not (
        ((policy.cable_factors or policy.terrestrial_factors) and has_features)
        or corridor
    ):
        return base_coo.data
    edge_factor = np.ones(graph.n_edges)
    if has_features:
        names = list(graph.feature_names)
        for name, f in policy.cable_factors:
            if name in names:
                edge_factor[graph.edge_feature == names.index(name)] = float(f)
        if policy.terrestrial_factors and node_cc is not None and "ITU" in names:
            node_cc = np.asarray(node_cc)
            itu = graph.edge_feature == names.index("ITU")
            for cc, f in policy.terrestrial_factors:
                if isinstance(cc, str):  # single country: internal links only
                    m = itu & (node_cc[graph.edge_src] == cc) & (node_cc[graph.edge_dst] == cc)
                else:  # group: any overland link touching the group
                    group = sorted(cc)
                    m = itu & (
                        np.isin(node_cc[graph.edge_src], group)
                        | np.isin(node_cc[graph.edge_dst], group)
                    )
                edge_factor[m] = np.maximum(edge_factor[m], float(f))
    for _name, (lat0, lat1, lon0, lon1), f in corridor:
        def in_box(idx):
            return (
                (graph.node_lat[idx] >= lat0) & (graph.node_lat[idx] <= lat1)
                & (graph.node_lon[idx] >= lon0) & (graph.node_lon[idx] <= lon1)
            )
        m = in_box(graph.edge_src) | in_box(graph.edge_dst)
        edge_factor[m] = np.maximum(edge_factor[m], float(f))
    edge_of = {
        (int(s), int(d)): e
        for e, (s, d) in enumerate(zip(graph.edge_src, graph.edge_dst))
    }
    data = base_coo.data.copy()
    for k, (r, c) in enumerate(zip(base_coo.row, base_coo.col)):
        e = edge_of.get((min(r, c), max(r, c)))
        if e is not None:
            data[k] *= edge_factor[e]
    return data


V2_POLICY = TransitPolicy(
    "v2-geopolitical",
    (
        no_transit("ZA"),
        no_transit("MN", "CN"),
        no_transit("AF"),
        small_country(5.0, exempt=SUEZ_CORRIDOR | PACIFIC_RELAY_ISLANDS),
        soviet_bloc(),
        africa_containment(),
    ),
)

# The Red Sea cut series (Feb 2024: SEACOM/TGN, AAE-1, EIG; May 2024:
# SEACOM+EASSy off ZA; Sept 2025: SMW4, IMEWE): repairs gated on Yemeni
# permits for months, reality detours via the Cape. Distrusting one system
# just displaced the model onto its trench-siblings (SEACOM -> IMEWE,
# residual 112.6 -> 110.0 ms), so the distrust applies corridor-wide.
RED_SEA_CUT_SERIES = (
    "TG:seacomtata-tgn-eurasia",
    "TG:imewe",
    "TG:europe-india-gateway-eig",
    "TG:seamewe-4",
    "TG:asia-africa-europe-1-aae-1",
)

# v3 (2026-07-06 research round): TW added (cables land there, carriers
# interconnect in HK/Tokyo instead — APNIC); Africa containment goes
# country-granular (intra-African coastal transit chains are also fiction —
# subsumes the old ZA rule); the Red Sea cut-series systems distrusted x2
# (real cables, chronically severed 2024-25). OM/JO/KH deliberately NOT
# banned: their residuals are endpoint-side (Gulf trombone / access
# overhead) which no transit rule can fix. IL/JP on watch.
V32_POLICY = TransitPolicy(
    "v3.2-geopolitical",
    (
        no_transit("MN", "CN"),
        no_transit("AF"),
        no_transit("TW"),
        small_country(5.0, exempt=SUEZ_CORRIDOR | PACIFIC_RELAY_ISLANDS),
        soviet_bloc(),
        africa_containment(country_granular=True),
    ),
    cable_factors=tuple((name, 2.0) for name in RED_SEA_CUT_SERIES),
    # Iraq overland crossings (ITU IQ links, 93 ms feature residual while
    # Gulf coastal cables through IQ waters are fine) — conflict-zone
    # terrestrial, targeted without severing the submarine chains.
    terrestrial_factors=(("IQ", 2.0),),
)

# v3.3 (2026-07-09): the granular Africa ban goes terrestrial-only. The
# east/west coastal submarine systems ARE how traffic rounds Africa; the
# node-level v3.2 ban severed them because ocean vertices geocode to the
# nearest coastal state, stranding pairs entirely (no allowed route) and
# producing the west-coast falsifiers (MR/EH/CV at 15-16% raw violations).
# ITU overland crossings of Africa stay banned for pairs without the
# matching African endpoint. Everything else is v3.2.
V33_POLICY = TransitPolicy(
    "v3.3-geopolitical",
    (
        no_transit("MN", "CN"),
        no_transit("AF"),
        no_transit("TW"),
        small_country(5.0, exempt=SUEZ_CORRIDOR | PACIFIC_RELAY_ISLANDS),
        soviet_bloc(),
        africa_containment(country_granular=True, terrestrial_only=True),
    ),
    cable_factors=tuple((name, 2.0) for name in RED_SEA_CUT_SERIES),
    terrestrial_factors=(("IQ", 2.0),),
)

# v3.4 (2026-07-09, from the v3.3 stranded-pair diagnosis — 12,180 sampled
# pairs still had an open route but no policy route): (1) soviet-bloc goes
# region-exempt — the per-country exemption left every landlocked bloc
# state with no exit (KZ 3.7k stranded pairs, KG/TJ/MN); (2) the Pacific
# relay-island lesson extends to the Caribbean and Atlantic/Indian oceans
# (small-country exemptions for the Antilles chain and CV/ST/GQ,
# MU/RE/SC/KM/MV). FALSIFIED same day: the enumerated-exemption approach
# is whack-a-mole — the west-Africa trunk still crossed the waters of
# non-island small states (GM/GW/GA/NA/EH/MR), forcing detours with
# floors ABOVE measurements (GM 72%, GW 78%, NG 80% raw violations), and
# 2.3k landlocked-African pairs stayed stranded. Kept for the progression.
V34_POLICY = TransitPolicy(
    "v3.4-geopolitical",
    (
        no_transit("MN", "CN"),
        no_transit("AF"),
        no_transit("TW"),
        small_country(5.0, exempt=SUEZ_CORRIDOR | CABLE_RELAY_ISLANDS),
        soviet_bloc(region_exempt=True),
        africa_containment(country_granular=True, terrestrial_only=True),
    ),
    cable_factors=tuple((name, 2.0) for name in RED_SEA_CUT_SERIES),
    terrestrial_factors=(("IQ", 2.0),),
)

# v3.5 (2026-07-09): the v3.3 terrestrial-only insight, generalized.
# (1) small-country goes terrestrial-only — the node ban kept severing
# submarine trunks at every small coastal state's waters (the v3.4
# falsifier); the access-not-transit intuition only ever applied to
# overland fiber. (2) Africa terrestrial containment goes back to REGION
# granularity: an African endpoint unlocks African terrestrial — the
# landlocked (UG/ZW/ZM/MW/BF/TD/SS/RW/CD/LS) must cross neighbors
# overland to reach the coast, which is geography, not routing fiction;
# the fiction the granular rule targeted (coastal transit chains) is
# submarine and stays governed by the terrestrial-only mechanism.
# Non-African pairs still cannot cross Africa overland. Every probe must
# be routable: floor_query raises NoRouteError where a policy strands an
# open-routable pair; test_no_policy_stranded_pairs pins the count at 0.
V35_POLICY = TransitPolicy(
    "v3.5-geopolitical",
    (
        no_transit("MN", "CN"),
        no_transit("AF"),
        no_transit("TW"),
        small_country(
            5.0, exempt=SUEZ_CORRIDOR | CABLE_RELAY_ISLANDS, terrestrial_only=True
        ),
        soviet_bloc(region_exempt=True),
        africa_containment(terrestrial_only=True),
    ),
    cable_factors=tuple((name, 2.0) for name in RED_SEA_CUT_SERIES),
    terrestrial_factors=(("IQ", 2.0),),
)

# v3.6 (2026-07-09, from the v3.5 offender map): three rules against the
# routes the relaxations exposed.
# (1) no-small-island-transit: small-population island nations never carry
#     non-island traffic (kills ZA<->IN over SAFE via Mauritius); class-
#     level exemption keeps island access chains routable; Pacific relays
#     exempt by v1 falsifier. Supersedes the v3.4 enumerated relay lists
#     in small_country's exemption (Pacific kept there for its ITU links).
# (2) RJCN/KJCN distrust x2 (land in Nakhodka, carriers interconnect in
#     Tokyo/HK — the Taiwan lesson, cable-level) + Central-Asia overland
#     group distrust x2 (TM/UZ/KG/TJ/AZ crossings at 96-115 ms even for
#     bloc-endpoint pairs — reality trombones via Moscow).
# (3) Yemen-waters corridor distrust x1.5 (geographic: every system
#     through Bab-el-Mandeb / Gulf of Aden inherits the war-zone repair
#     risk — catches future trench siblings automatically) + PEACE by
#     name x2 (it absorbed the displaced model traffic under v3.5:
#     165 ms, n=5,023 — the corridor-wide lesson, completed).
# FALSIFIED (same day, stranding): only small-island endpoints could
# unlock the island class, stranding Hispaniola — DO/HT are big islands
# served by the small-island Antilles chain (1,085 sampled pairs), plus
# BL/MF missing from the class. Kept for the progression; distrust rules
# (2)/(3) validated and carried forward.
_V36_ISLANDS = (
    SMALL_ISLAND_NATIONS - frozenset({"BL", "MF"})
) - PACIFIC_RELAY_ISLANDS
V36_POLICY = TransitPolicy(
    "v3.6-geopolitical",
    (
        no_transit("MN", "CN"),
        no_transit("AF"),
        no_transit("TW"),
        small_country(
            5.0, exempt=SUEZ_CORRIDOR | PACIFIC_RELAY_ISLANDS, terrestrial_only=True
        ),
        soviet_bloc(region_exempt=True),
        africa_containment(terrestrial_only=True),
        RegionRule("no-small-island-transit", _V36_ISLANDS),
    ),
    cable_factors=tuple((name, 2.0) for name in RED_SEA_CUT_SERIES)
    + (
        ("TG:peace-cable", 2.0),
        ("TG:russia-japan-cable-network-rjcn", 2.0),
        ("TG:korea-japan-cable-network-kjcn", 2.0),
    ),
    terrestrial_factors=(
        ("IQ", 2.0),
        (("TM", "UZ", "KG", "TJ", "AZ"), 2.0),
    ),
    corridor_factors=(("yemen-waters", (9.0, 18.0, 41.0, 56.0), 1.5),),
)

# v3.7 (2026-07-09): v3.6 with the island rule's exemption widened to ANY
# island-nation endpoint (ISLAND_NATIONS incl. DO/HT/CU/LK/...) and BL/MF
# added to the class — island traffic rides island chains; mainland<->
# mainland still cannot touch small islands.
V37_POLICY = TransitPolicy(
    "v3.7-geopolitical",
    (
        no_transit("MN", "CN"),
        no_transit("AF"),
        no_transit("TW"),
        small_country(
            5.0, exempt=SUEZ_CORRIDOR | PACIFIC_RELAY_ISLANDS, terrestrial_only=True
        ),
        soviet_bloc(region_exempt=True),
        africa_containment(terrestrial_only=True),
        small_island_transit(),
    ),
    cable_factors=tuple((name, 2.0) for name in RED_SEA_CUT_SERIES)
    + (
        ("TG:peace-cable", 2.0),
        ("TG:russia-japan-cable-network-rjcn", 2.0),
        ("TG:korea-japan-cable-network-kjcn", 2.0),
    ),
    terrestrial_factors=(
        ("IQ", 2.0),
        (("TM", "UZ", "KG", "TJ", "AZ"), 2.0),
    ),
    corridor_factors=(("yemen-waters", (9.0, 18.0, 41.0, 56.0), 1.5),),
)

# v3.8 (2026-07-10, from the fix-impact analysis — volume x residual):
# (1) no-indian-ocean-transit: the open Indian Ocean (pseudo-cc "XI",
#     node_cc_remaps box south of the Suez->India->Malacca mainline)
#     carries no transit unless an endpoint is an Indian-Ocean island —
#     ZA<->AU / Gulf<->AU crossings route via Europe or the Pacific-Asian
#     corridor instead; island chains keep working.
# (2) Russia overland: ITU RU internal links x2 (the Trans-Siberian
#     shortcut carried 6% of total residual mass at 32 ms median even
#     though only bloc-endpoint pairs may use it) + Hokkaido-Sakhalin x2
#     (the sibling that absorbed the RJCN/KJCN displacement, 102 ms).
DEFAULT_POLICY = TransitPolicy(
    "v3.8-geopolitical",
    (
        no_transit("MN", "CN"),
        no_transit("AF"),
        no_transit("TW"),
        small_country(
            5.0, exempt=SUEZ_CORRIDOR | PACIFIC_RELAY_ISLANDS, terrestrial_only=True
        ),
        soviet_bloc(region_exempt=True),
        africa_containment(terrestrial_only=True),
        small_island_transit(),
        indian_ocean_containment(),
    ),
    cable_factors=tuple((name, 2.0) for name in RED_SEA_CUT_SERIES)
    + (
        ("TG:peace-cable", 2.0),
        ("TG:russia-japan-cable-network-rjcn", 2.0),
        ("TG:korea-japan-cable-network-kjcn", 2.0),
        ("TG:hokkaido-sakhalin-cable-system-hscs", 2.0),
    ),
    terrestrial_factors=(
        ("IQ", 2.0),
        ("RU", 2.0),
        (("TM", "UZ", "KG", "TJ", "AZ"), 2.0),
    ),
    corridor_factors=(("yemen-waters", (9.0, 18.0, 41.0, 56.0), 1.5),),
    node_cc_remaps=(("XI", INDIAN_OCEAN_BOX),),
)

OPEN_POLICY = TransitPolicy("open", ())

# The original rule set as first stated, kept for progression comparisons.
# Falsified by validation (FI 79% violations, Pacific relays 60-91%) — see
# TRANSIT_POLICY.md. Do not use for real floors.
V1_POLICY = TransitPolicy(
    "v1-geopolitical",
    (
        no_transit("ZA"),
        no_transit("MN", "CN"),
        small_country(5.0),
        russia_borders(),
    ),
)


def allowed_node_mask(policy, node_cc, endpoint_ccs):
    """Boolean mask over graph nodes for a pair with the given endpoint
    countries. Terrestrial-only bans do not remove nodes — they mask ITU
    edges (see itu_entry_mask / _edge_allowed_mask)."""
    node_cc = np.asarray(node_cc)
    banned, _ = policy.banned_split(np.unique(node_cc), endpoint_ccs)
    return ~np.isin(node_cc, sorted(banned))


def itu_entry_mask(graph, base_row, base_col):
    """Boolean per COO entry of the graph CSR: the underlying undirected
    edge is ITU terrestrial fibre. All-False when the graph carries no
    edge features (synthetic test graphs)."""
    if graph.edge_feature is None or "ITU" not in graph.feature_names:
        return np.zeros(len(base_row), dtype=bool)
    fi = list(graph.feature_names).index("ITU")
    itu = graph.edge_feature == fi
    edge_of = {
        (min(int(s), int(d)), max(int(s), int(d))): e
        for e, (s, d) in enumerate(zip(graph.edge_src, graph.edge_dst))
    }
    return np.array(
        [
            bool(itu[edge_of[(min(r, c), max(r, c))]])
            if (min(r, c), max(r, c)) in edge_of
            else False
            for r, c in zip(base_row, base_col)
        ]
    )


def _edge_allowed_mask(node_ok, base_row, base_col, entry_itu, cc_row, cc_col, banned_terr):
    """Edge mask for one pair class: node bans knock out incident edges as
    before; terrestrial bans knock out only ITU edges touching a banned
    country."""
    emask = node_ok[base_row] & node_ok[base_col]
    banned_terr = sorted(banned_terr)
    if banned_terr:
        emask &= ~(
            entry_itu & (np.isin(cc_row, banned_terr) | np.isin(cc_col, banned_terr))
        )
    return emask


# ---------------------------------------------------------------------------
# Parallel policy floors: the computation is one Dijkstra per (VP, class) —
# embarrassingly parallel across VPs. Policy rules hold lambdas (unpicklable),
# so the parent precomputes banned-country sets per (vp_cc, class) and workers
# receive only plain arrays/dicts.
# ---------------------------------------------------------------------------

_W = {}


def _build_banned_of(policy, node_cc, loc_cc, classes):
    """{(vp_cc, class_key) -> (node_banned, terrestrial_banned) arrays}
    for worker consumption."""
    uniq_ccs = np.unique(np.concatenate([node_cc, loc_cc]))
    out = {}
    for vc in np.unique(loc_cc):
        for key, targets in classes.items():
            node_b, terr_b = policy.banned_split(uniq_ccs, {vc, loc_cc[targets[0]]})
            out[(vc, key)] = (
                np.array(sorted(node_b), dtype=node_cc.dtype),
                np.array(sorted(terr_b), dtype=node_cc.dtype),
            )
    return out


def _worker_init(payload):
    _W.update(payload)
    _W["kdtree_xyz"] = geo.unit_xyz(_W["node_lat"], _W["node_lon"])
    _W["cc_row"] = _W["node_cc"][_W["base_row"]]
    _W["cc_col"] = _W["node_cc"][_W["base_col"]]


def _worker_rows(v_indices):
    import numpy as _np
    from scipy.sparse import csr_matrix as _csr
    from scipy.sparse.csgraph import dijkstra as _dij

    n = len(_W["node_lat"])
    out = {}
    for v in v_indices:
        vp_km = geo.haversine_km(_W["lat"][v], _W["lon"][v], _W["node_lat"], _W["node_lon"])
        entry_rtt = _np.where(vp_km <= _W["lastmile_km_max"], geo.rtt_ms(vp_km), _np.inf)
        col = _np.full(len(_W["lat"]), _np.inf)
        d_direct = geo.haversine_km(_W["lat"][v], _W["lon"][v], _W["lat"], _W["lon"])
        _np.minimum(
            col,
            _np.where(d_direct <= _W["direct_km_max"], geo.rtt_ms(d_direct), _np.inf),
            out=col,
        )
        for key, targets in _W["classes"].items():
            banned_n, banned_t = _W["banned_of"][(_W["loc_cc"][v], key)]
            ok = ~_np.isin(_W["node_cc"], banned_n)
            emask = _edge_allowed_mask(
                ok, _W["base_row"], _W["base_col"], _W["entry_itu"],
                _W["cc_row"], _W["cc_col"], banned_t,
            )
            entry = _np.flatnonzero(ok & _np.isfinite(entry_rtt))
            aug = _csr(
                (
                    _np.concatenate([_W["base_data"][emask], entry_rtt[entry]]),
                    (
                        _np.concatenate([_W["base_row"][emask], _np.full(len(entry), n)]),
                        _np.concatenate([_W["base_col"][emask], entry]),
                    ),
                ),
                shape=(n + 1, n + 1),
            )
            dist = _dij(aug, directed=True, indices=n)
            for t in targets:
                tot = float(_np.min(dist[_W["cand"][t]] + _W["lm"][t]))
                if tot < col[t]:
                    col[t] = tot
        out[v] = col
    return out


def policy_floor_matrix_parallel(
    graph,
    node_cc,
    lat,
    lon,
    loc_cc,
    policy=DEFAULT_POLICY,
    direct_km_max=DEFAULT_DIRECT_KM_MAX,
    lastmile_km_max=DEFAULT_LASTMILE_KM_MAX,
    n_workers=None,
    chunk=64,
):
    """Exact policy-aware floor matrix, parallel across VPs. Same result as
    policy_floor_matrix (target-major out[t, v])."""
    import os
    from concurrent.futures import ProcessPoolExecutor

    node_cc = policy.remap_node_cc(node_cc, graph.node_lat, graph.node_lon)
    loc_cc = np.asarray(loc_cc)
    lat = np.asarray(lat, dtype=float)
    lon = np.asarray(lon, dtype=float)
    n, n_loc = graph.n_nodes, len(lat)

    classes = {}
    for t in range(n_loc):
        classes.setdefault(policy.class_signature(loc_cc[t]), []).append(t)
    banned_of = _build_banned_of(policy, node_cc, loc_cc, classes)

    chord, cand = graph.kdtree.query(geo.unit_xyz(lat, lon), k=min(512, n))
    km_cand = geo.chord_to_km(chord)
    lm = np.where(km_cand <= lastmile_km_max, geo.rtt_ms(km_cand), np.inf)

    base = graph.csr.tocoo()
    payload = dict(
        node_lat=graph.node_lat, node_lon=graph.node_lon,
        base_row=base.row, base_col=base.col,
        base_data=scaled_base_data(policy, graph, base, node_cc),
        node_cc=node_cc, lat=lat, lon=lon, loc_cc=loc_cc,
        classes=classes, banned_of=banned_of, cand=cand, lm=lm,
        entry_itu=itu_entry_mask(graph, base.row, base.col),
        direct_km_max=float(direct_km_max), lastmile_km_max=float(lastmile_km_max),
    )
    n_workers = n_workers or max(1, (os.cpu_count() or 4) - 1)
    out = np.full((n_loc, n_loc), np.inf)
    chunks = [list(range(i, min(i + chunk, n_loc))) for i in range(0, n_loc, chunk)]
    with ProcessPoolExecutor(
        max_workers=n_workers, initializer=_worker_init, initargs=(payload,)
    ) as ex:
        for res in ex.map(_worker_rows, chunks):
            for v, col in res.items():
                out[:, v] = col
    return out


def _worker_paths(args):
    """Route sampled pairs for a chunk of VPs on their policy-masked graphs.
    Returns {out_idx: (floor_ms, transit_ccs, edge_ids, grid_cells)}."""
    import numpy as _np
    from scipy.sparse import csr_matrix as _csr
    from scipy.sparse.csgraph import dijkstra as _dij

    n = len(_W["node_lat"])
    if "edge_of" not in _W:
        _W["edge_of"] = {
            (int(s), int(d)): e
            for e, (s, d) in enumerate(zip(_W["edge_src"], _W["edge_dst"]))
        }
    out = {}
    for v, by_class in args:
        vp_km = geo.haversine_km(_W["lat"][v], _W["lon"][v], _W["node_lat"], _W["node_lon"])
        entry_rtt = _np.where(vp_km <= _W["lastmile_km_max"], geo.rtt_ms(vp_km), _np.inf)
        for key, targets in by_class.items():
            banned_n, banned_t = _W["banned_of"][(_W["loc_cc"][v], key)]
            ok = ~_np.isin(_W["node_cc"], banned_n)
            emask = _edge_allowed_mask(
                ok, _W["base_row"], _W["base_col"], _W["entry_itu"],
                _W["cc_row"], _W["cc_col"], banned_t,
            )
            entry = _np.flatnonzero(ok & _np.isfinite(entry_rtt))
            aug = _csr(
                (
                    _np.concatenate([_W["base_data"][emask], entry_rtt[entry]]),
                    (
                        _np.concatenate([_W["base_row"][emask], _np.full(len(entry), n)]),
                        _np.concatenate([_W["base_col"][emask], entry]),
                    ),
                ),
                shape=(n + 1, n + 1),
            )
            dist, pred = _dij(aug, directed=True, indices=n, return_predecessors=True)
            for t, out_idx in targets:
                totals = dist[_W["cand"][t]] + _W["lm"][t]
                j = int(_W["cand"][t][_np.argmin(totals)])
                floor = float(_np.min(totals))
                d_direct = float(
                    geo.haversine_km(_W["lat"][v], _W["lon"][v], _W["lat"][t], _W["lon"][t])
                )
                if d_direct <= _W["direct_km_max"] and geo.rtt_ms(d_direct) <= floor:
                    out[out_idx] = (geo.rtt_ms(d_direct), (), (), ())
                    continue
                ccs, edges, cells, node, prev = set(), [], set(), j, -1
                while node != n and node >= 0:
                    ccs.add(_W["node_cc"][node])
                    cells.add(
                        (
                            int(_np.floor(_W["node_lat"][node] / _W["grid_deg"])),
                            int(_np.floor(_W["node_lon"][node] / _W["grid_deg"])),
                        )
                    )
                    if prev >= 0:
                        e = _W["edge_of"].get((min(prev, node), max(prev, node)))
                        if e is not None:
                            edges.append(e)
                    prev, node = node, pred[node]
                transit = tuple(ccs - {_W["loc_cc"][v], _W["loc_cc"][t]})
                out[out_idx] = (floor, transit, tuple(edges), tuple(cells))
    return out


def policy_paths_parallel(
    graph,
    node_cc,
    lat,
    lon,
    loc_cc,
    pairs,
    policy=DEFAULT_POLICY,
    direct_km_max=DEFAULT_DIRECT_KM_MAX,
    lastmile_km_max=DEFAULT_LASTMILE_KM_MAX,
    grid_deg=5.0,
    n_workers=None,
    chunk=48,
):
    """Route (src_loc, dst_loc) pairs on their policy-masked graphs; returns
    (floors, transit_sets, edge_tuples, cell_sets) aligned with `pairs`.
    With OPEN_POLICY this reproduces the unrestricted model's paths — one
    code path for both, per the no-branching principle."""
    import os
    from concurrent.futures import ProcessPoolExecutor

    node_cc = policy.remap_node_cc(node_cc, graph.node_lat, graph.node_lon)
    loc_cc = np.asarray(loc_cc)
    lat = np.asarray(lat, dtype=float)
    lon = np.asarray(lon, dtype=float)
    n_loc = len(lat)

    classes = {}
    for t in range(n_loc):
        classes.setdefault(policy.class_signature(loc_cc[t]), []).append(t)
    banned_of = _build_banned_of(policy, node_cc, loc_cc, classes)
    sig_of = {t: sig for sig, ts in classes.items() for t in ts}

    by_v = {}
    for out_idx, (v, t) in enumerate(pairs):
        by_v.setdefault(v, {}).setdefault(sig_of[t], []).append((t, out_idx))

    chord, cand = graph.kdtree.query(geo.unit_xyz(lat, lon), k=min(512, graph.n_nodes))
    km_cand = geo.chord_to_km(chord)
    lm = np.where(km_cand <= lastmile_km_max, geo.rtt_ms(km_cand), np.inf)

    base = graph.csr.tocoo()
    payload = dict(
        node_lat=graph.node_lat, node_lon=graph.node_lon,
        edge_src=graph.edge_src, edge_dst=graph.edge_dst,
        base_row=base.row, base_col=base.col,
        base_data=scaled_base_data(policy, graph, base, node_cc),
        node_cc=node_cc, lat=lat, lon=lon, loc_cc=loc_cc,
        banned_of=banned_of, cand=cand, lm=lm,
        entry_itu=itu_entry_mask(graph, base.row, base.col),
        direct_km_max=float(direct_km_max), lastmile_km_max=float(lastmile_km_max),
        grid_deg=float(grid_deg),
    )
    items = list(by_v.items())
    chunks = [items[i : i + chunk] for i in range(0, len(items), chunk)]
    n_workers = n_workers or max(1, (os.cpu_count() or 4) - 1)
    floors = np.full(len(pairs), np.inf)
    transit = [frozenset()] * len(pairs)
    edges = [()] * len(pairs)
    cells = [set()] * len(pairs)
    with ProcessPoolExecutor(
        max_workers=n_workers, initializer=_worker_init, initargs=(payload,)
    ) as ex:
        for res in ex.map(_worker_paths, chunks):
            for out_idx, (fl, tr, ed, ce) in res.items():
                floors[out_idx] = fl
                transit[out_idx] = frozenset(tr)
                edges[out_idx] = ed
                cells[out_idx] = set(ce)
    return floors, transit, edges, cells


def policy_floor_matrix(
    graph,
    node_cc,
    lat,
    lon,
    loc_cc,
    policy=DEFAULT_POLICY,
    direct_km_max=DEFAULT_DIRECT_KM_MAX,
    lastmile_km_max=DEFAULT_LASTMILE_KM_MAX,
    progress_every=0,
):
    """Exact policy-aware floor matrix between locations, target-major
    (out[t, v] = floor from location v to location t), matching
    FloorEstimator semantics plus the transit restriction.

    The pair-dependent graph only depends on the pair's endpoint countries
    through each rule's endpoint signature, so targets are grouped into
    signature classes: one Dijkstra per (VP, class)."""
    node_cc = policy.remap_node_cc(node_cc, graph.node_lat, graph.node_lon)
    loc_cc = np.asarray(loc_cc)
    lat = np.asarray(lat, dtype=float)
    lon = np.asarray(lon, dtype=float)
    n, n_loc = graph.n_nodes, len(lat)

    classes = {}
    for t in range(n_loc):
        classes.setdefault(policy.class_signature(loc_cc[t]), []).append(t)

    chord, cand = graph.kdtree.query(geo.unit_xyz(lat, lon), k=min(512, n))
    km_cand = geo.chord_to_km(chord)
    lm = np.where(km_cand <= lastmile_km_max, geo.rtt_ms(km_cand), np.inf)

    d_pair = geo.haversine_km(lat[:, None], lon[:, None], lat[None, :], lon[None, :])
    out = np.where(d_pair <= direct_km_max, geo.rtt_ms(d_pair), np.inf)

    base = graph.csr.tocoo()
    base_data = scaled_base_data(policy, graph, base, node_cc)
    entry_itu = itu_entry_mask(graph, base.row, base.col)
    cc_row, cc_col = node_cc[base.row], node_cc[base.col]
    uniq_ccs = np.unique(np.concatenate([node_cc, loc_cc]))
    for v in range(n_loc):
        vp_km = geo.haversine_km(lat[v], lon[v], graph.node_lat, graph.node_lon)
        entry_rtt = np.where(vp_km <= lastmile_km_max, geo.rtt_ms(vp_km), np.inf)
        for targets in classes.values():
            # any target in the class induces the same mask (same signature)
            eps = {loc_cc[v], loc_cc[targets[0]]}
            banned_n, banned_t = policy.banned_split(uniq_ccs, eps)
            ok = ~np.isin(node_cc, sorted(banned_n))
            emask = _edge_allowed_mask(
                ok, base.row, base.col, entry_itu, cc_row, cc_col, banned_t
            )
            entry = np.flatnonzero(ok & np.isfinite(entry_rtt))
            aug = csr_matrix(
                (
                    np.concatenate([base_data[emask], entry_rtt[entry]]),
                    (
                        np.concatenate([base.row[emask], np.full(len(entry), n)]),
                        np.concatenate([base.col[emask], entry]),
                    ),
                ),
                shape=(n + 1, n + 1),
            )
            dist = dijkstra(aug, directed=True, indices=n)
            for t in targets:
                out[t, v] = min(out[t, v], float(np.min(dist[cand[t]] + lm[t])))
        if progress_every and (v + 1) % progress_every == 0:
            print(f"  policy floors: VP {v + 1}/{n_loc}")
    return out
