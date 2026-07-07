"""Transit policies: whose fiber can carry through-traffic.

The validation runs showed the shortest-fiber model's residuals concentrate
on paths the model routes through countries that real traffic avoids (see
TRANSIT_POLICY.md for the evidence and literature). A TransitPolicy is a
named set of rules deciding, per country code, whether that country's
infrastructure may be used by traffic that neither originates nor
terminates there.

Semantics (v1, node-based): a node in restricted country X is removed from
the graph for pair (src, dst) unless X ∈ {cc(src), cc(dst)}. Removing a
node removes all its incident edges. Caveats, documented so we can refine:
  - Node countries come from nearest-city reverse geocoding, so mid-ocean
    cable vertices are attributed to the nearest coastal state; a cable
    merely passing NEAR a restricted island is treated as landing there
    (conservative — restricts too much, never too little).
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


@dataclass(frozen=True)
class CountryRule:
    """Node in a restricted country X is banned for a pair unless X is one
    of the pair's endpoint countries (per-country exemption)."""

    name: str
    member: Callable[[str], bool]

    def banned(self, cc: str, endpoint_ccs) -> bool:
        return self.member(cc) and cc not in endpoint_ccs

    def endpoint_signature(self, cc: str):
        # targets with equal signatures induce identical bans for this rule
        return cc if self.member(cc) else None


@dataclass(frozen=True)
class RegionRule:
    """Region containment: nodes in the region are banned unless ANY
    endpoint is in the region (continent-level exemption)."""

    name: str
    region: frozenset

    def banned(self, cc: str, endpoint_ccs) -> bool:
        return cc in self.region and not any(e in self.region for e in endpoint_ccs)

    def endpoint_signature(self, cc: str):
        return cc in self.region


def no_transit(*ccs):
    """These countries' fiber never carries third-party traffic."""
    s = frozenset(ccs)
    return CountryRule(f"no-transit[{','.join(sorted(s))}]", lambda cc: cc in s)


def small_country(min_population_m=5.0, exempt=frozenset()):
    """Small states' fiber is access infrastructure, not transit (population
    proxy; unknown codes count as small). KNOWN over-restrictive for
    cable-relay islands — see TRANSIT_POLICY.md. `exempt` carves out small
    states that are established relays (e.g. the Suez corridor's DJ/ER)."""
    exempt = frozenset(exempt)
    return CountryRule(
        f"small-country[<{min_population_m}M]",
        lambda cc: POP_MILLIONS.get(cc, 0.0) < min_population_m and cc not in exempt,
    )


def russia_borders():
    """v1 rule, kept for comparison runs; superseded by soviet_bloc()."""
    return CountryRule("russia-borders", lambda cc: cc in RUSSIA_LAND_BORDERS)


def soviet_bloc(exempt_eu=True):
    """Soviet-bloc overland corridors are not practical through-routes —
    except the ex-bloc EU members (Baltics, Poland, etc.), which are
    ordinary European transit fabric (the v1 Finland lesson, generalized)."""
    banned = SOVIET_BLOC - (EU_INTEGRATED_EX_BLOC if exempt_eu else frozenset())
    tag = "-minus-EU" if exempt_eu else ""
    return CountryRule(f"soviet-bloc{tag}", lambda cc: cc in banned)


def africa_containment(exempt_suez=True, country_granular=False):
    """African infrastructure only carries traffic with an African endpoint
    (the boomerang-routing reality). The Suez/Red-Sea corridor is exempt by
    default — banning it would ban the planet's main Asia<->Europe route.

    country_granular (v3): even African pairs only get their own two
    countries' fiber — the mesh showed intra-African transit chains
    (e.g. the Mozambique coastal hop) are also fiction; real intra-Africa
    traffic trombones via Europe."""
    region = AFRICA_CCS - (SUEZ_CORRIDOR if exempt_suez else frozenset())
    tag = "-except-suez" if exempt_suez else ""
    if country_granular:
        return CountryRule(f"no-africa-transit-granular{tag}", lambda cc: cc in region)
    return RegionRule(f"no-africa-transit{tag}", region)


@dataclass(frozen=True)
class TransitPolicy:
    name: str
    rules: Tuple
    # cable-level distrust: RTT multipliers on named source features, for
    # infrastructure that exists but is chronically degraded (e.g. the
    # 2024-25 Red Sea cut series). Pair-independent — applied to edge
    # weights before routing, so class signatures are unaffected.
    cable_factors: Tuple = ()
    # terrestrial distrust: RTT multipliers on ITU links INTERNAL to a
    # country (both edge endpoints there). Finer than a country ban: it
    # penalizes the overland crossing without severing coastal submarine
    # chains whose ocean vertices geocode to the same country (the Iraq
    # lesson: ITU IQ links at 93 ms while IQ 'transit' pairs on Gulf
    # cables are fine).
    terrestrial_factors: Tuple = ()

    def banned_set(self, ccs, endpoint_ccs) -> frozenset:
        """Countries banned for a pair with the given endpoint countries."""
        endpoint_ccs = frozenset(endpoint_ccs)
        return frozenset(
            cc for cc in set(ccs) if any(r.banned(cc, endpoint_ccs) for r in self.rules)
        )

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
        parts += [f"distrust-itu[{cc} x{f}]" for cc, f in self.terrestrial_factors]
        return f"{self.name}: " + " | ".join(parts)


def scaled_base_data(policy, graph, base_coo, node_cc=None):
    """Edge weights with the policy's cable- and terrestrial-distrust
    factors applied. base_coo entries map back to undirected edges via
    (min,max) lookup."""
    if (not policy.cable_factors and not policy.terrestrial_factors) or (
        graph.edge_feature is None
    ):
        return base_coo.data
    factor_by_feature = {}
    names = list(graph.feature_names)
    for name, f in policy.cable_factors:
        if name in names:
            factor_by_feature[names.index(name)] = float(f)
    edge_factor = np.ones(graph.n_edges)
    for fi, f in factor_by_feature.items():
        edge_factor[graph.edge_feature == fi] = f
    if policy.terrestrial_factors and node_cc is not None and "ITU" in names:
        node_cc = np.asarray(node_cc)
        itu = graph.edge_feature == names.index("ITU")
        for cc, f in policy.terrestrial_factors:
            m = itu & (node_cc[graph.edge_src] == cc) & (node_cc[graph.edge_dst] == cc)
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
DEFAULT_POLICY = TransitPolicy(
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
    countries."""
    node_cc = np.asarray(node_cc)
    banned = policy.banned_set(np.unique(node_cc), endpoint_ccs)
    return ~np.isin(node_cc, sorted(banned))


# ---------------------------------------------------------------------------
# Parallel policy floors: the computation is one Dijkstra per (VP, class) —
# embarrassingly parallel across VPs. Policy rules hold lambdas (unpicklable),
# so the parent precomputes banned-country sets per (vp_cc, class) and workers
# receive only plain arrays/dicts.
# ---------------------------------------------------------------------------

_W = {}


def _build_banned_of(policy, node_cc, loc_cc, classes):
    """{(vp_cc, class_key) -> banned cc array} for worker consumption."""
    uniq_ccs = np.unique(np.concatenate([node_cc, loc_cc]))
    return {
        (vc, key): np.array(
            sorted(policy.banned_set(uniq_ccs, {vc, loc_cc[targets[0]]})),
            dtype=node_cc.dtype,
        )
        for vc in np.unique(loc_cc)
        for key, targets in classes.items()
    }


def _worker_init(payload):
    _W.update(payload)
    _W["kdtree_xyz"] = geo.unit_xyz(_W["node_lat"], _W["node_lon"])


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
            banned = _W["banned_of"][(_W["loc_cc"][v], key)]
            ok = ~_np.isin(_W["node_cc"], banned)
            emask = ok[_W["base_row"]] & ok[_W["base_col"]]
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

    node_cc = np.asarray(node_cc)
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
            banned = _W["banned_of"][(_W["loc_cc"][v], key)]
            ok = ~_np.isin(_W["node_cc"], banned)
            emask = ok[_W["base_row"]] & ok[_W["base_col"]]
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

    node_cc = np.asarray(node_cc)
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
    node_cc = np.asarray(node_cc)
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
    for v in range(n_loc):
        vp_km = geo.haversine_km(lat[v], lon[v], graph.node_lat, graph.node_lon)
        entry_rtt = np.where(vp_km <= lastmile_km_max, geo.rtt_ms(vp_km), np.inf)
        for targets in classes.values():
            # any target in the class induces the same mask (same signature)
            ok = allowed_node_mask(policy, node_cc, {loc_cc[v], loc_cc[targets[0]]})
            emask = ok[base.row] & ok[base.col]
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
