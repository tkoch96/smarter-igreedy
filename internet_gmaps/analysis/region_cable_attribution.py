"""Name the cables behind a red region on the offenders map.

When to use: the cable_residual_offenders map shows high-error lines in
some part of the world, but the printed top-15 table doesn't name them
(it has a higher support cutoff than the map). This routes the standard
120k-pair sample under the current policy, keeps only edges inside a
lat/lon box, and ranks the cable systems there by how badly the paths
using them miss the model.

How to run (from internet_gmaps/; box via env vars, defaults to the
Oman / Arabian Sea region):

    BOX="8,28,50,66" ~/Documents/venv312/bin/python analysis/region_cable_attribution.py

Prints: per cable-system inside the box, the median error (measured
minus modeled floor, ms) of paths using it, and how many paths do.
Runtime ~2 min. NOTE the __main__ guard is required: macOS worker
processes re-import this file, and without the guard every worker
re-runs the whole analysis.
"""
import os
import sys
from collections import defaultdict

import numpy as np


def main():
    sys.path.insert(0, os.getcwd())
    sys.path.insert(0, os.path.join(os.getcwd(), "tests"))
    from test_mesh_validation import MeshEval
    from test_transit_analysis import TransitAnalysis

    # Box = lat_min, lat_max, lon_min, lon_max (degrees).
    box = os.environ.get("BOX", "8,28,50,66")
    LAT0, LAT1, LON0, LON1 = (float(x) for x in box.split(","))

    # Route the standard sample under the current policy and recover the
    # modeled path (edges) of every pair.
    ev = MeshEval()
    ana = TransitAnalysis(ev)
    g = ev.graph

    # Keep only graph edges with at least one endpoint inside the box.
    def inside(idx):
        return ((g.node_lat[idx] >= LAT0) & (g.node_lat[idx] <= LAT1)
                & (g.node_lon[idx] >= LON0) & (g.node_lon[idx] <= LON1))
    in_box = inside(g.edge_src) | inside(g.edge_dst)

    # For each in-box edge, collect the errors of all paths crossing it.
    edge_res = defaultdict(list)
    for r, edges in zip(ana.residual, ana.path_edges):
        for e in edges:
            if in_box[e]:
                edge_res[e].append(r)

    # Human-readable name for an edge: the cable system it belongs to,
    # or "ITU <countries>" for overland links.
    def label(e):
        fi = g.edge_feature[e]
        name = g.feature_names[fi] if fi >= 0 else "unknown"
        if name == "ITU":
            a, b = ana.node_cc[g.edge_src[e]], ana.node_cc[g.edge_dst[e]]
            return f"ITU {'-'.join(sorted({a, b}))}"
        return name

    by_label = defaultdict(list)
    for e, v in edge_res.items():
        by_label[label(e)].extend(v)

    print(f"cable systems inside ({LAT0}..{LAT1}N, {LON0}..{LON1}E), by median "
          "error of paths using them (n = path-uses in box):")
    rows = [(np.median(v), len(v), lab) for lab, v in by_label.items() if len(v) >= 50]
    for med, n, lab in sorted(rows, reverse=True):
        print(f"  {lab:<55s} {med:7.1f} ms  (n={n:,})")


if __name__ == "__main__":
    main()
