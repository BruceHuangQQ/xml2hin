"""Look at one node of a built HIN and its neighbours.

    python explore.py <out_dir> <node_type> <node_id>
    e.g. python explore.py out/mondial country D
"""
import glob
import os
import sys

import pandas as pd

out_dir, ntype, nid = sys.argv[1:4]
hin = os.path.join(out_dir, "hin")
nodes = pd.read_parquet(os.path.join(hin, f"nodes_{ntype}.parquet"))
row = nodes[nodes["node_id"] == nid]
if row.empty:
    sys.exit(f"No {ntype} with id {nid}")
print(f"{ntype} {nid}")
for k, v in row.iloc[0].items():
    if v and k not in ("idx", "node_id"):
        print(f"   {k}: {v[:80]}")

for path in sorted(glob.glob(os.path.join(hin, "edges_*.parquet"))):
    name = os.path.basename(path)[6:-8]
    e = pd.read_parquet(path)
    out = e[e["src_id"] == nid]["dst_id"].tolist()
    inc = e[(e["dst_id"] == nid) & (e["dst_type"] == ntype)]["src_id"].tolist()
    if out:
        print(f"  -[{name}]-> {len(out)}: {', '.join(out[:8])}{' ...' if len(out) > 8 else ''}")
    if inc:
        print(f"  <-[{name}]- {len(inc)}: {', '.join(inc[:8])}{' ...' if len(inc) > 8 else ''}")
