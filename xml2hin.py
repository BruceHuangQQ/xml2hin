"""Generic XML -> HIN pipeline.

    python xml2hin.py <data.xml> <schema.yaml> <out_dir>

The code knows nothing about any dataset. The schema file says:
  - which XML elements are nodes, and where their ID is
  - which fields hold references to other IDs (these become edges)
  - which nodes are related by nesting (an element inside another)

Step 1  XML  -> node tables + edge tables      (streaming, works on huge files)
Step 2  tables -> HIN: every ID gets an integer index per node type, every edge
        becomes (src_index, dst_index); references to IDs that do not exist are
        reported, not silently kept.
Output: Parquet files in <out_dir>/hin/ plus summary.txt
"""
import os
import sys
import time
from collections import Counter, defaultdict

import pandas as pd
import yaml
from lxml import etree


# ---------- schema ----------

def load_schema(path):
    with open(path, encoding="utf-8") as f:
        s = yaml.safe_load(f)
    if not s or "nodes" not in s or "edges" not in s:
        sys.exit(f"Schema error: {path} must have 'nodes' and 'edges'")
    for name, n in s["nodes"].items():
        if "tags" not in n and not n.get("from_values"):
            sys.exit(f"Schema error: node type '{name}' needs 'tags' (or from_values: true)")
    for e in s["edges"]:
        targets = e["to"] if isinstance(e["to"], list) else [e["to"]]
        for t in [e["from"]] + targets:
            if t not in s["nodes"]:
                sys.exit(f"Schema error: edge '{e['name']}' uses unknown node type '{t}'")
        if "ref" not in e and not e.get("nested_in"):
            sys.exit(f"Schema error: edge '{e['name']}' needs 'ref' or 'nested_in: true'")
    return s


# ---------- reading values out of an element ----------

def child_text(el):
    return "".join(el.itertext()).strip()


def read_field(el, spec):
    """Return the list of values at `spec`:
       '@attr'        attribute (space-separated lists like IDREFS are split)
       'child'        text of each direct child <child>
       'child/@attr'  attribute of each direct child <child>
    """
    if spec.startswith("@"):
        v = el.get(spec[1:])
        return v.split() if v else []
    if "/@" in spec:
        tag, attr = spec.split("/@")
        out = []
        for c in el.iterchildren(tag):
            v = c.get(attr)
            if v:
                out += v.split()
        return out
    return [t for t in (child_text(c) for c in el.iterchildren(spec)) if t]


def read_attributes(el, spec, record_tags):
    """Attribute values are passed through as-is (never interpreted)."""
    if spec == "all":   # every XML attribute + every simple child element
        row = dict(el.attrib)
        for c in el:
            if isinstance(c.tag, str) and c.tag not in record_tags and len(c) == 0:
                t = (c.text or "").strip()
                if t:
                    row[c.tag] = row[c.tag] + "|" + t if c.tag in row else t
        return row
    return {f.lstrip("@").replace("/@", "_"): "|".join(read_field(el, f)) for f in spec}


# ---------- step 1: XML -> tables ----------

def extract(xml_path, schema):
    nodes_cfg, edges_cfg = schema["nodes"], schema["edges"]
    tag_to_type = {t: name for name, n in nodes_cfg.items() for t in n.get("tags", [])}
    edges_from = defaultdict(list)
    for e in edges_cfg:
        edges_from[e["from"]].append(e)

    nodes = defaultdict(dict)      # type -> {id: attributes}
    edges = defaultdict(list)      # edge name -> [(src, dst)]
    stack = []                     # open node elements (for nesting edges)
    nested_pending = []            # (edge, child_id, parent_entry)
    stats = Counter()

    parser = etree.iterparse(xml_path, events=("start", "end"), tag=list(tag_to_type),
                             load_dtd=True, resolve_entities=True, huge_tree=True)
    for event, el in parser:
        if event == "start":
            stack.append({"type": tag_to_type[el.tag], "id": None})
            continue
        entry = stack.pop()
        ntype = entry["type"]
        cfg = nodes_cfg[ntype]
        ids = read_field(el, cfg["id"])
        if not ids:
            stats[f"skipped {ntype} without id"] += 1
        else:
            nid = ids[0]
            entry["id"] = nid
            attrs = read_attributes(el, cfg.get("attributes", "all"), tag_to_type)
            if cfg.get("keep_tag_as"):
                attrs[cfg["keep_tag_as"]] = el.tag
            nodes[ntype][nid] = attrs
            for e in edges_from[ntype]:
                if e.get("nested_in"):
                    targets = e["to"] if isinstance(e["to"], list) else [e["to"]]
                    parent = next((p for p in reversed(stack) if p["type"] in targets), None)
                    if parent is not None:
                        nested_pending.append((e["name"], nid, parent))
                    continue
                for v in read_field(el, e["ref"]):
                    if v not in e.get("ignore", []):
                        edges[e["name"]].append((nid, v))
        if not stack:              # top-level node finished: free memory
            el.clear()
            while el.getprevious() is not None:
                del el.getparent()[0]

    for name, child_id, parent in nested_pending:   # parent IDs are known now
        if parent["id"] is not None:
            edges[name].append((child_id, parent["id"]))

    # nodes that only exist as values (e.g. DBLP author names)
    for e in edges_cfg:
        to = e["to"]
        if isinstance(to, str) and nodes_cfg[to].get("from_values"):
            for _, v in edges[e["name"]]:
                nodes[to].setdefault(v, {})
    return nodes, edges, stats


# ---------- step 2: tables -> HIN ----------

def build_hin(nodes, edges, schema, out_dir):
    hin_dir = os.path.join(out_dir, "hin")
    os.makedirs(hin_dir, exist_ok=True)
    index = {}
    lines = ["NODE TYPES"]
    for ntype, recs in nodes.items():
        df = pd.DataFrame(list(recs.values()), index=range(len(recs))).fillna("").astype(str)
        df.insert(0, "node_id", list(recs.keys()))
        df.insert(0, "idx", range(len(df)))
        df.to_parquet(os.path.join(hin_dir, f"nodes_{ntype}.parquet"), index=False)
        index[ntype] = dict(zip(df["node_id"], df["idx"]))
        lines.append(f"  {ntype:<14} {len(df):>8,} nodes")

    lines.append("EDGE TYPES  (from -> to)")
    dangling = []
    for e in schema["edges"]:
        targets = e["to"] if isinstance(e["to"], list) else [e["to"]]
        rows = []
        for src, dst in edges.get(e["name"], []):
            hit = next((t for t in targets if dst in index.get(t, {})), None)
            if hit is None:
                dangling.append((e["name"], src, dst))
                continue
            rows.append((index[e["from"]][src], hit, index[hit][dst], src, dst))
        df = pd.DataFrame(rows, columns=["src_idx", "dst_type", "dst_idx", "src_id", "dst_id"]).drop_duplicates()
        df.to_parquet(os.path.join(hin_dir, f"edges_{e['name']}.parquet"), index=False)
        lines.append(f"  {e['from'] + ' -[' + e['name'] + ']-> ' + '/'.join(targets):<48} {len(df):>8,} edges")

    pd.DataFrame(dangling, columns=["edge", "src_id", "missing_id"]).to_csv(
        os.path.join(out_dir, "dangling_refs.csv"), index=False)
    lines.append(f"References to IDs not in this file: {len(dangling):,} (see dangling_refs.csv)")
    return lines


def main():
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    xml_path, schema_path, out_dir = sys.argv[1:]
    if not os.path.exists(xml_path):
        sys.exit(f"Input not found: {xml_path}")
    schema = load_schema(schema_path)
    t0 = time.time()
    nodes, edges, stats = extract(xml_path, schema)
    os.makedirs(out_dir, exist_ok=True)
    lines = [f"Dataset: {schema.get('dataset', xml_path)}   ({os.path.basename(xml_path)})"]
    lines += build_hin(nodes, edges, schema, out_dir)
    lines += [f"  note: {k}: {v}" for k, v in stats.items()]
    lines.append(f"Time: {time.time() - t0:.2f}s   Output: {os.path.join(out_dir, 'hin')}")
    text = "\n".join(lines)
    with open(os.path.join(out_dir, "summary.txt"), "w", encoding="utf-8") as f:
        f.write(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
