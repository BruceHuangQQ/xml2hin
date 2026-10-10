# xml2hin: any XML file -> Heterogeneous Information Network (HIN)

One generic pipeline. Each XML dataset only needs a small schema file (YAML).
New dataset = new schema file, no code change.

## Install
    pip install -r requirements.txt        (Python 3.9+)

## Run the demo (two datasets, same code)
    python run_demo.py
    python explore.py out/mondial country D

## Run on your own XML
    python xml2hin.py <file.xml> <schema.yaml> <out_dir>
Keep the DTD next to the XML if the file has one (DBLP needs it for characters like &uuml;).

## Writing a schema
```yaml
dataset: MyData
nodes:
  <node_type>:
    tags: [element names that are this node type]
    id: "@attr" | "child"            # where the node's ID is
    attributes: all | [fields]       # copied as-is, never interpreted (default: all)
    keep_tag_as: <column>            # optional: merge several tags into one type, keep the tag
  <node_type>:
    from_values: true                # nodes that only appear as values (e.g. author names)
edges:
  - {name: x, from: A, to: B, ref: "@attr"}          # ID reference in an attribute (IDREF/IDREFS)
  - {name: x, from: A, to: B, ref: "child"}          # ID reference as child element text
  - {name: x, from: A, to: B, ref: "child/@attr"}    # ID reference on a child element
  - {name: x, from: A, to: [B, C], nested_in: true}  # A sits inside B (or C) in the XML tree
```

## Output (`<out_dir>/`)
- `hin/nodes_<type>.parquet`: idx, node_id, attributes
- `hin/edges_<name>.parquet`: src_idx, dst_type, dst_idx, src_id, dst_id
- `dangling_refs.csv`: references pointing to IDs that are not in the file
- `summary.txt`: node and edge counts by type

## Data included (real, not hand-made)
- `data/dblp/dblp_sample.xml`: 505 real DBLP records (sample from github.com/pj4dev/dblp-sampling-tools), with `dblp.dtd`. Full dump: https://dblp.org/xml/
- `data/mondial/mondial.xml`: the full MONDIAL geography database (Univ. Göttingen; copy from github.com/ullenboom/mondial-database), with `mondial.dtd`.

The DBLP sample is random, so most `crossref`/`cite` targets are not in it; they show up in
`dangling_refs.csv`. On the full dblp.xml they resolve to publication nodes.
