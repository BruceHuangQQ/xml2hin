"""Runs the SAME pipeline code on two very different XML datasets.
Only the schema file changes.   python run_demo.py
"""
import os
import subprocess
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))

RUNS = [
    ("data/dblp/dblp_sample.xml", "schemas/dblp.yaml", "out/dblp"),
    ("data/mondial/mondial.xml", "schemas/mondial.yaml", "out/mondial"),
]
for xml, schema, out in RUNS:
    print("=" * 70)
    print(f"python xml2hin.py {xml} {schema} {out}")
    print("=" * 70)
    subprocess.run([sys.executable, "xml2hin.py", xml, schema, out], check=True)
    print()
print("Look at one node:  python explore.py out/mondial country D")
print("                   python explore.py out/dblp publication journals/fgcs/LealHL09")
