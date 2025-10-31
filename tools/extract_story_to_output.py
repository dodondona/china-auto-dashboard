#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, zipfile, glob
from pathlib import Path

series_id = os.environ.get("SERIES_ID","").strip()
if not series_id:
    print("SERIES_ID is required", file=sys.stderr); sys.exit(2)

cands = sorted(glob.glob(f"autohome-summary-{series_id}.zip")) or sorted(glob.glob("autohome-summary-*.zip"))
if not cands:
    print("zip not found", file=sys.stderr); sys.exit(3)

zip_path = Path(cands[0])
out_dir = Path("output")/series_id
out_dir.mkdir(parents=True, exist_ok=True)
cache_dir = Path("cache")/series_id
cache_dir.mkdir(parents=True, exist_ok=True)

with zipfile.ZipFile(zip_path, "r") as zf:
    names = zf.namelist()
    story_name = next((n for n in names if n.endswith("story.txt")), None)
    if not story_name:
        print("story.txt not found in zip", file=sys.stderr); sys.exit(4)
    data = zf.read(story_name)
    (out_dir/"story.txt").write_bytes(data)
    (cache_dir/"story.txt").write_bytes(data)

print(f"Wrote: {out_dir/'story.txt'}")
