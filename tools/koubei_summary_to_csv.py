#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json, sys, zipfile
from pathlib import Path
import pandas as pd

def main(zip_path: str):
    series_id = Path(zip_path).stem.split("_")[-1]
    rows = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in sorted(zf.namelist()):
            if not name.lower().endswith(".json"):
                continue
            data = json.loads(zf.read(name).decode("utf-8", "ignore"))
            rid = data.get("id") or Path(name).stem
            title = data.get("title", "").strip()
            text = data.get("text", "").strip()
            url = data.get("url", "")
            if text:
                rows.append({
                    "id": rid,
                    "pros": title,
                    "cons": "",
                    "pros_ja": text,
                    "cons_ja": "",
                    "sentiment": "",
                    "url": url
                })
    if not rows:
        raise RuntimeError("No valid JSONs found in zip")

    df = pd.DataFrame(rows)
    out_csv = f"autohome_reviews_{series_id}.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[done] wrote {out_csv}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/koubei_summary_to_csv.py <zipfile>")
        sys.exit(1)
    main(sys.argv[1])
