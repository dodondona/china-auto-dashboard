#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json, re, csv
from pathlib import Path
import argparse

def sniff_ids_from_json(p: Path):
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return set()
    ids = set()
    def add_from_obj(o):
        for k in ("id","review_id","kId","kid","KID"):
            if isinstance(o, dict) and k in o:
                v = o[k]
                if isinstance(v, (int, str)) and str(v).isdigit():
                    ids.add(str(v))
    if isinstance(data, list):
        for o in data: add_from_obj(o)
    elif isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                for o in v: add_from_obj(o)
            elif isinstance(v, dict):
                add_from_obj(v)
        add_from_obj(data)
    return ids

def sniff_ids_from_csv(p: Path):
    ids = set()
    try:
        with p.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            cols = [c.lower() for c in (reader.fieldnames or [])]
            candidates = [k for k in ("id","review_id","kid","kId","KID") if k in cols]
            for row in reader:
                for k in candidates:
                    v = row.get(k) or row.get(k.upper()) or row.get(k.capitalize())
                    if v and str(v).isdigit():
                        ids.add(str(v)); break
    except Exception:
        pass
    return ids

def collect_current_ids(series_id: str):
    """cache/koubei/<series_id>/ 内の全レビューjsonを収集"""
    ids = set()
    koubei_dir = Path("cache") / "koubei" / series_id
    if koubei_dir.exists():
        for p in koubei_dir.glob("*.json"):
            ids |= sniff_ids_from_json(p)
    # fallback: autohome_reviews_*.csv からも拾う
    for p in Path(".").glob(f"autohome_reviews_{series_id}.csv"):
        ids |= sniff_ids_from_csv(p)
    return ids

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-diff", type=int, default=3)
    args = ap.parse_args()

    series_id = os.environ.get("SERIES_ID", "").strip()
    if not series_id:
        print("SERIES_ID missing", file=sys.stderr)
        sys.exit(2)

    # review_ids.json のパスを cache/koubei/<id>/ に変更
    cache_dir = Path("cache") / "koubei" / series_id
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / "review_ids.json"

    prev_ids = set()
    if cache_file.exists():
        try:
            prev_ids = set(json.loads(cache_file.read_text(encoding="utf-8")))
        except Exception:
            prev_ids = set()

    cur_ids = collect_current_ids(series_id)

    new_ids = cur_ids - prev_ids
    do_story = len(new_ids) >= args.min_diff

    cache_file.write_text(json.dumps(sorted(cur_ids), ensure_ascii=False, indent=2), encoding="utf-8")

    gh_out = os.environ.get("GITHUB_OUTPUT")
    line = f"do_story={'true' if do_story else 'false'}\n"
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(line)
    else:
        print(line.strip())

    print(f"[diffguard] cur={len(cur_ids)} prev={len(prev_ids)} new={len(new_ids)} do_story={do_story}")

if __name__ == "__main__":
    main()
