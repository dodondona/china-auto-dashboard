#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json, re, glob, csv
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
                if isinstance(v, (int, str)) and re.match(r"^[0-9A-Za-z]+$", str(v)):
                    # 🚫 kmなどの単位を含む値を除外
                    if re.search(r"(km|mm|cm|kg|m)$", str(v).lower()):
                        return
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
            cols = [(c or "").strip().lower() for c in (reader.fieldnames or [])]
            candidates = [k for k in ("id","review_id","kid") if k in cols]
            for row in reader:
                for k in candidates:
                    v = row.get(k) or row.get(k.upper()) or row.get(k.capitalize())
                    if v and re.match(r"^[0-9A-Za-z]+$", str(v)):
                        # 🚫 kmなどの単位を含む値を除外
                        if re.search(r"(km|mm|cm|kg|m)$", str(v).lower()):
                            continue
                        ids.add(str(v))
                        break
    except Exception:
        pass
    return ids

def sniff_ids_from_txt(p: Path):
    ids = set()
    try:
        text = p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ids
    for m in re.finditer(r'\b(?:id|review_id|kid)\s*[:=]\s*([0-9A-Za-z]+)\b', text, flags=re.IGNORECASE):
        val = m.group(1)
        if not re.search(r"(km|mm|cm|kg|m)$", val.lower()):
            ids.add(val)
    for m in re.finditer(r'/([0-9A-Za-z]{4,})', text):
        val = m.group(1)
        if not re.search(r"(km|mm|cm|kg|m)$", val.lower()):
            ids.add(val)
    return ids

def collect_current_ids():
    ids = set()
    for pattern in [
        "autohome_reviews_*.json",
        "autohome_reviews_*.csv",
        "autohome_reviews_*.txt",
        "cache/**/*.json",
        "cache/**/*.csv",
        "cache/**/*.txt",
    ]:
        for p in sorted(Path(".").glob(pattern)):
            if p.suffix == ".json":
                ids |= sniff_ids_from_json(p)
            elif p.suffix == ".csv":
                ids |= sniff_ids_from_csv(p)
            elif p.suffix == ".txt":
                ids |= sniff_ids_from_txt(p)
    return ids

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-diff", type=int, default=3)
    args = ap.parse_args()

    series_id = os.environ.get("SERIES_ID","").strip()
    if not series_id:
        print("SERIES_ID is required", file=sys.stderr); sys.exit(2)

    cur_ids = collect_current_ids()
    cache_dir = Path("cache")/series_id
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir/"review_ids.json"

    prev_ids = set()
    if cache_file.exists():
        try:
            prev_ids = set(json.loads(cache_file.read_text(encoding="utf-8")))
        except Exception:
            prev_ids = set()

    new_ids = cur_ids - prev_ids
    do_story = len(new_ids) >= args.min_diff

    cache_file.write_text(json.dumps(sorted(cur_ids), ensure_ascii=False), encoding="utf-8")

    gh_out = os.environ.get("GITHUB_OUTPUT")
    line = f"do_story={'true' if do_story else 'false'}\n"
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(line)
    else:
        print(line.strip())

if __name__ == "__main__":
    main()
