#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json, re, csv
from pathlib import Path
import argparse

# ----------------------------------------
# ID抽出ユーティリティ
# ----------------------------------------
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
                if isinstance(v, int) or (isinstance(v, str) and v.isdigit()):
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
            candidates = [k for k in ("id","review_id","kid","kId","KID") if k.lower() in cols]
            for row in reader:
                for k in candidates:
                    v = row.get(k) or row.get(k.upper()) or row.get(k.capitalize())
                    if v and str(v).isdigit():
                        ids.add(str(v)); break
    except Exception:
        pass
    return ids

def sniff_ids_from_txt(p: Path):
    ids = set()
    try:
        text = p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ids
    for m in re.finditer(r'\b(?:id|review_id|kid)\s*[:=]\s*(\d+)\b', text, flags=re.IGNORECASE):
        ids.add(m.group(1))
    for m in re.finditer(r'/(\d{4,})', text):
        ids.add(m.group(1))
    return ids

# ----------------------------------------
# 現在のIDを収集
# ----------------------------------------
def collect_current_ids():
    ids = set()
    # ✅ CSVを優先（ZIP→CSV変換後に確実に存在）
    for p in sorted(Path(".").glob("autohome_reviews_*.csv")):
        ids |= sniff_ids_from_csv(p)
    # JSONも補助的にチェック（手動実行時など）
    for p in sorted(Path(".").glob("autohome_reviews_*.json")):
        ids |= sniff_ids_from_json(p)
    # テキスト（debug出力など）も拾う
    for p in sorted(Path(".").glob("autohome_reviews_*.txt")):
        ids |= sniff_ids_from_txt(p)
    return ids

# ----------------------------------------
# メイン処理
# ----------------------------------------
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

    # ✅ デバッグ出力で内部状態を明示
    print(f"[diffguard] cur={len(cur_ids)} prev={len(prev_ids)} new={len(new_ids)} do_story={do_story}")

    # ✅ review_ids.jsonを更新
    cache_file.write_text(json.dumps(sorted(cur_ids), ensure_ascii=False, indent=2), encoding="utf-8")

    # ✅ GitHub Actions出力
    gh_out = os.environ.get("GITHUB_OUTPUT")
    line = f"do_story={'true' if do_story else 'false'}\n"
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(line)
    else:
        print(line.strip())

if __name__ == "__main__":
    main()
