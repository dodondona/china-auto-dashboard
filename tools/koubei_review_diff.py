#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json, re, csv
from pathlib import Path
import argparse

# ==========================
# ID抽出ユーティリティ
# ==========================

def sniff_ids_from_json(p: Path):
    """JSON内からid/review_id/kid等を抽出"""
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
        for o in data:
            add_from_obj(o)
    elif isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                for o in v:
                    add_from_obj(o)
            elif isinstance(v, dict):
                add_from_obj(v)
        add_from_obj(data)
    return ids


def sniff_ids_from_csv(p: Path):
    """CSV内からid/review_id/kid等を抽出"""
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
                        ids.add(str(v))
                        break
    except Exception:
        pass
    return ids


def sniff_ids_from_txt(p: Path):
    """TXT内からidやURL末尾の数字を抽出"""
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


# ==========================
# メインロジック
# ==========================

def collect_current_ids():
    """ZIP展開結果 + cache/<series_id>/ の全JSONからIDを収集"""
    ids = set()

    # 1. ワークディレクトリ（autohome_reviews_*.json / *.csv / *.txt）
    for p in sorted(Path(".").glob("autohome_reviews_*.json")):
        ids |= sniff_ids_from_json(p)
    for p in sorted(Path(".").glob("autohome_reviews_*.csv")):
        ids |= sniff_ids_from_csv(p)
    for p in sorted(Path(".").glob("autohome_reviews_*.txt")):
        ids |= sniff_ids_from_txt(p)

    # 2. cache/<series_id>/ 内の個別JSONからも抽出
    series_id = os.environ.get("SERIES_ID","").strip()
    cache_dir = Path("cache") / series_id
    if cache_dir.exists():
        for p in cache_dir.glob("*.json"):
            # review_ids.json / columns.json 等は除外
            if p.name not in ("review_ids.json", "columns.json", "sections.json", "items.json", "values.json"):
                ids |= sniff_ids_from_json(p)

    return ids


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-diff", type=int, default=3)
    args = ap.parse_args()

    series_id = os.environ.get("SERIES_ID","").strip()
    if not series_id:
        print("SERIES_ID is required", file=sys.stderr)
        sys.exit(2)

    # 現在のIDリストを収集
    cur_ids = collect_current_ids()

    # キャッシュファイルの読み込み
    cache_dir = Path("cache") / series_id
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / "review_ids.json"

    prev_ids = set()
    if cache_file.exists():
        try:
            prev_ids = set(json.loads(cache_file.read_text(encoding="utf-8")))
        except Exception:
            prev_ids = set()

    # 差分を計算
    new_ids = cur_ids - prev_ids
    do_story = len(new_ids) >= args.min_diff

    # キャッシュを更新
    cache_file.write_text(json.dumps(sorted(cur_ids), ensure_ascii=False, indent=2), encoding="utf-8")

    # GitHub Actionsへの出力
    gh_out = os.environ.get("GITHUB_OUTPUT")
    line = f"do_story={'true' if do_story else 'false'}\n"
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(line)
    else:
        print(line.strip())

    # ログ出力
    print(f"[diffguard] cur={len(cur_ids)} prev={len(prev_ids)} new={len(new_ids)} do_story={do_story}")

if __name__ == "__main__":
    main()
