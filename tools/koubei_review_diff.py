#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
koubei_review_diff.py
差分比較＋再生成フラグ出力（安全版）

要件:
- cache/{series_id}/ の review_id.json と autohome_reviews_{series_id}.csv の差分を比較。
- 差分件数 >= MIN_DIFF の場合のみ LLM再生成 & キャッシュ更新。
- 差分件数 < MIN_DIFF の場合、キャッシュを維持し、何も更新しない。
"""

import os
import json
from pathlib import Path
import pandas as pd
import shutil

SERIES_ID = os.environ.get("SERIES_ID") or "7740"
MIN_DIFF = int(os.environ.get("MIN_DIFF", "3"))
GITHUB_OUTPUT = os.environ.get("GITHUB_OUTPUT")

cache_dir = Path(f"cache/koubei/{SERIES_ID}")
csv_path = Path(f"autohome_reviews_{SERIES_ID}.csv")
tmp_json_dir = Path(f"tmp_reviews_{SERIES_ID}")  # ZIP展開済みjson群を想定


def load_existing_ids(cache: Path):
    """既存キャッシュ内のjsonファイル名をIDとして取得"""
    if not cache.exists():
        return set()
    return {p.stem for p in cache.glob("*.json")}


def load_new_ids(csv_path: Path):
    """CSVからreview_idまたはid列を取得"""
    if not csv_path.exists():
        print(f"[warn] CSV not found: {csv_path}")
        return set()
    try:
        df = pd.read_csv(csv_path)
        for col in ["review_id", "id"]:
            if col in df.columns:
                return set(df[col].astype(str))
    except Exception as e:
        print(f"[error] Failed to load CSV: {e}")
    return set()


def refresh_cache(src_dir: Path, dst_dir: Path):
    """キャッシュ全削除＋再生成"""
    if not src_dir.exists():
        print(f"[warn] Source dir not found: {src_dir}")
        return
    if dst_dir.exists():
        shutil.rmtree(dst_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)
    for p in src_dir.glob("*.json"):
        shutil.copy2(p, dst_dir / p.name)
    print(f"[cache] refreshed {dst_dir} with {len(list(dst_dir.glob('*.json')))} files.")


# ===== メイン処理 =====
print(f"[series] {SERIES_ID}")

prev_ids = load_existing_ids(cache_dir)
new_ids = load_new_ids(csv_path)

added = new_ids - prev_ids
removed = prev_ids - new_ids
diff_count = len(added | removed)

print(f"[diffguard] prev={len(prev_ids)} new={len(new_ids)} diff={diff_count}")

# 差分閾値判定
if diff_count >= MIN_DIFF:
    print(f"[trigger] diff {diff_count} >= {MIN_DIFF} → regenerate & refresh cache")
    if GITHUB_OUTPUT:
        with open(GITHUB_OUTPUT, "a") as f:
            f.write("do_story=true\n")
    refresh_cache(tmp_json_dir, cache_dir)
else:
    print(f"[skip] diff below threshold ({diff_count} < {MIN_DIFF})")
    if GITHUB_OUTPUT:
        with open(GITHUB_OUTPUT, "a") as f:
            f.write("do_story=false\n")
