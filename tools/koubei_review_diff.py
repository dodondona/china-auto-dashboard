#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
目的:
  - AutohomeレビューCSV (autohome_reviews_{series_id}.csv) のIDリストを
    直近キャッシュと比較して差分を検出
  - 閾値 (MIN_DIFF) 以上の差分がある場合のみ LLM 要約を再実行する
"""

import os, sys, json, pandas as pd
from pathlib import Path

def load_ids_from_csv(csv_path: str):
    df = pd.read_csv(csv_path)
    if "id" in df.columns:
        return set(df["id"].astype(str))
    if "review_id" in df.columns:
        return set(df["review_id"].astype(str))
    return set()

def load_prev_ids(series_id: str):
    cache_dir = Path(f"cache/koubei/{series_id}")
    if not cache_dir.exists():
        return set()
    ids = set()
    for p in cache_dir.glob("*.json"):
        ids.add(p.stem)
    return ids

def main():
    series_id = os.environ.get("SERIES_ID", "").strip()
    if not series_id:
        print("❌ SERIES_ID is missing. Please set it in workflow env.")
        sys.exit(1)

    csv_path = f"autohome_reviews_{series_id}.csv"
    if not os.path.exists(csv_path):
        print(f"❌ CSV not found: {csv_path}")
        sys.exit(1)

    min_diff = int(os.environ.get("MIN_DIFF", "3"))
    print(f"[series] {series_id}")

    cur_ids = load_ids_from_csv(csv_path)
    prev_ids = load_prev_ids(series_id)

    new_ids = cur_ids - prev_ids
    diff_count = len(new_ids)

    print(f"[diffguard] prev={len(prev_ids)} new={len(cur_ids)} diff={diff_count}")

    do_story = diff_count >= min_diff
    if do_story:
        print(f"[run] diff {diff_count} >= {min_diff} → regenerate story")
    else:
        print(f"[skip] diff below threshold ({diff_count} < {min_diff})")

    # GitHub Actions 用出力
    output_line = f"do_story={'true' if do_story else 'false'}"
    print(f"::set-output name=do_story::{ 'true' if do_story else 'false' }")

if __name__ == "__main__":
    main()
