#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
koubei_review_diff.py
目的:
  - autohome_reviews_{series_id}.csv の review ID 一覧を取得
  - cache/koubei/{series_id}/ 内に存在する過去 JSON 群から前回 ID 一覧を取得
  - 新旧の差分数を比較して、MIN_DIFF を超えた場合のみ story 再生成フラグを立てる
出力:
  - diff 数などをログ出力
  - 環境ファイルに do_story=true/false を書き込む
"""

import os
import pandas as pd
from pathlib import Path

def load_ids_from_csv(csv_path: str):
    df = pd.read_csv(csv_path)
    # review_id 列があることを前提
    return df["id"].astype(str).tolist()

def load_ids_from_cache(cache_dir: Path):
    ids = []
    if cache_dir.exists():
        for f in cache_dir.glob("*.json"):
            ids.append(f.stem)
    return ids

def main():
    series_id = os.environ.get("SERIES_ID") or ""
    min_diff = int(os.environ.get("MIN_DIFF", "3"))
    csv_path = f"autohome_reviews_{series_id}.csv"
    cache_dir = Path(f"cache/koubei/{series_id}")

    print(f"[series] {series_id}")

    cur_ids = load_ids_from_csv(csv_path)
    prev_ids = load_ids_from_cache(cache_dir)

    diff = len(set(cur_ids) ^ set(prev_ids))
    print(f"[diffguard] prev={len(prev_ids)} new={len(cur_ids)} diff={diff}")

    do_story = diff >= min_diff
    if do_story:
        print(f"[run] diff {diff} >= {min_diff} → regenerate story")
    else:
        print(f"[skip] diff below threshold ({diff} < {min_diff})")

    # ✅ GitHub Actions 新方式: 環境ファイルに書き込む
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"do_story={'true' if do_story else 'false'}\n")

if __name__ == "__main__":
    main()
