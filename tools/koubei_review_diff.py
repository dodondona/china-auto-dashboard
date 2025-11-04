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
  - 環境ファイルに do_story=true/false を書き込む（YML 両対応）
"""

import os
import re
import glob
import pandas as pd
from pathlib import Path

def load_ids_from_csv(csv_path: str):
    df = pd.read_csv(csv_path)
    return df["id"].astype(str).tolist()

def load_ids_from_cache(cache_dir: Path):
    ids = []
    if cache_dir.exists():
        for f in cache_dir.glob("*.json"):
            ids.append(f.stem)
    return ids

def infer_series_id():
    """環境変数が無い場合のフォールバック（YML両対応）"""
    sid = os.environ.get("SERIES_ID") or os.environ.get("series_id") or ""
    if sid:
        return sid

    # autohome_reviews_XXXX.csv から推測
    matches = glob.glob("autohome_reviews_*.csv")
    for m in matches:
        res = re.search(r"autohome_reviews_(\d+)\.csv", m)
        if res:
            return res.group(1)

    # cache ディレクトリからも推測（過去実行時の残骸から）
    for path in Path("cache").rglob("koubei/*"):
        if path.is_dir() and path.name.isdigit():
            return path.name

    raise RuntimeError("series_id could not be inferred (no env, no file).")

def main():
    series_id = infer_series_id()
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
        print("::notice::Diff threshold exceeded, regenerating story.")
    else:
        print("::notice::No significant diff, skipping regeneration.")

    # ✅ GitHub Actions 新形式出力（両YML対応）
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a") as f:
            f.write(f"do_story={'true' if do_story else 'false'}\n")

if __name__ == "__main__":
    main()
