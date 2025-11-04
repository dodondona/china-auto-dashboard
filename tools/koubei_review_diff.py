#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
koubei_review_diff.py
目的:
  - cache/koubei/{series_id}/ 内に保存された既存レビューID群と
    今回ZIPから展開された autohome_reviews_{series_id}.csv (またはjson群)
    に含まれるレビューID群を比較し、差分数に応じて再生成を判定する。
  - 差分が閾値を超えた場合:
      → LLM再生成フラグをON
      → キャッシュ内の古いレビューJSONを全削除し、最新ZIP内容で再保存。
  - 差分が閾値未満の場合:
      → キャッシュ・storyともに変更せずスキップ。

使用例:
  python tools/koubei_review_diff.py 7740
"""

import os
import json
from pathlib import Path
import pandas as pd

# ====== 設定 ======
SERIES_ID = os.environ.get("SERIES_ID") or "7740"
MIN_DIFF = int(os.environ.get("MIN_DIFF", "3"))

base_cache_dir = Path(f"cache/koubei/{SERIES_ID}")
output_dir = Path(f"output/koubei/{SERIES_ID}")
csv_path = Path(f"autohome_reviews_{SERIES_ID}.csv")

# ====== 関数 ======
def load_existing_ids(cache_dir: Path):
    """キャッシュに存在するレビューID一覧を取得"""
    ids = set()
    if not cache_dir.exists():
        return ids
    for p in cache_dir.glob("*.json"):
        ids.add(p.stem)
    return ids


def load_new_ids_from_csv(csv_file: Path):
    """CSVからreview_id列を抽出"""
    if not csv_file.exists():
        print(f"[warn] CSV not found: {csv_file}")
        return set()
    try:
        df = pd.read_csv(csv_file)
        if "review_id" in df.columns:
            return set(df["review_id"].astype(str).tolist())
        elif "id" in df.columns:
            return set(df["id"].astype(str).tolist())
        else:
            # ID列がない場合、空集合を返す
            return set()
    except Exception as e:
        print(f"[error] CSV load failed: {e}")
        return set()


def clear_and_copy_cache(src_json_dir: Path, dst_cache_dir: Path):
    """キャッシュを全削除し、最新レビューJSON群をコピー"""
    import shutil
    if dst_cache_dir.exists():
        shutil.rmtree(dst_cache_dir)
    dst_cache_dir.mkdir(parents=True, exist_ok=True)
    for p in src_json_dir.glob("*.json"):
        shutil.copy2(p, dst_cache_dir / p.name)
    print(f"[cache] refreshed {dst_cache_dir}")


# ====== メイン処理 ======
print(f"[series] {SERIES_ID}")

# 現在キャッシュにあるID
prev_ids = load_existing_ids(base_cache_dir)
# 今回CSV内のID
new_ids = load_new_ids_from_csv(csv_path)

# 両者の差分
added = new_ids - prev_ids
removed = prev_ids - new_ids
diff_count = len(added | removed)

print(f"[diffguard] prev={len(prev_ids)} new={len(new_ids)} diff={diff_count}")

# 閾値判定
if diff_count >= MIN_DIFF:
    print(f"[trigger] Detected {diff_count} ID changes (>= {MIN_DIFF}), will regenerate story & refresh cache.")
    print("::set-output name=do_story::true")

    # キャッシュディレクトリを全更新
    src_json_dir = Path(f"tmp_reviews_{SERIES_ID}")  # ZIP展開先を仮定
    if src_json_dir.exists():
        clear_and_copy_cache(src_json_dir, base_cache_dir)
    else:
        print(f"[warn] source json dir not found: {src_json_dir}")
else:
    print(f"[skip] diff below threshold ({diff_count} < {MIN_DIFF})")
    print("::set-output name=do_story::false")
