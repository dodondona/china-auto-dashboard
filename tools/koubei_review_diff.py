#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json, os, sys
from pathlib import Path
import pandas as pd

MIN_DIFF = int(os.getenv("MIN_DIFF", "3"))

def load_jsons_from_cache(cache_dir):
    files = sorted(Path(cache_dir).glob("*.json"))
    ids = [f.stem for f in files]
    return set(ids)

def load_ids_from_csv(csv_path):
    df = pd.read_csv(csv_path)
    # 各レビューID列を特定（ID列のパターンに応じて柔軟に）
    candidates = [c for c in df.columns if "id" in c.lower()]
    if not candidates:
        print("[warn] no id column detected, returning empty set")
        return set()
    col = candidates[0]
    ids = df[col].astype(str).dropna().unique().tolist()
    return set(ids)

def save_cache_jsons(series_id, json_dir, output_dir):
    """キャッシュへの保存（LLM発火時のみ呼ばれる）"""
    cache_dir = Path(f"cache/koubei/{series_id}")
    cache_dir.mkdir(parents=True, exist_ok=True)
    for json_file in Path(json_dir).glob("*.json"):
        dest = cache_dir / json_file.name
        dest.write_bytes(json_file.read_bytes())
    story_src = Path(f"output/koubei/{series_id}/story.txt")
    if story_src.exists():
        story_dst = cache_dir / "story.txt"
        story_dst.write_bytes(story_src.read_bytes())
    print(f"[cache] updated cache for {series_id} ({len(list(cache_dir.glob('*.json')))} jsons)")

def main():
    if len(sys.argv) < 2:
        print("Usage: koubei_review_diff.py <series_id>")
        sys.exit(1)
    series_id = sys.argv[1]
    cache_dir = Path(f"cache/koubei/{series_id}")
    csv_path = Path(f"autohome_reviews_{series_id}.csv")

    # 現在・過去IDセット読み込み
    cur_ids = load_ids_from_csv(csv_path)
    prev_ids = load_jsons_from_cache(cache_dir) if cache_dir.exists() else set()

    # 差分判定
    diff_new = cur_ids - prev_ids
    diff_del = prev_ids - cur_ids
    diff_total = len(diff_new) + len(diff_del)

    print(f"[series] {series_id}")
    print(f"[diffguard] prev={len(prev_ids)} new={len(cur_ids)} diff={diff_total}")

    # LLM発火要否
    do_story = diff_total >= MIN_DIFF
    if do_story:
        print(f"[run] diff >= {MIN_DIFF} → regenerate story with LLM")
    else:
        print(f"[skip] diff below threshold ({diff_total} < {MIN_DIFF}) → reuse cache")

    # 結果を環境ファイルに出力
    with open(os.environ["GITHUB_ENV"], "a") as f:
        f.write(f"DO_STORY={'true' if do_story else 'false'}\n")

    # ✅ LLMを実行した場合のみ cache を更新
    if do_story:
        json_dir = Path(f"output/koubei/{series_id}")
        if json_dir.exists():
            save_cache_jsons(series_id, json_dir, "output")
        else:
            print(f"[warn] no json output directory found for {series_id}")
    else:
        print(f"[cache] unchanged; skip writing cache")

if __name__ == "__main__":
    main()
