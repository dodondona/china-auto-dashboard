#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
brand/model のグローバル英語名を Official -> Wikipedia -> LLM の優先順位で補完。
キャッシュは使わない（--no-cache デフォルト）。

使い方:
python tools/translate_brand_model_llm.py \
  --input data/autohome_raw_2025-08_with_brand.csv \
  --output data/autohome_raw_2025-08_with_brand_ja.csv \
  --brand-col brand --model-col model \
  --brand-ja-col brand_ja --model-ja-col model_ja \
  --model-official-en-col model_official_en \
  --source-col source_model \
  --use-official --use-wikipedia --use-llm \
  --cse-id <CSE_ID> --google-api-key <KEY> \
  --llm-model gpt-4o-mini \
  --sleep 0.6 --no-cache

注意:
- Official 検索: Google CSE で指定ブランドの英語公式サイト/グローバルサイトを狙い撃ち
- Wikipedia: zh/ja/en を順に確認
- LLM: 最後のフォールバック（環境変数 OPENAI_API_KEY 必須）
"""

import argparse
import csv
import os
import time
from typing import Optional

import pandas as pd

# --- 検索/取得系はモジュール分離していてもOK。ここでは最小限のスタブ実装 ---
import requests

OFFICIAL_HINTS = [
    "global site", "official site", "global website", "brand", "automobile",
]

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--brand-col", default="brand")
    p.add_argument("--model-col", default="model")
    p.add_argument("--brand-ja-col", default="brand_ja")
    p.add_argument("--model-ja-col", default="model_ja")
    p.add_argument("--model-official-en-col", default="model_official_en")
    p.add_argument("--source-col", default="source_model")

    p.add_argument("--use-official", action="store_true")
    p.add_argument("--use-wikipedia", action="store_true")
    p.add_argument("--use-llm", action="store_true")
    p.add_argument("--cse-id", default=os.getenv("GOOGLE_CSE_ID"))
    p.add_argument("--google-api-key", default=os.getenv("GOOGLE_API_KEY"))
    p.add_argument("--llm-model", default=os.getenv("LLM_MODEL", "gpt-4o-mini"))
    p.add_argument("--sleep", type=float, default=0.6)
    p.add_argument("--no-cache", action="store_true", default=True)
    return p.parse_args()

def _gcs_query(cse_id: str, key: str, q: str) -> list:
    url = "https://www.googleapis.com/customsearch/v1"
    params = {"cx": cse_id, "key": key, "q": q, "num": 3}
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        return []
    js = r.json()
    return js.get("items", []) or []

def try_official(brand_cn: str, model_cn: str, cse_id: str, key: str) -> Optional[str]:
    if not cse_id or not key:
        return None
    q = f"{brand_cn} {model_cn} official site"
    items = _gcs_query(cse_id, key, q)
    # タイトルやスニペットからモデル名らしい英語（語頭大文字/英数字/ダッシュ等）を抽出するだけの簡易版
    for it in items:
        title = it.get("title", "")
        # 例: "BYD SEAL 05 DM-i – BYD Global"
        tokens = [t for t in title.replace("–","-").split() if any(c.isalpha() for c in t)]
        cand = " ".join(tokens[:4]).strip()
        if cand:
            return cand
    return None

def try_wiki(brand_cn: str, model_cn: str) -> Optional[str]:
    # 実運用では Wikipedia API を使う。ここでは簡略に zh->en のページタイトルを引く
    try:
        url = "https://zh.wikipedia.org/w/api.php"
        params = {"action": "opensearch", "search": f"{brand_cn} {model_cn}",
                  "limit": 1, "namespace": 0, "format": "json"}
        r = requests.get(url, params=params, timeout=25)
        if r.status_code != 200:
            return None
        data = r.json()
        if len(data) >= 2 and data[1]:
            return data[1][0]
    except Exception:
        return None
    return None

def try_llm(brand_cn: str, model_cn: str, model_name: str) -> Optional[str]:
    # APIキーは環境変数 OPENAI_API_KEY を使用。ここはダミー(実環境でopenai呼び出しへ差し替え)
    # 失敗時は None を返す
    return f"{brand_cn} {model_cn}"  # フォールバック(最低限の返し)

def main():
    args = _parse_args()
    df = pd.read_csv(args.input)

    # 出力列を確保
    for col in [args.brand_ja_col, args.model_ja_col, args.model_official_en_col, args.source_col]:
        if col not in df.columns:
            df[col] = pd.NA

    for i, row in df.iterrows():
        brand_cn = str(row.get(args.brand_col) or "").strip()
        model_cn = str(row.get(args.model_col) or "").strip()
        src = None
        model_off_en = None

        # 1) Official
        if args.use_official:
            model_off_en = try_official(brand_cn, model_cn, args.cse_id, args.google_api_key)
            if model_off_en:
                src = "official"

        # 2) Wikipedia
        if not model_off_en and args.use_wikipedia:
            model_off_en = try_wiki(brand_cn, model_cn)
            if model_off_en:
                src = "wikipedia"

        # 3) LLM
        if not model_off_en and args.use_llm:
            model_off_en = try_llm(brand_cn, model_cn, args.llm_model)
            if model_off_en:
                src = "llm"

        if model_off_en:
            df.at[i, args.model_official_en_col] = model_off_en
            df.at[i, args.model_ja_col] = model_off_en  # 日本語列には英語名を暫定で入れる（必要なら後段でja化）
            df.at[i, args.brand_ja_col] = brand_cn if brand_cn else pd.NA
            df.at[i, args.source_col] = src

        time.sleep(args.sleep)

    df.to_csv(args.output, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"► {args.input} -> {args.output}")

if __name__ == "__main__":
    main()
