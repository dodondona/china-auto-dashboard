# tools/translate_brand_model_wikipedia.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
translate_brand_model_wikipedia.py
- 中国語(zh)の ブランド/モデル 名称から、Wikipedia(zh)だけを使って
  ja/en の言語間リンクタイトルを取得し、表示用の名称に変換します。
- LLMは使いません。Wikidataも使いません。Wikipedia APIのみ。
- 既存パイプラインと同じCLIに合わせ、brand_ja / model_ja 列を出力します。
  （命名は既存に合わせていますが、実際は「jaがあればja、なければen、無ければ原文zh」を入れます。）

使い方:
  python tools/translate_brand_model_wikipedia.py \
    --input data/autohome_raw_2025-09_with_brand.csv \
    --output data/autohome_raw_2025-09_with_brand_ja.csv \
    --brand-col brand --model-col model \
    --brand-ja-col brand_ja --model-ja-col model_ja

依存:
  - requests, pandas（既にrequirements.txtにあり）
"""

import os
import sys
import time
import json
import argparse
import requests
import pandas as pd
from typing import Dict, Optional

WIKI_API = "https://zh.wikipedia.org/w/api.php"

def is_latin(s: str) -> bool:
    try:
        s.encode('ascii')
        return True
    except Exception:
        return False

def wiki_query(params: Dict, timeout=15) -> Dict:
    p = {"format": "json"}
    p.update(params)
    r = requests.get(WIKI_API, params=p, timeout=timeout)
    r.raise_for_status()
    return r.json()

def get_langlinks_by_title(zh_title: str) -> Dict[str, Optional[str]]:
    data = wiki_query({
        "action": "query",
        "prop": "langlinks",
        "redirects": 1,
        "titles": zh_title,
        "lllimit": "500",
    })
    pages = data.get("query", {}).get("pages", {})
    en = ja = None
    for _, page in pages.items():
        if "missing" in page:
            continue
        for ll in page.get("langlinks", []) or []:
            if ll.get("lang") == "en":
                en = ll.get("*")
            elif ll.get("lang") == "ja":
                ja = ll.get("*")
    return {"en": en, "ja": ja}

def search_zh_title(kw: str) -> Optional[str]:
    data = wiki_query({
        "action": "query",
        "list": "search",
        "srsearch": kw,
        "srwhat": "nearmatch",
        "srlimit": 1,
    })
    hits = data.get("query", {}).get("search", [])
    if hits:
        return hits[0].get("title")
    data2 = wiki_query({
        "action": "query",
        "list": "search",
        "srsearch": kw,
        "srlimit": 1,
    })
    hits2 = data2.get("query", {}).get("search", [])
    if hits2:
        return hits2[0].get("title")
    return None

def resolve_name_via_wikipedia(zh_name: str, sleep_sec: float = 0.1) -> Dict[str, str]:
    name = str(zh_name or "").strip()
    if not name:
        return {"ja": "", "en": ""}

    if is_latin(name):
        return {"ja": name, "en": name}

    langlinks = get_langlinks_by_title(name)
    if not langlinks.get("en") and not langlinks.get("ja"):
        t = search_zh_title(name)
        if t:
            time.sleep(sleep_sec)
            langlinks = get_langlinks_by_title(t)

    en = langlinks.get("en")
    ja = langlinks.get("ja")
    if ja:
        return {"ja": ja, "en": en or ja}
    if en:
        return {"ja": en, "en": en}
    return {"ja": name, "en": name}

def load_cache(path: str) -> Dict:
    if not path or not os.path.exists(path):
        return {"brand": {}, "model": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"brand": {}, "model": {}}

def save_cache(path: str, data: Dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="input CSV (must contain brand/model)")
    ap.add_argument("--output", required=True, help="output CSV")
    ap.add_argument("--brand-col", default="brand")
    ap.add_argument("--model-col", default="model")
    ap.add_argument("--brand-ja-col", default="brand_ja")
    ap.add_argument("--model-ja-col", default="model_ja")
    ap.add_argument("--cache", default=".cache/wikipedia_map.json")
    ap.add_argument("--sleep", type=float, default=0.1, help="per-request sleep seconds")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    if args.brand_col not in df.columns or args.model_col not in df.columns:
        raise RuntimeError(f"Input must contain '{args.brand_col}' and '{args.model_col}'. columns={list(df.columns)}")

    cache = load_cache(args.cache)
    brand_map: Dict[str, str] = cache.get("brand", {})
    model_map: Dict[str, str] = cache.get("model", {})

    uniq_brands = sorted({str(x) for x in df[args.brand_col].dropna().unique()})
    uniq_models = sorted({str(x) for x in df[args.model_col].dropna().unique()})

    for b in uniq_brands:
        if b not in brand_map:
            res = resolve_name_via_wikipedia(b, sleep_sec=args.sleep)
            brand_map[b] = res["ja"]
    cache["brand"] = brand_map; save_cache(args.cache, cache)

    for m in uniq_models:
        if m not in model_map:
            res = resolve_name_via_wikipedia(m, sleep_sec=args.sleep)
            model_map[m] = res["ja"]
    cache["model"] = model_map; save_cache(args.cache, cache)

    df[args.brand_ja_col] = df[args.brand_col].map(lambda x: brand_map.get(str(x), str(x)))
    df[args.model_ja_col] = df[args.model_col].map(lambda x: model_map.get(str(x), str(x)))

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[OK] Wikipedia-normalized: {args.input} -> {args.output} (rows={len(df)})")

if __name__ == "__main__":
    main()
