#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
translate_brand_model_llm.py
ブランド・車種の中国語表記をWikipedia/Wikidata/公式サイトCSEから自動翻訳して日本語・英字名に変換。
"""

import os, re, csv, json, time, argparse, pandas as pd
from tqdm import tqdm

# === Wikipedia / Wikidata utilities =========================================

def lookup_wikipedia(term: str):
    import wikipediaapi
    # Wikipedia API の User-Agent ポリシー対応
    wiki = wikipediaapi.Wikipedia(
        language='zh',
        user_agent='china-auto-dashboard/1.0 (https://github.com/dodondona/china-auto-dashboard; contact: github-actions)'
    )
    p = wiki.page(term)
    if not p.exists():
        return None
    links = p.langlinks
    if 'ja' in links:
        return links['ja'].title
    elif 'en' in links:
        return links['en'].title
    return None

def lookup_wikidata(term: str):
    import requests
    try:
        url = f"https://www.wikidata.org/w/api.php?action=wbsearchentities&language=zh&format=json&search={term}"
        res = requests.get(url, timeout=10)
        data = res.json()
        if not data.get('search'):
            return None
        qid = data['search'][0]['id']
        url2 = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
        res2 = requests.get(url2, timeout=10)
        ent = res2.json()['entities'][qid]
        labels = ent.get('labels', {})
        return labels.get('ja', labels.get('en', {})).get('value')
    except Exception:
        return None

def resolve_with_optional_wikidata(term: str, use_wd=True, sleep_sec=0.1):
    """Wikipedia→Wikidataの順に解決。"""
    if not term or term.strip() == "":
        return {"ja": term}
    ja = lookup_wikipedia(term)
    if not ja and use_wd:
        ja = lookup_wikidata(term)
    time.sleep(sleep_sec)
    return {"ja": ja or term}

# === CSV translation main ====================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--brand-col", default="brand")
    ap.add_argument("--model-col", default="model")
    ap.add_argument("--brand-ja-col", default="brand_ja")
    ap.add_argument("--model-ja-col", default="model_ja")
    ap.add_argument("--cache", default=".cache/global_map.json")
    ap.add_argument("--sleep", type=float, default=0.1)
    ap.add_argument("--use-wikidata", action="store_true")
    ap.add_argument("--use-official", action="store_true",
                    help="Use official-site CSE fallback (GOOGLE_API_KEY/GOOGLE_CSE_ID required)")
    args = ap.parse_args()

    print(f"Translating: {args.input} -> {args.output}")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    os.makedirs(os.path.dirname(args.cache), exist_ok=True)

    if os.path.exists(args.cache):
        cache = json.load(open(args.cache, "r", encoding="utf-8"))
    else:
        cache = {}

    df = pd.read_csv(args.input)
    brands = df[args.brand-col].dropna().unique().tolist()
    models = df[args.model-col].dropna().unique().tolist()

    brand_map, model_map = {}, {}

    # --- brand ---------------------------------------------------------------
    for b in tqdm(brands, desc="brand"):
        key = f"brand::{b}"
        if key in cache:
            brand_map[b] = cache[key]
            continue
        res = resolve_with_optional_wikidata(b, use_wd=args.use_wikidata, sleep_sec=args.sleep)
        brand_map[b] = res["ja"]
        cache[key] = brand_map[b]

    # --- model ---------------------------------------------------------------
    for m in tqdm(models, desc="model"):
        key = f"model::{m}"
        if key in cache:
            model_map[m] = cache[key]
            continue
        res = resolve_with_optional_wikidata(m, use_wd=args.use_wikidata, sleep_sec=args.sleep)
        ja = res["ja"]
        # --- 公式CSEフォールバック ----------------------------------------
        if args.use_official and (not ja or ja == m):
            try:
                from tools.official_lookup import find_official_english
                guessed = find_official_english("", m)
                if guessed:
                    ja = guessed
            except Exception:
                pass
        model_map[m] = ja
        cache[key] = ja
        time.sleep(args.sleep)

    # --- 書き戻し -----------------------------------------------------------
    def _model_name(row):
        m = str(row[args.model_col])
        ja = model_map.get(m, m)
        if args.use_official and (not ja or ja == m):
            try:
                from tools.official_lookup import find_official_english
                guessed = find_official_english(str(row[args.brand_col]), m)
                if guessed:
                    return guessed
            except Exception:
                pass
        return ja

    df[args.brand_ja_col] = df[args.brand_col].map(lambda x: brand_map.get(str(x), str(x)))
    df[args.model_ja_col] = df.apply(_model_name, axis=1)

    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    json.dump(cache, open(args.cache, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

    print(f"✅ Done. Saved to {args.output}")

if __name__ == "__main__":
    main()
