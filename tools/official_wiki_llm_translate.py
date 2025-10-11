#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline:
 1) OFFICIAL (Google CSE で公式候補URLを検索 → official_extractor で抽出)
 2) WIKIPEDIA (wikipedia-api で brand+model ページの英語見出し)
 3) LLM (OpenAIに brand/model/タイトルを渡し、英名を1語～数語で返答させる)
Cache なし。失敗は静かにフォールバック。

Input CSV: --input with columns at least [brand, model]
Output CSV: 追加列 [brand_ja, model_ja, model_official_en, source_model]
"""

from __future__ import annotations
import os, csv, time, json, re, sys
import argparse
import pandas as pd
import requests
from urllib.parse import quote_plus

from official_extractor import extract_official_name  # 同ディレクトリ

# Optional deps
try:
    import wikipediaapi
except Exception:
    wikipediaapi = None

OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

def cse_search(q: str, api_key: str, cse_id: str, num: int = 5) -> list[dict]:
    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": api_key, "cx": cse_id, "q": q, "num": num}
    r = requests.get(url, params=params, timeout=20)
    if r.status_code != 200:
        return []
    data = r.json()
    return data.get("items", []) or []

ALLOW_EXT = (".html", "", "/")
BAD_IN_URL = ("pdf","/news/","/press","/media","/download","/spec","/brochure")

def pick_official_url(items: list[dict], brand: str, model: str) -> str|None:
    # 最初の 5 件から、タイトルに model 部分を含み、かつ NGワード不含のURLをスコア
    model_part = model.lower().replace(" ", "")
    def score(it):
        t = (it.get("title") or "").lower()
        u = (it.get("link") or "").lower()
        s = 0
        if model_part and model_part in t.replace(" ", ""): s += 5
        if brand.lower() in t: s += 2
        if any(b in u for b in BAD_IN_URL): s -= 5
        if not u.endswith(ALLOW_EXT): s -= 2
        return s
    items = sorted(items, key=score, reverse=True)
    for it in items:
        link = it.get("link") or ""
        if any(b in link.lower() for b in BAD_IN_URL): continue
        return link
    return None

def wiki_lookup(brand: str, model: str) -> str|None:
    if not wikipediaapi:
        return None
    wiki = wikipediaapi.Wikipedia("en")
    for q in [f"{brand} {model}", f"{model}"]:
        p = wiki.page(q)
        if p and p.exists():
            # ページタイトルが英名のことが多い
            title = p.title.strip()
            # 変な長文タイトルを抑制
            if 2 <= len(title) <= 40:
                return title
    return None

def llm_fallback(brand: str, model: str, title_raw: str) -> str|None:
    import openai
    openai.api_key = os.environ.get("OPENAI_API_KEY", "")
    if not openai.api_key:
        return None
    prompt = f"""
You are a car naming normalizer. Given a Chinese brand and model (with raw Chinese series page title),
return the concise official/global English model/series name only (no extra words).
Brand: {brand}
Model: {model}
Raw title: {title_raw}
Answer with only the name, e.g., "Qin PLUS", "Yuan PLUS", "Lavida", "Magotan", "Model Y".
"""
    try:
        resp = openai.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role":"user","content":prompt}],
            temperature=0
        )
        name = resp.choices[0].message.content.strip()
        name = re.sub(r'["\u3000]+', "", name).strip()
        return name or None
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--brand-col", default="brand")
    ap.add_argument("--model-col", default="model")
    ap.add_argument("--brand-ja-col", default="brand_ja")
    ap.add_argument("--model-ja-col", default="model_ja")
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--use-official", action="store_true")
    ap.add_argument("--use-wikipedia", action="store_true")
    ap.add_argument("--use-llm", action="store_true")
    args = ap.parse_args()

    df = pd.read_csv(args.input)

    out_rows = []
    api_key = os.environ.get("GOOGLE_API_KEY","")
    cse_id  = os.environ.get("GOOGLE_CSE_ID","")

    for _, row in df.iterrows():
        brand = str(row.get(args.brand_col, "")).strip()
        model = str(row.get(args.model_col, "")).strip()
        title_raw = str(row.get("title_raw", "")).strip()
        brand_ja = row.get(args.brand_ja_col, "")
        model_ja = row.get(args.model_ja_col, "")

        model_official = None
        source = ""

        # 1) OFFICIAL
        if args.use_official and api_key and cse_id:
            q = f'site: {brand} {model} official'
            items = cse_search(f"{brand} {model}", api_key, cse_id, num=5)
            url = pick_official_url(items, brand, model)
            if url:
                name, dbg = extract_official_name(url, brand_hint=brand)
                if name:
                    model_official = name
                    source = "official"

        # 2) WIKI
        if not model_official and args.use_wikipedia:
            w = wiki_lookup(brand, model)
            if w:
                model_official = w
                source = "wikipedia"

        # 3) LLM
        if not model_official and args.use_llm:
            name = llm_fallback(brand, model, title_raw)
            if name:
                model_official = name
                source = "llm"

        out_rows.append({
            **row.to_dict(),
            "model_official_en": model_official or "",
            "source_model": source or ""
        })

        if args.sleep > 0:
            time.sleep(args.sleep)

    pd.DataFrame(out_rows).to_csv(args.output, index=False)
    print(f"Wrote: {args.output}")

if __name__ == "__main__":
    main()
