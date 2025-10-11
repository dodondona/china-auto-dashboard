#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
translate_brand_model_llm.py

中国語の brand/model をグローバル英語表記へ変換する。
優先順位:
  1) --use-official が指定されていれば 公式サイト解析
  2) --use-wikidata が指定されていれば Wikipedia/Wikidata
  3) どちらでも得られなければ LLM で補完

キャッシュは使用しないが、既存 YAML 互換のため --cache を受理して無視する。
"""

import argparse
import time
import pandas as pd
from tqdm import tqdm

# OpenAI
from openai import OpenAI

# optional imports (存在しない環境でも落ちないように)
try:
    from tools.official_lookup import find_official_english
except Exception:
    def find_official_english(brand: str, model: str):
        return (None, None)

try:
    from tools.wikidata_lookup import find_wikidata_english
except Exception:
    def find_wikidata_english(brand: str, model: str):
        return (None, None)

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--brand-col", default="brand")
    p.add_argument("--model-col", default="model")
    p.add_argument("--brand-ja-col", default="brand_ja")  # 出力列名（英語だが既存列名を踏襲）
    p.add_argument("--model-ja-col", default="model_ja")
    p.add_argument("--model", dest="llm_model", default="gpt-4o")
    p.add_argument("--sleep", type=float, default=0.4)

    # 既存 YAML 互換用（使わないが受け付ける）
    p.add_argument("--cache", default=None)

    # 追加オプション
    p.add_argument("--use-wikidata", action="store_true",
                   help="Use Wikipedia/Wikidata lookup as a second priority")
    p.add_argument("--use-official", action="store_true",
                   help="Use Official website lookup as a first priority")
    return p

def llm_translate(client: OpenAI, model_name: str, text: str) -> str | None:
    if not text or text.strip() == "":
        return None
    prompt = (
        "以下の自動車ブランド名/車種名の公式英語表記を、余計な説明なしで1語～数語だけ返してください。\n"
        "対象: " + text.strip()
    )
    try:
        res = client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        return (res.choices[0].message.content or "").strip()
    except Exception as e:
        print(f"LLM error for '{text}': {e}")
        return None

def main():
    args = build_parser().parse_args()
    df = pd.read_csv(args.input)

    # OpenAI client
    client = OpenAI()

    out_brand = []
    out_model = []
    out_source = []

    it = tqdm(df.itertuples(index=False), total=len(df), desc="translate")
    for row in it:
        b = str(getattr(row, args.brand_col, "")).strip()
        m = str(getattr(row, args.model_col, "")).strip()

        brand_en = None
        model_en = None
        source = None

        # 1) Official
        if args.use_official:
            try:
                ob, om = find_official_english(b, m)
                if ob: brand_en = ob
                if om: model_en = om
                if ob or om:
                    source = "official"
            except Exception as e:
                print(f"[official] error ({b}, {m}): {e}")

        # 2) Wikipedia/Wikidata
        if (not brand_en or not model_en) and args.use_wikidata:
            try:
                wb, wm = find_wikidata_english(b, m)
                if not brand_en and wb: brand_en = wb
                if not model_en and wm: model_en = wm
                if (wb or wm) and source is None:
                    source = "wikidata"
            except Exception as e:
                print(f"[wikidata] error ({b}, {m}): {e}")

        # 3) LLM fallback
        if not brand_en:
            brand_en = llm_translate(client, args.llm_model, b)
        if not model_en:
            model_en = llm_translate(client, args.llm_model, m)
        if source is None:
            source = "llm"

        out_brand.append(brand_en or "")
        out_model.append(model_en or "")
        out_source.append(source)

        time.sleep(args.sleep)

    df[args.brand_ja_col] = out_brand
    df[args.model_ja_col] = out_model
    df["source_model"] = out_source

    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"Wrote: {args.output}")

if __name__ == "__main__":
    main()
