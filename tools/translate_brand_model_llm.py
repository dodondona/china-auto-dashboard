#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
translate_brand_model_llm.py
中国語のbrand/model列をグローバル表記へ変換する。

優先順位:
  1. --use-official が有効なら official_lookup.py
  2. --use-wikidata が有効なら wikidata_lookup.py
  3. 上記で取得できなければ LLM (OpenAI GPT-4o など)

キャッシュは使用しない。
"""

import argparse
import pandas as pd
import time
from tqdm import tqdm
from openai import OpenAI

try:
    from tools.official_lookup import find_official_english
except ImportError:
    find_official_english = lambda b, m: None
try:
    from tools.wikidata_lookup import find_wikidata_english
except ImportError:
    find_wikidata_english = lambda b, m: None

# ==== CLI ====
parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--brand-col", default="brand")
parser.add_argument("--model-col", default="model")
parser.add_argument("--brand-ja-col", default="brand_ja")
parser.add_argument("--model-ja-col", default="model_ja")
parser.add_argument("--model", default="gpt-4o")
parser.add_argument("--sleep", type=float, default=0.5)
parser.add_argument("--use-wikidata", action="store_true")
parser.add_argument("--use-official", action="store_true")
args = parser.parse_args()

client = OpenAI()

# ==== Load ====
df = pd.read_csv(args.input)

def llm_translate(text):
    prompt = f"自動車ブランド・車種の公式英語名を返してください。単語のみで。対象: {text}"
    try:
        res = client.chat.completions.create(
            model=args.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        print("LLM error:", e)
        return None

brand_en, model_en, src = [], [], []

for _, row in tqdm(df.iterrows(), total=len(df)):
    b, m = str(row[args.brand_col]).strip(), str(row[args.model_col]).strip()
    b_out, m_out, source = None, None, None

    # ① official
    if args.use_official:
        try:
            b_out, m_out = find_official_english(b, m)
            if b_out or m_out:
                source = "official"
        except Exception:
            pass

    # ② wikidata
    if (not b_out or not m_out) and args.use_wikidata:
        try:
            wb, wm = find_wikidata_english(b, m)
            b_out = b_out or wb
            m_out = m_out or wm
            if wb or wm:
                source = "wikidata"
        except Exception:
            pass

    # ③ fallback to LLM
    if not b_out:
        b_out = llm_translate(b)
    if not m_out:
        m_out = llm_translate(m)
    if not source:
        source = "llm"

    brand_en.append(b_out or "")
    model_en.append(m_out or "")
    src.append(source)

    time.sleep(args.sleep)

df[args.brand_ja_col] = brand_en
df[args.model_ja_col] = model_en
df["source_model"] = src
df.to_csv(args.output, index=False, encoding="utf-8-sig")
print(f"Wrote: {args.output}")
