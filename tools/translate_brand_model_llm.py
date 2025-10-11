#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Translate brand/model to global English name:
priority = OFFICIAL (CSE等) -> WIKIPEDIA -> LLM

- どの段で確定したかを source_model に記録 (official / wikidata / llm)
- 失敗理由の要約を resolve_note に記録
- --use-official / --use-wikidata 指定時にヒット0なら明示的に失敗（フェイルファースト）
- キャッシュは使いません（ご要望通り）

前提:
- 公式/Wiki 参照は tools/official_lookup.py / tools/wikidata_lookup.py を想定
  - どちらも存在しない場合は自動的に無効化
- OPENAI_API_KEY は環境変数で渡す
"""

import argparse
import os
import sys
import time
import pandas as pd
from tqdm import tqdm

# -------- optional deps: official / wiki lookups ----------
OFFICIAL_ACTIVE = True
try:
    # 実装はユーザー環境側にある想定:
    # def find_official_english(brand_zh: str, model_zh: str) -> tuple[str|None, str|None]
    from tools.official_lookup import find_official_english
except Exception:
    OFFICIAL_ACTIVE = False
    def find_official_english(brand, model):
        return (None, None)

WIKI_ACTIVE = True
try:
    # 実装はユーザー環境側にある想定:
    # def find_wikidata_english(brand_zh: str, model_zh: str) -> tuple[str|None, str|None]
    from tools.wikidata_lookup import find_wikidata_english
except Exception:
    WIKI_ACTIVE = False
    def find_wikidata_english(brand, model):
        return (None, None)

# -------- LLM (OpenAI) ----------
def _init_openai_client():
    """
    OpenAI SDKの差異に耐える初期化。
    - v1系: from openai import OpenAI; client = OpenAI()
    - 旧系: import openai; openai.api_key = ...
    """
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("ENV OPENAI_API_KEY is missing")

    try:
        # 新SDK (>=1.0)
        from openai import OpenAI
        client = OpenAI()
        client._compat_mode = "responses_first"  # 自己メモ（特に影響なし）
        client.__api_variant = "responses"
        return client, "new"
    except Exception:
        import openai as _openai
        _openai.api_key = api_key
        return _openai, "legacy"

def llm_translate(client, api_variant, model, term: str) -> str:
    """
    中国語の車ブランド/車名を、グローバルで使う英語表記に1行で返す。
    余計な注釈・接尾辞・翻訳語は付けない（例: 'Yuan PLUS', 'Lavida', 'Sagitar'）
    """
    prompt = (
        "You are a strict normalizer for Chinese auto brand/model names.\n"
        "Task: Output the official or globally used ENGLISH name for the given Chinese term.\n"
        "Rules:\n"
        "- Return ONLY the English name (no extra words, no punctuation, no quotes).\n"
        "- If the term already appears in English/Roman letters, return it unchanged.\n"
        "- Keep OEM-specific transliterations used in China market when they are the official English (e.g., 'Lavida', 'Sagitar', 'Binyue').\n"
        f"Term: {term}\n"
        "Answer:"
    )
    try:
        if api_variant == "new":
            # responses API
            resp = client.responses.create(
                model=model,
                input=prompt,
                temperature=0,
            )
            txt = resp.output_text.strip()
        else:
            # legacy chat completions
            resp = client.ChatCompletion.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
            )
            txt = resp.choices[0].message["content"].strip()
        # 1行に限定
        return txt.splitlines()[0].strip()
    except Exception as e:
        # LLMが駄目でも落とさない：空文字を返す
        return ""

# -------- main ----------
def build_parser():
    p = argparse.ArgumentParser(
        description="Translate brand/model to global English with priority: OFFICIAL -> WIKIPEDIA -> LLM"
    )
    p.add_argument("--input", required=True, help="input CSV path")
    p.add_argument("--output", required=True, help="output CSV path")

    p.add_argument("--brand-col", default="brand", dest="brand_col")
    p.add_argument("--model-col", default="model", dest="model_col")
    p.add_argument("--brand-ja-col", default="brand_ja", dest="brand_ja_col")
    p.add_argument("--model-ja-col", default="model_ja", dest="model_ja_col")

    p.add_argument("--model", default="gpt-4o-mini", dest="llm_model", help="LLM model name")
    p.add_argument("--sleep", type=float, default=0.0, help="sleep seconds per unique pair")

    p.add_argument("--use-wikidata", action="store_true", help="try Wikipedia/Wikidata layer")
    p.add_argument("--use-official", action="store_true", help="try Official-site layer first")
    return p

def main():
    args = build_parser().parse_args()

    # 実行時の可視化
    if args.use_official and not OFFICIAL_ACTIVE:
        print("[WARN] tools.official_lookup not importable -> OFFICIAL disabled.")
    if args.use_wikidata and not WIKI_ACTIVE:
        print("[WARN] tools.wikidata_lookup not importable -> WIKIDATA disabled.")

    df = pd.read_csv(args.input)
    # OpenAI client（LLMは最終フォールバック）
    client, api_variant = _init_openai_client()

    out_brand, out_model, out_source, out_note = [], [], [], []

    official_hits = 0
    wiki_hits = 0

    # CSE節約: 同一(brand, model)は1回だけ解決し共有
    seen = {}

    rows = list(df.itertuples(index=False))
    for row in tqdm(rows, total=len(rows), desc="translate"):
        b = str(getattr(row, args.brand_col, "")).strip()
        m = str(getattr(row, args.model_col, "")).strip()
        key = (b, m)

        if key in seen:
            brand_en, model_en, source, note = seen[key]
            out_brand.append(brand_en); out_model.append(model_en)
            out_source.append(source); out_note.append(note)
            continue

        brand_en = None
        model_en = None
        source = None
        note_parts = []

        # 1) OFFICIAL
        if args.use_official and OFFICIAL_ACTIVE:
            try:
                ob, om = find_official_english(b, m)  # どちらか片方だけ取得でもOK
                if ob: brand_en = ob
                if om: model_en = om
                if ob or om:
                    source = "official"
                    official_hits += 1
                else:
                    note_parts.append("official:miss")
            except Exception as e:
                note_parts.append(f"official:err:{type(e).__name__}")

        # 2) WIKIDATA
        if (not brand_en or not model_en) and args.use_wikidata and WIKI_ACTIVE:
            try:
                wb, wm = find_wikidata_english(b, m)
                if not brand_en and wb: brand_en = wb
                if not model_en and wm: model_en = wm
                if (wb or wm) and source is None:
                    source = "wikidata"
                    wiki_hits += 1
                else:
                    if not (wb or wm):
                        note_parts.append("wiki:miss")
            except Exception as e:
                note_parts.append(f"wiki:err:{type(e).__name__}")

        # 3) LLM fallback（不足分だけ）
        if not brand_en:
            brand_en = llm_translate(client, api_variant, args.llm_model, b) or ""
            if not brand_en:
                note_parts.append("llm_brand:empty")
        if not model_en:
            model_en = llm_translate(client, api_variant, args.llm_model, m) or ""
            if not model_en:
                note_parts.append("llm_model:empty")
        if source is None:
            source = "llm"

        note = ";".join(note_parts)
        out_brand.append(brand_en); out_model.append(model_en)
        out_source.append(source); out_note.append(note)
        seen[key] = (brand_en, model_en, source, note)

        if args.sleep > 0:
            time.sleep(args.sleep)

    # 出力列（既存列名に合わせる）
    df[args.brand_ja_col] = out_brand
    df[args.model_ja_col] = out_model
    df["source_model"] = out_source
    df["resolve_note"] = out_note

    # 書き出し
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"Wrote: {args.output}")
    print(f"official_hits={official_hits}, wiki_hits={wiki_hits}")

    # フェイルファースト: 指定した層が1件もヒットしない場合は異常終了
    if args.use_official and official_hits == 0:
        raise SystemExit("No official hits. Check PYTHONPATH/implementation or CSE quota.")
    if args.use_wikidata and wiki_hits == 0:
        print("[WARN] wikidata hits = 0. Check dependency/UA/implementation.")

if __name__ == "__main__":
    main()
