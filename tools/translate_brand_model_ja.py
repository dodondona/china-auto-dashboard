#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLMで中国名→グローバル英名へ正規化するスクリプト（辞書極小）
- ブランド/モデルをユニーク抽出してLLMにバッチ問い合わせ
- 返答はJSONのみ（{"map": {...}}）を強制
- 英数字（Latin）のものはそのまま
- グローバル英名が存在しない/不確実なら原文のまま（作り話を禁止）
- brand_ja / model_ja に結果を書き出す（既存カラムは保持）

例:
  python tools/translate_brand_model_llm.py \
    --input data/autohome_raw_2025-09_with_brand.csv \
    --output data/autohome_raw_2025-09_with_brand_ja.csv \
    --brand-col brand --model-col model \
    --model gpt-4o-mini --cache .cache/global_map.json
"""

import argparse, json, os, re, time, sys
from typing import Dict, List
import pandas as pd

LATIN_RE = re.compile(r"^[A-Za-z0-9\s\-\+\/\.\(\)]+$")

DEFAULT_MODEL = "gpt-4o-mini"
BATCH = 60
RETRY = 3
SLEEP = 1.0

PROMPT_BRAND = (
    "You normalize CAR BRAND names used in Mainland China to their established GLOBAL English brand names.\n"
    "Return ONLY a JSON object: {\"map\": {\"<input>\": \"<globalEnglishOrSame>\", ...}}\n"
    "- Keep items that are already Latin unchanged.\n"
    "- If there is no widely established global English brand name, return the original Chinese (do NOT invent).\n"
    "- No explanations or extra text; JSON only."
)

PROMPT_MODEL = (
    "You normalize CAR MODEL names sold in China to their widely used GLOBAL English model names when such exist.\n"
    "Return ONLY a JSON object: {\"map\": {\"<input>\": \"<globalEnglishOrSame>\", ...}}\n"
    "- Keep items that are already Latin (e.g., \"Model Y\", \"A6L\") unchanged.\n"
    "- If a brand fragment is prepended in Chinese (e.g., 本田CR-V), remove the brand part and keep the model (\"CR-V\").\n"
    "- Preserve meaningful suffixes that are part of the marketed name (e.g., PLUS / Pro / DM-i) only when appropriate.\n"
    "- If no established global English model exists, return the original Chinese (do NOT invent).\n"
    "- No explanations or extra text; JSON only."
)

def is_latin(x: str) -> bool:
    return isinstance(x, str) and LATIN_RE.match(x.strip() or "")

def load_cache(path: str) -> Dict[str, Dict[str, str]]:
    if not path or not os.path.isfile(path):
        return {"brand": {}, "model": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"brand": {}, "model": {}}

def save_cache(path: str, cache: Dict[str, Dict[str, str]]):
    if not path:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def call_llm(items: List[str], prompt: str, model: str) -> Dict[str, str]:
    from openai import OpenAI
    client = OpenAI()
    user = prompt + "\nInput list (JSON array):\n" + json.dumps(items, ensure_ascii=False)

    for attempt in range(RETRY):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "Reply with strict JSON only. No prose."},
                    {"role": "user", "content": user},
                ],
                temperature=0,
                response_format={"type": "json_object"},
            )
            txt = resp.choices[0].message.content.strip()
            obj = json.loads(txt)
            mp = obj.get("map", {})
            # 未返答は原文にフォールバック
            return {x: mp.get(x, x) for x in items}
        except Exception:
            if attempt == RETRY - 1:
                raise
            time.sleep(SLEEP * (attempt + 1))
    return {x: x for x in items}

def chunked(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--brand-col", default="brand")
    ap.add_argument("--model-col", default="model")
    ap.add_argument("--brand-ja-col", default="brand_ja")
    ap.add_argument("--model-ja-col", default="model_ja")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--cache", default=".cache/global_map.json")
    args = ap.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY is not set.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(args.input)
    if args.brand_col not in df.columns or args.model_col not in df.columns:
        raise RuntimeError(f"Input must contain '{args.brand_col}' and '{args.model_col}' columns. got={list(df.columns)}")

    cache = load_cache(args.cache)

    # ---- brand
    brands_all = [str(x) for x in df[args.brand_col].dropna().tolist()]
    uniq_brand = sorted(set(brands_all))
    need_brand = [b for b in uniq_brand if not is_latin(b) and b not in cache["brand"]]
    brand_map = dict(cache["brand"])
    for batch in chunked(need_brand, BATCH):
        brand_map.update(call_llm(batch, PROMPT_BRAND, args.model))
        cache["brand"] = brand_map; save_cache(args.cache, cache)
    for b in uniq_brand:
        if is_latin(b) and b not in brand_map:
            brand_map[b] = b

    # ---- model
    models_all = [str(x) for x in df[args.model_col].dropna().tolist()]
    uniq_model = sorted(set(models_all))
    need_model = [m for m in uniq_model if not is_latin(m) and m not in cache["model"]]
    model_map = dict(cache["model"])
    for batch in chunked(need_model, BATCH):
        model_map.update(call_llm(batch, PROMPT_MODEL, args.model))
        cache["model"] = model_map; save_cache(args.cache, cache)
    for m in uniq_model:
        if is_latin(m) and m not in model_map:
            model_map[m] = m

    # ---- apply
    df[args.brand_ja_col] = df[args.brand_col].map(lambda x: brand_map.get(str(x), str(x)))
    df[args.model_ja_col] = df[args.model_col].map(lambda x: model_map.get(str(x), str(x)))

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[OK] LLM-normalized: {args.input} -> {args.output} (rows={len(df)})")

if __name__ == "__main__":
    main()
