#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
brand/model のグローバル名を決定するパイプライン。
優先順位: 1) 公式サイト(CSE) → 2) Wikipedia → 3) LLM
キャッシュはデフォルト未使用。

出力列:
- brand_ja : ブランド名の標準化（基本は英語表記。中国語ブランドは英語社名。既存値があれば尊重）
- model_ja : シリーズ/モデルの英語グローバル名（なければ中国語原文）
- model_official_en : 公式サイトから抽出できた英名（抽出できたときのみ）
- source_model : 採用ソース "official" | "wikipedia" | "llm" | "current"

CLI:
  --use-official / --no-use-official
  --use-wikipedia / --no-use-wikipedia
  --model  (LLMモデル名; 例: gpt-4o)
"""

import argparse
import csv
import os
import sys
import time
from typing import Optional, Tuple

import pandas as pd

# 公式 / Wikipedia / LLM の実装
try:
    from tools.official_lookup import find_official_english
except Exception:
    # GH Actions で import パスの都合がある場合に備え、相対でも試す
    from official_lookup import find_official_english  # type: ignore

try:
    from tools.wiki_lookup import lookup_wikipedia_en_title
except Exception:
    from wiki_lookup import lookup_wikipedia_en_title  # type: ignore

# ---- LLM (OpenAI) ----------------------------------------------------------
def llm_guess(brand: str, model: str, llm_model: str) -> Optional[Tuple[str, str]]:
    """
    LLM で英名を推定する。返り値は (brand_en, model_en) または None。
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    try:
        from openai import OpenAI
    except Exception:
        return None

    prompt = f"""You are an expert vehicle product manager.
Given the brand and Chinese market series/model below, answer the **global English names** as used by the manufacturer (not a translation).
- Brand (may be Chinese): "{brand}"
- Model/Series in Chinese market: "{model}"

Rules:
1) Output ONLY brand_en and model_en in one line as: brand_en | model_en
2) If the global brand name is the same as already given in English, keep it.
3) If you are not confident about model_en, return brand_en | {model}
"""

    try:
        client = OpenAI(api_key=api_key)
        rsp = client.chat.completions.create(
            model=llm_model,
            messages=[
                {"role": "system", "content": "Return concise English model names."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        text = rsp.choices[0].message.content.strip()
        if "|" in text:
            b, m = [x.strip() for x in text.split("|", 1)]
            return (b or brand, m or model)
    except Exception:
        pass
    return None

# ---- ブランドの素朴標準化（英語社名の既知マップ + フォールバック） ------------
_BRAND_EN_FALLBACK = {
    "比亚迪": "BYD",
    "比亞迪": "BYD",
    "上汽大众": "Volkswagen",
    "大众": "Volkswagen",
    "大眾": "Volkswagen",
    "丰田": "Toyota",
    "豐田": "Toyota",
    "日产": "Nissan",
    "日產": "Nissan",
    "本田": "Honda",
    "梅赛德斯-奔驰": "Mercedes-Benz",
    "奔驰": "Mercedes-Benz",
    "寶馬": "BMW",
    "宝马": "BMW",
    "五菱汽车": "Wuling Motors",
    "五菱": "Wuling Motors",
    "吉利汽车": "Geely",
    "吉利银河": "Geely Galaxy",
    "红旗": "Hongqi",
    "哈弗": "Haval",
    "奇瑞": "Chery",
    "小鹏": "XPeng",
    "小米汽车": "Xiaomi Auto",
    "AITO": "AITO",
    "奥迪": "Audi",
    "大众汽车": "Volkswagen",
    "长安": "Changan",
    "长安启源": "Changan Qiyuan",
    "零跑汽车": "Leapmotor",
    "别克": "Buick",
    "奔驰AMG": "Mercedes-AMG",
}

def normalize_brand_en(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return n
    # 既に英語っぽい場合はそのまま
    if any(c.isalpha() for c in n) and not any("\u4e00" <= c <= "\u9fff" for c in n):
        return n
    return _BRAND_EN_FALLBACK.get(n, n)

# ---- メイン ---------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--brand-col", default="brand")
    parser.add_argument("--model-col", default="model")
    parser.add_argument("--brand-ja-col", default="brand_ja")
    parser.add_argument("--model-ja-col", default="model_ja")
    parser.add_argument("--model", dest="llm_model", default="gpt-4o")
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--use-wikipedia", dest="use_wikipedia", action="store_true", default=False)
    parser.add_argument("--use-official", dest="use_official", action="store_true", default=False)
    # 将来用の互換（指定されても無視する）
    parser.add_argument("--cache", default=None)

    args = parser.parse_args()

    inp = args.input
    outp = args.output
    print(f"Translating: {inp} -> {outp}", flush=True)

    df = pd.read_csv(inp)

    # 出力列を準備（存在すれば上書き、なければ追加）
    for c in (args.brand_ja_col, args.model_ja_col, "model_official_en", "source_model"):
        if c not in df.columns:
            df[c] = ""

    rows = df.to_dict(orient="records")

    for i, row in enumerate(rows, start=1):
        brand = str(row.get(args.brand_col, "")).strip()
        model = str(row.get(args.model_col, "")).strip()

        brand_en = normalize_brand_en(brand)
        model_best = None
        model_source = "current"
        model_official = ""

        # 1) 公式サイト（CSE）
        if args.use_official:
            try:
                off = find_official_english(brand, model)
                if off:
                    model_best = off
                    model_official = off
                    model_source = "official"
            except Exception:
                pass

        # 2) Wikipedia
        if not model_best and args.use_wikipedia:
            try:
                wk = lookup_wikipedia_en_title(brand, model)
                if wk:
                    model_best = wk
                    model_source = "wikipedia"
            except Exception:
                pass

        # 3) LLM
        if not model_best:
            guess = llm_guess(brand, model, args.llm_model)
            if guess:
                brand_en = guess[0] or brand_en
                model_best = guess[1] or model
                model_source = "llm"

        # フォールバック
        if not brand_en:
            brand_en = brand
        if not model_best:
            model_best = model

        row[args.brand_ja_col] = brand_en
        row[args.model_ja_col] = model_best
        row["model_official_en"] = model_official
        row["source_model"] = model_source

        if args.sleep:
            time.sleep(args.sleep)

    # 書き出し
    fieldnames = list(rows[0].keys()) if rows else df.columns.tolist()
    with open(outp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote: {outp}")

if __name__ == "__main__":
    main()
