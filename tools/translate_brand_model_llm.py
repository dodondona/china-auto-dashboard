#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
translate_brand_model_llm.py
ブランド名・車種名の翻訳を行い、日本語またはグローバル名を付与する。
（LLM翻訳の前に、公式サイトからの英字名を優先的に取得）

Usage:
  python translate_brand_model_llm.py \
    --input data/autohome_raw_2025-08_with_brand.csv \
    --output data/autohome_raw_2025-08_with_brand_ja.csv \
    --brand-col brand --model-col model \
    --brand-ja-col brand_ja --model-ja-col model_ja \
    --model gpt-4o \
    --cache .cache/global_map.json
"""

import os, sys, csv, json, time, argparse
from tqdm import tqdm
from openai import OpenAI

# tools ディレクトリをパスに追加（ModuleNotFoundError対策）
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# 公式サイト英字取得モジュール
from tools.official_lookup import find_official_english


def read_csv(path):
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fieldnames):
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_cache(path):
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cache(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def translate_with_llm(client, model, text, prompt=""):
    if not text:
        return ""
    try:
        rsp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": text}
            ],
            temperature=0
        )
        return rsp.choices[0].message.content.strip()
    except Exception:
        return ""


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
    ap.add_argument("--model", default="gpt-4o")
    # ▼ 互換性のために追加（挙動は変えずに無視）
    ap.add_argument("--use-wikidata", action="store_true")
    ap.add_argument("--use-official", action="store_true")
    args = ap.parse_args()

    rows = read_csv(args.input)
    cache = load_cache(args.cache)
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    all_brands = sorted(set(r[args.brand_col] for r in rows if r.get(args.brand_col)))
    all_models = sorted(set(r[args.model_col] for r in rows if r.get(args.model_col)))

    print(f"Translating: {args.input} -> {args.output}")

    # ブランド側：既存のLLM翻訳ロジック（そのまま）
    for key in tqdm(all_brands, desc="brand"):
        if key in cache:
            continue
        cache[key] = translate_with_llm(
            client, args.model,
            key,
            "以下の中国語の自動車メーカー名をグローバル名または日本語表記に翻訳してください。"
        )
        time.sleep(args.sleep)
        save_cache(args.cache, cache)

    # モデル側：まず公式サイトで英字名を試し、ダメならLLMへ（既存方針どおり）
    for key in tqdm(all_models, desc="model"):
        if key in cache:
            continue

        # 公式サイトから英字名を優先取得
        official_name = find_official_english("", key)
        if official_name:
            cache[key] = official_name
            save_cache(args.cache, cache)
            continue

        # 公式で決まらなければ LLM
        cache[key] = translate_with_llm(
            client, args.model,
            key,
            "以下の中国語の自動車モデル名をグローバル名または日本語表記に翻訳してください。\
             例：秦PLUS→Qin PLUS, 海豚→Dolphin, 凯美瑞→Camry。なければ簡体字を日本語漢字にしてください。"
        )
        time.sleep(args.sleep)
        save_cache(args.cache, cache)

    # 書き出し
    out = []
    for r in rows:
        brand = r.get(args.brand_col, "")
        model = r.get(args.model_col, "")
        r[args.brand_ja_col] = cache.get(brand, "")
        r[args.model_ja_col] = cache.get(model, "")
        out.append(r)

    write_csv(args.output, out, fieldnames=list(out[0].keys()))
    print(f"Wrote: {args.output}")


if __name__ == "__main__":
    main()
