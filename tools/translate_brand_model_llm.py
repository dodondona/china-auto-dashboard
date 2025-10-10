#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
translate_brand_model_llm.py

- 入力CSVの brand / model をグローバル名へ正規化
- 公式（Google CSE経由; tools/official_lookup.py）→ LLM → 既存値 の順で決定
- キャッシュの汚染（説明文混入・日本語/中国語混入）を自動で弾く
- 出力列: brand_ja, model_ja は従来通り。加えて model_official_en, source_model を追加（既存列は壊さない）
- YAMLは変更不要（CSEのキーは環境変数 GOOGLE_API_KEY / GOOGLE_CSE_ID から official_lookup が取得）
"""

import os
import re
import json
import time
import argparse
from typing import Dict, Any

import pandas as pd
from tqdm import tqdm

# ========== optional: OpenAI (LLM) ==========
OPENAI_AVAILABLE = False
try:
    import openai  # openai>=1.0 系
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except Exception:
    OPENAI_AVAILABLE = False

# ========== optional: official lookup (CSE) ==========
def _noop_brand(brand_raw: str, brand_guess: str) -> str:
    return brand_guess or brand_raw

def _noop_model(brand_en: str, model_raw: str, model_guess: str) -> str:
    return model_guess or model_raw

try:
    from tools.official_lookup import (
        official_brand_name_if_needed,
        official_model_name_if_needed,
    )
except Exception:
    try:
        import sys
        sys.path.append(os.path.dirname(__file__))
        from official_lookup import (
            official_brand_name_if_needed,
            official_model_name_if_needed,
        )
    except Exception:
        official_brand_name_if_needed = _noop_brand
        official_model_name_if_needed = _noop_model


# ========== ユーティリティ ==========
def _is_clean_ascii_name(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    if len(s) > 40:
        return False
    if re.search(r"[\u3040-\u30ff\u4e00-\u9fff\u3400-\u4dbf]", s):
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 .\-+/&]*", s))

def _shrink_brand(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s
    m = re.search(r"\b([A-Z][A-Za-z.-]{1,20}(?:\s+[A-Z][A-Za-z.-]{1,20})?)\b", s)
    if m:
        return m.group(1).strip()
    head = re.split(r"[、，。,(（,]\s*|\s{2,}", s, maxsplit=1)[0].strip()
    return head

def _normalize_model_loose(s: str) -> str:
    if not s:
        return s
    s = s.strip()
    s = re.sub(
        r"^(BYD|Geely(?: Galaxy)?|Wuling|Chery|Changan(?: Qiyuan)?|Haval|Hongqi|Leapmotor|XPeng|Xiaomi(?: Auto)?|Toyota|Nissan|Volkswagen|Honda|Audi|Buick|Mercedes\-Benz|BMW)\s+",
        "",
        s,
        flags=re.I,
    )
    s = re.sub(r"(?:逸动|逸動)", "Eado", s)
    s = re.sub(r"カローラ\s*クロス", "Corolla Cross", s, flags=re.I)
    s = re.sub(r"Sealion\s*0?\s*6(?:\s*New\s*Energy)?", "Sealion 06", s, flags=re.I)
    s = re.sub(r"Seal\s*0?\s*6(?:\s*New\s*Energy)?", "Seal 06", s, flags=re.I)
    if re.fullmatch(r"Seal\s*0?\s*5", s, flags=re.I):
        s = "Seal 05 DM-i"
    s = re.sub(r"(?:海豹\s*0?\s*5).*", "Seal 05 DM-i", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def _load_cache(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {"_version": 2, "brand": {}, "model": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"_version": 2, "brand": {}, "model": {}}
        if "_version" not in data or "brand" not in data or "model" not in data:
            return {"_version": 2, "brand": {}, "model": {}}
        return data
    except Exception:
        return {"_version": 2, "brand": {}, "model": {}}

def _save_cache(path: str, data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ========== LLM 補完 ==========
def _llm_client():
    api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("OPENAI_APIKEY")
    if not api_key:
        return None
    try:
        client = OpenAI(api_key=api_key)
        return client
    except Exception:
        return None

def llm_translate_brand_model(brand_cn: str, model_cn: str, llm_model: str) -> Dict[str, str]:
    out = {"brand_en": "", "model_en": ""}
    if not (OPENAI_AVAILABLE and llm_model and _llm_client()):
        return out
    client = _llm_client()
    if client is None:
        return out

    system = (
        "You are a precise automotive name normalizer. "
        "Return official global English names ONLY, not translations. "
        "Do not add extra words. Output strict JSON with keys brand_en and model_en."
    )
    user = (
        f"Brand (Chinese): {brand_cn}\n"
        f"Model/Series (Chinese): {model_cn}\n"
        "Rules:\n"
        "- If the brand has an established global English brand, return that (e.g., 比亚迪 -> BYD, 吉利 -> Geely, 吉利银河 -> Geely Galaxy, 五菱汽车 -> Wuling).\n"
        "- For BYD ocean/animal series, prefer official names (e.g., 海豚 -> Dolphin, 海豹06 -> Seal 06, 海狮06 -> Sealion 06).\n"
        "- For Volkswagen China-only models: 朗逸 -> Lavida, 速腾 -> Sagitar, 迈腾 -> Magotan, 途岳 -> Tharu, 探岳 -> Tayron.\n"
        "- For Toyota Corolla Cross in CN: 卡罗拉锐放 -> Corolla Cross.\n"
        "- For 长安 逸动 -> Eado.\n"
        "- If unknown, return the pinyin or commonly used export English name used by OEM; do not translate literally.\n"
        "- Do not include Chinese or Japanese characters in the output.\n"
        "- Respond JSON only."
    )
    try:
        rsp = client.chat.completions.create(
            model=llm_model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        txt = rsp.choices[0].message.content
        data = json.loads(txt)
        be = (data.get("brand_en") or "").strip()
        me = (data.get("model_en") or "").strip()
        if _is_clean_ascii_name(be):
            out["brand_en"] = be
        if _is_clean_ascii_name(me):
            out["model_en"] = _normalize_model_loose(me)
    except Exception:
        pass
    return out


# ========== メイン ==========
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="input CSV path")
    p.add_argument("--output", required=True, help="output CSV path")
    p.add_argument("--brand-col", dest="brand_col", default="brand")
    p.add_argument("--model-col", dest="model_col", default="model")
    p.add_argument("--brand-ja-col", dest="brand_ja_col", default="brand_ja")
    p.add_argument("--model-ja-col", dest="model_ja_col", default="model_ja")
    p.add_argument("--cache", default=".cache/global_map.json")
    p.add_argument("--sleep", type=float, default=0.1)
    # YAMLで渡される LLM モデル指定（--model-col とは別物）
    p.add_argument("--model", dest="llm_model", default="", help="LLM model name (e.g., gpt-4o)")
    # --- 追加：YAML互換のために受け付けるだけのダミーフラグ ---
    p.add_argument("--use-wikidata", action="store_true", help="(ignored for backward compatibility)")
    p.add_argument("--use-official", action="store_true", help="(ignored for backward compatibility)")
    return p.parse_args()

def main():
    args = parse_args()
    inp = args.input
    out = args.output

    print(f"Translating: {inp} -> {out}")

    df = pd.read_csv(inp)
    for col in [args.brand_col, args.model_col]:
        if col not in df.columns:
            raise RuntimeError(
                f"Input must contain '{args.brand_col}' and '{args.model_col}'. columns={list(df.columns)}"
            )

    if args.brand_ja_col not in df.columns:
        df[args.brand_ja_col] = ""
    if args.model_ja_col not in df.columns:
        df[args.model_ja_col] = ""
    if "model_official_en" not in df.columns:
        df["model_official_en"] = ""
    if "source_model" not in df.columns:
        df["source_model"] = ""

    cache = _load_cache(args.cache)
    brand_cache: Dict[str, str] = cache.get("brand", {})
    model_cache: Dict[str, str] = cache.get("model", {})

    brands = pd.Series(df[args.brand_col].astype(str).fillna("")).unique().tolist()
    brand_map: Dict[str, str] = {}

    print("\nbrand:")
    for b in tqdm(brands):
        key = b.strip()
        if not key:
            brand_map[b] = ""
            continue

        cached = brand_cache.get(key)
        if not _is_clean_ascii_name(cached):
            cached = None

        if cached:
            brand_map[b] = cached
            continue

        guess = ""
        if _is_clean_ascii_name(b):
            guess = b
        guess = _shrink_brand(guess or b)

        brand_fixed = official_brand_name_if_needed(b, guess)
        if not _is_clean_ascii_name(brand_fixed):
            if args.llm_model:
                got = llm_translate_brand_model(b, "", args.llm_model)
                be = got.get("brand_en", "")
                if _is_clean_ascii_name(be):
                    brand_fixed = be

        brand_fixed = _shrink_brand(brand_fixed)
        brand_map[b] = brand_fixed

        if _is_clean_ascii_name(brand_fixed):
            brand_cache[key] = brand_fixed

        time.sleep(args.sleep)

    df[args.brand_ja_col] = df[args.brand_col].astype(str).map(lambda x: brand_map.get(x, x))

    print("\nmodel:")
    for idx, row in tqdm(df.iterrows(), total=len(df)):
        brand_raw = str(row[args.brand_col] or "")
        model_raw = str(row[args.model_col] or "")

        brand_en = str(row[args.brand_ja_col] or "").strip()
        if not _is_clean_ascii_name(brand_en):
            brand_en = _shrink_brand(brand_en or brand_raw)

        current_model = str(row.get(args.model_ja_col, "") or "").strip()
        if not current_model:
            current_model = model_raw
        current_model = _normalize_model_loose(current_model)

        mc_key = f"{brand_raw}|{model_raw}"
        cached_model = model_cache.get(mc_key)
        if not _is_clean_ascii_name(cached_model):
            cached_model = None

        decided_model = None
        decided_src = None
        model_official = ""

        if cached_model:
            decided_model = cached_model
            decided_src = "cache"
        else:
            try_official = official_model_name_if_needed(brand_en, model_raw, current_model)
            if _is_clean_ascii_name(try_official):
                model_official = _normalize_model_loose(try_official)
                decided_model = model_official
                decided_src = "official"
            else:
                if args.llm_model:
                    got = llm_translate_brand_model(brand_raw, model_raw, args.llm_model)
                    me = got.get("model_en", "")
                    if _is_clean_ascii_name(me):
                        decided_model = _normalize_model_loose(me)
                        decided_src = "llm"

            if not decided_model:
                decided_model = current_model
                decided_src = decided_src or "current"

            if _is_clean_ascii_name(decided_model):
                model_cache[mc_key] = decided_model

        df.at[idx, args.model_ja_col] = decided_model
        if model_official and _is_clean_ascii_name(model_official):
            df.at[idx, "model_official_en"] = model_official
        else:
            df.at[idx, "model_official_en"] = decided_model if _is_clean_ascii_name(decided_model) else ""
        df.at[idx, "source_model"] = decided_src

        if args.sleep:
            time.sleep(args.sleep / 10.0)

    cache["brand"] = brand_cache
    cache["model"] = model_cache
    _save_cache(args.cache, cache)

    df.to_csv(out, index=False, encoding="utf-8")
    print(f"Wrote: {out}")


if __name__ == "__main__":
    main()
