# tools/translate_brand_model_llm.py
# 公式検索(CSE)を先に試し、ダメなら“現在の値”をゆるく正規化して使う最小差分版。
# 注意: キャッシュは一切使いません（--cache 引数は受けるが無視します）。
from __future__ import annotations
import os
import re
import sys
import time
import json
import argparse
from typing import Dict, Any
import pandas as pd
from tqdm import tqdm

# --- add: safe import for official lookup (CSE) ---
try:
    from tools.official_lookup import (
        official_brand_name_if_needed,
        official_model_name_if_needed,
    )
except Exception:
    sys.path.append(os.path.dirname(__file__))
    from official_lookup import (
        official_brand_name_if_needed,
        official_model_name_if_needed,
    )
# -----------------------------------------------

ASCII_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 .\-+/&]*$")

def _is_clean_ascii_name(s: str) -> bool:
    if not s: return False
    s = s.strip()
    if len(s) > 40: return False
    if re.search(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff]", s):
        return False
    return bool(ASCII_NAME.fullmatch(s))

def _shrink_brand(s: str) -> str:
    if not s: return s
    s = s.strip()
    m = re.search(r"\b([A-Z][A-Za-z.-]{1,20}(?:\s+[A-Z][A-Za-z.-]{1,20})?)\b", s)
    if m:
        return m.group(1).strip()
    head = re.split(r"[、，。,(（,]\s*|\s{2,}", s, maxsplit=1)[0].strip()
    return head

def _normalize_model_loose(s: str) -> str:
    if not s: return s
    s = s.strip()
    # 先頭ブランドを剥がす（一般形）
    s = re.sub(r"^(BYD|Geely(?: Galaxy)?|Wuling|Chery|Changan(?: Qiyuan)?|Haval|Hongqi|Leapmotor|XPeng|Xiaomi(?: Auto)?|Toyota|Nissan|Volkswagen|Honda|Audi|Buick|Mercedes\-Benz|BMW)\s+", "", s, flags=re.I)
    # 代表的な揺れ
    s = re.sub(r"(?:逸动|逸動)", "Eado", s)
    s = re.sub(r"カローラ\s*クロス", "Corolla Cross", s, flags=re.I)
    s = re.sub(r"Sealion\s*0?\s*6(?:\s*New\s*Energy)?", "Sealion 06", s, flags=re.I)
    s = re.sub(r"Seal\s*0?\s*6(?:\s*New\s*Energy)?", "Seal 06", s, flags=re.I)
    if re.fullmatch(r"Seal\s*0?\s*5", s, flags=re.I):
        s = "Seal 05 DM-i"
    s = re.sub(r"(?:海豹\s*0?\s*5).*", "Seal 05 DM-i", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="input CSV path")
    p.add_argument("--output", required=True, help="output CSV path")
    p.add_argument("--brand-col", default="brand")
    p.add_argument("--model-col", default="model")
    p.add_argument("--brand-ja-col", default="brand_ja")
    p.add_argument("--model-ja-col", default="model_ja")
    p.add_argument("--cache", default=".cache/global_map.json")  # 受けるだけ（無視）
    p.add_argument("--sleep", type=float, default=0.1)
    p.add_argument("--model", default="gpt-4o")  # 受けるだけ（LLMは今回未使用）
    return p.parse_args()

def main():
    args = parse_args()
    if not os.path.exists(args.input):
        raise FileNotFoundError(args.input)

    df = pd.read_csv(args.input)
    # 出力列の存在を担保
    if args.brand_ja_col not in df.columns:
        df[args.brand_ja_col] = ""
    if args.model_ja_col not in df.columns:
        df[args.model_ja_col] = ""
    if "model_official_en" not in df.columns:
        df["model_official_en"] = ""
    if "source_model" not in df.columns:
        df["source_model"] = ""

    # --- ブランドを先に一括処理（ユニーク） ---
    brand_map: Dict[str, str] = {}
    brands = pd.Series(df[args.brand_col].fillna("")).unique().tolist()
    for b in tqdm(brands, desc="brand"):
        b_raw = str(b or "")
        cur = ""  # 現列は無視してCSE優先でもOKだが、残っていれば使う
        fixed = official_brand_name_if_needed(b_raw, cur)
        if not _is_clean_ascii_name(fixed):
            fixed = _shrink_brand(fixed or b_raw)
        brand_map[b_raw] = fixed

    # --- 行ごとにモデルを処理 ---
    out_rows = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="model"):
        row_out = row.to_dict()
        brand_raw = str(row.get(args.brand_col, "") or "")
        model_raw = str(row.get(args.model_col, "") or "")

        # ブランド確定
        brand_en = brand_map.get(brand_raw, "")
        if not _is_clean_ascii_name(brand_en):
            brand_en = official_brand_name_if_needed(brand_raw, brand_en)
            if not _is_clean_ascii_name(brand_en):
                brand_en = _shrink_brand(brand_en or brand_raw)

        row_out[args.brand_ja_col] = brand_en

        # 既存の model_ja をゆるく整形（無ければ原語）
        model_now = str(row.get(args.model_ja_col, "") or model_raw)
        model_now = _normalize_model_loose(model_now)

        # 公式CSE で補強
        model_official = official_model_name_if_needed(brand_en, model_raw, model_now)

        # 公式の方が自然なら採用
        if _is_clean_ascii_name(model_official) and len(model_official) <= max(12, len(model_now) + 6):
            final_model = model_official
            source = "official"
        else:
            final_model = model_now
            source = "llm" if _is_clean_ascii_name(model_now) else "raw"

        row_out[args.model_ja_col] = final_model
        row_out["model_official_en"] = final_model if source == "official" else ""
        row_out["source_model"] = source

        out_rows.append(row_out)

        # レート制御（CSEクォータ対策）
        if args.sleep and args.sleep > 0:
            time.sleep(args.sleep)

    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"Wrote: {args.output}")

if __name__ == "__main__":
    main()
