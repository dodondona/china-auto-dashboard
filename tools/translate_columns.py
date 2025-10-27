#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, sys, json, math
import pandas as pd
from pathlib import Path
from openai import OpenAI

# =========================================================
# 設定
# =========================================================
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_KEY = os.environ.get("OPENAI_API_KEY", "")
client = OpenAI(api_key=OPENAI_KEY)

# キャッシュ再利用を完全停止
ENABLE_CACHE = False

# 日本円換算レート
JPY_RATE = 20.0  # 仮値：1元=20円。環境変数で上書き可
try:
    JPY_RATE = float(os.environ.get("JPY_RATE", "20"))
except:
    pass

# ノイズ文字列
NOISE_PRICE_TAIL = [
    "询价", "计算器", "询底价", "报价", "价格询问", "起", "起售", "到店", "经销商", "計算機"
]

# =========================================================
# ユーティリティ
# =========================================================
def clean_price_cell(val: str) -> str:
    if not val:
        return ""
    s = str(val)
    for kw in NOISE_PRICE_TAIL:
        s = s.replace(kw, "")
    s = re.sub(r"\s+", "", s)
    return s.strip()

def yuan_to_jpy_str(val: str) -> str:
    m = re.match(r"([\d\.]+)\s*万", val)
    if not m:
        return val
    try:
        yuan = float(m.group(1)) * 10000
        jpy = int(yuan * JPY_RATE)
        return f"{m.group(1)}万元（約¥{jpy:,}）"
    except:
        return val

def cut_before_year_or_kuan(name: str) -> str:
    if not name:
        return name
    m = re.search(r"(\d{4}款.*)", name)
    return m.group(1) if m else name

# =========================================================
# 翻訳関数
# =========================================================
def translate_text(text: str) -> str:
    if not text or text.strip() == "":
        return text
    try:
        rsp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "你是日中翻译专家。准确自然地翻译为日语。"},
                {"role": "user", "content": text},
            ],
        )
        return rsp.choices[0].message.content.strip()
    except Exception as e:
        print(f"[warn] translation failed for '{text}': {e}", file=sys.stderr)
        return text

# =========================================================
# メイン処理
# =========================================================
def main():
    csv_in = os.environ.get("CSV_IN", "").strip()
    csv_out = os.environ.get("CSV_OUT", "").strip()

    if not csv_in:
        print("CSV_INが指定されていません", file=sys.stderr)
        sys.exit(1)

    if not Path(csv_in).exists():
        print(f"入力ファイルが存在しません: {csv_in}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(csv_in)

    # 翻訳列を追加
    df["セクション_ja"] = ""
    df["項目_ja"] = ""

    for i, row in df.iterrows():
        sec = str(row.get("セクション", "")).strip()
        itm = str(row.get("項目", "")).strip()
        val = str(row.get("値", "")).strip() if "値" in df.columns else ""

        # セクション・項目翻訳
        df.at[i, "セクション_ja"] = translate_text(sec)
        df.at[i, "項目_ja"] = translate_text(itm)

        # 値が価格らしい場合
        if "価格" in itm or "价" in itm or "售" in itm:
            val = clean_price_cell(val)
            if "万" in val:
                val = yuan_to_jpy_str(val)
            df.at[i, "値"] = val

    # 先頭列名翻訳（例：奔驰E级 2025款 改款 E 260 L → 2025款 改款 E 260 Lのみ翻訳）
    if len(df.columns) > 2:
        cols = list(df.columns)
        first_col = cols[2]
        trimmed = cut_before_year_or_kuan(first_col)
        translated = translate_text(trimmed)
        if translated:
            new_cols = cols.copy()
            new_cols[2] = translated
            df.columns = new_cols

    # 保存
    if not csv_out:
        csv_out = str(Path(csv_in).with_suffix(".ja.csv"))

    Path(csv_out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"✅ 出力完了: {csv_out}")

if __name__ == "__main__":
    main()
