#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
translate_brand_model_ja.py

中国語のブランド/車種名を「グローバル名優先 → 例外はカタカナ → それ以外は原文」
のルールで日本語列を付与する軽量スクリプト。

入出力:
  - ディレクトリ内の CSV から最新のもの（パターン指定で複数のうち最終更新が新しい）を選び、
    brand_ja / model_ja 列を追加して別名で保存します。
  - 既存列名は保持します。未知値は元の値を温存します。

想定カラム:
  - brand / model （存在しない場合は大小文字ゆらぎを吸収します）

使い方例:
  python scripts/translate_brand_model_ja.py \
    --inp data --pattern "autohome_raw_*.csv" --out-suffix "_ja"

依存:
  pandas, unidecode, opencc-python-reimplemented
"""

import argparse
import glob
import os
import re
import sys
from datetime import datetime

import pandas as pd
from unidecode import unidecode
try:
    from opencc import OpenCC
    _CC = OpenCC('s2tjp')  # 簡体→日本語向け繁体（近似）
except Exception:
    _CC = None  # なくても動く（原文のままにする）

# --- 1) ブランドの中国語 → グローバル英語名 ---
BRAND_CN_TO_GLOBAL = {
    "比亚迪": "BYD",
    "丰田": "Toyota",
    "一汽丰田": "Toyota",
    "广汽丰田": "Toyota",
    "本田": "Honda",
    "东风本田": "Honda",
    "广汽本田": "Honda",
    "日产": "Nissan",
    "东风日产": "Nissan",
    "大众": "Volkswagen",
    "上汽大众": "Volkswagen",
    "一汽-大众": "Volkswagen",
    "别克": "Buick",
    "雪佛兰": "Chevrolet",
    "宝马": "BMW",
    "奔驰": "Mercedes-Benz",
    "奥迪": "Audi",
    "保时捷": "Porsche",
    "理想": "Li Auto",
    "蔚来": "NIO",
    "小鹏": "Xpeng",
    "吉利": "Geely",
    "长安": "Changan",
    "奇瑞": "Chery",
    "红旗": "Hongqi",
    "问界": "AITO",
    "华为": "Huawei",  # 製品ブランドとして
    "极氪": "Zeekr",
    "极狐": "Arcfox",
    "腾势": "DENZA",
    "哪吒": "Nezha",
    "小米": "Xiaomi",
}

# --- 2) 車種の中国語 → グローバル英語名（最小限・安全寄り） ---
MODEL_CN_TO_GLOBAL = {
    # 日系（確度高）
    "轩逸": "Sylphy",
    "卡罗拉": "Corolla",
    "凯美瑞": "Camry",
    "雅阁": "Accord",
    "思域": "Civic",
    "天籁": "Altima",
    # BYD 系（直訳英語が定着）
    "海狮": "Sea Lion",
    "海豹": "Seal",
    "海豚": "Dolphin",
    "汉": "Han",
    "秦": "Qin",
    "宋": "Song",
    "唐": "Tang",
    "元": "Yuan",
    # 汎用的に見かけるもの
    "途观": "Tiguan",
    "帕萨特": "Passat",
    "迈腾": "Magotan",
    "奥迪A4L": "A4L",
    "奥迪A6L": "A6L",
}

# --- 3) 「グローバル英語でもカタカナで出したい」例外 ---
GLOBAL_EN_TO_KATA = {
    "Sylphy": "シルフィ",
    "Accord": "アコード",
    "Camry": "カムリ",
    # 必要に応じて追加してください
}

LATIN_RE = re.compile(r"^[A-Za-z0-9\s\-\+\/\.]+$")


def prefer_global_brand(brand_raw: str) -> str:
    """ブランドはグローバル英語優先、なければ原文（必要なら簡→日繁変換）。"""
    if not isinstance(brand_raw, str):
        return brand_raw
    brand_raw = brand_raw.strip()
    if brand_raw in BRAND_CN_TO_GLOBAL:
        return BRAND_CN_TO_GLOBAL[brand_raw]
    # すでにラテン文字ならそのまま
    if LATIN_RE.match(brand_raw):
        return brand_raw
    # 可能なら簡→日繁（厳密ではないため“参考程度”）
    if _CC:
        try:
            return _CC.convert(brand_raw)
        except Exception:
            pass
    return brand_raw


def prefer_global_model(model_raw: str) -> str:
    """
    モデルは:
      1) 中国語→グローバル英語に変換できればそれを採用
      2) ただし例外はカタカナに置換（Sylphy/Accord/Camry 等）
      3) 変換できなければ、ラテン文字はそのまま / それ以外は原文（必要なら簡→日繁）
    """
    if not isinstance(model_raw, str):
        return model_raw
    text = model_raw.strip()
    if text in MODEL_CN_TO_GLOBAL:
        en = MODEL_CN_TO_GLOBAL[text]
        return GLOBAL_EN_TO_KATA.get(en, en)

    if LATIN_RE.match(text):
        # 例: "SEAL DM-i", "Model Y" 等はそのまま
        return text

    # 未知の中国語は原文維持（安全）
    if _CC:
        try:
            return _CC.convert(text)
        except Exception:
            pass
    return text


def find_latest_csv(directory: str, pattern: str) -> str:
    paths = glob.glob(os.path.join(directory, pattern))
    if not paths:
        raise FileNotFoundError(f"No CSV matched: {os.path.join(directory, pattern)}")
    # 最終更新日時が新しいもの
    paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return paths[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inp", required=True, help="入力ディレクトリ")
    ap.add_argument("--pattern", default="*.csv", help="入力CSVのglobパターン")
    ap.add_argument("--out-suffix", default="_ja", help="出力ファイル名のサフィックス")
    args = ap.parse_args()

    src = find_latest_csv(args.inp, args.pattern)
    df = pd.read_csv(src)

    # カラム名ゆらぎ対応
    cols = {c.lower(): c for c in df.columns}
    brand_col = cols.get("brand") or cols.get("brand_cn") or "brand"
    model_col = cols.get("model") or cols.get("model_cn") or "model"

    if brand_col not in df.columns or model_col not in df.columns:
        raise RuntimeError(
            f"CSVに brand/model カラムが見つかりません: columns={list(df.columns)}"
        )

    # 変換
    df["brand_ja"] = df[brand_col].map(prefer_global_brand)
    df["model_ja"] = df[model_col].map(prefer_global_model)

    # 出力名
    base = os.path.splitext(os.path.basename(src))[0]
    out = os.path.join(os.path.dirname(src), f"{base}{args.out_suffix}.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"[OK] {src} -> {out}  (rows={len(df)})")


if __name__ == "__main__":
    main()
