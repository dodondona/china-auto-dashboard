#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Translate/normalize columns & values for Autohome config CSV.

使い方:
    python tools/translate_columns.py output/autohome/5714/config_5714.csv

挙動:
- 引数で受け取った CSV が存在しなければ
    "Skip translate: <path> not found." と出して 0 終了（ジョブを落とさない）
- 既定の日本語見出しへ変換:
    "厂商指导价" -> "メーカー希望小売価格"
    "经销商报价"/"经销商参考价"/"经销商价" -> "ディーラー販売価格（元）"
    "model_name" -> "モデル名"
    "spec_id" -> "仕様ID"
- 値の整形:
    - MSRP（元データが "11.98万" 等）を
        "11.98万元（日本円X,XXX,XXX円）" に整形（為替: 環境 EXRATE_CNY_TO_JPY、既定 21.0）
    - ディーラー価格は常に「…元」表記（円は付けない）。"询价/价格咨询" などのノイズ除去。
- メーカー名の日本語化:
    - 列 "manufacturer" があれば "manufacturer_ja" を追加（辞書優先、なければそのまま）
    - OPENAI_API_KEY が設定され、辞書未ヒットの場合のみ LLM で補完（安全にオフ可）
- 出力:
    入力ファイルの隣に <basename>.ja.csv を生成（例: config_5714.csv → config_5714.ja.csv）
"""

from __future__ import annotations

import csv
import os
import re
import sys
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Tuple, Optional

# ---- 為替など可変パラメータ --------------------------------------
EXRATE_CNY_TO_JPY = Decimal(os.getenv("EXRATE_CNY_TO_JPY", "21.0"))  # 1元あたりの円
ROUNDING = os.getenv("JPY_ROUND", "1")  # 1円単位で丸め
# ------------------------------------------------------------------

# 既知メーカーの簡易辞書（必要に応じて拡張）
MAKER_JA: Dict[str, str] = {
    "比亚迪": "BYD",
    "BYD": "BYD",
    "上汽大众": "上汽-フォルクスワーゲン",
    "一汽大众": "一汽-フォルクスワーゲン",
    "长安汽车": "長安汽車",
    "吉利汽车": "吉利汽車",
    "广汽本田": "広汽ホンダ",
    "东风本田": "東風ホンダ",
    "东风日产": "東風日産",
    "广汽丰田": "広汽トヨタ",
    "一汽丰田": "一汽トヨタ",
    "小米": "小米汽車",
    "蔚来": "NIO",
    "理想": "Li Auto",
    "小鹏": "Xpeng",
}

# 見出しマップ
HEADER_MAP = {
    "spec_id": "仕様ID",
    "model_name": "モデル名",
    "厂商指导价": "メーカー希望小売価格",
    "经销商报价": "ディーラー販売価格（元）",
    "经销商参考价": "ディーラー販売価格（元）",
    "经销商价": "ディーラー販売価格（元）",
    "manufacturer": "manufacturer",  # オリジナルも保持（右に _ja を追加）
}

NOISE_PATTERNS = [
    r"价格?咨询", r"询价", r"暂无报价", r"--", r"—", r"─", r"－", r"无", r"^$",
]

def jpy_format(n: Decimal) -> str:
    s = f"{int(n):,}"
    return f"{s}円"

def clean_text(v: str) -> str:
    if not v:
        return ""
    v2 = v.strip()
    for pat in NOISE_PATTERNS:
        v2 = re.sub(pat, "", v2, flags=re.IGNORECASE)
    return v2.strip()

def parse_price_to_yuan(value: str) -> Optional[Decimal]:
    """
    "11.98万" -> 119800
    "9.88-12.98万" -> 9.88万 を採用（最小値）
    "15.38万元" -> 153800
    "129800元" -> 129800
    """
    if not value:
        return None
    s = value.replace(",", "").replace("　", "").strip()
    # レンジは先頭側を採用
    s = re.split(r"[～\-~–to至]", s)[0].strip()

    # 万元表記
    m = re.search(r"(\d+(?:\.\d+)?)\s*万", s)
    if m:
        return (Decimal(m.group(1)) * Decimal(10000)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)

    # 元表記
    m = re.search(r"(\d{1,9})(?:\.\d+)?\s*元", s)
    if m:
        return Decimal(m.group(1)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)

    # 純数字のみ（元）とみなす
    m = re.fullmatch(r"\d{1,9}", s)
    if m:
        return Decimal(m.group(0))

    return None

def format_msrp(value: str) -> str:
    """
    MSRP列の正規化:
      - 表示は常に「<X.XX>万元（日本円<#,###,###円>）」に統一
      - 入力が元単位でも “万元” に直してから表記
    """
    raw = clean_text(value)
    if not raw:
        return ""

    yuan = parse_price_to_yuan(raw)
    if yuan is None:
        # 解釈できなければそのまま返す（安全第一）
        return raw

    wan = (yuan / Decimal(10000)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    jpy = (yuan * EXRATE_CNY_TO_JPY).quantize(Decimal(ROUNDING), rounding=ROUND_HALF_UP)
    return f"{wan}万元（日本円{jpy_format(jpy)}）"

def format_dealer_price(value: str) -> str:
    """
    ディーラー価格の正規化:
      - ノイズ語を除去
      - 常に「...元」表記（円は付けない）
      - レンジは先頭側を採用
    """
    raw = clean_text(value)
    if not raw:
        return ""
    yuan = parse_price_to_yuan(raw)
    if yuan is None:
        # “面议”等は空にする
        return ""
    return f"{int(yuan):,}元"

def translate_header(h: str) -> str:
    return HEADER_MAP.get(h, h)

def translate_manufacturer_ja(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    if v in MAKER_JA:
        return MAKER_JA[v]
    # OpenAI補完（任意）
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return v  # キー未設定ならそのまま返す（オフでも動作させる）
    try:
        # 遅延 import（未使用なら依存不要）
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=api_key)
        prompt = f"次の中国の自動車メーカー名を日本語の一般的な表記に変換してください。略称は英字ブランドを優先（例: 比亚迪→BYD）。\n\nメーカー: {v}\n\n出力は変換後の名前のみ。"
        resp = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        text = (resp.choices[0].message.content or "").strip()
        if text:
            return text.splitlines()[0].strip()
    except Exception:
        pass
    return v

def process_file(input_path: str) -> int:
    if not os.path.exists(input_path):
        print(f"Skip translate: {input_path} not found.")
        return 0

    out_path = re.sub(r"\.csv$", ".ja.csv", input_path)
    rows_in: List[Dict[str, str]] = []

    with open(input_path, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        fieldnames_in = rdr.fieldnames or []
        for r in rdr:
            rows_in.append({k: (r.get(k) or "").strip() for k in fieldnames_in})

    # ヘッダ変換準備
    fieldnames_out: List[str] = []
    for h in (rows_in[0].keys() if rows_in else []):
        fieldnames_out.append(translate_header(h))

    # manufacturer_ja の列を追加（manufacturer列がある場合のみ右隣）
    if "manufacturer" in (rows_in[0].keys() if rows_in else []):
        idx = fieldnames_out.index("manufacturer")
        fieldnames_out.insert(idx + 1, "manufacturer_ja")

    # 値変換
    rows_out: List[Dict[str, str]] = []
    for r in rows_in:
        o: Dict[str, str] = {}
        # 先に全コピー（元ヘッダ名で）
        for k, v in r.items():
            o[translate_header(k)] = v

        # MSRP
        for k_cn in ("厂商指导价",):
            if translate_header(k_cn) in o:
                o[translate_header(k_cn)] = format_msrp(r.get(k_cn, ""))

        # ディーラー価格（どれが来ても1列に集約されている前提）
        for k_cn in ("经销商报价", "经销商参考价", "经销商价"):
            if k_cn in r and r.get(k_cn):
                o["ディーラー販売価格（元）"] = format_dealer_price(r.get(k_cn, ""))

        # manufacturer_ja
        if "manufacturer" in r:
            o["manufacturer_ja"] = translate_manufacturer_ja(r.get("manufacturer", ""))

        rows_out.append(o)

    # 書き出し
    if not rows_out:
        # 空行でもヘッダは出す
        if not fieldnames_out:
            fieldnames_out = ["仕様ID", "モデル名", "メーカー希望小売価格", "ディーラー販売価格（元）"]
        with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames_out)
            w.writeheader()
        print(f"Wrote (empty): {out_path}")
        return 0

    # フィールド名は rows_out から再構成（manufacturer_ja を含む）
    fieldnames_final: List[str] = []
    for h in rows_out[0].keys():
        if h not in fieldnames_final:
            fieldnames_final.append(h)

    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames_final)
        w.writeheader()
        w.writerows(rows_out)

    print(f"Wrote: {out_path}")
    return 0

def main() -> None:
    if len(sys.argv) < 2:
        print("usage: translate_columns.py <input_csv_path>")
        sys.exit(0)
    sys.exit(process_file(sys.argv[1]))

if __name__ == "__main__":
    main()
