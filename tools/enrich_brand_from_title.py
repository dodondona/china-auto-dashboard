#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
enrich_brand_from_title.py
Autohome の series ページ <title> を取得し、
LLM で brand / series 名を抽出して CSV に追記する。

- API キーは環境変数 OPENAI_API_KEY を使用（GitHub Secrets を想定）
- 変更点（最小）:
  * 出力列に 'model' を追加
  * model は series から brand を除去→分割→末尾トークンを採用
  * 「银河星愿」のようにサブブランド「银河」が先頭残りの場合は除去
"""

import os
import re
import csv
import time
import json
import random
import argparse
from typing import Dict, Any, List, Tuple

import requests
from tqdm import tqdm
from openai import OpenAI

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/120.0.0.0 Safari/537.36")

PROMPT = """あなたは中国の自動車情報サイトの<title>文字列から、ブランド名（brand）と車系名（series）を正確に抽出するアシスタントです。
入力は例: 「【比亚迪】宋Pro 2024款 …… - 汽车之家」など。
出力は必ず JSON 形式で、スキーマは次の通り:
{"brand": "...", "series": "...", "confidence": {"brand": 0.0-1.0, "series": 0.0-1.0}}

ルール:
- 「汽车之家」などサイト名は無視。
- ブランドは BYD/比亚迪、长安、上汽大众、广汽丰田 等（中英混在可）。シリーズは具体的な車系名称（宋Pro、汉、UNI-K 等）。
- 雑多な装飾語（报价、图片、配置 等）は除外。
- brand/series が曖昧なら、合理的に推定し、confidence を下げて返す。
"""

def get_title(url: str) -> str:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=15)
        r.encoding = r.apparent_encoding or "utf-8"
        m = re.search(r"<title>(.*?)</title>", r.text, re.S | re.I)
        return m.group(1).strip() if m else ""
    except Exception:
        return ""

def parse_by_regex(title: str) -> Tuple[str, str]:
    """フォールバック用の素朴な抽出。"""
    if not title:
        return "未知", "未知"
    # よくあるサイト接尾辞を除去
    t = re.sub(r"\s*[-–—\|｜]\s*汽车之家.*$", "", title)
    # 先頭2トークンを brand / series とみなす簡易規則
    m = re.search(r"^([^\s\-\|_【】]+)[\s_【】]+([^\s\-\|_【】]+)", t)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return "未知", t.strip() or "未知"

def extract_by_llm(title: str, model: str, client: OpenAI) -> Dict[str, Any]:
    if not title:
        return {"brand": "未知", "series": "未知", "confidence": {"brand": 0, "series": 0}}
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": PROMPT},
                {"role": "user", "content": title},
            ],
            max_tokens=180,
        )
        return json.loads(resp.choices[0].message.content)
    except Exception:
        b, s = parse_by_regex(title)
        return {"brand": b, "series": s, "confidence": {"brand": 0.5, "series": 0.5}}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")
    ap.add_argument("--conf-threshold", type=float, default=0.7)
    ap.add_argument("--url-col", default="series_url")
    args = ap.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        print("OPENAI_API_KEY が未設定です。")
        return

    with open(args.input, "r", encoding="utf-8-sig") as f:
        rows: List[Dict[str, str]] = list(csv.DictReader(f))
    if not rows:
        print("入力CSVが空です。")
        return

    # 出力フィールド（既存列を保ちつつ強化）
    fields = list(rows[0].keys())
    for col in ["brand", "series", "model", "brand_conf", "series_conf", "title_raw"]:
        if col not in fields:
            fields.append(col)

    client = OpenAI()
    out: List[Dict[str, str]] = []

    for r in tqdm(rows, desc="enrich brand/series by LLM"):
        url = r.get(args.url_col, "")
        if not url:
            out.append(r)
            continue

        title = get_title(url)

        # まず LLM で brand/series
        result = extract_by_llm(title, args.model, client)
        brand = result.get("brand", "未知") if isinstance(result, dict) else "未知"
        series = result.get("series", "未知") if isinstance(result, dict) else "未知"
        conf = result.get("confidence", {}) if isinstance(result, dict) else {}
        cb = float(conf.get("brand", 0) or 0)
        cs = float(conf.get("series", 0) or 0)

        # 低信頼なら簡易正規表現で補完（未知のみ上書き）
        if cb < args.conf_threshold or cs < args.conf_threshold:
            rb, rs = parse_by_regex(title)
            if brand == "未知" and rb != "未知":
                brand = rb
            if series == "未知" and rs != "未知":
                series = rs

        # === model 生成（ここが今回の最小修正）===
        # 1) series から先頭 brand を除去
        s = series
        if brand and s:
            s = re.sub(rf"^{re.escape(brand)}[\s_]*", "", s)

        # 2) 分割（空白・アンダー・中黒・ハイフン）→ 末尾トークンを採用
        parts = [p for p in re.split(r"[\s_·\-]+", s or "") if p]
        if len(parts) >= 2:
            model_val = parts[-1]
        else:
            model_val = s or series

        # 3) サブブランド「银河」先頭残りを安全に除去（例: 银河星愿 → 星愿）
        if model_val.startswith("银河") and len(model_val) > 2:
            model_val = model_val[2:]
        model_val = model_val.strip()

        r2 = dict(r)
        r2.update({
            "brand": brand,
            "series": series,
            "model": model_val,
            "brand_conf": f"{cb:.2f}",
            "series_conf": f"{cs:.2f}",
            "title_raw": title
        })
        out.append(r2)
        time.sleep(random.uniform(0.03, 0.08))  # 軽いレート調整

    with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(out)

    print(f"✅ brand/series/model 追記完了: {args.output}")

if __name__ == "__main__":
    main()
