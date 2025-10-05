#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
enrich_brand_from_title.py
Autohomeのseriesページ<title>を取得し、
LLMでbrand / series名を抽出してCSVに追記する。

- 既存CSVは読み取り専用（別名で保存）
- APIキーはSecretsのOPENAI_API_KEYを自動使用
"""

import os, re, csv, time, json, random, argparse, requests
from typing import Dict, Any, List, Tuple
from tqdm import tqdm
from openai import OpenAI

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

PROMPT = """あなたは<title>文字列から「ブランド名」と「車系名」を抽出します。
推測は禁止。不明な場合は'未知'。UTF-8 JSON一行で返答。

例:
1) "〖星愿〗吉利银河_星愿报价_星愿图片_汽车之家" → brand="吉利银河", series="星愿"
2) "〖宏光MINIEV〗五菱汽车_宏光MINIEV报价_宏光MINIEV图片_汽车之家" → brand="五菱汽车", series="宏光MINIEV"
3) "吉利银河 星愿 参数配置 | 汽车之家" → brand="吉利银河", series="星愿"

返答スキーマ:
{"brand": "...", "series": "...", "confidence": {"brand": 0.0-1.0, "series": 0.0-1.0}}
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
    if not title:
        return "未知", "未知"
    m1 = re.search(r'〖([^〗]+)〗', title)
    series = m1.group(1) if m1 else None
    m2 = re.search(r'〗([^_]+)_', title) if m1 else None
    brand = m2.group(1) if m2 else None
    if not brand and not series:
        m3 = re.search(r"^([^\s\-\|_]+)\s+([^\s\-\|_]+)", title)
        if m3:
            brand, series = m3.group(1), m3.group(2)
    return brand or "未知", series or "未知"

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
            max_tokens=150,
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
    args = ap.parse_args()

    rows = []
    with open(args.input, "r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print("入力CSVが空です。"); return

    fields = list(rows[0].keys())
    for col in ["brand", "series", "brand_conf", "series_conf", "title_raw"]:
        if col not in fields:
            fields.append(col)

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    out = []

    for r in tqdm(rows, desc="enrich"):
        url = (r.get("series_url") or "").strip()
        title = get_title(url)
        data = extract_by_llm(title, args.model, client)
        b, s = data.get("brand", "未知"), data.get("series", "未知")
        cb, cs = data.get("confidence", {}).get("brand", 0), data.get("confidence", {}).get("series", 0)
        if cb < args.conf_threshold: b = "未知"
        if cs < args.conf_threshold: s = "未知"
        r2 = dict(r)
        r2.update({
            "brand": b, "series": s,
            "brand_conf": f"{cb:.2f}", "series_conf": f"{cs:.2f}",
            "title_raw": title
        })
        out.append(r2)
        time.sleep(random.uniform(0.03, 0.08))

    with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(out)

    print(f"✅ brand/series追記完了: {args.output}")

if __name__ == "__main__":
    main()
