#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
autohome_rank_llm_pipeline.py
- /rank/1 のHTMLをHTTPで取得（JS不要）
- 出現順で series_id を抽出（=順位）
- 各 series_url の <title> をHTTPで取得
- title を LLM (gpt-4o-mini) で解析して brand/model を抽出
- CSV: data/autohome_raw_YYYY-MM_with_brand.csv を保存

依存: requests, pandas, beautifulsoup4, openai
"""

import os
import re
import time
import json
import argparse
from datetime import datetime
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup
from openai import OpenAI

RANK_URL_DEFAULT = "https://www.autohome.com.cn/rank/1"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
HDRS = {
    "User-Agent": UA,
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

PROMPT_JSON = (
    "你将看到一个中国汽车之家车系页面的标题，请解析并输出 JSON："
    "{\"brand\":\"品牌名\",\"model\":\"车系名\"}。"
    "如果无法判断则使用空字符串。只输出 JSON，不要多余文字。"
)

def fetch_html(url: str, retries: int = 4, timeout: int = 20) -> str:
    last_exc = None
    for i in range(retries):
        try:
            resp = requests.get(url, headers=HDRS, timeout=timeout)
            # 一部ページは GBK/GB2312。requestsの自動判定だと崩れるので手当て。
            enc = resp.apparent_encoding or "utf-8"
            resp.encoding = enc
            if resp.status_code == 200 and resp.text:
                return resp.text
        except Exception as e:
            last_exc = e
        time.sleep(1.2 + i * 0.8)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"Failed to fetch: {url}")

def extract_series_ids(html: str, max_items: int = 60) -> list[str]:
    """data-series-id 優先 → hrefフォールバック。出現順のユニーク。"""
    ids = re.findall(r'data-series-id\s*=\s*"(\d+)"', html)
    if not ids:
        # //www.autohome.com.cn/1234/ または https://... の両方に対応
        ids = re.findall(r'href="(?:https:)?//www\.autohome\.com\.cn/(\d{3,7})/?[^"]*"', html, flags=re.I)
    uniq = []
    seen = set()
    for sid in ids:
        if sid not in seen:
            seen.add(sid)
            uniq.append(sid)
    return uniq[:max_items]

def extract_counts_heuristic(html: str, series_ids: list[str]) -> dict[str, int|None]:
    """行の近傍から '车系销量' の数字を拾う（オプション）。なければ None。"""
    res = {}
    for sid in series_ids:
        idx = html.find(sid)
        val = None
        if idx != -1:
            chunk = html[max(0, idx - 800): idx + 800]
            m = re.search(r'(\d{4,6})\s*车系销量', chunk)
            if m:
                try:
                    val = int(m.group(1))
                except Exception:
                    val = None
        res[sid] = val
    return res

def fetch_title(url: str) -> str:
    try:
        html = fetch_html(url, retries=3, timeout=15)
        soup = BeautifulSoup(html, "html.parser")
        # 通常 <title>…</title>
        t = soup.title.string.strip() if soup.title and soup.title.string else ""
        if not t:
            # 予備：<meta property="og:title"> 等
            m = soup.find("meta", attrs={"property": "og:title"}) or soup.find("meta", attrs={"name": "title"})
            if m and m.get("content"):
                t = m["content"].strip()
        return t
    except Exception:
        return ""

def llm_brand_model(client: OpenAI, title: str, model_name: str = "gpt-4o-mini") -> tuple[str, str]:
    if not title:
        return ("", "")
    try:
        resp = client.chat.completions.create(
            model=model_name,
            temperature=0,
            max_tokens=120,
            messages=[
                {"role": "system", "content": PROMPT_JSON},
                {"role": "user", "content": title},
            ],
        )
        text = (resp.choices[0].message.content or "").strip()
        # JSONだけ抽出（保険）
        m = re.search(r"\{.*\}", text, re.S)
        data = json.loads(m.group(0)) if m else {}
        brand = (data.get("brand") or "").strip()
        model = (data.get("model") or "").strip()
        return (brand, model)
    except Exception:
        return ("", "")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", default=RANK_URL_DEFAULT)
    ap.add_argument("--output", default=f"data/autohome_raw_{datetime.now():%Y-%m}_with_brand.csv")
    ap.add_argument("--max-items", type=int, default=60)
    ap.add_argument("--model", default="gpt-4o-mini")
    args = ap.parse_args()

    Path("data").mkdir(parents=True, exist_ok=True)
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # 1) rankページHTML
    print(f"🌐 GET {args.rank_url}")
    rank_html = fetch_html(args.rank_url)
    # デバッグ保存（何かあった時に見返せる）
    Path("data/_rankpage_debug.html").write_text(rank_html, encoding="utf-8", errors="ignore")

    # 2) 出現順で series_id を抽出（= 順位）
    sids = extract_series_ids(rank_html, max_items=args.max_items)
    if not sids:
        raise SystemExit("❌ series_id を検出できませんでした（WAF/構造変更の可能性）")

    counts = extract_counts_heuristic(rank_html, sids)

    # 3) 各 series_url の <title> を取得
    rows = []
    for i, sid in enumerate(sids, start=1):
        url = f"https://www.autohome.com.cn/{sid}/"
        title = fetch_title(url)
        # 4) LLMで brand / model を解析
        brand, model = llm_brand_model(client, title, model_name=args.model)
        rows.append({
            "rank": i,
            "series_url": url,
            "title": title,
            "brand": brand,
            "model": model,
            "count": counts.get(sid)
        })
        # サイト負荷軽減
        time.sleep(0.25)

    df = pd.DataFrame(rows).sort_values("rank").reset_index(drop=True)
    out = Path(args.output)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"✅ Saved: {out} ({len(df)} rows)")

if __name__ == "__main__":
    main()
