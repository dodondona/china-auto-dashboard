#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_from_web.py

Autohome ランキング実ページ（https://www.autohome.com.cn/rank/1）を直接取得し、
埋め込みの "seriesid" / "seriesname" から各車種の series_url を生成。
既存CSVの末尾に 'series_url' を追記して別名保存します。

- Playwright不要（requestsのみ）
- "seriesid":"7806","seriesname":"星愿" を優先抽出
- 保険として seriesid=#### / /####/ パターンでも拾う
- CSVとの対応付けは ①名前突合（完全→部分）→ ②順番埋め
"""

import re
import csv
import sys
import argparse
import time
from typing import List, Dict, Tuple, Optional

import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://www.autohome.com.cn/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

# ---------- 正規表現 ----------
RE_SERIES_PAIR = re.compile(r'"seriesid"\s*:\s*"(\d{3,7})"\s*,\s*"seriesname"\s*:\s*"([^"]+)"')
RE_SERIES_APP  = re.compile(r'seriesid\s*=\s*(\d{3,7})', re.I)
RE_SERIES_PATH = re.compile(r'/(\d{3,7})(?:/|[?#]|\")')

def normalize_name(s: str) -> str:
    return re.sub(r'\s+', '', (s or '')).lower()

def to_series_url(series_id: str) -> str:
    return f"https://www.autohome.com.cn/{series_id}/"

def fetch_rank_page(url: str, timeout: int = 15) -> str:
    # 軽いリトライ
    for i in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
            r.encoding = r.apparent_encoding or "utf-8"
            if r.status_code == 200 and ("seriesid" in r.text or "/rank/" in url):
                return r.text
        except Exception:
            pass
        time.sleep(0.8 * (i + 1))
    raise RuntimeError("ランキングページの取得に失敗しました。ネットワークやブロックを確認してください。")

def extract_pairs_from_html(html: str) -> List[Tuple[str, str]]:
    """
    (series_id, series_name) をページ出現順で返す。
    まず "seriesid"/"seriesname" のペア、無ければ seriesid= / /####/ で補完。
    """
    pairs: List[Tuple[str, str]] = []

    for sid, sname in RE_SERIES_PAIR.findall(html):
        pairs.append((sid, sname))

    if not pairs:
        sids = RE_SERIES_APP.findall(html)
        if not sids:
            sids = RE_SERIES_PATH.findall(html)
        seen = set()
        for sid in sids:
            if sid not in seen:
                seen.add(sid)
                pairs.append((sid, ""))

    # 重複series_idの除去（最初の出現を採用）
    seen2 = set(); uniq: List[Tuple[str,str]] = []
    for sid, sname in pairs:
        if sid not in seen2:
            seen2.add(sid); uniq.append((sid, sname))
    return uniq

def read_csv_rows(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv_rows(path: str, rows: List[Dict[str, str]]) -> None:
    if not rows: return
    fields = list(rows[0].keys())
    if "series_url" not in fields:
        fields.append("series_url")
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)

def detect_name_col(fieldnames: List[str], preferred: Optional[str]) -> str:
    if preferred and preferred in fieldnames:
        return preferred
    for c in ["model_text", "model", "name", "car", "series_name", "title"]:
        if c in fieldnames: return c
    return fieldnames[0]

def attach_by_name_then_order(rows: List[Dict[str,str]], pairs: List[Tuple[str,str]], name_col: str) -> None:
    page_names = [normalize_name(n) for _, n in pairs]
    page_urls  = [to_series_url(sid) for sid, _ in pairs]

    used = set()

    # 1) 名前一致（完全→部分）
    for i, row in enumerate(rows):
        name = normalize_name(row.get(name_col, ""))
        url = ""
        if name:
            for j, pn in enumerate(page_names):
                if j in used: continue
                if pn and pn == name:
                    url = page_urls[j]; used.add(j); break
            if not url:
                for j, pn in enumerate(page_names):
                    if j in used: continue
                    if pn and (pn in name or name in pn):
                        url = page_urls[j]; used.add(j); break
        rows[i]["series_url"] = url  # 空のままでもセット

    # 2) 残りは順序で埋める
    k = 0
    for i, row in enumerate(rows):
        if row.get("series_url"): continue
        while k < len(page_urls) and k in used:
            k += 1
        if k < len(page_urls):
            rows[i]["series_url"] = page_urls[k]
            used.add(k); k += 1
        else:
            rows[i]["series_url"] = ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--name-col", default=None)
    args = ap.parse_args()

    rows = read_csv_rows(args.input)
    if not rows:
        print("入力CSVが空です。", file=sys.stderr); sys.exit(1)
    name_col = detect_name_col(list(rows[0].keys()), args.name_col)
    print(f"[*] rank-url: {args.rank-url if hasattr(args,'rank-url') else args.rank_url}")
    print(f"[*] name-col : {name_col}")

    html = fetch_rank_page(args.rank_url)
    pairs = extract_pairs_from_html(html)
    if not pairs:
        print("HTMLから series 情報を抽出できませんでした。", file=sys.stderr); sys.exit(2)

    attach_by_name_then_order(rows, pairs, name_col)
    write_csv_rows(args.output, rows)
    print(f"✔ 出力: {args.output} （{len(rows)}行 / 取得series {len(pairs)}件）")

if __name__ == "__main__":
    main()
