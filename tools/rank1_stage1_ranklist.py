#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# PlaywrightでスクロールしてHTMLを丸ごと取得する（既存インターフェイス踏襲）
from playwright.sync_api import sync_playwright

NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(?P<json>{.+?})</script>',
    re.DOTALL
)

# "EV19916  PHEV4437" のようなテキストから抽出
EV_PHEV_RE = re.compile(r'EV\s*(\d+).{0,5}PHEV\s*(\d+)', re.IGNORECASE)

def fetch_html_with_playwright(url: str, wait_ms: int, max_scrolls: int) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded")
        # スクロールで後半が出るレイジーロード対策
        last_height = 0
        for _ in range(max_scrolls):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(wait_ms / 1000.0)
            height = page.evaluate("document.body.scrollHeight")
            if height == last_height:
                break
            last_height = height
        html = page.content()
        ctx.close()
        browser.close()
        return html

def find_all_series_items(obj: Any) -> List[Dict[str, Any]]:
    """
    __NEXT_DATA__ の深いツリーから
    rank/seriesid/seriesname/count/rankchange/rcmdesc 等を持つ配列を見つけて返す。
    """
    found = []

    def walk(x: Any):
        if isinstance(x, list):
            for it in x:
                walk(it)
        elif isinstance(x, dict):
            # 候補: rankNum と seriesid を同時に持つ辞書
            keys = set(x.keys())
            if {"rankNum", "seriesid"} <= keys:
                found.append(x)
            # 再帰
            for v in x.values():
                walk(v)

    walk(obj)
    return found

def extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    m = NEXT_DATA_RE.search(html)
    if not m:
        return None
    try:
        return json.loads(m.group("json"))
    except json.JSONDecodeError:
        return None

def parse_rank_list_from_next(next_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    NEXT_DATA からランキング配列を抽出。
    """
    items = find_all_series_items(next_data)
    results = []
    for it in items:
        try:
            seriesid = it.get("seriesid")
            seriesname = it.get("seriesname")
            rank = it.get("rankNum")
            # 総台数
            count = it.get("count") or it.get("saleCount") or it.get("salecount")
            # ランク変動
            rank_change = it.get("rankchange") or it.get("rankChange")
            # EV/PHEVの内訳説明（あれば）
            rcmdesc = it.get("rcmdesc") or it.get("rcmDesc") or ""
            ev_count = phev_count = ""
            if rcmdesc:
                m = EV_PHEV_RE.search(rcmdesc.replace("\u3000", " ").replace("&nbsp;", " "))
                if m:
                    ev_count, phev_count = m.group(1), m.group(2)

            if seriesid and seriesname and rank:
                results.append({
                    "rank_seq": int(rank),
                    "rank": int(rank),
                    "seriesname": seriesname,
                    "series_url": f"https://www.autohome.com.cn/{seriesid}/",
                    "count": str(count) if count is not None else "",
                    "rank_change": str(rank_change) if rank_change is not None else "",
                    "ev_count": ev_count,
                    "phev_count": phev_count,
                })
        except Exception:
            continue

    # rank順に並べて上位50だけ
    results.sort(key=lambda r: r["rank_seq"])
    return results[:50]

def write_base_csv(rows: List[Dict[str, Any]], out_csv: Path):
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "rank_seq","rank","seriesname","series_url",
        "brand","model","brand_conf","series_conf",
        "title_raw","count","ev_count","phev_count","rank_change"
    ]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({
                "rank_seq": r.get("rank_seq",""),
                "rank": r.get("rank",""),
                "seriesname": r.get("seriesname",""),
                "series_url": r.get("series_url",""),
                "brand": "",
                "model": "",
                "brand_conf": "0.0",
                "series_conf": "0.0",
                "title_raw": "",
                "count": r.get("count",""),
                "ev_count": r.get("ev_count",""),
                "phev_count": r.get("phev_count",""),
                "rank_change": r.get("rank_change",""),
            })

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--wait-ms", type=int, default=200)
    ap.add_argument("--max-scrolls", type=int, default=200)
    args = ap.parse_args()

    html = fetch_html_with_playwright(args.url, args.wait_ms, args.max_scrolls)
    next_data = extract_next_data(html)
    if not next_data:
        print("[warn] __NEXT_DATA__ not found; no rows")
        rows = []
    else:
        rows = parse_rank_list_from_next(next_data)

    write_base_csv(rows, Path(args.out))
    print(f"[ok] base rows={len(rows)} -> {args.out}")

if __name__ == "__main__":
    sys.exit(main() or 0)
