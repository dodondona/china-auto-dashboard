#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, csv, json, re, sys, time
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright

# __NEXT_DATA__ をより寛容に拾う
NEXT_DATA_RE = re.compile(
    r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.*?\})\s*</script>',
    re.DOTALL | re.IGNORECASE
)

# 「EV19916  PHEV4437」や「纯电19916 插电4437」なども拾えるよう幅広に
EV_PHEV_RE = re.compile(
    r'(?:EV|纯电|纯电动)\s*[:：]?\s*(\d+)[^\d]{0,8}(?:PHEV|插电|插电混动|插电式混合动力)\s*[:：]?\s*(\d+)',
    re.IGNORECASE
)

def fetch_html_with_playwright(url: str, wait_ms: int, max_scrolls: int) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        last_h = 0
        for _ in range(max_scrolls):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(wait_ms / 1000.0)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h
        html = page.content()
        ctx.close(); browser.close()
        return html

def extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    m = NEXT_DATA_RE.search(html)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None

def _walk(obj: Any):
    if isinstance(obj, dict):
        for v in obj.values():
            yield from _walk(v)
        yield obj
    elif isinstance(obj, list):
        for it in obj:
            yield from _walk(it)

def parse_rank_list_from_next(next_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for node in _walk(next_data):
        if not isinstance(node, dict): 
            continue
        # 最低限の鍵が揃うものを候補化
        if "seriesid" in node and ("rankNum" in node or "ranknum" in node):
            sid = node.get("seriesid")
            name = node.get("seriesname") or node.get("seriesName")
            rank = node.get("rankNum") or node.get("ranknum")
            cnt = node.get("count") or node.get("saleCount") or node.get("salecount")
            chg = node.get("rankchange") or node.get("rankChange")
            rcmdesc = node.get("rcmdesc") or node.get("rcmDesc") or ""
            ev_cnt = phev_cnt = ""
            if isinstance(rcmdesc, str) and rcmdesc:
                m = EV_PHEV_RE.search(rcmdesc.replace("\u3000"," ").replace("&nbsp;"," "))
                if m: ev_cnt, phev_cnt = m.group(1), m.group(2)
            if sid and name and rank:
                try:
                    items.append({
                        "rank_seq": int(rank),
                        "rank": int(rank),
                        "seriesname": str(name),
                        "series_url": f"https://www.autohome.com.cn/{sid}/",
                        "count": str(cnt) if cnt is not None else "",
                        "rank_change": str(chg) if chg is not None else "",
                        "ev_count": ev_cnt,
                        "phev_count": phev_cnt,
                    })
                except Exception:
                    continue
    items.sort(key=lambda r: r["rank_seq"])
    return items[:50]

def write_base_csv(rows: List[Dict[str, Any]], out_csv: Path):
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "rank_seq","rank","seriesname","series_url",
        "brand","model","brand_conf","series_conf",
        "title_raw","count","ev_count","phev_count","rank_change"
    ]
    with out_csv.open("w", newline="", encoding="utf-8-sig") as f:  # ← Excel対策
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
    rows = parse_rank_list_from_next(next_data) if next_data else []
    write_base_csv(rows, Path(args.out))
    print(f"[ok] base rows={len(rows)} -> {args.out}")

if __name__ == "__main__":
    sys.exit(main() or 0)
