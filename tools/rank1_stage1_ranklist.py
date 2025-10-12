#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Autohome rank/1（車系月销量榜）上位50件をベースCSVに出力。
この段階では各シリーズURL等のリンクと、行テキストから取れる情報のみを保存する。
(タイトルやエネルギー種別は第2段階で各シリーズページから取得)

出力列:
rank_seq,rank,seriesname,series_url,count,ev_count,phev_count,price,rank_change
"""

import argparse
import csv
import os
import re
from typing import List, Dict, Any
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, Browser, Page

ABS_BASE = "https://www.autohome.com.cn"

def _abs_url(u: str) -> str:
    if not u: return ""
    u = u.strip()
    if u.startswith("//"):  return "https:" + u
    if u.startswith("/"):   return ABS_BASE + u
    if u.startswith("http"):return u
    return urljoin(ABS_BASE + "/", u)

def _wait_rank_list_ready(page: Page, wait_ms: int, max_scrolls: int):
    # 初期ロードを広めに待つ（GH Actionsの遅延対策）
    page.goto(target_url, wait_until="networkidle")
    page.wait_for_timeout(8000)
    # 末尾まで2段階スクロールし、JSによる20位以降の挿入を待つ
    last_cnt = -1
    for i in range(max_scrolls):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(wait_ms)
        cnt = page.evaluate("() => document.querySelectorAll('[data-rank-num]').length")
        if cnt >= 50: break
        if cnt == last_cnt and i > 10: break
        last_cnt = cnt

COUNT_RE_GENERIC = re.compile(r"(\d{1,3}(?:,\d{3})+|\d{4,6})")
PRICE_RE = re.compile(r"(?:指导价|售价|厂商指导价)[:：]?\s*([0-9]+(?:\.[0-9]+)?(?:\s*-\s*[0-9]+(?:\.[0-9]+)?)?)\s*万")
EV_PATTERNS = [
    r"(?:EV|纯电|纯电动)\s*[:：]?\s*(\d{1,3}(?:,\d{3})+|\d{4,6})\s*辆?",
    r"(\d{1,3}(?:,\d{3})+|\d{4,6})\s*辆?\s*(?:EV|纯电|纯电动)",
]
PHEV_PATTERNS = [
    r"(?:PHEV|插电|插混|DM-?i|DMI|插电混合)\s*[:：]?\s*(\d{1,3}(?:,\d{3})+|\d{4,6})\s*辆?",
    r"(\d{1,3}(?:,\d{3})+|\d{4,6})\s*辆?\s*(?:PHEV|插电|插混|DM-?i|DMI|插电混合)",
]
ARROW_CHANGE_RE = re.compile(r"(↑|↓)\s*(\d+)")
HOLD_PAT = re.compile(r"(持平|平|—|-)")

def _max_number(text: str) -> str:
    best, best_val = "", -1
    for m in COUNT_RE_GENERIC.findall(text.replace("\u00A0"," ")):
        v = int(m.replace(",", ""))
        if v > best_val:
            best_val, best = v, str(v)
    return best

def _pick_price(text: str) -> str:
    m = PRICE_RE.search(text.replace("\u00A0", " "))
    return (m.group(1) + "万") if m else ""

def _pick_first_number_by_patterns(text: str, patterns: List[str]) -> str:
    t = text.replace("\u00A0", " ")
    for pat in patterns:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if m:
            num = m.group(1) if m.lastindex else None
            if num:
                return str(int(num.replace(",", "")))
    return ""

def _series_name_from_row(row_el) -> str:
    for sel in [".tw-text-lg", ".tw-font-medium", ".rank-name", ".main-title"]:
        el = row_el.query_selector(sel)
        if el:
            s = (el.inner_text() or "").strip()
            if s: return s
    a = row_el.query_selector("a")
    if a:
        s = (a.inner_text() or "").strip()
        if s and "查成交价" not in s:
            return s.splitlines()[0].strip()
    return ""

def _series_id_from_row(row_el) -> str:
    btn = row_el.query_selector("button[data-series-id]")
    if btn:
        sid = (btn.get_attribute("data-series-id") or "").strip()
        if sid.isdigit(): return sid
    a = row_el.query_selector('a[href^="/series/"][href$=".html"]')
    if a:
        href = a.get_attribute("href") or ""
        m = re.search(r"/series/(\d+)\.html", href)
        if m: return m.group(1)
    a2 = row_el.query_selector("a[href]")
    if a2:
        href = a2.get_attribute("href") or ""
        m = re.search(r"/(\d+)/?$", href)
        if m: return m.group(1)
    return ""

def _rank_change_from_row(row_el) -> str:
    t = (row_el.inner_text() or "").strip()
    m = ARROW_CHANGE_RE.search(t)
    if m: return f"{'+' if m.group(1)=='↑' else '-'}{m.group(2)}"
    if HOLD_PAT.search(t): return "0"

    for el in row_el.query_selector_all("*"):
        for attr in ("title","aria-label","data-tip","data-title"):
            v = el.get_attribute(attr)
            if not v: continue
            m = ARROW_CHANGE_RE.search(v)
            if m: return f"{'+' if m.group(1)=='↑' else '-'}{m.group(2)}"
            if HOLD_PAT.search(v): return "0"

    html = (row_el.inner_html() or "")
    up_m = re.search(r"(?:↑|icon[-_ ]?up|rise)[^0-9]{0,12}(\d+)", html, flags=re.IGNORECASE)
    if up_m: return f"+{up_m.group(1)}"
    down_m = re.search(r"(?:↓|icon[-_ ]?down|fall|drop)[^0-9]{0,12}(\d+)", html, flags=re.IGNORECASE)
    if down_m: return f"-{down_m.group(1)}"
    if re.search(r"(持平|平|—|-)", html): return "0"
    return ""

def collect_rank_rows(page: Page, topk: int = 50) -> List[Dict[str, Any]]:
    data = []
    for el in page.query_selector_all("[data-rank-num]")[:topk]:
        rank_str = (el.get_attribute("data-rank-num") or "").strip()
        try:
            rank = int(rank_str) if rank_str.isdigit() else len(data) + 1
        except Exception:
            rank = len(data) + 1

        name = _series_name_from_row(el)
        sid = _series_id_from_row(el)
        url = f"{ABS_BASE}/{sid}" if sid else ""
        txt = (el.inner_text() or "").strip()

        row = {
            "rank": rank,
            "seriesname": name,
            "series_url": url,
            "count": _max_number(txt),
            "price": _pick_price(txt),
            "rank_change": _rank_change_from_row(el),
            "ev_count": _pick_first_number_by_patterns(txt, EV_PATTERNS),
            "phev_count": _pick_first_number_by_patterns(txt, PHEV_PATTERNS),
        }
        data.append(row)
    data.sort(key=lambda r: r["rank"])
    return data[:topk]

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--wait-ms", type=int, default=220)
    ap.add_argument("--max-scrolls", type=int, default=220)
    args = ap.parse_args()

    global target_url
    target_url = args.url

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = browser.new_context(user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                                              "Chrome/124.0.0.0 Safari/537.36"))
        page = ctx.new_page()
        page.set_default_timeout(45000)  # 45s
        _wait_rank_list_ready(page, args.wait_ms, args.max_scrolls)
        rows = collect_rank_rows(page, topk=50)

        with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=[
                "rank_seq","rank","seriesname","series_url","count","ev_count","phev_count","price","rank_change"
            ])
            w.writeheader()
            for i, r in enumerate(rows, start=1):
                w.writerow({
                    "rank_seq": i,
                    "rank": r["rank"],
                    "seriesname": r["seriesname"],
                    "series_url": r["series_url"],
                    "count": r["count"],
                    "ev_count": r["ev_count"],
                    "phev_count": r["phev_count"],
                    "price": r["price"],
                    "rank_change": r["rank_change"],
                })
        print(f"[ok] rows={len(rows)} -> {args.out}")
        ctx.close(); browser.close()
