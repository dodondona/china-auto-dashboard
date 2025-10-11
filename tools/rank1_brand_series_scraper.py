#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Autohome rank/1 上位50件を CSV 出力。
- ランキング行から: seriesname / series_url / count / price / rank_change / (EV/PHEV 内訳)
- 各シリーズページから: title_raw を取得
- type_hint は行テキストからの簡易推定（EV/PHEV/EV+PHEV/Unknown）

出力列:
rank_seq,rank,seriesname,series_url,brand,model,brand_conf,series_conf,title_raw,
count,ev_count,phev_count,type_hint,price,rank_change
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
    if not u:
        return ""
    u = u.strip()
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return ABS_BASE + u
    if u.startswith("http"):
        return u
    return urljoin(ABS_BASE + "/", u)

def scroll_to_load_all(page: Page, need_rows: int = 50, wait_ms: int = 220, max_scrolls: int = 220) -> None:
    last_cnt = -1
    stagnation = 0
    for _ in range(max_scrolls):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(wait_ms)
        cnt = page.evaluate("() => document.querySelectorAll('[data-rank-num]').length")
        if cnt >= need_rows:
            break
        if cnt == last_cnt:
            stagnation += 1
        else:
            stagnation = 0
        last_cnt = cnt
        if stagnation >= 25:
            break

# ---- 抽出ユーティリティ ----

COUNT_RE_GENERIC = re.compile(r"(\d{1,3}(?:,\d{3})+|\d{4,6})")
PRICE_RE = re.compile(r"(?:指导价|售价|厂商指导价)[:：]?\s*([0-9]+(?:\.[0-9]+)?(?:\s*-\s*[0-9]+(?:\.[0-9]+)?)?)\s*万")
ARROW_CHANGE_RE = re.compile(r"(↑|↓)\s*(\d+)")
HOLD_PAT = re.compile(r"(持平|平|—|-)")

EV_PATTERNS = [
    r"(?:EV|纯电|纯电动)[^\d]{0,8}(\d{1,3}(?:,\d{3})+|\d{4,6})",
    r"(\d{1,3}(?:,\d{3})+|\d{4,6})[^\d]{0,8}(?:EV|纯电|纯电动)",
]
PHEV_PATTERNS = [
    r"(?:PHEV|插电|插混|DM-?i|DMI)[^\d]{0,8}(\d{1,3}(?:,\d{3})+|\d{4,6})",
    r"(\d{1,3}(?:,\d{3})+|\d{4,6})[^\d]{0,8}(?:PHEV|插电|插混|DM-?i|DMI)",
]

def _max_number(text: str) -> str:
    if not text: return ""
    best, best_val = "", -1
    for m in COUNT_RE_GENERIC.findall(text.replace("\u00A0", " ")):
        v = int(m.replace(",", ""))
        if v > best_val:
            best_val, best = v, str(v)
    return best

def _pick_price(text: str) -> str:
    if not text: return ""
    m = PRICE_RE.search(text.replace("\u00A0", " "))
    return (m.group(1) + "万") if m else ""

def _pick_first_number_by_patterns(text: str, patterns: List[str]) -> str:
    if not text: return ""
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
    """
    行内の rank 変動は inner_text に出ないことがある。
    そこで、行配下の要素を走査して text/title/aria-label から抽出。
    """
    # 1) まず行テキスト全体
    t = (row_el.inner_text() or "").strip()
    m = ARROW_CHANGE_RE.search(t)
    if m:
        return f"{'+' if m.group(1)=='↑' else '-'}{m.group(2)}"
    if HOLD_PAT.search(t): return "0"

    # 2) 属性に潜んでいるケース（title / aria-label）
    for el in row_el.query_selector_all("*"):
        for attr in ("title", "aria-label", "data-tip", "data-title"):
            v = el.get_attribute(attr)
            if not v: continue
            m = ARROW_CHANGE_RE.search(v)
            if m:
                return f"{'+' if m.group(1)=='↑' else '-'}{m.group(2)}"
            if HOLD_PAT.search(v):
                return "0"
        # テキストが短い span 等にも一応目を通す
        s = (el.inner_text() or "").strip()
        if s:
            m2 = ARROW_CHANGE_RE.search(s)
            if m2:
                return f"{'+' if m2.group(1)=='↑' else '-'}{m2.group(2)}"
            if HOLD_PAT.search(s):
                return "0"

    return ""  # 見つからなければ空（0 断定はしない）

def _text_of_row(row_el) -> str:
    return (row_el.inner_text() or "").strip()

def collect_rank_rows(page: Page, topk: int = 50) -> List[Dict[str, Any]]:
    row_els = page.query_selector_all("[data-rank-num]")
    out: List[Dict[str, Any]] = []
    for el in row_els[:topk]:
        rank_str = (el.get_attribute("data-rank-num") or "").strip()
        try:
            rank = int(rank_str) if rank_str.isdigit() else len(out) + 1
        except Exception:
            rank = len(out) + 1

        name = _series_name_from_row(el)
        sid = _series_id_from_row(el)
        url = f"{ABS_BASE}/{sid}" if sid else ""
        txt = _text_of_row(el)

        count = _max_number(txt)
        price = _pick_price(txt)
        change = _rank_change_from_row(el)  # ← 強化版
        ev_count   = _pick_first_number_by_patterns(txt, EV_PATTERNS)
        phev_count = _pick_first_number_by_patterns(txt, PHEV_PATTERNS)

        if ev_count and phev_count:
            t_hint = "EV+PHEV"
        elif ev_count:
            t_hint = "EV"
        elif phev_count:
            t_hint = "PHEV"
        else:
            t_hint = "Unknown"

        out.append({
            "rank": rank,
            "seriesname": name,
            "series_url": url,
            "count": count,
            "price": price,
            "rank_change": change,
            "ev_count": ev_count,
            "phev_count": phev_count,
            "type_hint": t_hint,
        })

    out.sort(key=lambda r: r["rank"])
    return out[:topk]

# ---- シリーズページから title_raw を取得 ----

def fetch_title_raw(page: Page, url: str, timeout_ms: int = 15000) -> str:
    if not url:
        return ""
    try:
        page.set_default_timeout(timeout_ms)
        page.goto(url, wait_until="domcontentloaded")
        # document.title をそのまま
        return page.title() or ""
    except Exception:
        return ""

# ---- main ----

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="例: https://www.autohome.com.cn/rank/1")
    ap.add_argument("--out", required=True, help="出力CSVパス（例: data/rank1_top50.csv）")
    ap.add_argument("--wait-ms", type=int, default=220, help="スクロール待機ms")
    ap.add_argument("--max-scrolls", type=int, default=220, help="最大スクロール回数")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
        )

        # ランキングを収集
        rank_page = context.new_page()
        rank_page.goto(args.url, wait_until="domcontentloaded")
        scroll_to_load_all(rank_page, need_rows=50, wait_ms=args.wait_ms, max_scrolls=args.max_scrolls)
        rows = collect_rank_rows(rank_page, topk=50)

        # title_raw 取得（直列でOK）
        series_page = context.new_page()
        for r in rows:
            r["title_raw"] = fetch_title_raw(series_page, r.get("series_url", ""))

        # CSV（従来カラム + 今回の rank_change 強化 & title_raw 復活）
        fieldnames = [
            "rank_seq", "rank", "seriesname", "series_url",
            "brand", "model", "brand_conf", "series_conf", "title_raw",
            "count", "ev_count", "phev_count", "type_hint", "price", "rank_change",
        ]
        with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for i, r in enumerate(rows, start=1):
                w.writerow({
                    "rank_seq": i,
                    "rank": r.get("rank", i),
                    "seriesname": r.get("seriesname", ""),
                    "series_url": r.get("series_url", ""),
                    "brand": "",
                    "model": "",
                    "brand_conf": 0.0,
                    "series_conf": 0.0,
                    "title_raw": r.get("title_raw", ""),
                    "count": r.get("count", ""),
                    "ev_count": r.get("ev_count", ""),
                    "phev_count": r.get("phev_count", ""),
                    "type_hint": r.get("type_hint", "Unknown"),
                    "price": r.get("price", ""),
                    "rank_change": r.get("rank_change", ""),
                })

        print(f"[ok] rows={len(rows)} -> {args.out}")

        series_page.close()
        context.close()
        browser.close()

if __name__ == "__main__":
    main()
