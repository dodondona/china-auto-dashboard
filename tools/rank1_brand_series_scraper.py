#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Autohome rank/1 上位50件を CSV 出力。
- ランキング行から: seriesname / series_url / count / price / rank_change / (EV/PHEV 台数内訳があれば拾う)
- 各シリーズ詳細ページへ: 「能源类型」等から種別(EV/PHEV/EREV/HEV/MHEV/ICE)を取得して補完
- title_raw はシリーズページ <title> をそのまま

出力列（既存互換＋末尾に補完列を追加）:
rank_seq,rank,seriesname,series_url,brand,model,brand_conf,series_conf,title_raw,
count,ev_count,phev_count,type_hint,price,rank_change,
series_energy_raw,type_from_page,type_final,is_ev_binary

- ev_count/phev_count … ランキング行に明示がある時のみ数値。無ければ空欄
- type_hint … ランキング行テキストからの推定（従来のまま）
- series_energy_raw … シリーズページで見つけた原文（例: 纯电动 / 插电混动 / 增程式 / 燃油 等）
- type_from_page … 上記原文を EV/PHEV/EREV/HEV/MHEV/ICE/Unknown に正規化
- type_final … page側の情報を優先（Unknown のときだけ type_hint を採用）
- is_ev_binary … EVなら1、それ以外は0（レポートで使う用、PHEV/EREV/HEV/ICEは0）
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
        stagnation = stagnation + 1 if cnt == last_cnt else 0
        last_cnt = cnt
        if stagnation >= 25:
            break

# ---------- ランキング行抽出 ----------

COUNT_RE_GENERIC = re.compile(r"(\d{1,3}(?:,\d{3})+|\d{4,6})")
PRICE_RE = re.compile(r"(?:指导价|售价|厂商指导价)[:：]?\s*([0-9]+(?:\.[0-9]+)?(?:\s*-\s*[0-9]+(?:\.[0-9]+)?)?)\s*万")

# ランク変動: ↑n / ↓n / 持平（=0）を robust に
def _rank_change_from_row(row_el) -> str:
    ARROW_CHANGE_RE = re.compile(r"(↑|↓)\s*(\d+)")
    HOLD_PAT = re.compile(r"(持平|平|—|-)")

    t = (row_el.inner_text() or "").strip()
    m = ARROW_CHANGE_RE.search(t)
    if m:
        return f"{'+' if m.group(1)=='↑' else '-'}{m.group(2)}"
    if HOLD_PAT.search(t):
        return "0"

    for el in row_el.query_selector_all("*"):
        for attr in ("title", "aria-label", "data-tip", "data-title"):
            v = el.get_attribute(attr)
            if not v: 
                continue
            m = ARROW_CHANGE_RE.search(v)
            if m:
                return f"{'+' if m.group(1)=='↑' else '-'}{m.group(2)}"
            if HOLD_PAT.search(v):
                return "0"

    html = (row_el.inner_html() or "")
    up_m = re.search(r"(?:↑|icon[-_ ]?up|rise)[^0-9]{0,12}(\d+)", html, flags=re.IGNORECASE)
    if up_m:
        return f"+{up_m.group(1)}"
    down_m = re.search(r"(?:↓|icon[-_ ]?down|fall|drop)[^0-9]{0,12}(\d+)", html, flags=re.IGNORECASE)
    if down_m:
        return f"-{down_m.group(1)}"
    if re.search(r"(持平|平|—|-)", html):
        return "0"

    return ""

# ランキング行に出る EV/PHEV 内訳（表記ゆらぎ許容）
EV_PATTERNS = [
    r"(?:EV|纯电|纯电动)\s*[:：]?\s*(\d{1,3}(?:,\d{3})+|\d{4,6})\s*辆?",
    r"(\d{1,3}(?:,\d{3})+|\d{4,6})\s*辆?\s*(?:EV|纯电|纯电动)",
]
PHEV_PATTERNS = [
    r"(?:PHEV|插电|插混|DM-?i|DMI|插电混合)\s*[:：]?\s*(\d{1,3}(?:,\d{3})+|\d{4,6})\s*辆?",
    r"(\d{1,3}(?:,\d{3})+|\d{4,6})\s*辆?\s*(?:PHEV|插电|插混|DM-?i|DMI|插电混合)",
]

def _max_number(text: str) -> str:
    if not text:
        return ""
    best, best_val = "", -1
    for m in COUNT_RE_GENERIC.findall(text.replace("\u00A0", " ")):
        v = int(m.replace(",", ""))
        if v > best_val:
            best_val, best = v, str(v)
    return best

def _pick_price(text: str) -> str:
    if not text:
        return ""
    m = PRICE_RE.search(text.replace("\u00A0", " "))
    return (m.group(1) + "万") if m else ""

def _pick_first_number_by_patterns(text: str, patterns: List[str]) -> str:
    if not text:
        return ""
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
            if s:
                return s
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
        if sid.isdigit():
            return sid
    a = row_el.query_selector('a[href^="/series/"][href$=".html"]')
    if a:
        href = a.get_attribute("href") or ""
        m = re.search(r"/series/(\d+)\.html", href)
        if m:
            return m.group(1)
    a2 = row_el.query_selector("a[href]")
    if a2:
        href = a2.get_attribute("href") or ""
        m = re.search(r"/(\d+)/?$", href)
        if m:
            return m.group(1)
    return ""

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
        change = _rank_change_from_row(el)
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

# ---------- シリーズページから title_raw & エネルギー種別 ----------

ENERGY_LABEL_PAT = re.compile(r"(?:能源类型|能源|动力|驱动|动力类型)\s*[:：]?\s*([^\s/|·、，,　]{1,12})")
NORMALIZE_TABLE = {
    "纯电": "EV", "纯电动": "EV", "电动": "EV", "EV": "EV",
    "插电混动": "PHEV", "插混": "PHEV", "PHEV": "PHEV", "插电": "PHEV",
    "增程": "EREV", "增程式": "EREV", "增程式电动": "EREV", "REEV": "EREV",
    "混动": "HEV", "油电混合": "HEV", "HEV": "HEV",
    "轻混": "MHEV", "MHEV": "MHEV",
    "汽油": "ICE", "燃油": "ICE", "柴油": "ICE",
}
HINT_WORDS = ["纯电", "纯电动", "EV", "插电", "插混", "PHEV", "增程", "增程式", "REEV", "混动", "HEV", "MHEV", "轻混", "燃油", "汽油", "柴油"]

def _normalize_energy(word: str) -> str:
    if not word:
        return "Unknown"
    w_cn = word.strip()
    w_up = w_cn.upper()
    if w_cn in NORMALIZE_TABLE:
        return NORMALIZE_TABLE[w_cn]
    if w_up in NORMALIZE_TABLE:
        return NORMALIZE_TABLE[w_up]
    for k, v in NORMALIZE_TABLE.items():
        if k in w_cn or k in w_up:
            return v
    return "Unknown"

def fetch_title_and_energy(page: Page, url: str, timeout_ms: int = 15000) -> Dict[str, str]:
    out = {"title_raw": "", "series_energy_raw": "", "type_from_page": "Unknown"}
    if not url:
        return out
    try:
        page.set_default_timeout(timeout_ms)
        page.goto(url, wait_until="domcontentloaded")
        out["title_raw"] = page.title() or ""

        # スペック/概要領域を優先
        block_text = page.evaluate("""
            () => {
              const parts = [];
              const sels = ['.specs','.spec','.para','.information','.configs','.card','.main','.content','body'];
              for (const s of sels) {
                const el = document.querySelector(s);
                if (el) parts.push(el.innerText);
              }
              return parts.join('\\n\\n');
            }
        """) or ""

        m = ENERGY_LABEL_PAT.search(block_text)
        if m:
            raw = m.group(1).strip()
            out["series_energy_raw"] = raw
            out["type_from_page"] = _normalize_energy(raw)
            return out

        # ラベル見つからない場合は、bodyテキストからヒント語
        full = page.evaluate("() => document.body.innerText") or ""
        for w in HINT_WORDS:
            if w.lower() in full.lower():
                out["series_energy_raw"] = w
                out["type_from_page"] = _normalize_energy(w)
                break
        return out
    except Exception:
        return out

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="例: https://www.autohome.com.cn/rank/1")
    ap.add_argument("--out", required=True, help="例: data/rank1_top50.csv")
    ap.add_argument("--wait-ms", type=int, default=220)
    ap.add_argument("--max-scrolls", type=int, default=220)
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
        )

        # ランキング
        rank_page = context.new_page()
        rank_page.goto(args.url, wait_until="domcontentloaded")
        scroll_to_load_all(rank_page, need_rows=50, wait_ms=args.wait_ms, max_scrolls=args.max_scrolls)
        rows = collect_rank_rows(rank_page, topk=50)

        # シリーズページで title_raw & 種別補完
        detail_page = context.new_page()
        for r in rows:
            info = fetch_title_and_energy(detail_page, r.get("series_url", ""))
            # title_raw を上書き（空でなければ）
            if info.get("title_raw"):
                r["title_raw"] = info["title_raw"]
            r["series_energy_raw"] = info.get("series_energy_raw", "")
            r["type_from_page"] = info.get("type_from_page", "Unknown")
            # 最終判定: ページ側を優先、Unknownなら従来の推定(type_hint)
            r["type_final"] = r["type_from_page"] if r["type_from_page"] != "Unknown" else r.get("type_hint", "Unknown")
            r["is_ev_binary"] = 1 if r["type_final"] == "EV" else 0

        # CSV
        fieldnames = [
            "rank_seq", "rank", "seriesname", "series_url",
            "brand", "model", "brand_conf", "series_conf", "title_raw",
            "count", "ev_count", "phev_count", "type_hint", "price", "rank_change",
            "series_energy_raw", "type_from_page", "type_final", "is_ev_binary",
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
                    "series_energy_raw": r.get("series_energy_raw", ""),
                    "type_from_page": r.get("type_from_page", "Unknown"),
                    "type_final": r.get("type_final", "Unknown"),
                    "is_ev_binary": r.get("is_ev_binary", 0),
                })

        print(f"[ok] rows={len(rows)} -> {args.out}")

        detail_page.close()
        context.close()
        browser.close()

if __name__ == "__main__":
    main()
