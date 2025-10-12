#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Autohome rank/1（車系月销量榜）上位N件をベースCSVに出力。
この段階では各シリーズURL等のリンクと、行テキストから取れる情報のみを保存する。
(タイトルやエネルギー種別は第2段階で各シリーズページから取得)

出力列:
rank_seq,rank,seriesname,series_url,count,ev_count,phev_count,price,rank_change
"""

import argparse
import csv
import os
import re
import sys
import time
from typing import List, Dict, Any, Optional

from playwright.sync_api import sync_playwright


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", type=str, default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--out", type=str, default="data/rank1_base.csv")
    ap.add_argument("--topk", type=int, default=50)
    ap.add_argument("--timeout", type=int, default=45000)
    ap.add_argument("--viewport_w", type=int, default=1280)
    ap.add_argument("--viewport_h", type=int, default=1400)
    ap.add_argument("--headless", action="store_true")
    return ap.parse_args()


PRICE_RE = re.compile(r"^\s*\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?万\s*$")
INT_RE   = re.compile(r"(-?\d+)")
COUNT_RE = re.compile(r"(\d{1,3}(?:,\d{3})*|\d+)")

def _text(node) -> str:
    try:
        return (node.inner_text() or "").strip()
    except Exception:
        return ""

def _first(locator):
    try:
        return locator.first if locator.count() > 0 else None
    except Exception:
        return None

def _parse_int_from_text(s: str) -> Optional[int]:
    if not s:
        return None
    m = INT_RE.search(s.replace(",", ""))
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def _extract_price_from_row(row) -> str:
    """
    行内から「7.48-11.98万」のような価格帯テキストを抽出。
    クラス名に依存せず、'万' を含む要素のうち PRICE_RE に最初にマッチしたものを採用。
    """
    try:
        cands = row.locator(":text('万')")
        n = cands.count()
        for i in range(n):
            t = _text(cands.nth(i))
            if PRICE_RE.match(t):
                return t.strip()
    except Exception:
        pass
    return ""

def _extract_rank_change_from_row(row) -> str:
    """
    順位カラム内の svg（三角）親要素の textContent に混在する数値を抽出し、矢印向きで符号付け。
    下向き判定ができない保存版の場合はフォールバックで正符号（上昇扱い）。
    """
    try:
        svg = _first(row.locator("svg"))
        if svg is None:
            return ""
        parent = svg.locator("xpath=..")
        txt = _text(parent)
        num = _parse_int_from_text(txt)
        if num is None:
            return ""
        # 下向き判定（保存HTMLでは上向きのみ見えるケースあり。将来 down の path が分かればここに判定を足す）
        is_down = False
        # 例: down の特徴的な path を検知して is_down = True にする……（未取得時は False のまま）
        return str(-num if is_down else num)
    except Exception:
        return ""

def _extract_rank_series_count(row) -> Dict[str, Any]:
    """
    既存項目を壊さないことを最優先に、data-rank-num の行から
    rank / seriesname / series_url / count / ev_count / phev_count を抽出。
    - rank は data-rank-num
    - seriesname は車名テキスト（.tw-text-lg など）を優先し、fallback で最初の太字/大きめテキスト
    - series_url は車名リンクの href
    - count は右側の「销量」数（数字のみ抽出）
    - ev_count / phev_count は既存の実装方針を踏襲（見当たらなければ空）
    """
    r = {
        "rank": "",
        "seriesname": "",
        "series_url": "",
        "count": "",
        "ev_count": "",
        "phev_count": "",
    }
    try:
        # rank
        rank_attr = row.get_attribute("data-rank-num")
        if rank_attr:
            r["rank"] = rank_attr.strip()

        # seriesname / series_url
        # 太字・大きめのテキストに車系名が入っているケースが多い
        name_el = _first(row.locator(".tw-text-lg, .tw-text-xl, .tw-font-bold"))
        if name_el is None:
            # fallback: 行内リンクのうち、車系ページっぽいもの
            name_el = _first(row.locator("a[href*='autohome.com.cn/']"))

        if name_el is not None:
            # テキスト
            r["seriesname"] = _text(name_el)
            # URL（親aまたは自身がaの場合）
            a_node = _first(name_el.locator("xpath=ancestor-or-self::a[1]"))
            if a_node is not None:
                href = a_node.get_attribute("href") or ""
                r["series_url"] = href.strip()

        # count（右側の销量数：カンマ・空白を除去し数値だけ）
        # 行内で数字が大きく目立つ要素から抽出（'销量' 近傍や末尾数値）。確実に1つ目をとる。
        digits = ""
        try:
            # 数が大きいブロックほど右寄りにあることが多いので、右側の数値候補を優先
            num_cands = row.locator(":text-matches('^\\s*\\d{1,3}(?:,\\d{3})*\\s*$')")
            if num_cands.count() == 0:
                # fallback: 行全体テキストから最後に出てくる大きめ数字を拾う
                fulltxt = _text(row)
                m_all = list(COUNT_RE.finditer(fulltxt))
                if m_all:
                    digits = m_all[-1].group(1)
            else:
                digits = _text(num_cands.last)
        except Exception:
            pass
        if digits:
            r["count"] = digits.replace(",", "").strip()

        # ev_count / phev_count（表示があれば拾う。無ければ空欄のまま）
        # キーワード近傍から抽出（簡易）：'EV' / 'PHEV' の直前直後で数値
        try:
            fulltxt = _text(row)
            # 例: "EV 1234" / "PHEV 567"
            mev = re.search(r"\bEV\b\D{0,4}(\d{1,3}(?:,\d{3})*|\d+)", fulltxt, re.I)
            mphev = re.search(r"\bPHEV\b\D{0,4}(\d{1,3}(?:,\d{3})*|\d+)", fulltxt, re.I)
            if mev:
                r["ev_count"] = mev.group(1).replace(",", "")
            if mphev:
                r["phev_count"] = mphev.group(1).replace(",", "")
        except Exception:
            pass

    except Exception:
        pass
    return r


def infinite_scroll_until(page, topk: int, timeout_ms: int = 45000):
    """
    無限スクロール： [data-rank-num] の増加を監視しながら、topk 到達 or 伸びが止まるまでスクロール。
    """
    page.set_default_timeout(timeout_ms)
    # 初期数
    prev = page.evaluate("() => document.querySelectorAll('[data-rank-num]').length")
    same_cnt = 0
    start_ts = time.time()

    while True:
        # window スクロールを段階的に送る（要素スクロールではなくウィンドウでOKな構造）
        page.evaluate("() => window.scrollBy(0, Math.floor(window.innerHeight * 0.9))")
        page.wait_for_timeout(450)

        cur = page.evaluate("() => document.querySelectorAll('[data-rank-num]').length")
        if cur > prev:
            same_cnt = 0
            prev = cur
        else:
            same_cnt += 1

        # 到達判定
        if cur >= topk:
            break
        # 伸びが止まった（3回連続増えない）か、時間超過（安全ブレーキ）
        if same_cnt >= 3:
            break
        if (time.time() - start_ts) > max(15, timeout_ms / 1000):
            break


def collect_rows(page, topk: int) -> List[Dict[str, Any]]:
    """
    取得済みの [data-rank-num] 行をパースして、指定件数まで返す（足りなければある分だけ）。
    """
    rows_out: List[Dict[str, Any]] = []

    row_locator = page.locator("[data-rank-num]")
    total = row_locator.count()
    take = min(total, topk) if total > 0 else 0

    for i in range(take):
        row = row_locator.nth(i)

        base = _extract_rank_series_count(row)  # 既存の主要項目
        price = _extract_price_from_row(row)    # 価格帯（テキスト）
        diff  = _extract_rank_change_from_row(row)  # 先月比（符号付き）

        r = {
            "rank": base.get("rank", ""),
            "seriesname": base.get("seriesname", ""),
            "series_url": base.get("series_url", ""),
            "count": base.get("count", ""),
            "ev_count": base.get("ev_count", ""),
            "phev_count": base.get("phev_count", ""),
            "price": price,
            "rank_change": diff,
        }
        rows_out.append(r)

    return rows_out


def main():
    args = parse_args()
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True if args.headless else True)
        ctx = browser.new_context(
            viewport={"width": args.viewport_w, "height": args.viewport_h},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
            java_script_enabled=True,
        )
        page = ctx.new_page()
        page.set_default_timeout(args.timeout)

        # 1) ランキングページへ
        page.goto(args.url, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(800)

        # 2) 無限スクロールで [data-rank-num] を増やす（topk まで or 伸び止まり）
        infinite_scroll_until(page, topk=args.topk, timeout_ms=args.timeout)

        # 3) 収集
        rows = collect_rows(page, topk=args.topk)

        # 4) CSV出力（列順は固定・既存互換）
        fieldnames = [
            "rank_seq",
            "rank",
            "seriesname",
            "series_url",
            "count",
            "ev_count",
            "phev_count",
            "price",
            "rank_change",
        ]
        with open(args.out, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
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
        ctx.close()
        browser.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
