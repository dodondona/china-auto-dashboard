#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_from_rank.py
/rank/1 を開いて、無限スクロールで全件ロード完了までスクロール。
最終HTMLから seriesid/seriesname を抽出し、CSV に series_url を追記する。
"""

import re, csv, sys, argparse, time
from typing import List, Dict, Tuple, Optional
from playwright.sync_api import sync_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")

RE_SERIES_PAIR = re.compile(r'"seriesid"\s*:\s*"(\d{3,7})"\s*,\s*"seriesname"\s*:\s*"([^"]+)"')
RE_SERIES_APP  = re.compile(r'seriesid\s*=\s*(\d{3,7})', re.I)
RE_SERIES_PATH = re.compile(r'/(\d{3,7})(?:/|[?#]|\")')

def normalize_name(s: str) -> str:
    import re as _re
    return _re.sub(r'\s+', '', (s or '')).lower()

def to_series_url(series_id: str) -> str:
    return f"https://www.autohome.com.cn/{series_id}"

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

def detect_name_col(fields: List[str], preferred: Optional[str]) -> str:
    if preferred and preferred in fields: return preferred
    for c in ["model_text","model","name","car","series_name","title"]:
        if c in fields: return c
    return fields[0]

def extract_pairs_from_html(html: str) -> List[Tuple[str, str]]:
    pairs = [(sid, sname) for sid, sname in RE_SERIES_PAIR.findall(html)]
    if not pairs:
        sids = RE_SERIES_APP.findall(html) or RE_SERIES_PATH.findall(html)
        seen = set()
        for sid in sids:
            if sid not in seen:
                seen.add(sid); pairs.append((sid, ""))
    # 去重
    seen2, uniq = set(), []
    for sid, sname in pairs:
        if sid not in seen2:
            seen2.add(sid); uniq.append((sid, sname))
    return uniq

def infinite_scroll_until_stable(page, max_rounds: int = 30, idle_ms: int = 600, min_delta: int = 5):
    """
    無限スクロール：
    - ページ末尾にスクロール→ネットワーク安定を待つ
    - seriesid の検出件数が増えなくなるまで繰り返す
    """
    last_count = 0
    stable_rounds = 0
    for i in range(max_rounds):
        # 一番下へ
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(idle_ms)
        page.wait_for_load_state("networkidle")

        html = page.content()
        cnt = len(RE_SERIES_PAIR.findall(html)) or len(RE_SERIES_APP.findall(html)) or len(RE_SERIES_PATH.findall(html))

        # 十分に増えない (=もう出し切った) と判断
        if cnt - last_count < min_delta:
        # 伸びない時はホイールで軽く刺激（IntersectionObserver発火漏れ対策）
        try:
            page.mouse.wheel(0, 1800)
            page.wait_for_timeout(400)
        except Exception:
            pass
            stable_rounds += 1
        else:
            stable_rounds = 0
        last_count = cnt

        # 2回連続で増えなければ終了（調整可）
        if stable_rounds >= 2:
            break

def attach_by_name_then_order(rows: List[Dict[str,str]], pairs: List[Tuple[str,str]], name_col: str):
    page_names = [normalize_name(n) for _, n in pairs]
    page_urls  = [to_series_url(sid) for sid, _ in pairs]
    used = set()
    # 名前一致（完全→部分）
    for i, r in enumerate(rows):
        name = normalize_name(r.get(name_col, ""))
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
        rows[i]["series_url"] = url
    # 残りは順序で埋める
    k = 0
    for i, r in enumerate(rows):
        if r.get("series_url"): continue
        while k < len(page_urls) and k in used:
            k += 1
        rows[i]["series_url"] = page_urls[k] if k < len(page_urls) else ""
        used.add(k); k += 1

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--name-col", default=None)
    ap.add_argument("--max-rounds", type=int, default=30, help="スクロール試行の上限")
    ap.add_argument("--idle-ms", type=int, default=600, help="各スクロール後の待ち時間(ms)")
    ap.add_argument("--min-delta", type=int, default=5, help="増加件数がこの閾値未満なら“増えてない”とみなす")
    args = ap.parse_args()

    rows = read_csv_rows(args.input)
    if not rows:
        print("入力CSVが空です。"); sys.exit(1)
    name_col = detect_name_col(list(rows[0].keys()), args.name_col)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=UA, viewport={"width":1280,"height":1700})
        # 画像・動画・フォントはブロックして高速化
        context.route("**/*", lambda route: route.abort()
                      if route.request.resource_type in ["image","media","font"] else route.continue_())

        page = context.new_page()
        page.goto(args.rank_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_load_state("networkidle")

        
    # 指紋クッキー `_ac` が入るまで待機（無いと無限スクロールのXHRが落ちることがある）
    page.wait_for_function(\"document.cookie.includes('_ac=')\", timeout=15000)
# 無限スクロールで全件を出し切る
        infinite_scroll_until_stable(page, max_rounds=args.max_rounds, idle_ms=args.idle_ms, min_delta=args.min_delta)

        html = page.content()
        context.close(); browser.close()

    pairs = extract_pairs_from_html(html)
    attach_by_name_then_order(rows, pairs, name_col)
    write_csv_rows(args.output, rows)
    print(f"✔ 出力: {args.output}  （抽出 {len(pairs)}件 / CSV {len(rows)}行）")
    print(f"  使用した車名列: {name_col}")

if __name__ == "__main__":
    main()
