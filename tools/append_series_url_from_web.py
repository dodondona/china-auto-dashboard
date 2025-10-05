#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_from_web.py
/rank/1 を開いて無限スクロール。
スクロール中に XHR レスポンスを傍受して seriesid を収集し、
最後に DOM のリンクからも seriesid を回収。
CSV に series_url を追記して保存。

使い方（workflow内）:
  python tools/append_series_url_from_web.py \
    --rank-url https://www.autohome.com.cn/rank/1 \
    --input data/autohome_raw_YYYY-MM.csv \
    --output data/autohome_raw_YYYY-MM_with_series.csv \
    --name-col model \
    --max-rounds 30 --idle-ms 650 --min-delta 3
"""

import re, csv, sys, argparse, time
from typing import List, Dict, Tuple, Optional, Set
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")

# HTML内/レスポンス内から seriesid を拾う正規表現
RE_SERIES_PAIR = re.compile(r'"seriesid"\s*:\s*"(\d{3,7})"\s*,\s*"seriesname"\s*:\s*"([^"]+)"')
RE_SERIES_ID   = re.compile(r'"seriesid"\s*:\s*"(\d{3,7})"')
RE_SERIES_EQ   = re.compile(r'seriesid\s*=\s*(\d{3,7})', re.I)
RE_ID_IN_PATH  = re.compile(r'/(\d{3,7})(?:/|[?#]|\")')

def normalize_name(s: str) -> str:
    import re as _re
    return _re.sub(r'\s+', '', (s or '')).lower()

def to_series_url(series_id: str) -> str:
    return f"https://www.autohome.com.cn/{series_id}/"

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

def collect_from_html_text(html: str, ids: Set[str], pairs: List[Tuple[str,str]]):
    # ペアがあれば名前もとれる
    for sid, sname in RE_SERIES_PAIR.findall(html):
        if sid not in ids:
            ids.add(sid); pairs.append((sid, sname))
    # IDだけでも拾う
    for sid in RE_SERIES_ID.findall(html):
        ids.add(sid)
    for sid in RE_SERIES_EQ.findall(html):
        ids.add(sid)
    for sid in RE_ID_IN_PATH.findall(html):
        ids.add(sid)

def collect_from_dom(page, ids: Set[str]):
    # DOM上の全リンクから 1234 のようなIDを収集
    anchors = page.eval_on_selector_all(
        "a[href]", "els => els.map(a => a.getAttribute('href'))"
    ) or []
    for href in anchors:
        if not href: continue
        m = RE_ID_IN_PATH.search(href)
        if m: ids.add(m.group(1))

def infinite_scroll_with_xhr_sniff(page, max_rounds: int, idle_ms: int, min_delta: int) -> Tuple[Set[str], List[Tuple[str,str]]]:
    """
    無限スクロール＋XHR傍受。seriesid をセットで返す。
    """
    ids: Set[str] = set()
    pairs: List[Tuple[str,str]] = []

    # レスポンスハンドラで XHR 本文から抽出
    def on_response(res):
        try:
            ct = res.headers.get("content-type","").lower()
            # JSON/HTMLっぽいレスポンスのみ対象
            if "json" in ct or "html" in ct or "text" in ct:
                urlpath = urlparse(res.url).path.lower()
                # rank/1 関連 or 文字列に seriesid を含むレスポンスだけ見る
                if "rank" in urlpath or "series" in urlpath:
                    body = res.text()
                    collect_from_html_text(body, ids, pairs)
        except Exception:
            pass

    page.context.on("response", on_response)

    # 初期HTMLからも拾う（ここで20件くらい入る）
    collect_from_html_text(page.content(), ids, pairs)

    last_n = 0
    stable = 0
    for _ in range(max_rounds):
        # 最下部へ
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(idle_ms)
        page.wait_for_load_state("networkidle")

        # ループごとに HTML/DOM からも拾っておく
        collect_from_html_text(page.content(), ids, pairs)
        collect_from_dom(page, ids)

        if len(ids) - last_n < min_delta:
            stable += 1
        else:
            stable = 0
        last_n = len(ids)
        if stable >= 2:  # 2回連続で増えなければ打ち切り
            break

    # 最後にもう一度 DOM から拾う
    collect_from_dom(page, ids)

    # 重複除いて (sid, name) を整える（名前はわかる分だけ）
    seen = set()
    merged: List[Tuple[str,str]] = []
    # 先に pairs（名前あり）を優先
    for sid, name in pairs:
        if sid not in seen:
            seen.add(sid); merged.append((sid, name))
    # 残りのID（名前不明）は空名で追加
    for sid in sorted(ids):
        if sid not in seen:
            seen.add(sid); merged.append((sid, ""))
    return ids, merged

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
    ap.add_argument("--max-rounds", type=int, default=30)
    ap.add_argument("--idle-ms", type=int, default=650)
    ap.add_argument("--min-delta", type=int, default=3)
    args = ap.parse_args()

    rows = read_csv_rows(args.input)
    if not rows:
        print("入力CSVが空です。"); sys.exit(1)
    name_col = detect_name_col(list(rows[0].keys()), args.name_col)

    print("[append_series_url] mode=infinite-scroll + XHR sniff", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=UA, viewport={"width":1280,"height":1700})
        # 軽量化：画像・動画・フォントはブロック
        context.route("**/*", lambda route: route.abort()
                      if route.request.resource_type in ["image","media","font"] else route.continue_())
        page = context.new_page()
        page.goto(args.rank_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_load_state("networkidle")

        ids, pairs = infinite_scroll_with_xhr_sniff(
            page, max_rounds=args.max_rounds, idle_ms=args.idle_ms, min_delta=args.min_delta
        )

        context.close(); browser.close()

    attach_by_name_then_order(rows, pairs, name_col)
    write_csv_rows(args.output, rows)
    print(f"✔ 出力: {args.output}  （抽出 {len(ids)}件 / CSV {len(rows)}行）")
    print(f"  使用した車名列: {name_col}")

if __name__ == "__main__":
    main()
