#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_from_web.py

/rank/1 を開いて無限スクロール。
スクロールの各ステップで:
  - DOMに見えている a[href] から /{seriesid}/ 形式のみ収集（広告/トラッキング除外）
  - XHRレスポンス本文からも "seriesid"/"seriesname" を抽出し補完
最終的にユニークな seriesid の集合を得て、CSVの車名と "名前一致のみ" で対応付ける。
（順番埋めはしない -> ズレ防止）

使い方（Actions/ローカル共通）:
  python tools/append_series_url_from_web.py \
    --rank-url "https://www.autohome.com.cn/rank/1" \
    --input data/autohome_raw_YYYY-MM.csv \
    --output data/autohome_raw_YYYY-MM_with_series.csv \
    --name-col model \
    --max-rounds 40 \
    --idle-ms 700
"""

import re
import csv
import sys
import argparse
from typing import List, Dict, Optional, Tuple, Set
from urllib.parse import urlparse, urljoin

from playwright.sync_api import sync_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "KHTML, like Gecko) Chrome/120 Safari/537.36")

# ---------- 正規表現 ----------
# XHR/HTMLの埋め込みから
RE_SERIES_PAIR = re.compile(r'"seriesid"\s*:\s*"(\d{3,7})"\s*,\s*"seriesname"\s*:\s*"([^"]+)"')
RE_SERIES_ID   = re.compile(r'"seriesid"\s*:\s*"(\d{3,7})"')

# DOMの a[href] から /{id}/ だけ許可（末尾 / / ? # まで）
RE_ID_STRICT   = re.compile(r'^/(\d{3,7})(?:/|[?#]?$)')

# series_url の最終整形用
RE_SERIES_ID_IN_URL = re.compile(r'/(\d{3,7})(?:/|[?#])')

# ---------- ユーティリティ ----------
def normalize_name(s: str) -> str:
    """空白/記号を最低限除去して小文字化（簡易）"""
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r'[ \t\u3000・·•／/（）()\-\+]+', '', s)
    return s

def to_series_url(url_or_id: str) -> str:
    if not url_or_id:
        return ""
    m = RE_SERIES_ID_IN_URL.search(url_or_id)
    if m:
        sid = m.group(1)
    else:
        sid = re.sub(r'\D', '', url_or_id)
    return f"https://www.autohome.com.cn/{sid}/" if sid else ""

def read_csv_rows(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv_rows(path: str, rows: List[Dict[str, str]]) -> None:
    if not rows:
        return
    fields = list(rows[0].keys())
    if "series_url" not in fields:
        fields.append("series_url")
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

def detect_name_col(fields: List[str], preferred: Optional[str]) -> str:
    if preferred and preferred in fields:
        return preferred
    for c in ["model_text", "model", "name", "car", "series_name", "title"]:
        if c in fields:
            return c
    return fields[0]

# ---------- 収集ロジック ----------
def collect_ids_from_dom(page, ids: Set[str], name_map: Dict[str, str]) -> None:
    """
    画面に見えている a[href] をすべて走査し、/123456/ 形式だけを採用。
    広告/トラッキング/内部ナビは除外。ついでにテキスト（正規化）も sid に紐付ける。
    """
    pairs = page.eval_on_selector_all(
        "a[href]", "els => els.map(a => [a.getAttribute('href'), a.textContent || ''])"
    ) or []
    for href, text in pairs:
        if not href or href == "javascript:void(0)":
            continue

        # 相対→絶対
        if href.startswith("//"):
            href_abs = "https:" + href
        elif href.startswith("/"):
            href_abs = urljoin("https://www.autohome.com.cn/", href)
        else:
            # rank内部の相対や外部URLは除外（/xxx から始まらないもの）
            continue

        # トラッキング/内部ナビは除外
        path = urlparse(href_abs).path.lower()
        if "pvareaid" in href_abs or "/rank/" in path or "/news/" in path or "/club/" in path:
            continue

        # /{id}/ 形式のみ許可
        m = RE_ID_STRICT.match(path)
        if not m:
            continue
        sid = m.group(1)
        ids.add(sid)

        # DOMで見えた名前（正規化）を sid にメモ（空でなければ）
        nn = normalize_name(text)
        if nn and sid not in name_map:
            name_map[sid] = nn

def infinite_scroll_collect_all(page, max_rounds: int, idle_ms: int) -> Tuple[Set[str], List[Tuple[str, str]], Dict[str, str]]:
    """
    無限スクロールしながら:
      - DOMに見えている a[href] から seriesid 集合を増やす
      - XHRレスポンス本文から (seriesid, seriesname) を抽出し補完
    2回連続で件数が増えなければ打ち切り。
    戻り値: (ids_set, pairs[(sid, sname)], name_map[sid->norm_name])
    """
    ids: Set[str] = set()
    pairs: List[Tuple[str, str]] = []
    name_map: Dict[str, str] = {}

    # XHR本文から抽出
    def on_response(res):
        try:
            ct = res.headers.get("content-type", "").lower()
            if "json" in ct or "html" in ct or "text" in ct:
                body = res.text()
                # "seriesid":"####","seriesname":"…"
                for sid, sname in RE_SERIES_PAIR.findall(body):
                    ids.add(sid)
                    pairs.append((sid, sname))
                # idだけの出現も拾う
                for sid in RE_SERIES_ID.findall(body):
                    ids.add(sid)
        except Exception:
            pass

    page.context.on("response", on_response)

    # 初期画面
    collect_ids_from_dom(page, ids, name_map)

    last = 0
    stable = 0
    for _ in range(max_rounds):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(idle_ms)
        page.wait_for_load_state("networkidle")

        collect_ids_from_dom(page, ids, name_map)

        if len(ids) == last:
            stable += 1
        else:
            stable = 0
        last = len(ids)
        if stable >= 2:  # 2回連続増えなければ終了
            break

    # pairs を重複除去（先勝ち）
    seen = set()
    uniq_pairs: List[Tuple[str, str]] = []
    for sid, sname in pairs:
        if sid not in seen:
            seen.add(sid)
            uniq_pairs.append((sid, sname))

    return ids, uniq_pairs, name_map

# ---------- 突合（名前一致のみ／順番埋め無し） ----------
def attach_by_name_only(rows: List[Dict[str, str]],
                        pairs: List[Tuple[str, str]],
                        name_map: Dict[str, str],
                        name_col: str,
                        min_score: float = 0.70) -> None:
    """
    sid -> normalized name の辞書を作り、CSVの車名（正規化）に最も近い sid を割り当てる。
    閾値未満は空のまま（順番埋めは絶対にしない）。
    """
    # sid -> name（XHRのseriesname優先、なければDOMで見えたテキスト）
    sid2name: Dict[str, str] = {}
    for sid, sname in pairs:
        nn = normalize_name(sname)
        if nn:
            sid2name[sid] = nn
    for sid, nn in name_map.items():
        if sid not in sid2name and nn:
            sid2name[sid] = nn

    def score(a: str, b: str) -> float:
        """超軽量：先頭一致長/長い方長さ"""
        if not a or not b:
            return 0.0
        la, lb = len(a), len(b)
        k = 0
        for i in range(min(la, lb)):
            if a[i] == b[i]:
                k += 1
            else:
                break
        return k / max(la, lb)

    for i, r in enumerate(rows):
        target = normalize_name(r.get(name_col, ""))
        best_sid, best_s = None, 0.0
        for sid, nn in sid2name.items():
            s = score(target, nn)
            if s > best_s:
                best_sid, best_s = sid, s
        r["series_url"] = to_series_url(best_sid) if (best_sid and best_s >= min_score) else ""

# ---------- メイン ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--name-col", default=None)
    ap.add_argument("--max-rounds", type=int, default=40)
    ap.add_argument("--idle-ms", type=int, default=700)
    args = ap.parse_args()

    rows = read_csv_rows(args.input)
    if not rows:
        print("入力CSVが空です。", file=sys.stderr)
        sys.exit(1)
    name_col = detect_name_col(list(rows[0].keys()), args.name_col)

    print("[append_series_url] mode=INFINITE-SCROLL (DOM+XHR), no order fallback", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=UA, viewport={"width": 1280, "height": 1700})
        # 軽量化：画像/動画/フォントはブロック
        context.route("**/*", lambda route: route.abort()
                      if route.request.resource_type in ["image", "media", "font"] else route.continue_())
        page = context.new_page()
        page.goto(args.rank_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_load_state("networkidle")

        ids, pairs, name_map = infinite_scroll_collect_all(page, max_rounds=args.max_rounds, idle_ms=args.idle_ms)

        context.close()
        browser.close()

    attach_by_name_only(rows, pairs, name_map, name_col=name_col, min_score=0.70)
    write_csv_rows(args.output, rows)
    print(f"✔ 出力: {args.output}  （抽出 {len(ids)}件 / CSV {len(rows)}行）")
    print(f"  使用した車名列: {name_col}")

if __name__ == "__main__":
    main()
