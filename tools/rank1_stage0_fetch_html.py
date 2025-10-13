#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
下ごしらえ専用:
- ランキングページ (例: https://www.autohome.com.cn/rank/1) を開き、上位50件の车系列リンクを収集
- 各 车系列 ページの HTML を保存
- ついでにランキングページの HTML スナップショットも保存
- 既存の Stage1/Stage2 には影響を与えない（出力は専用ディレクトリに保存）

出力:
  outdir/
    rank_page.html
    index.csv  (rank, series_id, series_url)
    series_01_7806.html
    series_02_5964.html
    ... (最大50件)

使い方:
  python tools/rank1_stage0_fetch_html.py \
    --url "https://www.autohome.com.cn/rank/1" \
    --outdir "data/html_rank1" \
    --max 50 \
    --wait-ms 220 \
    --max-scrolls 220
"""

import argparse
import csv
import os
import re
import time
from typing import List, Tuple
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, Page, Browser

ABS_BASE = "https://www.autohome.com.cn"
SERIES_ID_RE = re.compile(r"/series/(\d+)\.html|/(\d+)/?$")


def abs_url(u: str) -> str:
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


def extract_series_id(href: str) -> str:
    if not href:
        return ""
    m = SERIES_ID_RE.search(href)
    if not m:
        return ""
    # グループ1が /series/12345.html、グループ2が /12345/ のパターン
    sid = m.group(1) or m.group(2) or ""
    return sid.strip() if (sid and sid.isdigit()) else ""


def wait_rank_list_ready(page: Page, target_url: str, wait_ms: int, max_scrolls: int):
    """
    無限スクロール&仮想リスト対策:
    - networkidle で大枠の読み込み完了を待つ
    - 0.8画面ずつ段階スクロールして、IntersectionObserver を確実に発火
    - 最後に全行を中心表示して描画を確定
    """
    page.goto(target_url, wait_until="networkidle")
    page.wait_for_timeout(1500)

    last_height = 0
    last_count = -1
    for i in range(max_scrolls):
        page.evaluate("() => window.scrollBy(0, Math.floor(window.innerHeight * 0.8))")
        page.wait_for_timeout(wait_ms)

        # 新しい行が増えているか
        cnt = page.evaluate("() => document.querySelectorAll('[data-rank-num]').length")
        if cnt >= 50:
            break
        if cnt == last_count and i > 10:
            # スクロールしても増えないなら軽く待つ
            page.wait_for_timeout(800)
        last_count = cnt

        # 高さの変化（レンダリング進捗）も観測
        new_h = page.evaluate("() => document.body.scrollHeight")
        last_height = new_h

    # 全行を一度は可視化（描画を確定させる）
    page.evaluate("""
        Array.from(document.querySelectorAll('[data-rank-num]')).forEach(e => {
            try { e.scrollIntoView({block: 'center'}); } catch (e) {}
        });
    """)
    page.wait_for_timeout(1200)


def collect_series_links(page: Page, topk: int = 50) -> List[Tuple[int, str, str]]:
    """
    [ (rank, series_id, series_url), ... ] を rank 昇順で返す
    """
    items = []
    rows = page.query_selector_all("[data-rank-num]")[:topk]
    for el in rows:
        rank_str = (el.get_attribute("data-rank-num") or "").strip()
        try:
            rank = int(rank_str) if rank_str.isdigit() else len(items) + 1
        except Exception:
            rank = len(items) + 1

        a = el.query_selector("a[href]")
        href = a.get_attribute("href") if a else ""
        sid = extract_series_id(href or "")
        url = abs_url(href or "")
        if sid and url:
            items.append((rank, sid, url))

    # rank順で整列 & 上位topkまで
    items.sort(key=lambda x: x[0])
    # 同一series_idが重複する場合（あり得ないが）を防御的に除外
    seen = set()
    dedup = []
    for r, sid, url in items:
        if sid in seen:
            continue
        seen.add(sid)
        dedup.append((r, sid, url))
    return dedup[:topk]


def save_text(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def fetch_series_html(page: Page, url: str, retries: int = 3, wait_ms: int = 800) -> str:
    """
    车系列ページのHTMLを取得（単純保存用）
    - HTML本体が欲しいだけなので、networkidle→短い待機でOK
    - 失敗時はリトライ
    """
    last_err = None
    for _ in range(retries):
        try:
            page.goto(url, wait_until="networkidle")
            page.wait_for_timeout(wait_ms)
            return page.content()
        except Exception as e:
            last_err = e
            time.sleep(1.0)
    raise last_err


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="ランキングURL (例: https://www.autohome.com.cn/rank/1)")
    ap.add_argument("--outdir", required=True, help="保存先ディレクトリ")
    ap.add_argument("--max", type=int, default=50, help="収集上限（デフォルト50）")
    ap.add_argument("--wait-ms", type=int, default=220, help="スクロール間隔ms（デフォルト220）")
    ap.add_argument("--max-scrolls", type=int, default=220, help="最大スクロール回数（デフォルト220）")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"))
        page = ctx.new_page()
        page.set_default_timeout(45000)

        # 1) ランキングページを安定描画させる
        wait_rank_list_ready(page, args.url, args.wait_ms, args.max_scrolls)

        # 2) スナップショット保存（参考: 解析用）
        rank_html = page.content()
        save_text(os.path.join(args.outdir, "rank_page.html"), rank_html)

        # 3) 上位max件の series リンク抽出
        links = collect_series_links(page, topk=args.max)
        if not links:
            raise SystemExit("No series links collected.")

        # 4) インデックスCSV保存
        index_csv = os.path.join(args.outdir, "index.csv")
        with open(index_csv, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["rank", "series_id", "series_url"])
            for r, sid, url in links:
                w.writerow([r, sid, url])

        # 5) 各 车系列ページ HTML を保存
        # 同じタブ page を使い回す（安定・高速）
        for r, sid, url in links:
            try:
                html = fetch_series_html(page, url, retries=3, wait_ms=800)
                fname = f"series_{r:02d}_{sid}.html"
                save_text(os.path.join(args.outdir, fname), html)
                # ランキングページに戻さなくてよい（HTML保存が目的のため）
            except Exception as e:
                # 失敗しても全体を止めたくない場合はログだけにする
                err_name = f"series_{r:02d}_{sid}.error.txt"
                save_text(os.path.join(args.outdir, err_name), f"{type(e).__name__}: {e}")

        print(f"[ok] saved rank page + {len(links)} series pages to: {args.outdir}")

        ctx.close()
        browser.close()


if __name__ == "__main__":
    main()
