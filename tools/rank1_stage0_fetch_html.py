#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
下ごしらえ専用:
- ランキングページ (例: https://www.autohome.com.cn/rank/1) を開き、上位50件の车系列リンクを収集
- 各 车系列 ページの HTML を保存
- ランキングページの HTML スナップショットも保存
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
SERIES_ANCHOR_RE = re.compile(r'href="(?:/series/(\d+)\.html|/(\d+)/?)"')
DATA_RANK_RE = re.compile(r'data-rank-num="(\d+)"')

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
    m = re.search(r"/series/(\d+)\.html|/(\d+)/?$", href)
    if not m:
        return ""
    sid = m.group(1) or m.group(2) or ""
    return sid if (sid and sid.isdigit()) else ""

def wait_rank_list_ready(page: Page, target_url: str, wait_ms: int, max_scrolls: int):
    # 中国サイトで networkidle が詰まりやすい環境があるため domcontentloaded に寄せる
    page.goto(target_url, wait_until="domcontentloaded")
    page.wait_for_timeout(1000)

    last_count = -1
    for i in range(max_scrolls):
        # 段階スクロールで仮想リストを必ず可視化
        page.evaluate("() => window.scrollBy(0, Math.floor(window.innerHeight * 0.85))")
        page.wait_for_timeout(wait_ms)
        cnt = page.evaluate("() => document.querySelectorAll('[data-rank-num]').length")
        if cnt >= 50:
            break
        if cnt == last_count and i > 10:
            page.wait_for_timeout(500)
        last_count = cnt

    # すべての行を一度は可視化
    page.evaluate("""
        Array.from(document.querySelectorAll('[data-rank-num]')).forEach(e => {
            try { e.scrollIntoView({block: 'center'}); } catch (e) {}
        });
    """)
    page.wait_for_timeout(800)

def collect_series_links_dom(page: Page, topk: int = 50) -> List[Tuple[int, str, str]]:
    """
    DOMから rank, series_id, series_url を収集
    """
    items: List[Tuple[int, str, str]] = []
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

    # rank順＆重複排除
    items.sort(key=lambda x: x[0])
    seen, dedup = set(), []
    for r, sid, url in items:
        if sid in seen:
            continue
        seen.add(sid)
        dedup.append((r, sid, url))
    return dedup[:topk]

def collect_series_links_fallback(rank_html: str, topk: int = 50) -> List[Tuple[int, str, str]]:
    """
    フォールバック: HTMLテキストを正規表現で直読みしてリンク抽出。
    data-rank-num が見つからない/DOMが読めない環境でも、href の出現順で rank を付与。
    """
    links: List[Tuple[int, str, str]] = []

    # 1) data-rank-num に紐づく近傍の href を優先的に拾う（あればより正確）
    ranks = [int(x) for x in DATA_RANK_RE.findall(rank_html)]
    if ranks:
        # data-rank-num の並びに沿って、その近傍に現れる series リンクを紐付け
        # シンプルに、全hrefを前から拾いながら rank を割り振る
        seen = set()
        idx = 0
        for m in SERIES_ANCHOR_RE.finditer(rank_html):
            sid = m.group(1) or m.group(2)
            if not (sid and sid.isdigit()):
                continue
            if sid in seen:
                continue
            seen.add(sid)
            rank = ranks[idx] if idx < len(ranks) else (idx + 1)
            url = abs_url(m.group(0).split('"')[1])
            links.append((rank, sid, url))
            idx += 1
            if len(links) >= topk:
                break
    else:
        # 2) 最悪ケース: href 出現順で rank=1.. を割当
        seen = set()
        rank = 1
        for m in SERIES_ANCHOR_RE.finditer(rank_html):
            sid = m.group(1) or m.group(2)
            if not (sid and sid.isdigit()):
                continue
            if sid in seen:
                continue
            seen.add(sid)
            url = abs_url(m.group(0).split('"')[1])
            links.append((rank, sid, url))
            rank += 1
            if len(links) >= topk:
                break

    # rank昇順＋重複排除（念のため）
    links.sort(key=lambda x: x[0])
    dedup, seen2 = [], set()
    for r, sid, url in links:
        if sid in seen2:  # 同じsidが混じった場合の防御
            continue
        seen2.add(sid)
        dedup.append((r, sid, url))
    return dedup[:topk]

def save_text(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def fetch_series_html(page: Page, url: str, retries: int = 3, wait_ms: int = 800) -> str:
    last_err = None
    for _ in range(retries):
        try:
            page.goto(url, wait_until="domcontentloaded")
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
                        "Chrome/124.0.0.0 Safari/537.36"),
            locale="zh-CN",
            **{"accept_downloads": True}
        )
        page = ctx.new_page()
        page.set_default_timeout(45000)

        # 1) ランキングページ描画
        wait_rank_list_ready(page, args.url, args.wait_ms, args.max_scrolls)

        # 2) スナップショット保存
        rank_html = page.content()
        save_text(os.path.join(args.outdir, "rank_page.html"), rank_html)

        # 3) 上位max件の series リンク抽出（DOM → 失敗時はHTML直読みフォールバック）
        links = collect_series_links_dom(page, topk=args.max)
        if not links:
            links = collect_series_links_fallback(rank_html, topk=args.max)

        if not links:
            # 403/検証ページの可能性もあるのでヒントを書き出して終了
            hint_path = os.path.join(args.outdir, "rank_page.head.txt")
            save_text(hint_path, rank_html[:4000])
            raise SystemExit("No series links collected. (Wrote rank_page.html for debugging)")

        # 4) インデックスCSV保存
        index_csv = os.path.join(args.outdir, "index.csv")
        with open(index_csv, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["rank", "series_id", "series_url"])
            for r, sid, url in links:
                w.writerow([r, sid, url])

        # 5) 各 车系列ページ HTML を保存
        for r, sid, url in links:
            try:
                html = fetch_series_html(page, url, retries=3, wait_ms=800)
                fname = f"series_{r:02d}_{sid}.html"
                save_text(os.path.join(args.outdir, fname), html)
            except Exception as e:
                err_name = f"series_{r:02d}_{sid}.error.txt"
                save_text(os.path.join(args.outdir, err_name), f"{type(e).__name__}: {e}")

        print(f"[ok] saved rank page + {len(links)} series pages to: {args.outdir}")

        ctx.close()
        browser.close()

if __name__ == "__main__":
    main()
