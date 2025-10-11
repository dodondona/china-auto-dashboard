#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Autohome の /rank/ ページからシリーズ詳細 URL を取得し、入力CSVに
series_url を付与するユーティリティ。

・Playwright (Chromium) でまず取得
・失敗/空のときは requests + HTML でフォールバック
・ページ中の https://www.autohome.com.cn/<digits>/ を全列挙し、出現順を rank=1..N に採番
・入力側は rank / rank_seq のどちらでも自動対応
・スクレイピング結果が空でも落とさず素通し（series_url を欠損で出力）

使い方例:
  python tools/append_series_url_from_web.force_dom.py \
    --input data/autohome_raw_2025-08.csv \
    --output data/autohome_raw_2025-08_with_series.csv \
    --rank-url https://www.autohome.com.cn/rank/1-3-1071-x/

"""

import argparse
import re
import sys
from typing import List, Optional

import pandas as pd

# フォールバック用
import requests
from bs4 import BeautifulSoup

# Playwright は任意（import 失敗時はフォールバック一本に）
try:
    from playwright.sync_api import sync_playwright
    HAS_PW = True
except Exception:
    HAS_PW = False


URL_PATTERN = re.compile(r"https?://www\.autohome\.com\.cn/(\d+)/")


def _extract_series_urls_from_html(html: str) -> List[str]:
    """HTML からシリーズ詳細URLを順序付きで重複排除して抽出"""
    urls = []
    seen = set()
    # まず正規表現で拾う（順序維持）
    for m in URL_PATTERN.finditer(html):
        url = f"https://www.autohome.com.cn/{m.group(1)}/"
        if url not in seen:
            seen.add(url)
            urls.append(url)

    # セレクタでも一応拾う（順番は DOM 順）
    try:
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = URL_PATTERN.match(href)
            if m:
                url = f"https://www.autohome.com.cn/{m.group(1)}/"
                if url not in seen:
                    seen.add(url)
                    urls.append(url)
    except Exception:
        pass

    return urls


def _scrape_with_playwright(url: str, timeout_ms: int = 12000) -> Optional[str]:
    """Playwright でページHTMLを取得（失敗時は None）"""
    if not HAS_PW:
        return None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ))
            page = ctx.new_page()
            page.set_default_timeout(timeout_ms)
            page.goto(url, wait_until="load")
            # JSレンダ待ちの余白（軽め）
            page.wait_for_timeout(800)
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        print(f"⚠ Playwright failed: {e}", file=sys.stderr)
        return None


def _scrape_with_requests(url: str, timeout_s: int = 12) -> Optional[str]:
    """requests でページHTMLを取得（失敗時は None）"""
    try:
        headers = {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/124.0.0.0 Safari/537.36")
        }
        r = requests.get(url, headers=headers, timeout=timeout_s)
        if r.status_code == 200 and r.text:
            return r.text
        print(f"⚠ requests got status={r.status_code}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"⚠ requests failed: {e}", file=sys.stderr)
        return None


def build_web_df(rank_url: str) -> pd.DataFrame:
    """rankページから [rank, series_url] のDFを作る。空でも返す。"""
    html = _scrape_with_playwright(rank_url) or _scrape_with_requests(rank_url)
    if not html:
        print("⚠ Unable to fetch HTML from rank_url", file=sys.stderr)
        return pd.DataFrame(columns=["rank", "series_url"])

    urls = _extract_series_urls_from_html(html)
    if not urls:
        print("⚠ No series urls found in HTML", file=sys.stderr)
        return pd.DataFrame(columns=["rank", "series_url"])

    # 最初の50件を rank=1.. として採番（必要なら数は自動で増える）
    data = [{"rank": i + 1, "series_url": u} for i, u in enumerate(urls)]
    return pd.DataFrame(data)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="入力CSV")
    ap.add_argument("--output", required=True, help="出力CSV")
    ap.add_argument("--rank-url",
                    default="https://www.autohome.com.cn/rank/1-3-1071-x/",
                    help="オートホームのランクページURL")
    args = ap.parse_args()

    print(f"📥 input: {args.input}")
    print(f"🌐 scraping: {args.rank_url}")

    df = pd.read_csv(args.input)

    # 入力側キーを自動判定
    left_key = "rank" if "rank" in df.columns else ("rank_seq" if "rank_seq" in df.columns else None)
    if left_key is None:
        print("⚠ input has no 'rank' nor 'rank_seq' — will add blank series_url and exit.")
        if "series_url" not in df.columns:
            df["series_url"] = None
        df.to_csv(args.output, index=False)
        return

    # 文字→数値へ（混入対策）
    df[left_key] = pd.to_numeric(df[left_key], errors="coerce").astype("Int64")

    # スクレイピング
    web = build_web_df(args.rank_url)

    # 結果が空 or rank欠損なら素通し
    if web.empty or "rank" not in web.columns:
        print("⚠ scraped 'web' has no usable rank; keep input and add missing series_url as NA.")
        if "series_url" not in df.columns:
            df["series_url"] = None
        df.to_csv(args.output, index=False)
        return

    # マージ（右側のrankを数値化）
    web["rank"] = pd.to_numeric(web["rank"], errors="coerce").astype("Int64")

    out = df.merge(web[["rank", "series_url"]],
                   left_on=left_key, right_on="rank", how="left")

    # rank_x / rank_y の後始末
    if "rank_y" in out.columns:
        out = out.drop(columns=["rank_y"])
        if "rank_x" in out.columns and left_key == "rank":
            out = out.rename(columns={"rank_x": "rank"})
        elif "rank_x" in out.columns and left_key == "rank_seq":
            # 入力のrank_seqは保持、rank_xは不要
            out = out.drop(columns=["rank_x"])

    out.to_csv(args.output, index=False)
    print(f"💾 wrote: {args.output}")


if __name__ == "__main__":
    main()
