#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, re, json
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

"""
Usage:
  python tools/koubei_summary_playwright.py <series_id> <pages>
Description:
  一覧ページから reviewid を収集し、個別の詳細ページ(view_<id>.html)にアクセスして
  タイトルと本文（テキストのみ）を抽出。
  cache/<series_id>/<review_id>.json に保存（既存はスキップ）。
  取得結果は autohome_reviews_<series_id>.zip に固める（artifact用）。
"""

DETAIL_URL = "https://k.autohome.com.cn/detail/view_{reviewid}.html"

def build_list_url(series_id: str, page: int) -> str:
    # 1ページ目と2ページ目以降でURLが異なる
    if page == 1:
        return f"https://k.autohome.com.cn/{series_id}#pvareaid=3454440"
    else:
        return f"https://k.autohome.com.cn/{series_id}/index_{page}.html?#listcontainer"

def extract_detail_text(html: str):
    soup = BeautifulSoup(html, "lxml")
    title = ""
    t = soup.find("title")
    if t:
        title = re.sub(r"_口碑_汽车之家.*", "", t.get_text(strip=True))
    text_blocks = [p.get_text(" ", strip=True) for p in soup.select(".text-con p")]
    if not text_blocks:
        text_blocks = [soup.get_text(" ", strip=True)]
    text = "\n".join(text_blocks)
    text = re.sub(r"\s+", " ", text).strip()
    return {"title": title, "text": text}

def fetch_detail(playwright, reviewid: str, cache_dir: Path):
    cache_file = cache_dir / f"{reviewid}.json"
    if cache_file.exists():
        return
    url = DETAIL_URL.format(reviewid=reviewid)
    print(f"  fetching detail {url}")
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto(url, wait_until="networkidle", timeout=30000)
    html = page.content()
    data = extract_detail_text(html)
    data["id"] = reviewid
    data["url"] = url
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    browser.close()

def extract_review_ids(html: str):
    soup = BeautifulSoup(html, "lxml")
    ids = set()
    # ★ 新方式: 詳細ページリンクから抽出（旧data-reviewidも併用）
    for a in soup.select('a[href*="/detail/view_"]'):
        href = a.get("href") or ""
        m = re.search(r"/detail/view_([A-Za-z0-9]+)\.html", href)
        if m:
            ids.add(m.group(1))
    for li in soup.select("li[data-reviewid]"):
        rid = li.get("data-reviewid")
        if rid:
            ids.add(rid)
    return list(ids)

def main(series_id: str, pages: int):
    cache_dir = Path("cache") / series_id
    cache_dir.mkdir(parents=True, exist_ok=True)

    all_ids = set()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        for i in range(1, pages + 1):
            url = build_list_url(series_id, i)
            print(f"[page {i}] fetching… {url}")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_selector('a[href*="/detail/view_"]', timeout=20000)
            except Exception as e:
                print(f"  !! timeout or load error on page {i}: {e}")
                continue
            html = page.content()
            ids = extract_review_ids(html)
            print(f"[page {i}] found {len(ids)} reviews")
            all_ids.update(ids)
        browser.close()

        print(f"[total] unique reviews: {len(all_ids)}")
        for rid in sorted(all_ids):
            try:
                fetch_detail(p, rid, cache_dir)
            except Exception as e:
                print(f"  !! failed {rid}: {e}")

    import shutil
    zipname = f"autohome_reviews_{series_id}"
    shutil.make_archive(zipname, "zip", cache_dir)
    print(f"[done] cached and zipped -> {zipname}.zip")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python tools/koubei_summary_playwright.py <series_id> <pages>")
        sys.exit(1)
    series_id = sys.argv[1].strip()
    pages = int(sys.argv[2])
    main(series_id, pages)
