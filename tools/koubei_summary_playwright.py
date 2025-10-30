#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, re, json, time
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

"""
Usage:
  python tools/koubei_summary_playwright.py <series_id> <pages>

方針:
- 一覧抽出は従来通り（正規表現＋左カラム優先）
- 詳細ページは JS描画後のDOMを page.content() で取得
- 本文は div.tw-whitespace-pre-wrap（現行Tailwind構造）から抽出
- キャッシュ構造やJSON出力形式は完全維持
"""

DETAIL_URL = "https://k.autohome.com.cn/detail/view_{reviewid}.html"

def build_list_url(series_id: str, page: int) -> str:
    if page == 1:
        return f"https://k.autohome.com.cn/{series_id}#pvareaid=3454440"
    else:
        return f"https://k.autohome.com.cn/{series_id}/index_{page}.html?#listcontainer"

# ---------- 一覧抽出（変更なし） ----------
ID_PAT = re.compile(r"/detail/view_([A-Za-z0-9]+)(?:\\.html|\\.)")

def extract_review_ids_from_html(html: str):
    soup = BeautifulSoup(html, "lxml")
    left = soup.select_one(".con-left")
    if not left:
        return []
    ids = []
    for a in left.find_all("a", href=True):
        m = ID_PAT.search(a["href"])
        if m:
            ids.append(m.group(1))
    return list(dict.fromkeys(ids))

# ---------- 詳細取得（Tailwind構造対応） ----------
def fetch_detail_into_cache(pw, reviewid: str, cache_dir: Path) -> None:
    cache_file = cache_dir / f"{reviewid}.json"
    if cache_file.exists():
        return

    url = DETAIL_URL.format(reviewid=reviewid)
    print(f"  fetching detail {url}")

    def _once() -> dict | None:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"})
        page.set_viewport_size({"width": 1280, "height": 1800})
        try:
            page.goto(url, wait_until="networkidle", timeout=60000)

            # 展开全文クリック（あれば）
            try:
                page.get_by_text("展开全文", exact=False).click(timeout=2000)
            except Exception:
                pass

            # 本文DOM出現を待つ（Tailwind構造）
            try:
                page.wait_for_selector("div.tw-whitespace-pre-wrap", timeout=15000)
            except PWTimeout:
                pass

            html = page.content()
            soup = BeautifulSoup(html, "lxml")

            # タイトル
            title = ""
            h1 = soup.select_one("h1")
            if h1:
                title = h1.get_text(strip=True)
            if not title:
                t = soup.find("title")
                if t:
                    title = t.get_text(strip=True)

            # 本文（Tailwind対応）
            body = ""
            cont = soup.select_one("div.tw-whitespace-pre-wrap")
            if cont:
                body = cont.get_text("\n", strip=True)

            # 保険：旧構造(div.contentなど)
            if not body:
                for sel in ["div.content", "section.content", "article", "div#content"]:
                    n = soup.select_one(sel)
                    if n:
                        body = n.get_text("\n", strip=True)
                        if body:
                            break

            return {"id": reviewid, "url": url, "title": title, "text": body}

        finally:
            page.close()
            browser.close()

    data = _once()
    if not data or not data.get("text"):
        time.sleep(1.0)
        data = _once()

    if not data:
        data = {"id": reviewid, "url": url, "title": "", "text": ""}

    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# ---------- main ----------
def main():
    if len(sys.argv) < 3:
        print("Usage: python tools/koubei_summary_playwright.py <series_id> <pages>")
        sys.exit(1)
    series_id = sys.argv[1]
    pages = int(sys.argv[2])
    cache_dir = Path("cache") / series_id
    out_file = Path(f"autohome_reviews_{series_id}.jsonl")

    all_ids = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        for i in range(1, pages + 1):
            url = build_list_url(series_id, i)
            print(f"[page {i}] fetching…")
            try:
                page.goto(url, wait_until="networkidle", timeout=60000)
                html = page.content()
                ids = extract_review_ids_from_html(html)
                print(f"[page {i}] found {len(ids)} reviews")
                all_ids.extend(ids)
            except Exception as e:
                print(f"[page {i}] error: {e}")
            time.sleep(1)
        browser.close()

        print(f"[done] parsed {len(all_ids)} reviews")

        for rid in all_ids:
            fetch_detail_into_cache(pw, rid, cache_dir)
            time.sleep(0.5)

    with open(out_file, "w", encoding="utf-8") as f:
        for rid in all_ids:
            cf = cache_dir / f"{rid}.json"
            if cf.exists():
                f.write(cf.read_text(encoding="utf-8").strip() + "\n")

if __name__ == "__main__":
    main()
