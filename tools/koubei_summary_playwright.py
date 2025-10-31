#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, os, re, time, json
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# =====================================================
# ユーティリティ
# =====================================================
def decode_html(body: bytes) -> str:
    try:
        return body.decode("utf-8", errors="ignore")
    except Exception:
        try:
            return body.decode("gb18030", errors="ignore")
        except Exception:
            return body.decode("latin1", errors="ignore")


def save_json(obj, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def save_text(text, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


# =====================================================
# HTML解析: ここだけ修正版
# =====================================================
def parse_detail_html_bytes(body: bytes) -> dict:
    html = decode_html(body)
    soup = BeautifulSoup(html, "lxml")

    title = ""
    t = soup.find("title")
    if t:
        title = re.sub(r"_口碑_汽车之家.*", "", t.get_text(strip=True))

    # === 新レイアウト: 本文は p.kb-item-msg に格納 ===
    # 例: div.kb-con > div.kb-item > p.kb-item-msg
    nodes = soup.select(".kb-con .kb-item .kb-item-msg, .kb-item-msg")
    if nodes:
        text_blocks = [n.get_text(" ", strip=True) for n in nodes]
    else:
        # 旧レイアウトフォールバック（既存順序維持）
        text_blocks = [p.get_text(" ", strip=True) for p in soup.select(".text-con p")]
        if not text_blocks:
            for css in [
                ".koubei-txt p",
                ".mouthcon-text p",
                ".text-con",
                ".koubei-txt",
                ".mouthcon-text",
                "article",
            ]:
                nodes = soup.select(css)
                if nodes:
                    if css.endswith(" p"):
                        text_blocks = [p.get_text(" ", strip=True) for p in nodes]
                    else:
                        text_blocks = [" ".join(n.get_text(" ", strip=True) for n in nodes)]
                    break

    text = "\n".join([s for s in text_blocks if s]).strip()
    text = re.sub(r"\s+", " ", text)
    return {"title": title, "text": text}


# =====================================================
# ページ取得と解析
# =====================================================
def fetch_reviews_for_series(playwright, series_id: str, max_pages: int):
    base_url = f"https://k.autohome.com.cn/{series_id}"
    results = []
    with playwright.chromium.launch(headless=True) as browser:
        context = browser.new_context(locale="zh-CN")
        page = context.new_page()
        for page_num in range(1, max_pages + 1):
            url = f"{base_url}/index_{page_num}.html#listcontainer" if page_num > 1 else base_url
            print(f"[page {page_num}] fetching…")
            page.goto(url, wait_until="networkidle", timeout=60000)
            html = page.content()
            soup = BeautifulSoup(html, "lxml")
            items = soup.select(".mouthcon-cont .mouthcon-cont-right, .kb-list .kb-item")
            print(f"[page {page_num}] found {len(items)} reviews")
            for it in items:
                a = it.select_one("a")
                if not a or not a.get("href"):
                    continue
                link = a["href"]
                if not link.startswith("http"):
                    link = "https://k.autohome.com.cn" + link
                results.append(link)
        context.close()
    return results


# =====================================================
# メイン
# =====================================================
def main(series_id: str, pages: int):
    outdir = Path(f"autohome_reviews_{series_id}")
    outdir.mkdir(exist_ok=True)
    all_reviews = []

    with sync_playwright() as p:
        urls = fetch_reviews_for_series(p, series_id, pages)
        print(f"[done] collected {len(urls)} review links")
        context = p.chromium.launch(headless=True).new_context(locale="zh-CN")
        page = context.new_page()
        for i, url in enumerate(urls, 1):
            print(f"[{i}/{len(urls)}] parsing {url}")
            try:
                page.goto(url, wait_until="networkidle", timeout=60000)
                body = page.content().encode("utf-8", errors="ignore")
                data = parse_detail_html_bytes(body)
                data["url"] = url
                all_reviews.append(data)
            except Exception as e:
                print("Error:", e)
                continue
        context.close()

    # 保存
    save_json(all_reviews, outdir / f"reviews_{series_id}.json")
    text_concat = "\n\n".join([f"{r['title']}\n{r['text']}" for r in all_reviews])
    save_text(text_concat, outdir / f"reviews_{series_id}.txt")
    print(f"[done] parsed {len(all_reviews)} reviews")


# =====================================================
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python koubei_summary_playwright.py <series_id> <pages>")
        sys.exit(1)
    series_id = sys.argv[1]
    pages = int(sys.argv[2])
    main(series_id, pages)
