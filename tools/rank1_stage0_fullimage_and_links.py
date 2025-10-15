# tools/rank1_stage0_fullimage_and_links.py
# === Autohome rank list full capture + series links (with --pre-wait restored) ===

import os
import csv
import time
import argparse
from playwright.sync_api import sync_playwright

def scroll_page(page, max_scrolls=200, wait_ms=220):
    """段階的にスクロールして全要素を読み込む"""
    for i in range(max_scrolls):
        page.evaluate("window.scrollBy(0, document.body.scrollHeight / 10)")
        print(f"scroll {i*10}/{max_scrolls*10}")
        time.sleep(wait_ms / 1000)

def collect_links(page):
    """ランキングページから車系リンクを取得"""
    links = set()

    # パターン1（PCページ）
    anchors = page.query_selector_all("a[href*='/auto/'], a[href*='/spec/'], a[href*='/series/']")
    for a in anchors:
        href = a.get_attribute("href")
        if href and href.startswith("https://www.autohome.com.cn/") and "series" in href:
            links.add(href.split("#")[0])

    # パターン2（モバイル or 動的読み込み）
    if not links:
        anchors = page.query_selector_all("a[href]")
        for a in anchors:
            href = a.get_attribute("href")
            if href and "series" in href:
                if href.startswith("//"):
                    href = "https:" + href
                elif href.startswith("/"):
                    href = "https://www.autohome.com.cn" + href
                links.add(href.split("#")[0])

    print(f"[debug] Collected {len(links)} links")
    return sorted(links)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--max", type=int, default=50)
    parser.add_argument("--wait-ms", type=int, default=220)
    parser.add_argument("--max-scrolls", type=int, default=220)
    parser.add_argument("--pre-wait", type=int, default=1500)  # ← 復活
    parser.add_argument("--image-name", default="rank_full.png")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    out_img = os.path.join(args.outdir, args.image_name)
    out_csv = os.path.join(args.outdir, "index.csv")
    out_html = os.path.join(args.outdir, "rank_page.html")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1500, "height": 2400})

        print(f"[info] Navigating to {args.url}")
        page.goto(args.url, timeout=90000)

        # ここで pre-wait
        time.sleep(args.pre_wait / 1000)

        scroll_page(page, args.max_scrolls, args.wait_ms)

        print(f"[info] Saving full screenshot: {out_img}")
        page.screenshot(path=out_img, full_page=True)

        html = page.content()
        with open(out_html, "w", encoding="utf-8") as f:
            f.write(html)

        links = collect_links(page)
        if not links:
            print("No series links collected.")
            browser.close()
            exit(1)

        links = links[:args.max]
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["rank", "series_url"])
            for i, link in enumerate(links, start=1):
                writer.writerow([i, link])

        print(f"[info] Saved {len(links)} links → {out_csv}")
        browser.close()

if __name__ == "__main__":
    main()
