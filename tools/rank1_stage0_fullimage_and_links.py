# tools/rank1_stage0_fullimage_and_links.py
# =========================================================
# Autohome月間ランキングページのフル画像キャプチャ＋車両リンク収集
# =========================================================

import time
import json
import os
from pathlib import Path
from playwright.sync_api import sync_playwright

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--max", type=int, default=50)
    parser.add_argument("--wait-ms", type=int, default=200)
    parser.add_argument("--max-scrolls", type=int, default=2200)
    parser.add_argument("--image-name", default="rank_full.png")
    parser.add_argument("--pre-wait", type=int, default=1500)
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    out_img = os.path.join(args.outdir, args.image_name)
    out_json = os.path.join(args.outdir, "debug_links.json")
    out_csv = os.path.join(args.outdir, "index.csv")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1200, "height": 1600})
        print(f"[info] Navigating to {args.url}")
        page.goto(args.url, wait_until="networkidle")
        time.sleep(args.pre_wait / 1000)

        # ===== スクロール処理 =====
        total_scrolls = args.max_scrolls
        for i in range(0, total_scrolls, 10):
            page.evaluate("window.scrollBy(0, 300)")
            time.sleep(args.wait_ms / 1000)
            if i % 200 == 0:
                print(f"scroll {i}/{total_scrolls}")

        # ===== ページHTMLを保存 =====
        html_path = os.path.join(args.outdir, "rank_page.html")
        Path(html_path).write_text(page.content(), encoding="utf-8")
        print(f"[info] Saved HTML snapshot to {html_path}")

        # ===== スクリーンショット保存 =====
        page.screenshot(path=out_img, full_page=True)
        print(f"[info] Saving full screenshot: {out_img}")

        # ===== 車両リンク収集（data-seriesid対応） =====
        anchors = page.query_selector_all("a[data-seriesid]")
        links = []
        for a in anchors:
            href = a.get_attribute("href")
            sid = a.get_attribute("data-seriesid")
            if href and href.startswith("https://www.autohome.com.cn/"):
                links.append(href)
            elif sid:
                links.append(f"https://www.autohome.com.cn/{sid}/")

        print(f"[debug] Collected {len(links)} links")

        # ===== 保存 =====
        json.dump(links, open(out_json, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        with open(out_csv, "w", encoding="utf-8") as f:
            f.write("rank,series_url\n")
            for i, link in enumerate(links, 1):
                f.write(f"{i},{link}\n")

        if len(links) == 0:
            raise SystemExit("No series links collected.")

        browser.close()


if __name__ == "__main__":
    main()
