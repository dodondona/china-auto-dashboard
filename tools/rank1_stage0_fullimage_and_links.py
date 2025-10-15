# tools/rank1_stage0_fullimage_and_links.py
import json
import time
import os
import argparse
from pathlib import Path
from playwright.sync_api import sync_playwright

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--max-scrolls", type=int, default=200)
    parser.add_argument("--wait-ms", type=int, default=200)
    parser.add_argument("--image-name", default="rank_full.png")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    html_path = os.path.join(args.outdir, "rank_page.html")
    img_path = os.path.join(args.outdir, args.image_name)
    csv_path = os.path.join(args.outdir, "index.csv")
    json_path = os.path.join(args.outdir, "captured_api.json")

    captured = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        def handle_response(resp):
            if "rank/series/ranklist" in resp.url and resp.status == 200:
                try:
                    data = resp.json()
                    captured.append(data)
                except Exception:
                    pass

        page.on("response", handle_response)

        print(f"[info] Navigating to {args.url}")
        page.goto(args.url, wait_until="networkidle")
        for i in range(0, args.max_scrolls, 20):
            page.evaluate(f"window.scrollBy(0, document.body.scrollHeight)")
            time.sleep(args.wait_ms / 1000)
            print(f"scroll {i}/{args.max_scrolls}")

        Path(html_path).write_text(page.content(), encoding="utf-8")
        page.screenshot(path=img_path, full_page=True)
        print(f"[info] Saved HTML snapshot: {html_path}")
        print(f"[info] Saved full screenshot: {img_path}")

        # JSON保存
        Path(json_path).write_text(json.dumps(captured, ensure_ascii=False, indent=2), encoding="utf-8")

        # seriesId抽出
        links = []
        for block in captured:
            if not block:
                continue
            series_list = block.get("data", {}).get("series", [])
            for s in series_list:
                sid = s.get("seriesId")
                if sid:
                    links.append(f"https://www.autohome.com.cn/{sid}/")

        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("rank,series_url\n")
            for i, link in enumerate(links, 1):
                f.write(f"{i},{link}\n")

        print(f"[debug] Collected {len(links)} links")
        browser.close()

if __name__ == "__main__":
    main()
