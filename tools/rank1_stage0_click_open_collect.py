#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, time, csv, argparse
from pathlib import Path
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

def extract_series_id(url: str):
    m = re.search(r"/series/(\d+)\.html|/(\d{4,6})(?:/|$)", url)
    return m.group(1) or m.group(2) if m else ""

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--max", type=int, default=50)
    ap.add_argument("--wait-ms", type=int, default=220)
    ap.add_argument("--max-scrolls", type=int, default=220)
    ap.add_argument("--pre-wait", type=int, default=1200)
    ap.add_argument("--image-name", default="rank_full.png")
    args = ap.parse_args()

    ensure_dir(args.outdir)
    html_dir = os.path.join(args.outdir, "series_html")
    ensure_dir(html_dir)

    csv_path = os.path.join(args.outdir, "index.csv")
    img_path = os.path.join(args.outdir, args.image_name)
    html_page = os.path.join(args.outdir, "rank_page.html")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1280, "height": 900})
        page = ctx.new_page()
        print(f"[info] goto: {args.url}")

        page.goto(args.url, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(args.pre_wait)

        # progressive scroll
        for _ in range(args.max_scrolls):
            page.evaluate("window.scrollBy(0, window.innerHeight * 0.9)")
            page.wait_for_timeout(args.wait_ms)

        Path(html_page).write_text(page.content(), encoding="utf-8")
        page.screenshot(path=img_path, full_page=True)
        print(f"[info] saved screenshot: {img_path}")

        # 各車種ブロックを探す
        rows = page.query_selector_all("[data-rank-num]")
        if not rows:
            rows = page.query_selector_all("li [data-seriesid], li a[href]")
        print(f"[info] detected rows: {len(rows)}")

        results = []
        rank = 0
        for row in rows:
            if rank >= args.max:
                break
            rank += 1
            try:
                row.scroll_into_view_if_needed(timeout=3000)
            except Exception:
                pass

            popup = None
            try:
                with page.expect_popup(timeout=4000) as popinfo:
                    row.click()
                popup = popinfo.value
            except PWTimeout:
                # fallback
                href = ""
                for a in row.query_selector_all("a[href]"):
                    h = a.get_attribute("href") or ""
                    if "/series/" in h or re.match(r"^/\d+/?$", h):
                        href = h
                        break
                if href:
                    if href.startswith("//"):
                        href = "https:" + href
                    elif href.startswith("/"):
                        href = "https://www.autohome.com.cn" + href
                    popup = ctx.new_page()
                    popup.goto(href, timeout=45000, wait_until="domcontentloaded")

            if not popup:
                print(f"[warn] rank {rank}: no popup")
                continue

            popup.wait_for_load_state("domcontentloaded", timeout=15000)
            final_url = popup.url
            sid = extract_series_id(final_url)

            # HTML保存
            html = popup.content()
            fname = f"{rank:02d}_{sid or 'unknown'}.html"
            Path(os.path.join(html_dir, fname)).write_text(html, encoding="utf-8")

            results.append((rank, sid, f"https://www.autohome.com.cn/{sid}/" if sid else final_url))
            popup.close()
            page.bring_to_front()

        # 出力
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["rank", "series_id", "series_url"])
            for r, sid, url in results:
                w.writerow([r, sid, url])

        print(f"[ok] collected {len(results)} links -> {csv_path}")
        ctx.close()
        browser.close()

if __name__ == "__main__":
    main()
