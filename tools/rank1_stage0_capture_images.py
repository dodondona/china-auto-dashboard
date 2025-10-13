#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ランキングページを連続スクショで“タイル化保存”するだけの下ごしらえスクリプト。
- DOM解析・リンク抽出・HTML保存は一切しない
- 人の目に見えるとおりの画面をそのままPNGで保存
- 仮想リスト対策として段階スクロールしながら撮影

出力例:
  outdir/
    tile_001.png
    tile_002.png
    ...
    meta.txt  (撮影条件メモ)
"""

import argparse
import os
import time
from playwright.sync_api import sync_playwright, Browser, Page

def _save(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def capture_tiles(page: Page, outdir: str, tile_height: int, max_tiles: int, wait_ms: int, stride_ratio: float):
    os.makedirs(outdir, exist_ok=True)
    for i in range(1, max_tiles + 1):
        png_path = os.path.join(outdir, f"tile_{i:03d}.png")
        page.screenshot(path=png_path, full_page=False)
        page.evaluate(f"() => window.scrollBy(0, Math.floor(window.innerHeight * {stride_ratio}))")
        page.wait_for_timeout(wait_ms)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--viewport-w", type=int, default=1280)
    ap.add_argument("--tile-height", type=int, default=900)
    ap.add_argument("--max-tiles", type=int, default=12)
    ap.add_argument("--wait-ms", type=int, default=350)
    ap.add_argument("--pre-wait", type=int, default=1500)
    ap.add_argument("--stride-ratio", type=float, default=0.90)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    # 👇 ここを修正：args.viewport-w → args.viewport_w
    meta = (
        f"url={args.url}\n"
        f"viewport=({args.viewport_w}x{args.tile_height})\n"
        f"max_tiles={args.max_tiles}\n"
        f"wait_ms={args.wait_ms}\n"
        f"pre_wait={args.pre_wait}\n"
        f"stride_ratio={args.stride_ratio}\n"
        f"timestamp={int(time.time())}\n"
    )
    _save(os.path.join(args.outdir, "meta.txt"), meta)

    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            locale="zh-CN",
            viewport={"width": args.viewport_w, "height": args.tile_height}
        )
        page = ctx.new_page()
        page.set_default_timeout(45000)

        page.goto(args.url, wait_until="domcontentloaded")
        page.wait_for_timeout(args.pre_wait)
        page.evaluate("() => window.scrollTo(0, 0)")
        page.wait_for_timeout(300)

        capture_tiles(
            page,
            outdir=args.outdir,
            tile_height=args.tile_height,
            max_tiles=args.max_tiles,
            wait_ms=args.wait_ms,
            stride_ratio=args.stride_ratio
        )

        ctx.close()
        browser.close()

if __name__ == "__main__":
    main()
