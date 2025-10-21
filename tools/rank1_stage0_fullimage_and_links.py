#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# autohomeランキングを1枚キャプチャ＋seriesId抽出 (簡易版)

import os, re, csv, json, argparse
from playwright.sync_api import sync_playwright

BASE = "https://www.autohome.com.cn"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--image-name", default="rank_full.png")
    ap.add_argument("--pre-wait", type=int, default=1500)
    ap.add_argument("--wait-ms", type=int, default=300)
    ap.add_argument("--max-scrolls", type=int, default=200)
    a = ap.parse_args()

    os.makedirs(a.outdir, exist_ok=True)
    captured = []

    with sync_playwright() as p:
        b = p.chromium.launch(headless=True, args=["--no-sandbox","--lang=zh-CN"])
        ctx = b.new_context(locale="zh-CN", viewport={"width":1280,"height":900}, device_scale_factor=2)
        page = ctx.new_page()

        def grab(resp):
            try:
                if "application/json" in (resp.headers.get("content-type") or ""):
                    u = resp.url
                    if any(k in u for k in ("rank","series","config","list","car")):
                        captured.append(resp.json())
            except: pass
        page.on("response", grab)

        page.goto(a.url, wait_until="domcontentloaded")
        page.wait_for_timeout(a.pre_wait)
        for _ in range(a.max_scrolls):
            page.evaluate("() => window.scrollBy(0, window.innerHeight*0.85)")
            page.wait_for_timeout(a.wait_ms)
        page.wait_for_timeout(800)

        # トップに戻してから、上部だけ長めに撮るために一時的にビューポートを拡張
        page.evaluate("() => window.scrollTo(0, 0)")
        page.wait_for_function("() => window.scrollY === 0")
        page.wait_for_timeout(400)

        scroll_height = page.evaluate("() => document.body.scrollHeight")
        capture_height = min(scroll_height, 12000)  # 目安: 100位前後
        page.set_viewport_size({"width": 1280, "height": capture_height})
        page.screenshot(path=os.path.join(a.outdir, a.image_name), full_page=False)

        # 保存
        with open(os.path.join(a.outdir,"page.html"),"w",encoding="utf-8") as f: f.write(page.content())
        for i,d in enumerate(captured,1):
            with open(os.path.join(a.outdir,f"resp_{i:02d}.json"),"w",encoding="utf-8") as f: json.dump(d,f,ensure_ascii=False,indent=2)

        # seriesId抽出
        s=set()
        for d in captured:
            for sid in re.findall(r'["\']seriesi?d["\']\s*:\s*(\d+)', json.dumps(d), flags=re.I): s.add(sid)
        with open(os.path.join(a.outdir,"index.csv"),"w",newline="",encoding="utf-8-sig") as f:
            w=csv.writer(f); w.writerow(["rank_seq","series_id","series_url"])
            for i,sid in enumerate(sorted(s,key=int),1): w.writerow([i,sid,f"{BASE}/series/{sid}.html"])

        print(f"[ok] {len(s)} series captured → {a.outdir}")
        ctx.close(); b.close()

if __name__=="__main__":
    main()
