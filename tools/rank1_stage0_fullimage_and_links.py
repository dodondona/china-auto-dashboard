#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
目的:
- ランキングページを“分割せず”に超縦長1枚でキャプチャ (full_page PNG)
- 同時に Playwright の network から JSON レスポンスを横取りして seriesId を抽出 (構成変更に強い)
- 取得した seriesId を index.csv として保存

最小主義:
- 既存の他スクリプト/ワークフローは変更しない
- 本スクリプト単体で完結 (出力先 outdir のみ)

出力 (outdir):
  - rank_full.png            : ランキングページの超縦長スクショ (分割なし)
  - captured_json/resp_XX.json : 捕捉したJSONレスポンス一式 (デバッグ/保険)
  - index.csv                : 抽出した (rank, series_id, series_url)  ※rankは未知のため連番
  - page.html                : 最終時点のHTML (参考)

使い方例:
  python tools/rank1_stage0_fullimage_and_links.py \
    --url "https://www.autohome.com.cn/rank/1" \
    --outdir "data/rank1_full" \
    --pre-wait 1500 \
    --wait-ms 300 \
    --max-scrolls 220 \
    --image-name "rank_full.png"
"""

import argparse
import csv
import json
import os
import re
import time
from typing import List, Tuple
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, Browser, Page

ABS_BASE = "https://www.autohome.com.cn"

def _abs_url(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    if u.startswith("//"):  return "https:" + u
    if u.startswith("/"):   return ABS_BASE + u
    if u.startswith("http"):return u
    return urljoin(ABS_BASE + "/", u)

def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def _save_text(path: str, text: str):
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def _save_json(path: str, data):
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _progressive_scroll(page: Page, wait_ms: int, max_scrolls: int):
    """
    仮想リスト対策: 0.85×ビューポートずつ段階スクロールして
    IntersectionObserver を確実に発火 → 全要素を一度は可視化させる
    """
    last_h = 0
    for i in range(max_scrolls):
        page.evaluate("() => window.scrollBy(0, Math.floor(window.innerHeight * 0.85))")
        page.wait_for_timeout(wait_ms)
        new_h = page.evaluate("() => document.body.scrollHeight")
        # 高さが伸びなくなってきたら軽く待つ
        if new_h == last_h and i > 10:
            page.wait_for_timeout(500)
        last_h = new_h

def _collect_series_from_json_blobs(blobs: List[dict]) -> List[str]:
    """
    受け取ったJSONブロブ群から seriesId を網羅的に抽出
    - キー名のバリエーションや入れ子を考慮し、json.dumps → 正規表現で拾う
    """
    ids = set()
    for blob in blobs:
        try:
            js = json.dumps(blob, ensure_ascii=False)
            # "seriesid": 7806  /  "seriesId": 7806  /  'seriesId':7806 など幅広く
            for sid in re.findall(r'["\']seriesi?d["\']\s*:\s*(\d+)', js, flags=re.I):
                ids.add(sid)
        except Exception:
            pass
    return sorted(ids, key=lambda x: int(x))

def _to_series_links(series_ids: List[str]) -> List[Tuple[int, str, str]]:
    """
    rank はランキング順を保証できないため、ここでは 1..N の連番を割り当てる。
    後段 (OCR/LLM) で順位は画像から確定させる運用。
    """
    out = []
    for i, sid in enumerate(series_ids, start=1):
        url = f"{ABS_BASE}/series/{sid}.html"
        out.append((i, sid, url))
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--image-name", default="rank_full.png")
    ap.add_argument("--pre-wait", type=int, default=1500)   # 初回ロード後の待機
    ap.add_argument("--wait-ms", type=int, default=300)     # スクロール間の待機
    ap.add_argument("--max-scrolls", type=int, default=220) # スクロール最大回数
    args = ap.parse_args()

    _ensure_dir(args.outdir)
    captured_json = []  # (url, data) 群

    with sync_playwright() as p:
        # フォントはワークフローでインストールする想定（Noto CJK等）
        browser: Browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--lang=zh-CN"])
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            locale="zh-CN",
            viewport={"width": 1280, "height": 900},  # ここは固定（分割しないのでOK）
            device_scale_factor=2                      # 文字をくっきり (任意だが有効)
        )
        page: Page = ctx.new_page()
        page.set_default_timeout(45000)

        # ネットワーク横取り (C)
        def _grab(resp):
            try:
                ct = (resp.headers.get("content-type") or "").lower()
                url = resp.url
                # ランク/シリーズ/コンフィグ関連の JSON を広めに捕捉
                if "application/json" in ct and any(k in url for k in ("rank", "series", "config", "list", "car")):
                    data = resp.json()
                    captured_json.append({"url": url, "data": data})
            except Exception:
                pass

        page.on("response", _grab)

        # 1) 初回ロード（domcontentloaded を優先）
        page.goto(args.url, wait_until="domcontentloaded")
        page.wait_for_timeout(args.pre_wait)

        # 2) 上から段階スクロールして“全要素を一度は可視化”させる
        page.evaluate("() => window.scrollTo(0, 0)")
        page.wait_for_timeout(300)
        _progressive_scroll(page, wait_ms=args.wait_ms, max_scrolls=args.max_scrolls)

        # 3) 念のため最下端へ & 少し待つ（画像/遅延ロードの取りこぼし防止）
        page.evaluate("() => window.scrollTo(0, Math.max(0, document.body.scrollHeight - window.innerHeight))")
        page.wait_for_timeout(800)

        # 4) フルページ 1枚キャプチャ（分割なし）
        full_png = os.path.join(args.outdir, args.image_name)
        page.screenshot(path=full_png, full_page=True)

        # 5) JSON, HTML の保存
        if captured_json:
            for i, blob in enumerate(captured_json, start=1):
                _save_json(os.path.join(args.outdir, "captured_json", f"resp_{i:02d}.json"), blob)
        _save_text(os.path.join(args.outdir, "page.html"), page.content())

        # 6) JSONから seriesId を抽出 → index.csv に保存
        series_ids = _collect_series_from_json_blobs([c["data"] for c in captured_json])
        links = _to_series_links(series_ids)
        with open(os.path.join(args.outdir, "index.csv"), "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["rank_seq", "series_id", "series_url"])
            for r, sid, url in links:
                w.writerow([r, sid, url])

        print(f"[ok] saved full image + {len(links)} series links to: {args.outdir}")

        ctx.close()
        browser.close()

if __name__ == "__main__":
    main()
