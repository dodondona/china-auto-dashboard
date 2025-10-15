#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import os
import re
from typing import List, Tuple, Dict
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, Browser, Page

ABS_BASE = "https://www.autohome.com.cn"
SERIES_HREF_RE = re.compile(r"(?:/series/(\d+)\.html)|(?:/(\d+)/?)$", re.I)

def _abs_url(u: str) -> str:
    if not u: return ""
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
    last_h = 0
    for _ in range(max_scrolls):
        page.evaluate("() => window.scrollBy(0, Math.floor(window.innerHeight * 0.85))")
        page.wait_for_timeout(wait_ms)
        new_h = page.evaluate("() => document.body.scrollHeight")
        if new_h == last_h:
            page.wait_for_timeout(400)
        last_h = new_h

def _series_id_from_href(href: str) -> str:
    m = SERIES_HREF_RE.search(href or "")
    if not m: return ""
    sid = m.group(1) or m.group(2) or ""
    return sid if (sid and sid.isdigit()) else ""

def _extract_rank_link_pairs(page: Page) -> List[Tuple[int, str]]:
    """
    1行=1車種の [data-rank-num] を走査し、
    行内の a[href] から **seriesページ** だけを厳格に抽出して (rank, href) を返す。
    """
    pairs = page.evaluate(
        """
        () => {
          const isSeries = (h) => {
            if (!h) return false;
            try {
              const u = h.trim();
              if (/^https?:/.test(u)) return /\\/series\\/\\d+\\.html/.test(u) || /\\/\\d+\\/?$/.test(new URL(u).pathname);
              if (u.startsWith('/')) return /\\/series\\/\\d+\\.html/.test(u) || /\\/\\d+\\/?$/.test(u);
              return false;
            } catch { return false; }
          };

          const rows = Array.from(document.querySelectorAll('[data-rank-num]'));
          const out = [];
          for (const row of rows) {
            const rStr = row.getAttribute('data-rank-num') || '';
            const r = parseInt(rStr, 10);
            if (!Number.isFinite(r)) continue;

            // 行内の a[href] を全列挙 → seriesパターンのみ残す
            const as = Array.from(row.querySelectorAll('a[href]'))
              .map(a => a.getAttribute('href') || '')
              .filter(h => isSeries(h));

            if (as.length === 0) continue;

            // 優先順位: /series/xxxx.html を最優先、なければ /xxxx/ を採用
            let chosen = as.find(h => /\\/series\\/\\d+\\.html/.test(h)) || as[0];
            out.push([r, chosen]);
          }
          return out;
        }
        """
    ) or []

    # 絶対URL化＆rank型整形
    norm: List[Tuple[int, str]] = []
    for r, href in pairs:
        try:
            r = int(r)
        except:
            continue
        norm.append((r, _abs_url(href)))
    return norm

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--image-name", default="rank_full.png")
    ap.add_argument("--pre-wait", type=int, default=1500)
    ap.add_argument("--wait-ms", type=int, default=300)
    ap.add_argument("--max-scrolls", type=int, default=220)
    args = ap.parse_args()

    _ensure_dir(args.outdir)
    captured_json = []

    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--lang=zh-CN"])
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
            locale="zh-CN",
            viewport={"width": 1280, "height": 900},
            device_scale_factor=2
        )
        page: Page = ctx.new_page()
        page.set_default_timeout(45000)

        # ネットワークJSONは保険として保存（順位付けには使わない）
        def _grab(resp):
            try:
                ct = (resp.headers.get("content-type") or "").lower()
                url = resp.url
                if "application/json" in ct and any(k in url for k in ("rank", "series", "config", "list", "car")):
                    data = resp.json()
                    captured_json.append({"url": url, "data": data})
            except:  # noqa
                pass
        page.on("response", _grab)

        page.goto(args.url, wait_until="domcontentloaded")
        page.wait_for_timeout(args.pre_wait)
        page.evaluate("() => window.scrollTo(0, 0)")
        page.wait_for_timeout(300)

        # スクロールしながら描画を発火させる
        _progressive_scroll(page, args.wait_ms, args.max_scrolls)
        # 念のため最下端へ
        page.evaluate("() => window.scrollTo(0, Math.max(0, document.body.scrollHeight - window.innerHeight))")
        page.wait_for_timeout(600)
        # もう一度トップへ（可視判定に依らず全行がDOM上に残っているケースに対応）
        page.evaluate("() => window.scrollTo(0, 0)")
        page.wait_for_timeout(300)

        # ★ ここで最終的に rank→seriesリンク を収集
        pairs = _extract_rank_link_pairs(page)

        # フルページ1枚キャプチャ
        page.screenshot(path=os.path.join(args.outdir, args.image_name), full_page=True)

        # デバッグ用保存
        if captured_json:
            for i, blob in enumerate(captured_json, start=1):
                _save_json(os.path.join(args.outdir, "captured_json", f"resp_{i:02d}.json"), blob)
        _save_text(os.path.join(args.outdir, "page.html"), page.content())

        # rank順に整列し、series_id を付与して CSV
        rank2href: Dict[int, str] = {}
        for r, href in pairs:
            # 重複は最初の検出を優先
            if r not in rank2href:
                rank2href[r] = href

        rows: List[Tuple[int, str, str]] = []
        for r in sorted(rank2href.keys()):
            href = rank2href[r]
            sid = _series_id_from_href(href)
            if not sid:
                continue
            rows.append((r, sid, _abs_url(href)))

        # 1..N 連番になっているか簡易チェック（欠番があればログ）
        if rows:
            ranks = [r for r, _, _ in rows]
            missing = [x for x in range(1, max(ranks)+1) if x not in ranks]
            if missing:
                print(f"[warn] missing ranks detected: {missing}")

        with open(os.path.join(args.outdir, "index.csv"), "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["rank", "series_id", "series_url"])
            for r, sid, url in rows:
                w.writerow([r, sid, url])

        print(f"[ok] mapped {len(rows)} rank→series links (sorted by rank) -> {os.path.join(args.outdir, 'index.csv')}")

        ctx.close()
        browser.close()

if __name__ == "__main__":
    main()
