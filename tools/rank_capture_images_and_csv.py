# -*- coding: utf-8 -*-
"""
Autohome ランキングを取得し、確実に image_url を埋める版
- スクロール完了後に待機を入れて lazy-load 解決
- 画像URLは data-src/data-original/srcset/src の優先順で抽出
- それでも空なら詳細ページ(シリーズURL)の og:image をフォールバック取得
- 出力: public/autohome_ranking_with_image_urls.csv

依存:
  pip install playwright pandas
  python -m playwright install chromium
"""

import asyncio
import re
import time
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

RANK_URL = "https://www.autohome.com.cn/rank/1"
TARGET_COUNT = 100  # 100位まで
OUT_CSV = Path("public/autohome_ranking_with_image_urls.csv")

# ---- ユーティリティ ----

def parse_srcset(srcset: str) -> str:
    """srcset から最大解像度の URL を返す"""
    if not srcset:
        return ""
    # "url1 1x, url2 2x" or "url1 320w, url2 640w"
    cand = []
    for part in srcset.split(","):
        part = part.strip()
        if not part:
            continue
        m = re.match(r"(\S+)\s+(\d+)(w|x)", part)
        if m:
            url = m.group(1)
            val = int(m.group(2))
            cand.append((val, url))
        else:
            # 単独URLだけのケース
            if part.startswith("http"):
                cand.append((1, part.split()[0]))
    if not cand:
        return ""
    cand.sort(key=lambda x: x[0], reverse=True)
    return cand[0][1]

async def get_og_image_from_detail(ctx, url: str) -> str:
    """詳細ページから og:image を拾うフォールバック"""
    try:
        # request 経由で HTML を取ってパース（描画しないので速い）
        r = await ctx.request.get(url, timeout=30000, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            "Referer": "https://www.autohome.com.cn/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        if r.ok:
            html = await r.text()
            m = re.search(r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', html, re.I)
            if m:
                og = m.group(1)
                if og.startswith("http"):
                    return og
    except Exception:
        pass
    return ""

async def scroll_to_load(page, want_count=TARGET_COUNT):
    """100位まで読み込まれるまでスクロール。毎回待機を入れて lazy-load を解消"""
    seen = 0
    last_inc = time.time()
    while True:
        # 下までスクロール
        await page.evaluate("""
            () => { window.scrollTo(0, document.body.scrollHeight); }
        """)
        # ネットワークとレンダリング待ち
        await page.wait_for_load_state("domcontentloaded")
        # 画像の lazy 解決時間を与える
        await page.wait_for_timeout(1200)

        # 現在の件数を page 側で数える（アイテムのセレクタは緩めに）
        count = await page.evaluate("""
            () => {
              const cards = document.querySelectorAll('[data-rank-item], .athm-rank__item, li, .tw-flex, .tw-card');
              // 車シリーズページへのリンクっぽい a を数える
              let n = 0;
              cards.forEach(c => {
                const a = c.querySelector('a[href^="https://www.autohome.com.cn/"]');
                if (a && /https:\/\/www\.autohome\.com\.cn\/\d+\/?$/.test(a.href)) n++;
              });
              return n;
            }
        """)
        if count > seen:
            seen = count
            last_inc = time.time()

        if seen >= want_count:
            break

        # 一定時間増えなければ終了（安全弁）
        if time.time() - last_inc > 5:
            break

async def extract_rows(page, ctx):
    """ランキング行を抽出。画像URLは優先順＋詳細ページフォールバックで埋める"""
    rows = await page.evaluate("""
        () => {
          // 緩めに全アイテムを拾い、必要情報が揃うものに絞る
          const nodes = Array.from(document.querySelectorAll('[data-rank-item], .athm-rank__item, li, .tw-flex, .tw-card'));
          const arr = [];
          let rankCounter = 0;
          for (const n of nodes) {
            const a = n.querySelector('a[href^="https://www.autohome.com.cn/"]');
            if (!a) continue;
            const href = a.href;
            if (!/^https:\/\/www\.autohome\.com\.cn\/\d+\/?$/.test(href)) continue;

            // rank（見出しや数字を拾う。なければカウンタ）
            let rankTxt = "";
            const rankEl = n.querySelector('.rank, .tw-rank, [data-rank], .athm-rank__num, .tw-text-\\[\\#FF5500\\]');
            if (rankEl) rankTxt = rankEl.textContent.trim();
            if (!rankTxt) {
              rankCounter += 1;
              rankTxt = String(rankCounter);
            }

            // name / title（車名）
            let title = "";
            const nameEl = n.querySelector('h3, h4, .name, .tw-text-base, .tw-font-semibold, .athm-rank__title');
            if (nameEl) title = nameEl.textContent.trim();

            // 画像タグ
            const img = n.querySelector('img');
            let src = "", dataSrc = "", dataOriginal = "", srcset = "", dataSrcset = "";
            if (img) {
              src = img.getAttribute('src') || "";
              dataSrc = img.getAttribute('data-src') || "";
              dataOriginal = img.getAttribute('data-original') || "";
              srcset = img.getAttribute('srcset') || "";
              dataSrcset = img.getAttribute('data-srcset') || "";
            }

            arr.push({
              rank: rankTxt,
              name: title,
              url: href,
              img_src: src,
              img_data_src: dataSrc,
              img_data_original: dataOriginal,
              img_srcset: srcset,
              img_data_srcset: dataSrcset,
            });
          }
          return arr;
        }
    """)

    records = []
    for r in rows:
        # 画像URLの優先順位
        cand = [
            r.get("img_data_src") or "",
            r.get("img_data_original") or "",
        ]
        # srcset 系を解析して最大解像度を拾うため、ここではダミー・実処理はPython側
        cand.append(r.get("img_data_srcset") or "")
        cand.append(r.get("img_srcset") or "")
        cand.append(r.get("img_src") or "")

        # srcset を展開
        final = ""
        for c in cand:
            if not c:
                continue
            if " " in c and ("," in c or c.strip().endswith(("w","x"))):
                # srcset 風味
                parsed = parse_srcset(c)
                if parsed:
                    final = parsed
                    break
            else:
                final = c
                break

        # data:image（Base64プレースホルダ）や空はフォールバック
        if (not final) or final.startswith("data:image"):
            og = await get_og_image_from_detail(ctx, r["url"])
            if og:
                final = og

        records.append({
            "rank": r.get("rank"),
            "name": r.get("name"),
            "url": r.get("url"),
            "image_url": final,
        })
    return records

async def main():
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        )
        page = await ctx.new_page()

        print(f"🌐 Visiting: {RANK_URL}")
        await page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60000)

        # 100位まで読み込み（lazy-load待ちを含む）
        print(f"🔄 Scrolling until {TARGET_COUNT}th rank loaded...")
        await scroll_to_load(page, want_count=TARGET_COUNT)

        rows = await extract_rows(page, ctx)
        # 万一 100 未満なら、その時点で終了
        if not rows:
            print("❌ No rows extracted.")
            await browser.close()
            return

        # DataFrame にして保存
        df = pd.DataFrame(rows)
        # rank を数値にしてソート（保険）
        with pd.option_context('mode.chained_assignment', None):
            df["rank_num"] = pd.to_numeric(df["rank"], errors="coerce")
        df = df.sort_values(by=["rank_num", "rank"], ascending=True).drop(columns=["rank_num"])
        OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        print(f"✅ Done. Saved → {OUT_CSV}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
