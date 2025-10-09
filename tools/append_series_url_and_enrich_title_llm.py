#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
append_series_url_and_enrich_title_llm.py
Autohome ランキングページから rank / series_url / title_raw / count を抽出。
brand, model の抽出は別工程（LLM補完側）で実施。
"""

import argparse, asyncio, os, time, pandas as pd
from playwright.async_api import async_playwright

PC_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

async def scrape_rank(url: str) -> pd.DataFrame:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            user_agent=PC_UA, viewport={"width": 1366, "height": 900}
        )
        page = await ctx.new_page()

        # === PC版を明示的に開く ===
        await page.goto(url, wait_until="domcontentloaded", timeout=120000)
        if page.url.startswith("https://m.autohome.com.cn/"):
            desk = url.replace("https://m.autohome.com.cn", "https://www.autohome.com.cn")
            await page.goto(desk, wait_until="domcontentloaded", timeout=120000)

        try:
            await page.wait_for_load_state("networkidle", timeout=120000)
        except:
            pass

        # === 旧(table)構造対応 ===
        await page.add_script_tag(content=r"""
          (function(){
            const rows = [];
            const trs = document.querySelectorAll('table tbody tr');
            trs.forEach((tr) => {
              const tds = tr.querySelectorAll('td');
              if (!tds.length) return;
              const rank = (tds[0]?.textContent || '').trim();
              const a = tr.querySelector('a');
              const title = (a?.getAttribute('title') || a?.textContent || '').trim();
              const btn = tr.querySelector('button[data-series-id]');
              const sid = btn ? btn.getAttribute('data-series-id') : '';
              const cntText = (tds[3]?.textContent || '').replace(/[, \t\r\n]/g, '');
              rows.push({ rank, title_raw: title, series_id: sid, count: cntText });
            });
            window.__rankData = rows;
          })();
        """)
        data = await page.evaluate("window.__rankData || []")

        # === 新(div構造)構造対応（title_raw空ならのみ発動） ===
        if not data or all(not (r.get("title_raw") or "").strip() for r in data):
            print("Fallback: Detected new Vue structure, retrying div.rank-list__item ...")
            data = await page.evaluate(r"""
            (() => {
              const out = [];
              document.querySelectorAll('div.rank-list__item').forEach((el) => {
                const rank = el.querySelector('.rank-num, [data-rank-num]')?.textContent?.trim() || '';
                const title = el.querySelector('.rank-model__name, a')?.textContent?.trim() || '';
                const sid = el.querySelector('[data-series-id]')?.getAttribute('data-series-id') || '';
                const cnt = el.querySelector('.rank-model__sales, .data-num, .num')?.textContent?.replace(/[, \s]/g, '') || '';
                out.push({ rank, title_raw: title, series_id: sid, count: cnt });
              });
              return out;
            })();
            """)

        await browser.close()

    # === CSV整形 ===
    recs = []
    for i, r in enumerate(data, start=1):
        recs.append({
            "rank_seq": i,
            "rank": r.get("rank") or "",
            "brand": "",        # ← LLM補完用の空欄
            "model": "",        # ← LLM補完用の空欄
            "count": r.get("count") or "",
            "series_url": f"https://www.autohome.com.cn/{r.get('series_id','').strip()}/" if r.get("series_id") else "",
            "brand_conf": 1.0,
            "series_conf": 1.0,
            "title_raw": r.get("title_raw") or "",
        })
    print(f"Saved rows: {len(recs)}")
    return pd.DataFrame(recs)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")  # ダミー互換
    args = ap.parse_args()

    df = asyncio.run(scrape_rank(args.rank_url))
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"Saved: {args.output}")

if __name__ == "__main__":
    main()
