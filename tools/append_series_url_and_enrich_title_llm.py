#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, asyncio, os, time
import pandas as pd
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

        # PC版を明示。m.autohome に飛ばされたら戻す
        await page.goto(url, wait_until="domcontentloaded", timeout=120000)
        if page.url.startswith("https://m.autohome.com.cn/"):
            desk = url.replace("https://m.autohome.com.cn", "https://www.autohome.com.cn")
            desk = desk.replace("https://www.autohome.com.cn", "https://www.autohome.com.cn")
            await page.goto(desk, wait_until="domcontentloaded", timeout=120000)

        # さらに安定化：完全読み込み待ち
        try:
            await page.wait_for_load_state("networkidle", timeout=120000)
        except:
            pass

        # ランク表の行が出るまで待つ（PC版側のセレクタ）
        await page.wait_for_selector(
            "table tbody tr td, div.rank-num, [data-rank-num], button[data-series-id]",
            timeout=180000,
        )

        # DOM 走査（軽量化）
        await page.add_script_tag(content=r"""
          (function(){
            const rows = [];
            const trs = document.querySelectorAll('table tbody tr');
            trs.forEach((tr, idx) => {
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

        for r in data:
            sid = (r.get("series_id") or "").strip()
            r["series_url"] = f"https://www.autohome.com.cn/{sid}/" if sid else ""

        await browser.close()

    # 既存CSV互換のカラムで返す（LLM整形は別工程）
    recs = []
    for i, r in enumerate(data, start=1):
        title = r.get("title_raw", "")
        brand, model = "", ""
        # 「【モデル】ブランド_モデル…」形式に対応
        if "】" in title and "_" in title:
            try:
                right = title.split("】", 1)[1]
                # 例: "比亚迪 秦PLUS报价..." → 最初の空白までブランド、残りをモデル
                tmp = right.split("_", 1)[0].strip()
                parts = tmp.split()
                if len(parts) >= 2:
                    brand = parts[0].strip()
                    model = "".join(parts[1:]).strip()
            except Exception:
                pass

        recs.append({
            "rank_seq": i,
            "rank": r.get("rank") or "",
            "brand": brand,
            "model": model,
            "count": r.get("count") or "",
            "series_url": r.get("series_url") or "",
            "brand_conf": 1.0,
            "series_conf": 1.0,
            "title_raw": title,
        })
    return pd.DataFrame(recs)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")  # 既存互換のダミー引数
    args = ap.parse_args()

    df = asyncio.run(scrape_rank(args.rank_url))
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"Saved: {args.output}")

if __name__ == "__main__":
    main()
