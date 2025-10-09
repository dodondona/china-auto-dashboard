#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, asyncio, os
import pandas as pd
from playwright.async_api import async_playwright

PC_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

COLUMNS = [
    "rank_seq","rank","brand","model","count","series_url",
    "brand_conf","series_conf","title_raw"
]

async def scrape_rank(url: str) -> list[dict]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            user_agent=PC_UA, viewport={"width": 1366, "height": 900}
        )
        page = await ctx.new_page()

        # PC版を明示。もし m.* に飛ばされたら www.* に戻す
        await page.goto(url, wait_until="domcontentloaded", timeout=120000)
        if page.url.startswith("https://m.autohome.com.cn/"):
            desk = page.url.replace("https://m.autohome.com.cn", "https://www.autohome.com.cn")
            await page.goto(desk, wait_until="domcontentloaded", timeout=120000)

        # 安定化
        try:
            await page.wait_for_load_state("networkidle", timeout=120000)
        except:
            pass

        # まずPC版のtable行を待つ。無ければフォールバックの選択子を待つ。
        try:
            await page.wait_for_selector("table tbody tr td", timeout=60000)
        except:
            await page.wait_for_selector(
                "div.rank-num, [data-rank-num], button[data-series-id]",
                timeout=60000
            )

        # 1) PC版table優先
        rows = await page.evaluate(r"""
        (() => {
          const out = [];
          const trs = document.querySelectorAll('table tbody tr');
          if (trs.length) {
            trs.forEach(tr => {
              const tds = tr.querySelectorAll('td');
              if (!tds.length) return;
              const rank = (tds[0]?.textContent || '').trim();
              const a = tr.querySelector('a');
              const title = (a?.getAttribute('title') || a?.textContent || '').trim();
              const btn = tr.querySelector('button[data-series-id]');
              const sid = btn ? btn.getAttribute('data-series-id') : '';
              const cntText = (tds[3]?.textContent || '').replace(/[, \s]/g, '');
              out.push({ rank, title_raw: title, series_id: sid, count: cntText });
            });
          }
          return out;
        })();
        """)

        # 2) フォールバック：divベース（モバイル/変形ページ）
        if not rows or all((r.get("title_raw","").strip()=="") for r in rows):
            rows = await page.evaluate(r"""
            (() => {
              const out = [];
              const items = document.querySelectorAll('[data-rank-num]') || [];
              items.forEach(el => {
                const rank = el.getAttribute('data-rank-num') || (el.textContent||'').trim();
                const card = el.closest('li,div') || document;
                const a = card.querySelector('a[href*="/"]');
                const title = (a?.getAttribute('title') || a?.textContent || '').trim();
                const btn = card.querySelector('button[data-series-id]');
                const sid = btn ? btn.getAttribute('data-series-id') : '';
                let count = '';
                const cntEl = card.querySelector('.c-blue, .data-num, .num, .count');
                if (cntEl) count = (cntEl.textContent || '').replace(/[, \s]/g, '');
                out.push({rank, title_raw: title, series_id: sid, count});
              });
              return out;
            })();
            """)

        await browser.close()
        return rows or []

def split_brand_model_from_title(title: str) -> tuple[str, str]:
    brand, model = "", ""
    if not title:
        return brand, model
    # 【モデル】ブランド_モデル… 形式を最優先
    if "】" in title and "_" in title:
        try:
            right = title.split("】", 1)[1]
            first = right.split("_", 1)[0].strip()   # "比亚迪 秦PLUS"
            parts = first.split()
            if len(parts) >= 2:
                brand = parts[0].strip()
                model = "".join(parts[1:]).strip()
        except:
            pass
    return brand, model

def build_df(rows: list[dict]) -> pd.DataFrame:
    recs = []
    for i, r in enumerate(rows, start=1):
        title = (r.get("title_raw") or "").strip()
        brand, model = split_brand_model_from_title(title)
        sid = (r.get("series_id") or "").strip()
        recs.append({
            "rank_seq": i,
            "rank": r.get("rank") or "",
            "brand": brand,
            "model": model,
            "count": r.get("count") or "",
            "series_url": f"https://www.autohome.com.cn/{sid}/" if sid else "",
            "brand_conf": 1.0,
            "series_conf": 1.0,
            "title_raw": title,
        })
    # 行が0でもヘッダーは必ず出す
    if not recs:
        return pd.DataFrame(columns=COLUMNS)
    return pd.DataFrame(recs, columns=COLUMNS)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")  # 互換ダミー
    args = ap.parse_args()

    rows = asyncio.run(scrape_rank(args.rank_url))
    df = build_df(rows)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"Saved rows: {len(df)}")
    print(f"Saved: {args.output}")

if __name__ == "__main__":
    main()
