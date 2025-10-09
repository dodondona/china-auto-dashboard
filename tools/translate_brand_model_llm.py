#!/usr/bin/env python3
import argparse, asyncio, json, os, sys, time
import pandas as pd
from playwright.async_api import async_playwright

PC_UA = (
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

def month_stamp():
    return time.strftime("%Y-%m")

async def scrape_rank(url: str) -> pd.DataFrame:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=PC_UA, viewport={"width":1366,"height":900})
        page = await ctx.new_page()
        # m. に飛ばされてもPC版に戻す
        await page.goto(url, wait_until="domcontentloaded")
        if page.url.startswith("https://m.autohome.com.cn/"):
            desktop = url.replace("https://www.autohome.com.cn", "https://www.autohome.com.cn")
            await page.goto(desktop, wait_until="domcontentloaded")

        # ランク行が出るまで待機（PC版セレクタ）
        await page.wait_for_selector("div.rank-num, em.rank, [data-rank-num], button[data-series-id]", timeout=120000)

        # 必要情報を window.__rankData に集約して取り出す（DOM走査は最小限）
        await page.add_script_tag(content="""
          (function(){
            const rows = [];
            document.querySelectorAll('tbody tr').forEach(tr=>{
              const tds = tr.querySelectorAll('td');
              if(!tds.length) return;
              const rank = (tds[0]?.textContent||'').trim();
              const titleA = tr.querySelector('a'); // 車種リンク
              const title = (titleA?.getAttribute('title')||titleA?.textContent||'').trim();
              const seriesIdBtn = tr.querySelector('button[data-series-id]');
              const seriesId = seriesIdBtn ? seriesIdBtn.getAttribute('data-series-id') : '';
              const count = (tds[3]?.textContent||'').replace(/[,\\s]/g,'').trim();
              rows.push({rank, title_raw: title, series_id: seriesId, count});
            });
            window.__rankData = rows;
          })();
        """)
        data = await page.evaluate("window.__rankData || []")
        # series_url を付与
        for row in data:
            sid = row.get("series_id","").strip()
            row["series_url"] = f"https://www.autohome.com.cn/{sid}/" if sid else ""
        await browser.close()

    # brand と model を title_raw から素直に分離（あなたの従来ロジックに合わせて最小限）
    recs = []
    for i, r in enumerate(data, start=1):
        t = r.get("title_raw","")
        # 【Model】ブランド_… 形式に対応
        brand, model = "", ""
        if "】" in t and "_" in t:
            try:
                right = t.split("】",1)[1]
                parts = right.split("_",1)[0].split()
                # e.g. "比亚迪 秦PLUS"
                if len(parts)>=2:
                    brand, model = parts[0], "".join(parts[1:])
            except Exception:
                pass
        recs.append({
            "rank_seq": i,
            "rank": r.get("rank",""),
            "brand": brand,
            "model": model,
            "count": r.get("count",""),
            "series_url": r.get("series_url",""),
            "title_raw": t,
            "brand_conf": 1.0,
            "series_conf": 1.0
        })
    return pd.DataFrame(recs)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")  # 既存互換のダミー
    args = ap.parse_args()

    df = asyncio.run(scrape_rank(args.rank_url))
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"Saved: {args.output}")

if __name__ == "__main__":
    main()
