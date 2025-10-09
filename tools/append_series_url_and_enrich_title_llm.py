#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, asyncio, os
import pandas as pd
from playwright.async_api import async_playwright

# ==== OpenAI (brand/model 抽出に使用) ====
from openai import OpenAI
OPENAI_MODEL_DEFAULT = "gpt-4o-mini"

PC_UA = (
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

async def _scrape_rank(url: str) -> list[dict]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=PC_UA, viewport={"width":1366,"height":900})
        page = await ctx.new_page()

        # PC版で開く（mに飛ばされたらPCに戻す）
        await page.goto(url, wait_until="domcontentloaded", timeout=120000)
        if page.url.startswith("https://m.autohome.com.cn/"):
            desk = page.url.replace("https://m.autohome.com.cn","https://www.autohome.com.cn")
            await page.goto(desk, wait_until="domcontentloaded", timeout=120000)
        try:
            await page.wait_for_load_state("networkidle", timeout=120000)
        except: pass

        # PC版の表を読む（rank / count / series_id / title）
        await page.add_script_tag(content=r"""
          (function(){
            const rows = [];
            const trs = document.querySelectorAll('table tbody tr');
            trs.forEach(tr => {
              const tds = tr.querySelectorAll('td');
              if (!tds.length) return;
              const rank = (tds[0]?.textContent||'').trim();
              const a = tr.querySelector('a');
              const title = (a?.getAttribute('title') || a?.textContent || '').trim();
              const btn = tr.querySelector('button[data-series-id]');
              const sid = btn ? btn.getAttribute('data-series-id') : '';
              const cntText = (tds[3]?.textContent || '').replace(/[, \s]/g,'');
              rows.push({rank, title_raw:title, series_id:sid, count:cntText});
            });
            window.__rankData = rows;
          })();
        """)
        data = await page.evaluate("window.__rankData || []")
        await browser.close()

    # series_url付与
    for r in data:
        sid = (r.get("series_id") or "").strip()
        r["series_url"] = f"https://www.autohome.com.cn/{sid}/" if sid else ""
    return data

def _extract_brand_model_with_openai(rows: list[dict], model:str) -> None:
    """title_raw を OpenAI に渡し、brand/model を埋める（上書き）"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("[FATAL] OPENAI_API_KEY not set.")
    client = OpenAI(api_key=api_key)

    prompt_sys = (
      "You extract car brand and model from a Chinese Autohome series <title>. "
      "Return compact JSON: {\"brand\":\"…\",\"model\":\"…\"}. "
      "Do not add extra keys or text."
    )
    for r in rows:
        title = (r.get("title_raw") or "").strip()
        if not title:
            r["brand"], r["model"] = "", ""
            continue
        try:
            msg = client.chat.completions.create(
                model=model,
                messages=[
                    {"role":"system","content":prompt_sys},
                    {"role":"user","content":f"title: {title}"}
                ],
                temperature=0
            )
            txt = msg.choices[0].message.content.strip()
            s = txt.find("{"); e = txt.rfind("}")
            brand, model_cn = "", ""
            if s!=-1 and e!=-1 and e>s:
                import json
                o = json.loads(txt[s:e+1])
                brand = (o.get("brand") or "").strip()
                model_cn = (o.get("model") or "").strip()
            r["brand"] = brand
            r["model"] = model_cn
        except Exception:
            r["brand"], r["model"] = "", ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--model", default=OPENAI_MODEL_DEFAULT)  # OpenAI 用
    args = ap.parse_args()

    rows = asyncio.run(_scrape_rank(args.rank_url))
    # ここで OpenAI による brand/model 抽出（従来仕様）
    _extract_brand_model_with_openai(rows, args.model)

    # CSV 出力（従来カラムを維持）
    recs = []
    for i, r in enumerate(rows, start=1):
        recs.append({
            "rank_seq": i,
            "rank": r.get("rank") or "",
            "brand": r.get("brand") or "",
            "model": r.get("model") or "",
            "count": r.get("count") or "",
            "series_url": r.get("series_url") or "",
            "brand_conf": 1.0,
            "series_conf": 1.0,
            "title_raw": r.get("title_raw") or "",
        })
    df = pd.DataFrame(recs)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"Saved rows: {len(df)}")
    print(f"Saved: {args.output}")

if __name__ == "__main__":
    main()
