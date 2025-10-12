#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ステージ1で作ったCSV（series_url入り）を読み、
各シリーズページへアクセスして <title> と エネルギー種別 を追記する。

入力列（最低限必要）: rank_seq,seriesname,series_url,...
出力列:
rank_seq,rank,seriesname,series_url,count,ev_count,phev_count,price,rank_change,
title_raw,series_energy_raw,type_from_page,type_final,is_ev_binary
"""

import argparse
import csv
import os
import re
from typing import Dict, List
from playwright.sync_api import sync_playwright, Browser, Page

ENERGY_LABEL_PAT = re.compile(r"(?:能源类型|能源|动力|驱动|动力类型)\s*[:：]?\s*([^\s/|·、，,　]{1,12})")
NORMALIZE = {
    "纯电":"EV","纯电动":"EV","电动":"EV","EV":"EV",
    "插电混动":"PHEV","插混":"PHEV","PHEV":"PHEV","插电":"PHEV","插电式混合动力":"PHEV",
    "增程":"EREV","增程式":"EREV","增程式电动":"EREV","REEV":"EREV",
    "混动":"HEV","油电混合":"HEV","HEV":"HEV",
    "轻混":"MHEV","MHEV":"MHEV",
    "汽油":"ICE","燃油":"ICE","柴油":"ICE",
}
HINT_WORDS = ["纯电","纯电动","EV","插电","插混","PHEV","增程","增程式","REEV","混动","HEV","MHEV","轻混","燃油","汽油","柴油"]

def _norm(word: str) -> str:
    if not word: return "Unknown"
    wcn = word.strip()
    wup = wcn.upper()
    if wcn in NORMALIZE: return NORMALIZE[wcn]
    if wup in NORMALIZE: return NORMALIZE[wup]
    for k,v in NORMALIZE.items():
        if k in wcn or k in wup: return v
    return "Unknown"

def fetch_title_and_energy(page: Page, url: str) -> Dict[str, str]:
    out = {"title_raw":"", "series_energy_raw":"", "type_from_page":"Unknown"}
    if not url: return out
    try:
        page.set_default_timeout(45000)
        page.goto(url, wait_until="networkidle")
        page.wait_for_timeout(1500)
        out["title_raw"] = page.title() or ""

        # スペック/概要領域優先で取得
        block = page.evaluate("""
            () => {
              const parts = [];
              const sels = ['.specs','.spec','.para','.information','.configs','.card','.main','.content','body'];
              for (const s of sels) { const el = document.querySelector(s); if (el) parts.push(el.innerText); }
              return parts.join('\\n\\n');
            }
        """) or ""

        m = ENERGY_LABEL_PAT.search(block)
        if m:
            raw = m.group(1).strip()
            out["series_energy_raw"] = raw
            out["type_from_page"]    = _norm(raw)
            return out

        # ラベル見つからない時は全体からヒント語
        body_text = page.evaluate("() => document.body.innerText") or ""
        for w in HINT_WORDS:
            if w.lower() in body_text.lower():
                out["series_energy_raw"] = w
                out["type_from_page"]    = _norm(w)
                break
        return out
    except Exception:
        return out

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--inp", required=True, help="ステージ1CSV")
    ap.add_argument("--out", required=True, help="追記後CSV")
    args = ap.parse_args()

    rows: List[Dict[str,str]] = []
    with open(args.inp, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = browser.new_context(user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                                              "Chrome/124.0.0.0 Safari/537.36"))
        page = ctx.new_page()

        for r in rows:
            info = fetch_title_and_energy(page, r.get("series_url",""))
            r["title_raw"]        = info.get("title_raw","")
            r["series_energy_raw"]= info.get("series_energy_raw","")
            r["type_from_page"]   = info.get("type_from_page","Unknown")
            # final: ページ情報優先（ページで不明なら Unknown のまま）
            r["type_final"]       = r["type_from_page"]
            r["is_ev_binary"]     = "1" if r["type_final"] == "EV" else "0"

        fieldnames = [
            "rank_seq","rank","seriesname","series_url","count","ev_count","phev_count","price","rank_change",
            "title_raw","series_energy_raw","type_from_page","type_final","is_ev_binary"
        ]
        with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k,"") for k in fieldnames})

        print(f"[ok] enriched rows={len(rows)} -> {args.out}")
        ctx.close(); browser.close()
