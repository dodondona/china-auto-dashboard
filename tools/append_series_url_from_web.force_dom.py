#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_from_web.force_dom.py
Autohome /rank/1 ページから series_id と name を抽出し、
rank 列を基準に series_url を確実に付与する。

改訂内容：
- rank属性値ではなく「DOM表示順」をrankとして採用（ズレ防止）
- rank優先付与 → 名前一致 → 順序埋め の三段階ロジック
- gotoタイムアウトを90秒に延長
"""

import os, re, time, csv, argparse
from playwright.sync_api import sync_playwright

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

def normalize_name(s: str) -> str:
    """全角・半角・記号を統一して比較しやすくする"""
    return re.sub(r"[\s_·\-　]+", "", s or "").lower()

def to_series_url(sid: str) -> str:
    """series_id から URL 生成"""
    return f"https://www.autohome.com.cn/{sid}/" if sid else ""

def extract_entries_from_dom(page):
    """
    表示順（DOM出現順）を rank=1,2,3... として採用。
    Autohome は data-rank-num が実際の順位とズレることがあるため。
    """
    data = page.evaluate("""() => Array.from(
      document.querySelectorAll('[data-rank-num]')
    ).map(row => {
      const btn  = row.querySelector('button[data-series-id]');
      const sid  = btn ? btn.getAttribute('data-series-id') : '';
      const name = row.querySelector('.tw-text-lg, .tw-font-medium')?.textContent?.trim() || '';
      return { sid, name };
    }).filter(x => x.sid)""")

    out, seen = [], set()
    rank_counter = 1
    for x in data:
        if x["sid"] in seen:
            continue
        seen.add(x["sid"])
        out.append({"rank": rank_counter, "sid": x["sid"], "name": x["name"]})
        rank_counter += 1
    return out

def attach_by_rank_name_order(rows, entries, name_col):
    """1) rank直付け 2) 名前一致 3) 順序埋め"""
    used = set()
    rank2sid = {e["rank"]: e["sid"] for e in entries}

    # 1️⃣ rank直付け
    for r in rows:
        rk_raw = r.get("rank", "")
        try:
            rk = int(str(rk_raw).strip())
        except Exception:
            rk = None
        if rk and rk in rank2sid and not r.get("series_url"):
            sid = rank2sid[rk]
            if sid not in used:
                r["series_url"] = to_series_url(sid)
                used.add(sid)

    # 2️⃣ 名前一致
    name2sid = {}
    for e in entries:
        key = normalize_name(e["name"])
        if key and e["sid"] not in used:
            name2sid[key] = e["sid"]
    for r in rows:
        if r.get("series_url"):
            continue
        nm = normalize_name(r.get(name_col, ""))
        sid = name2sid.get(nm)
        if sid and sid not in used:
            r["series_url"] = to_series_url(sid)
            used.add(sid)

    # 3️⃣ 順序埋め
    for e in entries:
        if e["sid"] in used:
            continue
        for r in rows:
            if not r.get("series_url"):
                r["series_url"] = to_series_url(e["sid"])
                used.add(e["sid"])
                break

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--name-col", default="model")
    ap.add_argument("--idle-ms", type=int, default=600)
    ap.add_argument("--max-rounds", type=int, default=25)
    ap.add_argument("--min-delta", type=int, default=3)
    args = ap.parse_args()

    # === CSV読み込み ===
    with open(args.input, "r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print("⚠ 入力CSVが空です。")
        return

    # === Playwright動作 ===
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=UA, viewport={"width":1280,"height":1600})
        page = context.new_page()

        print(f"📥 {args.rank_url} にアクセス中...")
        page.goto(args.rank_url, wait_until="load", timeout=90000)
        page.wait_for_load_state("networkidle")

        prev_count, stable_rounds = 0, 0
        for _ in range(args.max_rounds):
            page.mouse.wheel(0, 20000)
            time.sleep(args.idle_ms / 1000)
            n = len(page.query_selector_all("[data-rank-num]"))
            if n - prev_count < args.min_delta:
                stable_rounds += 1
                if stable_rounds >= 3:
                    break
            else:
                stable_rounds = 0
            prev_count = n

        entries = extract_entries_from_dom(page)
        print(f"✅ 抽出 {len(entries)} 件")

    # === series_url付与 ===
    attach_by_rank_name_order(rows, entries, args.name_col)

    # === 出力 ===
    with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ series_url 追記完了: {args.output}")

if __name__ == "__main__":
    main()
