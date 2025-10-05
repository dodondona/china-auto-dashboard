
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_from_rank.py (force-DOM minimal change)
- /rank/1 を開き、全件出るまで強制スクロール（多段テクニック）
- 取得は DOM ベース（[data-rank-num] 行→ button[data-series-id]）を **常に**使用
- URLは https://www.autohome.com.cn/{seriesid} （末尾スラ無し）
- 既存の CSV マージは従来通り（名前一致 → 残り順序）
"""
import re, csv, sys, argparse, time
from typing import List, Dict, Tuple, Optional
from playwright.sync_api import sync_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")

def normalize_name(s: str) -> str:
    import re as _re
    return _re.sub(r'\s+', '', (s or '')).lower()

def to_series_url(series_id: str) -> str:
    return f"https://www.autohome.com.cn/{series_id}"

def read_csv_rows(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv_rows(path: str, rows: List[Dict[str, str]]) -> None:
    if not rows: return
    fields = list(rows[0].keys())
    if "series_url" not in fields:
        fields.append("series_url")
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

def attach_by_name_then_order(rows: List[Dict[str,str]], pairs: List[Tuple[str,str]], name_col: str) -> None:
    # 1) 名前一致
    name2sid = {}
    for sid, sname in pairs:
        key = normalize_name(sname)
        if key: name2sid[key] = sid
    used = set()
    for r in rows:
        nm = normalize_name(r.get(name_col, ""))
        sid = name2sid.get(nm)
        if sid and sid not in used:
            r["series_url"] = to_series_url(sid)
            used.add(sid)
    # 2) 残りは順序埋め（pairsの順を使う）
    k = 0
    for r in rows:
        if not r.get("series_url"):
            while k < len(pairs) and pairs[k][0] in used:
                k += 1
            if k < len(pairs):
                sid = pairs[k][0]
                r["series_url"] = to_series_url(sid)
                used.add(sid)
                k += 1

def autoscroll_full(page, target: int, max_rounds: int = 800, idle_ms: int = 500) -> None:
    """
    無限スクロールを確実に発火させるための多段テクニック。
    - window.scrollBy（大）→ wheel → 最終行 scrollIntoView → scroll イベント手動発火
    - 件数が伸びなければ小刻みスクロールを繰り返す
    """
    last = -1
    stagnate = 0
    for i in range(max_rounds):
        page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        page.wait_for_timeout(idle_ms)
        page.mouse.wheel(0, 1800)
        page.wait_for_timeout(200)
        # 最終行を強制的に可視へ
        page.evaluate("""() => {
            const rows = document.querySelectorAll('[data-rank-num]');
            const last = rows[rows.length - 1];
            if (last) last.scrollIntoView({behavior:'instant', block:'end'});
            window.dispatchEvent(new Event('scroll'));
        }""")
        page.wait_for_timeout(300)
        page.wait_for_load_state("networkidle")

        cnt = page.evaluate("document.querySelectorAll('[data-rank-num]').length")
        # print(f"scroll round {i}, count={cnt}")
        if cnt >= target and target > 0:
            break
        if cnt == last:
            stagnate += 1
            # 小刻み
            page.mouse.wheel(0, 400)
            page.wait_for_timeout(250)
            page.evaluate("window.scrollBy(0, 800)")
            page.wait_for_timeout(250)
        else:
            stagnate = 0
        if stagnate >= 8:  # かなり停滞 → 一旦トップに戻ってから再スクロール
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(400)
            stagnate = 0
        last = cnt

def extract_pairs_from_dom(page) -> List[Tuple[str,str]]:
    data = page.evaluate("""() => Array.from(
        document.querySelectorAll('[data-rank-num]')
    ).map(row => {
        const rank = Number(row.getAttribute('data-rank-num'));
        const btn  = row.querySelector('button[data-series-id]');
        const sid  = btn ? btn.getAttribute('data-series-id') : '';
        const name = row.querySelector('.tw-text-lg, .tw-font-medium')?.textContent?.trim() || '';
        return { rank, sid, name };
    }).filter(x => x.sid).sort((a,b)=>a.rank-b.rank)""")
    out = []
    seen = set()
    for x in data:
        if x["sid"] and x["sid"] not in seen:
            out.append((x["sid"], x["name"]))
            seen.add(x["sid"])
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--name-col", default="model")  # あなたの実行ログに合わせて 'model' をデフォルトに
    ap.add_argument("--idle-ms", type=int, default=500)
    ap.add_argument("--max-rounds", type=int, default=800)
    args = ap.parse_args()

    rows = read_csv_rows(args.input)
    name_col = args.name_col

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=UA, viewport={"width":1280,"height":1600})
        page = context.new_page()
        # 画像・フォントはブロック（スクリプトは通す）
        page.route("**/*.{png,jpg,jpeg,gif,webp,svg}", lambda r: r.abort())
        page.route("**/*.woff*", lambda r: r.abort())

        page.goto(args.rank_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_load_state("networkidle")
        # 指紋クッキー `_ac`
        page.wait_for_function("document.cookie.includes('_ac=')", timeout=20000)

        # 目標件数（__NEXT_DATA__）
        meta = page.evaluate("""() => {
            const el = document.querySelector('#__NEXT_DATA__');
            if (!el) return null;
            try {
                const d = JSON.parse(el.textContent);
                const { pagecount, pagesize } = d.props?.pageProps?.listRes || {};
                return { pagecount, pagesize };
            } catch (e) { return null; }
        }""")
        target = 600  # フォールバックの既定値
        if meta and meta.get("pagecount") and meta.get("pagesize"):
            try:
                target = int(meta["pagecount"]) * int(meta["pagesize"])
            except Exception:
                pass

        autoscroll_full(page, target, max_rounds=args.max_rounds, idle_ms=args.idle_ms)

        # DOM から最終抽出（常にDOMを採用）
        pairs = extract_pairs_from_dom(page)

        context.close(); browser.close()

    attach_by_name_then_order(rows, pairs, name_col)
    write_csv_rows(args.output, rows)
    print(f"✔ 出力: {args.output}  （抽出 {len(pairs)}件 / CSV {len(rows)}行）")
    print(f"  使用した車名列: {name_col}")

if __name__ == "__main__":
    main()
