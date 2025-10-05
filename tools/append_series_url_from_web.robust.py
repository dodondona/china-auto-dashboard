
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_from_rank.py (robust minimal patch)
- /rank/1 を開き、無限スクロールで全件ロード。
- まずHTMLから正規表現で seriesid/seriesname を抽出（従来ロジック）。
- もし件数が少なすぎる場合（例: 40未満）、DOMベースでの seriesid/name 収集をフォールバック。
- URLは https://www.autohome.com.cn/{seriesid} で生成（末尾スラ無し）。
"""
import re, csv, sys, argparse, time
from typing import List, Dict, Tuple, Optional
from playwright.sync_api import sync_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")

RE_SERIES_PAIR = re.compile(r'"seriesid"\s*:\s*"(\d{3,7})"\s*,\s*"seriesname"\s*:\s*"([^"]+)"')
RE_SERIES_APP  = re.compile(r'autohome://car/seriesmain\?seriesid=(\d{3,7})')
RE_SERIES_PATH = re.compile(r'/(\d{3,7})(?:/|[?#]|\")')

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
    # 2) 残りは順序埋め
    # 既存行の順を保ちつつ、未使用sidを前から消費
    k = 0
    while k < len(pairs) and k in used:
        k += 1
    for r in rows:
        if not r.get("series_url"):
            # 次の未使用 sid を割り当て
            while k < len(pairs) and pairs[k][0] in used:
                k += 1
            if k < len(pairs):
                sid = pairs[k][0]
                r["series_url"] = to_series_url(sid)
                used.add(sid)
                k += 1

def extract_pairs_from_html(html: str) -> List[Tuple[str,str]]:
    pairs = []
    for sid, sname in RE_SERIES_PAIR.findall(html):
        pairs.append((sid, sname))
    if not pairs:
        # 他のパターンも拾う
        sids = set(RE_SERIES_APP.findall(html))
        sids.update(RE_SERIES_PATH.findall(html))
        for sid in sids:
            pairs.append((sid, ""))
    # uniq & 順序維持
    seen, out = set(), []
    for sid, sname in pairs:
        if sid not in seen:
            seen.add(sid); out.append((sid, sname))
    return out

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

def autoscroll_to_target(page, target: int, idle_ms: int = 600, max_rounds: int = 400) -> None:
    last = -1
    for i in range(max_rounds):
        page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        page.wait_for_timeout(idle_ms)
        page.wait_for_load_state("networkidle")
        cnt = page.evaluate("document.querySelectorAll('[data-rank-num]').length")
        if cnt >= target:
            break
        if cnt == last:
            try:
                page.mouse.wheel(0, 1800)
                page.wait_for_timeout(400)
            except Exception:
                pass
        last = cnt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--input", required=True, help="CSV input")
    ap.add_argument("--output", required=True, help="CSV output")
    ap.add_argument("--name-col", default="model_text")
    ap.add_argument("--idle-ms", type=int, default=600)
    ap.add_argument("--max-rounds", type=int, default=400)
    args = ap.parse_args()

    rows = read_csv_rows(args.input)
    name_col = args.name_col

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=UA)
        page = context.new_page()
        page.route("**/*.{png,jpg,jpeg,gif,webp,svg}", lambda r: r.abort())
        page.route("**/*.woff*", lambda r: r.abort())

        page.goto(args.rank_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_load_state("networkidle")
        # 指紋クッキー `_ac`
        page.wait_for_function("document.cookie.includes('_ac=')", timeout=15000)

        # 総件数（pagecount*pagesize）を __NEXT_DATA__ から取得し、そこまでスクロール
        meta = page.evaluate("""() => {
            const el = document.querySelector('#__NEXT_DATA__');
            if (!el) return null;
            try {
                const d = JSON.parse(el.textContent);
                const { pagecount, pagesize } = d.props.pageProps.listRes || {};
                return { pagecount, pagesize };
            } catch (e) { return null; }
        }""")
        target = 0
        if meta and meta.get("pagecount") and meta.get("pagesize"):
            target = int(meta["pagecount"]) * int(meta["pagesize"])
        if target > 0:
            autoscroll_to_target(page, target, idle_ms=args.idle_ms, max_rounds=args.max_rounds)
        else:
            # 従来どおり、しばらくスクロール
            autoscroll_to_target(page, 1000, idle_ms=args.idle_ms, max_rounds=args.max_rounds)

        html = page.content()
        # 先にHTML正規表現で抽出（従来）
        pairs = extract_pairs_from_html(html)
        # 件数が少ない（20件付近）の場合は DOM フォールバックで再取得
        if len(pairs) < 40:
            pairs = extract_pairs_from_dom(page)

        context.close(); browser.close()

    attach_by_name_then_order(rows, pairs, name_col)
    write_csv_rows(args.output, rows)
    print(f"✔ 出力: {args.output}  （抽出 {len(pairs)}件 / CSV {len(rows)}行）")
    print(f"  使用した車名列: {name_col}")

if __name__ == "__main__":
    main()
