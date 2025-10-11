#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rank/1（车系ランキング）から上位50件の series_url と各シリーズページの <title>、
および行近傍の“销量”数値（台数）をCSV出力する最小スクリプト（成功例準拠）。

出力スキーマ（下流互換）:
rank_seq, rank, seriesname, series_url, brand, model, brand_conf, series_conf, title_raw, count
- brand/model は空, conf は 0.0
- count は行近傍から抽出した数字（半角数値）を格納
"""

import argparse
import csv
import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright

# 正規表現抽出（成功例の一次候補）
RE_ITEM = re.compile(
    r'linkurl":"autohome://car/seriesmain\?seriesid=(\d+)[^"]*".*?'
    r'"rank":"(\d+)".*?'
    r'"seriesname":"([^"]+)"',
    re.S
)

def wait_for_ac_cookie(page, timeout_ms: int = 6000) -> None:
    """_ac クッキーが入るまで待機（20件止まり対策）。"""
    page.wait_for_function("document.cookie.includes('_ac=')", timeout=timeout_ms)

def get_target_count_from_next_data(page) -> Optional[int]:
    """#__NEXT_DATA__ から pagecount×pagesize を推定。"""
    try:
        raw = page.eval_on_selector("#__NEXT_DATA__", "el => el.textContent")
        data = json.loads(raw)
        def dig(obj) -> Optional[int]:
            if isinstance(obj, dict):
                if "pagecount" in obj and "pagesize" in obj:
                    pc = int(obj.get("pagecount") or 0)
                    ps = int(obj.get("pagesize") or 0)
                    if pc > 0 and ps > 0:
                        return pc * ps
                for v in obj.values():
                    r = dig(v)
                    if r:
                        return r
            elif isinstance(obj, list):
                for it in obj:
                    r = dig(it);  # noqa
                    if r:
                        return r
            return None
        return dig(data)
    except Exception:
        return None

def auto_scroll_to_count(page, min_needed: int, hard_limit_scrolls: int, wait_ms: int) -> None:
    """リスト末尾まで到達するまでスクロール。"""
    last = -1
    stagnation = 0
    for _ in range(hard_limit_scrolls):
        page.mouse.wheel(0, 5000)
        page.wait_for_timeout(wait_ms)
        cnt = page.evaluate("() => document.querySelectorAll('button[data-series-id]').length")
        if cnt >= min_needed:
            break
        if cnt == last:
            stagnation += 1
        else:
            stagnation = 0
            last = cnt
        if stagnation >= 25:
            break

def extract_by_regex(html: str) -> List[Tuple[int, str, str]]:
    """(rank, seriesid, seriesname) を正規表現で抽出。"""
    items: List[Tuple[int, str, str]] = []
    for sid, rank, name in RE_ITEM.findall(html):
        try:
            r = int(rank)
        except Exception:
            continue
        items.append((r, sid, name))
    items.sort(key=lambda x: x[0])
    return items

def extract_dom_bundle(page) -> List[Dict[str, Any]]:
    """
    DOM全件バンドル取得：
    - 行基準：button[data-series-id]
    - 物理順（index順）で rank を付与（data-rank-numは不一致があり得るため）
    - 同一行の近傍テキストから “数字のみ” を1つ抽出 → count
      （成功例メモ：行近傍に“销售量/车系销量”等があり、その近傍で数字が拾える）  # noqa
    """
    js = r"""
    () => {
      const btns = Array.from(document.querySelectorAll('button[data-series-id]'));
      const rows = [];
      const numRe = /(\d{1,3}(?:,\d{3})+|\d{1,6})/g; // 1～6桁 or カンマ区切り
      for (let i = 0; i < btns.length; i++) {
        const b = btns[i];
        const sid = b.getAttribute('data-series-id') || '';
        const seriesname = (b.textContent || '').trim();
        // 行（rank-list-item）コンテナを辿る
        const item = b.closest('.rank-list-item') || b.closest('li') || b.parentElement;
        let count = '';
        if (item) {
          // 行テキストから数字を収集
          const tx = (item.textContent || '').replace(/\s+/g, ' ');
          const matches = tx.match(numRe) || [];
          // 数字候補のうち “大きめ” を優先（販売台数の方が桁が大きい）
          let best = '';
          let bestVal = -1;
          for (const m of matches) {
            const v = parseInt(m.replace(/,/g, ''), 10);
            if (!Number.isNaN(v) && v > bestVal) {
              bestVal = v;
              best = String(v);
            }
          }
          if (bestVal >= 0) count = best;
        }
        rows.push({
          rank: i + 1,  // 物理順
          sid,
          seriesname,
          count
        });
      }
      return rows;
    }
    """
    return page.evaluate(js)

def compose_series_url(seriesid: str) -> str:
    return f"https://www.autohome.com.cn/{seriesid}"

def fetch_series_titles_in_place(page, rows: List[Dict[str, Any]], wait_ms: int) -> None:
    """各 series_url を開いて <title> を title_raw に格納。"""
    for row in rows:
        page.goto(row["series_url"], wait_until="domcontentloaded")
        row["title_raw"] = page.title()
        time.sleep(0.15)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="例: https://www.autohome.com.cn/rank/1")
    ap.add_argument("--out", required=True, help="出力CSVパス（例: data/series.csv）")
    ap.add_argument("--wait-ms", type=int, default=250)
    ap.add_argument("--max-scrolls", type=int, default=200)
    ap.add_argument("--topk", type=int, default=50)
    args = ap.parse_args()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        page.goto(args.url, wait_until="domcontentloaded")
        wait_for_ac_cookie(page, timeout_ms=6000)

        target = get_target_count_from_next_data(page)
        min_needed = max(args.topk, target or 0) or args.topk
        auto_scroll_to_count(page, min_needed=min_needed,
                             hard_limit_scrolls=args.max_scrolls, wait_ms=args.wait_ms)

        html = page.content()

        # 1) 正規表現で候補（rank, sid, name）取得
        regex_items = extract_by_regex(html)

        # 2) DOMバンドル（物理順・count含む）取得
        dom_bundle = extract_dom_bundle(page)

        # マージ：rank（1-based）をキーに、regex優先で sid/name を補完しつつ count を付与
        by_rank: Dict[int, Dict[str, Any]] = {}
        for r, sid, name in regex_items:
            by_rank[r] = {"rank_seq": r, "sid": sid, "seriesname": name}

        for obj in dom_bundle:
            r = int(obj.get("rank") or 0)
            if r <= 0:
                continue
            if r not in by_rank:
                by_rank[r] = {"rank_seq": r, "sid": obj.get("sid") or "", "seriesname": obj.get("seriesname") or ""}
            # count は DOMから
            by_rank[r]["count"] = obj.get("count") or ""

        # 上位 topk
        ranks = sorted([k for k in by_rank.keys() if k >= 1])[: args.topk]
        rows: List[Dict[str, Any]] = []
        for r in ranks:
            sid = by_rank[r].get("sid", "")
            name = by_rank[r].get("seriesname", "")
            cnt = by_rank[r].get("count", "")
            rows.append({
                "rank_seq": r,
                "rank": r,
                "seriesname": name,
                "series_url": compose_series_url(sid) if sid else "",
                "brand": "",
                "model": "",
                "brand_conf": 0.0,
                "series_conf": 0.0,
                "title_raw": "",
                "count": cnt
            })

        # title_raw を埋める
        fetch_series_titles_in_place(page, rows, wait_ms=args.wait_ms)

        # CSV保存
        fieldnames = [
            "rank_seq", "rank", "seriesname", "series_url",
            "brand", "model", "brand_conf", "series_conf",
            "title_raw", "count"
        ]
        with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow(r)

        print(f"[ok] rows={len(rows)} -> {args.out}")

        ctx.close()
        browser.close()

if __name__ == "__main__":
    main()
