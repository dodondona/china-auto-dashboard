# tools/rank1_brand_series_scraper.py
# 目的:
# - https://www.autohome.com.cn/rank/1 をPlaywrightで開き、無限スクロールで上位50件をロード
# - 各行から rank / seriesid / seriesname / count を取得
#   * seriesid は button[data-series-id] から取得 → series_url を合成
#   * seriesname は 行内の見出し (.tw-text-lg, .tw-font-medium) から取得（※ボタンの「查成交价」は使わない）
#   * count は 行テキストから「(\d{4,6}) 车系销量」を抽出（見つからなければ空欄）
# - series_url の各ページ <title> を title_raw として取得
# - 出力CSV列は下記の通り（brand/modelは空、confは0.0のまま）
#   rank_seq,rank,seriesname,series_url,brand,model,brand_conf,series_conf,title_raw,count

import argparse
import csv
import re
import time
from typing import List, Dict, Any

from playwright.sync_api import sync_playwright, Browser, Page


def scroll_to_load_all(page: Page, need_rows: int = 50, wait_ms: int = 200, max_scrolls: int = 200) -> None:
    """無限スクロールして、最低 need_rows 行が現れるまで粘る"""
    last_height = 0
    for i in range(max_scrolls):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(wait_ms)
        # 読み込み済み行数
        rows = page.query_selector_all('[data-rank-num]')
        if len(rows) >= need_rows:
            return
        # 高さ変化が止まっていないかの簡易チェック
        cur_height = page.evaluate("document.body.scrollHeight")
        if cur_height == last_height:
            # 一呼吸おいて再試行
            page.wait_for_timeout(wait_ms * 2)
            cur_height = page.evaluate("document.body.scrollHeight")
            if cur_height == last_height:
                # 打ち切り
                return
        last_height = cur_height


def collect_rows(page: Page) -> List[Dict[str, Any]]:
    """各ランキング行から rank / seriesid / seriesname / row_text を収集"""
    rows = page.evaluate(
        """() => Array.from(document.querySelectorAll('[data-rank-num]')).map(row => {
            const rank = Number(row.getAttribute('data-rank-num'));
            const btn  = row.querySelector('button[data-series-id]');
            const sid  = btn ? (btn.getAttribute('data-series-id') || '').trim() : '';
            // 車名は見出しテキストから拾う（ボタンは「查成交价」なので使わない）
            const nameEl = row.querySelector('.tw-text-lg, .tw-font-medium');
            const name = nameEl ? (nameEl.textContent || '').trim() : '';
            const text = (row.innerText || '').trim();
            return { rank, sid, name, text };
        })"""
    )
    # seriesid が空の行は捨てる
    rows = [r for r in rows if r.get("sid")]
    # rank 昇順に揃える
    rows.sort(key=lambda x: x.get("rank", 0))
    return rows


COUNT_PAT = re.compile(r"(\d{4,6})\s*车系销量")


def parse_count(text: str) -> str:
    """行テキストから '(\d{4,6}) 车系销量' を抽出。なければ空文字。"""
    if not text:
        return ""
    m = COUNT_PAT.search(text.replace("\u00A0", " "))
    return m.group(1) if m else ""


def build_series_url(seriesid: str) -> str:
    # 末尾スラッシュなしで合わせる
    return f"https://www.autohome.com.cn/{seriesid}"


def fetch_title(page: Page, url: str, timeout_ms: int = 10000) -> str:
    """対象URLの<title>を取得（タイムアウト時は空文字）"""
    try:
        page.set_default_timeout(timeout_ms)
        page.goto(url, wait_until="domcontentloaded")
        return page.title() or ""
    except Exception:
        return ""


def run(url: str, out_path: str, wait_ms: int, max_scrolls: int) -> None:
    with sync_playwright() as p:
        browser: Browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ))
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded")
        # 無限スクロールで全件ロード
        scroll_to_load_all(page, need_rows=50, wait_ms=wait_ms, max_scrolls=max_scrolls)
        rows = collect_rows(page)

        # 上位50件に制限
        rows = rows[:50]

        # series_url / count
        for r in rows:
            r["series_url"] = build_series_url(str(r["sid"]))
            r["count"] = parse_count(r.get("text", ""))

        # 各シリーズページの<title>を取得
        # 1ページを使い回して直列で取りに行く（安定優先）
        series_page = context.new_page()
        for r in rows:
            r["title_raw"] = fetch_title(series_page, r["series_url"])

        # CSV出力
        fieldnames = [
            "rank_seq", "rank", "seriesname", "series_url",
            "brand", "model", "brand_conf", "series_conf",
            "title_raw", "count",
        ]
        with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for i, r in enumerate(rows, start=1):
                writer.writerow({
                    "rank_seq": i,
                    "rank": r.get("rank", i),
                    "seriesname": r.get("name", ""),            # ← 「查成交价」ではなく見出し名
                    "series_url": r.get("series_url", ""),
                    "brand": "",
                    "model": "",
                    "brand_conf": 0.0,
                    "series_conf": 0.0,
                    "title_raw": r.get("title_raw", ""),
                    "count": r.get("count", ""),
                })

        series_page.close()
        context.close()
        browser.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="例: https://www.autohome.com.cn/rank/1")
    ap.add_argument("--out", required=True, help="出力CSVパス（例: data/series_top50.csv）")
    ap.add_argument("--wait-ms", type=int, default=200, help="スクロールごとの待機ms")
    ap.add_argument("--max-scrolls", type=int, default=200, help="最大スクロール回数")
    args = ap.parse_args()
    run(args.url, args.out, args.wait_ms, args.max_scrolls)


if __name__ == "__main__":
    main()
