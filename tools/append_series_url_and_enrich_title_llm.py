#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
append_series_url_and_enrich_title_llm.py

Autohome 月間ランキング一覧を Playwright で PC版として開き、
rank / brand / model / count / series_url / title_raw を抽出して CSV 出力する。

※ このスクリプトは「抽出専用」です。翻訳や表記ゆらぎの処理は
   translate_brand_model_llm.py（Claude/GPT など）側で実行します。

Usage:
  python tools/append_series_url_and_enrich_title_llm.py \
    --rank-url "https://www.autohome.com.cn/rank/1" \
    --output "data/autohome_raw_2025-08_with_brand.csv"

Options:
  --rank-url     ランキングのベースURL（PC）。月別ページでも可。
  --output       出力CSVパス
  --timeout-ms   セレクタ待ちタイムアウト（ms）[default: 120000]
  --headless     ヘッドレス実行（デフォルト: True）--no-headless で可視化
"""

from __future__ import annotations
import argparse
import csv
import dataclasses
import re
import sys
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright

# =========================
# Dataclass
# =========================

@dataclasses.dataclass
class RankRow:
    rank_seq: int
    rank: int
    brand: str
    model: str
    count: int
    series_url: str
    brand_conf: float
    series_conf: float
    title_raw: str


# =========================
# Helpers
# =========================

PC_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

def force_pc_url(url: str) -> str:
    """m.autohome → www.autohome に強制"""
    parsed = urlparse(url)
    host = parsed.netloc.replace("m.autohome.com.cn", "www.autohome.com.cn")
    return parsed._replace(netloc=host).geturl()

def to_int(text: str) -> int:
    t = re.sub(r"[^\d]", "", text or "")
    return int(t) if t else 0

def text_or_empty(el) -> str:
    if not el:
        return ""
    try:
        return (el.inner_text() or "").strip()
    except Exception:
        try:
            return (el.text_content() or "").strip()
        except Exception:
            return ""

def abs_url(base: str, href: str) -> str:
    if not href:
        return ""
    return urljoin(base, href)

def safe_get_attr(el, name: str) -> str:
    try:
        return (el.get_attribute(name) or "").strip()
    except Exception:
        return ""


# =========================
# Extraction
# =========================

def extract_rows(page) -> List[RankRow]:
    """
    Autohome のランキング DOM は複数パターンがあるため、
    代表的なパターンを順にトライする。
    戻り値は RankRow の配列。
    """
    rows: List[RankRow] = []
    base_url = page.url

    # ---- セレクタ候補（上から順に試す） ----
    # 1) PC版 旧構造: <table class="rank-list"> / <tr>
    # 2) PC版 新構造: <div class="rank-list"> / <div class="item"> など
    # 3) 汎用: data-series-id を持つ要素 + 同一行内のテキスト
    patterns: List[Dict[str, Any]] = [
        {
            "name": "table_tr",
            "container": "table.rank-list, table#rankList",
            "row": "tr",
            "rank": "td:nth-child(1), .rank-num, em.rank",
            "brand": "td:nth-child(2) .brand, td:nth-child(2) a, td:nth-child(2)",
            "model": "td:nth-child(3) .model, td:nth-child(3) a, td:nth-child(3)",
            "count": "td:nth-child(4), .amount, .count",
            "series_link": "td a[href*='/series/'], a.series-link",
            "title": "td:nth-child(2), td:nth-child(3), .title",
        },
        {
            "name": "div_items",
            "container": "div.rank-list, ul.rank-list, div#rankList",
            "row": "div.item, li.item, li, div.row",
            "rank": ".rank-num, em.rank, [data-rank-num]",
            "brand": ".brand, .series .brand, .info .brand",
            "model": ".model, .series .name, .info .model",
            "count": ".amount, .count, .num",
            "series_link": "a[href*='/series/'], a[data-series-id]",
            "title": ".title, .series, .info",
        },
        {
            "name": "generic_series_attr",
            "container": "body",
            "row": "[data-series-id]",
            "rank": "[data-rank-num], .rank-num, em.rank",
            "brand": ".brand, .series .brand, .info .brand",
            "model": ".model, .series .name, .info .model",
            "count": ".amount, .count, .num",
            "series_link": "a[href], a",
            "title": ".title, .series, .info, :scope",
        },
    ]

    for pat in patterns:
        containers = page.locator(pat["container"])
        if containers.count() == 0:
            continue

        # 最初に見つかったコンテナで行を探索
        container = containers.first
        row_loc = container.locator(pat["row"])
        n = row_loc.count()
        if n == 0:
            continue

        for i in range(n):
            item = row_loc.nth(i)

            rank_txt = text_or_empty(item.locator(pat["rank"]).first)
            brand_txt = text_or_empty(item.locator(pat["brand"]).first)
            model_txt = text_or_empty(item.locator(pat["model"]).first)
            count_txt = text_or_empty(item.locator(pat["count"]).first)
            title_txt = text_or_empty(item.locator(pat["title"]).first)

            # link
            link_el = item.locator(pat["series_link"]).first
            href = safe_get_attr(link_el, "href")
            if not href:
                # data-series-id をもっていれば /series/{id}/ 形式を組み立て
                dsid = safe_get_attr(item, "data-series-id") or safe_get_attr(link_el, "data-series-id")
                if dsid:
                    href = f"/series/{dsid}.html"
            series_url = abs_url(base_url, href)

            # brand / model が空で、title から拾えるなら保険で切り出し（簡易）
            if not brand_txt or not model_txt:
                # 「【MODEL】BRAND_～」形式のタイトルに対応（Autohome よくある）
                # 例: 【秦PLUS】比亚迪_秦PLUS报价_...
                m = re.search(r"【(.+?)】\s*([^\s_]+)_", title_txt)
                if m:
                    model_from_title = m.group(1).strip()
                    brand_from_title = m.group(2).strip()
                    brand_txt = brand_txt or brand_from_title
                    model_txt = model_txt or model_from_title

            # さらに保険：_または空白で分割して先頭を brand 候補に
            if (not brand_txt) and model_txt:
                # 例: "比亚迪 秦PLUS" などを想定
                s = model_txt.split()
                if len(s) >= 2 and re.search(r"[\u4e00-\u9fff]", s[0]):
                    brand_txt = s[0]
                    model_txt = " ".join(s[1:])

            # 整形
            rank = to_int(rank_txt) if rank_txt else (i + 1)
            count = to_int(count_txt)

            if not brand_txt and not model_txt and not series_url:
                # データが成立しない行はスキップ
                continue

            rows.append(
                RankRow(
                    rank_seq=rank,
                    rank=rank,
                    brand=brand_txt or "",
                    model=model_txt or "",
                    count=count,
                    series_url=series_url or "",
                    brand_conf=1.0,   # ここでは 1.0 固定（抽出のみ）
                    series_conf=1.0,  # 同上
                    title_raw=title_txt or "",
                )
            )

        if rows:
            break  # 何か取れたら終了

    return rows


# =========================
# Main
# =========================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", required=True, help="Autohome rank base URL (PC)")
    ap.add_argument("--output", required=True, help="CSV output path")
    ap.add_argument("--timeout-ms", type=int, default=120000)
    ap.add_argument("--headless", dest="headless", action="store_true", default=True)
    ap.add_argument("--no-headless", dest="headless", action="store_false")
    args = ap.parse_args()

    rank_url = force_pc_url(args.rank_url)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context(
            user_agent=PC_UA,
            viewport={"width": 1440, "height": 900},
            locale="zh-CN",
        )
        page = context.new_page()

        # PC版を明示
        page.goto(rank_url, wait_until="domcontentloaded")
        # モバイルに飛ばされた場合に備えて再度PCへ
        if "m.autohome.com.cn" in page.url:
            page.goto(force_pc_url(page.url), wait_until="domcontentloaded")

        # 想定される要素のどれかが現れるまで待機（OR待ち）
        selectors_to_wait = [
            "table.rank-list",
            "table#rankList",
            "div.rank-list",
            "ul.rank-list",
            "[data-series-id]",
        ]
        # いずれかがヒットするまで総当たり
        success = False
        for sel in selectors_to_wait:
            try:
                page.wait_for_selector(sel, timeout=args.timeout_ms, state="visible")
                success = True
                break
            except Exception:
                pass

        if not success:
            # 最後にページ読み込み完了は保証しておく
            try:
                page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass

        rows = extract_rows(page)

        browser.close()

    # CSV 出力
    fieldnames = [
        "rank_seq",
        "rank",
        "brand",
        "model",
        "count",
        "series_url",
        "brand_conf",
        "series_conf",
        "title_raw",
    ]
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(dataclasses.asdict(r))

    print(f"Saved {len(rows)} rows -> {args.output}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(2)
