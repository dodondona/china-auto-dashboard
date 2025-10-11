#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
autohome_link_title_scraper.py

Autohome の月次ランキング一覧ページから
- 車系ページの URL（series_url）
- アンカーの title / テキスト（title_raw）
- rank / count（取得できた範囲で）
- brand / model（title からの素朴な分割。ここは LLM 翻訳前の“中国語素データ”段階）
を抜き出して CSV 化する。

【重要】本スクリプトは “リンク取得＋titleからのタグ取得まで” に限定。
brand_ja / model_ja は空欄のまま列だけ用意（後段 LLM 翻訳で埋める）。
以前のカラム方針を踏襲:
  rank_seq,rank,brand,model,count,series_url,brand_conf,series_conf,title_raw,brand_ja,model_ja

使い方例:
  python autohome_link_title_scraper.py --month 2025-08 \
    --out data/autohome_raw_2025-08_with_brand.csv

  # 直接 URL を渡す場合
  python autohome_link_title_scraper.py \
    --url "https://www.autohome.com.cn/rank/1-3-1071-x/2025-08.html" \
    --out data/autohome_raw_2025-08_with_brand.csv

  # 保存済みのローカル HTML を解析（Playwright 未使用）
  python autohome_link_title_scraper.py --from-html path/to/saved.html --out data/autohome_raw_local_with_brand.csv

依存:
  pip install playwright beautifulsoup4 lxml pandas
  playwright install chromium
"""
from __future__ import annotations
import re, csv, time, argparse, os, sys
from typing import List, Dict, Optional, Tuple
from bs4 import BeautifulSoup
import pandas as pd

# Playwright は --from-html でないときだけ読み込む（CI最小化）
def _lazy_import_playwright():
    from playwright.sync_api import sync_playwright
    return sync_playwright

AUTOCSS_ANCHOR_PATTERNS = [
    # 典型的な「車系詳細」へのリンク（数値IDで終端）
    r"https?://www\.autohome\.com\.cn/\d+/?$",
    r"//www\.autohome\.com\.cn/\d+/?$",
]

INT_RE = re.compile(r"\d+")
COUNT_RE = re.compile(r"(\d{2,})")  # 桁数2以上の数字を優先（週次/月次の販売台数らしさ）

def build_url_from_month(month: str) -> str:
    # month = "YYYY-MM"
    return f"https://www.autohome.com.cn/rank/1-3-1071-x/{month}.html"

def normalize_url(u: str) -> str:
    if u.startswith("//"):
        return "https:" + u
    return u

def extract_rows_via_dom_html(html: str) -> List[Dict]:
    """
    HTML から “リンク要素（車系URL）＋title + テキスト” を拾い、
    近傍のテキストから rank / count の候補も素朴に拾う。

    ※ Autohome 側の構造変化に耐えるため、「a[href~=数値ID]」を基点に行コンテキストテキストを解析。
    """
    soup = BeautifulSoup(html, "lxml")
    anchors = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if any(re.search(p, href) for p in AUTOCSS_ANCHOR_PATTERNS):
            anchors.append(a)

    seen = set()
    rows = []
    for idx, a in enumerate(anchors, 1):
        href = normalize_url(a["href"].strip())
        if href in seen:
            continue
        seen.add(href)

        title_attr = (a.get("title") or "").strip()
        text = (a.get_text(strip=True) or "").strip()
        title_raw = title_attr or text

        # “行”テキストを推定（親のTRや近傍ブロック）
        row_text = ""
        tr = a.find_parent("tr")
        if tr:
            row_text = tr.get_text(" ", strip=True)
        else:
            # 近い div/li を探索
            parent = a
            hop = 0
            while parent and hop < 4 and not row_text:
                parent = parent.find_parent()
                hop += 1
                if parent and parent.name in ("li", "div", "section"):
                    row_text = parent.get_text(" ", strip=True)

        # rank / count の素朴抽出（失敗時は空欄）
        # - rank は行頭 or “第xx名/名次/排名”等の近傍にある小さめ数字を優先
        # - count は行末側の大きめ桁を優先
        rank = ""
        count = ""

        if row_text:
            # 数字列を全部拾ってヒューリスティックで決める
            nums = [int(m.group()) for m in INT_RE.finditer(row_text)]
            if nums:
                # ヒュー: 最初の小さめ数字を rank、最大 or 末尾の大きめ数字を count
                # 例外も多いので完璧は目指さず“最低限”の復元
                rank = str(nums[0])
                # count 候補: 大きい数字優先
                count_candidate = max(nums)
                count = str(count_candidate)

        # brand / model（中国語）は title_raw を素朴分割で推定
        brand, model, b_conf, m_conf = split_brand_model_from_title(title_raw)

        rows.append({
            "rank_seq": idx,
            "rank": rank,
            "brand": brand,
            "model": model,
            "count": count,
            "series_url": href,
            "brand_conf": f"{b_conf:.2f}",
            "series_conf": f"{m_conf:.2f}",
            "title_raw": title_raw,
            "brand_ja": "",
            "model_ja": "",
        })
    return rows

def split_brand_model_from_title(title_raw: str) -> Tuple[str, str, float, float]:
    """
    title の素朴な分割で brand / model（ともに中国語）を推定。
    ※ ここは“LLM 翻訳前”の下ごしらえ段階。厳密さより “壊れない” 方針。
    ルール（上から順に適用）:
      1) 空なら両方空（conf=0）
      2) 中点/ダッシュ/コロン/空白などの区切りで分割 → 先頭=brand, 残り=model
      3) それでもダメなら全体を model として brand=""（LLM 側で最終確定）
    """
    s = (title_raw or "").strip()
    if not s:
        return "", "", 0.0, 0.0

    # よくある区切り候補
    seps = ["：", ":", "·", "・", "—", "－", "-", "—", "—", "—", "—", "—"]  # ダッシュ系多め
    # スペースは最後に（多すぎると誤爆するため）
    # 連続スペースを単一化
    s = re.sub(r"\s+", " ", s)

    # 先にコロン/中点/ダッシュで試す
    for sep in seps:
        if sep in s:
            left, right = s.split(sep, 1)
            left = left.strip()
            right = right.strip()
            if left and right:
                return left, right, 0.7, 0.7

    # 次にスペースでの二分（先頭トークン=brand、残り=model）
    parts = s.split(" ")
    if len(parts) >= 2:
        return parts[0].strip(), " ".join(parts[1:]).strip(), 0.6, 0.6

    # どうにもならない場合は全部 model として返す
    return "", s, 0.3, 0.6

def fetch_html_with_playwright(url: str, wait_ms: int = 1200, max_scrolls: int = 12) -> str:
    """
    Playwright でページ全体をロード→ゆっくりスクロール（遅延読み込み対策）→HTML取得。
    """
    sp = _lazy_import_playwright()()
    html = ""
    with sp() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1400, "height": 2000},
        )
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        # 追加の安定化待機
        page.wait_for_timeout(wait_ms)

        # スクロール（“20件で止まる”対策）
        last_height = 0
        for _ in range(max_scrolls):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(wait_ms)
            height = page.evaluate("document.body.scrollHeight")
            if height == last_height:
                break
            last_height = height

        # “加载更多/查看更多”ボタンがあれば押す（あれば）
        try:
            # 中国語や英語 UI 両対応の緩い探索
            more_btn = page.locator("text=加载更多,查看更多,更多,More,Load more").first
            if more_btn and more_btn.is_visible():
                more_btn.click()
                page.wait_for_timeout(wait_ms)
        except Exception:
            pass

        html = page.content()
        context.close()
        browser.close()
    return html

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", help="YYYY-MM 形式。指定時は Autohome の月別URLを生成します。", default=None)
    ap.add_argument("--url", help="直接 URL を指定。--month より優先。", default=None)
    ap.add_argument("--from-html", help="保存済みのHTMLファイルを直接解析（Playwright不要）", default=None)
    ap.add_argument("--out", required=True, help="出力CSVパス")
    ap.add_argument("--wait-ms", type=int, default=1200)
    ap.add_argument("--max-scrolls", type=int, default=12)
    args = ap.parse_args()

    if args.from_html:
        with open(args.from_html, "r", encoding="utf-8", errors="ignore") as f:
            html = f.read()
        rows = extract_rows_via_dom_html(html)
    else:
        url = args.url or (build_url_from_month(args.month) if args.month else None)
        if not url:
            print("ERROR: --month または --url の指定が必要です。", file=sys.stderr)
            sys.exit(1)
        html = fetch_html_with_playwright(url, wait_ms=args.wait_ms, max_scrolls=args.max_scrolls)
        rows = extract_rows_via_dom_html(html)

    # DataFrame化して出力（カラム順を固定）
    cols = ["rank_seq","rank","brand","model","count","series_url",
            "brand_conf","series_conf","title_raw","brand_ja","model_ja"]
    df = pd.DataFrame(rows, columns=cols)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False, encoding="utf-8-sig")

    print(f"[ok] rows={len(df)} -> {args.out}")

if __name__ == "__main__":
    main()
