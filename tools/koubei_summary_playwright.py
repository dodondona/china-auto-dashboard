#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, re, json, time
from pathlib import Path
from typing import Iterable, List, Dict, Optional
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ========= 定数 =========
LIST_URL_P1 = "https://k.autohome.com.cn/{series_id}"
LIST_URL_PN = "https://k.autohome.com.cn/{series_id}/index_{page}.html?#listcontainer"
DETAIL_URL   = "https://k.autohome.com.cn/detail/view_{reviewid}.html"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ========= 一覧ページから左カラムのみで review_id を抜く（現行方針のまま） =========
def extract_review_ids_from_list_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    left = soup.select_one(".con-left") or soup  # 念のためフォールバック
    ids: List[str] = []
    for a in left.select("a[href]"):
        m = re.search(r"/detail/view_([^.]+)\.html", a.get("href",""))
        if m:
            ids.append(m.group(1))
    # 重複除去（順序維持）
    seen = set()
    out = []
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def fetch_list_review_ids(pw, series_id: str, pages: int) -> List[str]:
    browser = pw.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_extra_http_headers({
        "User-Agent": UA,
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": f"https://k.autohome.com.cn/{series_id}"
    })

    all_ids: List[str] = []
    try:
        for p in range(1, pages+1):
            url = LIST_URL_P1.format(series_id=series_id) if p==1 \
                  else LIST_URL_PN.format(series_id=series_id, page=p)
            print(f"[page {p}] fetching… {url}")
            # 一覧は静的に取れているので従来通り domcontentloaded でOK
            resp = page.goto(url, wait_until="domcontentloaded", timeout=60000)
            if not resp:
                print(f"[page {p}] no response")
                continue
            html = page.content()  # ← ページ側のエンコードを意識せずUTF-8で取れる
            ids = extract_review_ids_from_list_html(html)
            print(f"[page {p}] found {len(ids)} reviews")
            all_ids.extend(ids)
    finally:
        page.close()
        browser.close()

    # 重複除去
    seen, uniq = set(), []
    for rid in all_ids:
        if rid not in seen:
            seen.add(rid)
            uniq.append(rid)
    return uniq

# ========= Shadow DOMを含め「描画後DOM」から本文を抜く =========
_JS_GET_TEXT = r"""
() => {
  function collectText(root) {
    let buf = [];
    function walk(node) {
      if (!node) return;
      if (node.nodeType === Node.TEXT_NODE) {
        const t = node.nodeValue.trim();
        if (t) buf.push(t);
      }
      // Shadow DOM
      if (node.shadowRoot) walk(node.shadowRoot);
      // 子ノード
      const kids = node.childNodes || [];
      for (let i=0; i<kids.length; i++) walk(kids[i]);
    }
    walk(root);
    let text = buf.join("\n");
    // 連続改行の圧縮
    text = text.replace(/\n{3,}/g, "\n\n");
    return text;
  }

  // よくある候補を優先的に
  const candidates = [
    "article", ".text-con", ".content", ".con", ".review", ".kb-detail", ".kb-con",
    ".main", ".wrap", "body"
  ];
  for (const sel of candidates) {
    const el = document.querySelector(sel);
    if (el) {
      const t = collectText(el);
      if (t && t.length > 80) return t;
    }
  }
  // 最悪 body 全体
  return collectText(document.body);
}
"""

def fetch_detail_rendered(page, reviewid: str) -> Optional[Dict]:
    url = DETAIL_URL.format(reviewid=reviewid)
    # SPA/ShadowDOM のため networkidle まで待つ（XHR/水位が落ちるまで）
    page.set_extra_http_headers({
        "User-Agent": UA,
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://k.autohome.com.cn/"
    })
    page.goto(url, wait_until="networkidle", timeout=90000)
    # 明示的に body の可視化を待つ
    page.wait_for_selector("body", timeout=30000)

    # タイトルは document.title
    title = page.evaluate("() => document.title || ''") or ""

    # Shadow DOM 貫通で本文抽出
    content = page.evaluate(_JS_GET_TEXT) or ""

    # 文字数が少なすぎる場合は失敗扱い
    if len(content.strip()) < 40:
        return None

    # ついでに目に見える日付らしき文字列を軽く拾う（失敗してもOK）
    # 例: 2025-08-01 / 2025/08/01 / 2025年08月01日
    visible = content
    m = re.search(r"(20\d{2}[./\-年]\s*\d{1,2}[./\-月]\s*\d{1,2}日?)", visible)
    date_guess = m.group(1) if m else None

    return {
        "title": title.strip() or None,
        "date": date_guess,
        "url": url,
        "content": content.strip()
    }

# ========= 詳細をキャッシュに保存（JSON; UTF-8, ensure_ascii=False） =========
def fetch_detail_into_cache(pw, reviewid: str, cache_dir: Path) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{reviewid}.json"
    if cache_file.exists():
        return

    browser = pw.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1280, "height": 1600})

    try:
        # 1回目トライ
        data = fetch_detail_rendered(page, reviewid)
        if data is None:
            # 軽い再試行（クッキー／ブロック対策）
            time.sleep(1.0)
            page.reload(wait_until="networkidle", timeout=90000)
            data = fetch_detail_rendered(page, reviewid)

        if data is None:
            print(f"    [warn] empty/short content: {reviewid}")
            data = {"title": None, "date": None, "url": DETAIL_URL.format(reviewid=reviewid), "content": None}

        with cache_file.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    finally:
        page.close()
        browser.close()

# ========= メイン =========
def main(series_id: str, pages: int) -> None:
    out_dir  = Path("output") / "koubei" / series_id
    cache_dir = Path("cache") / "koubei" / series_id
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        # 一覧（既存の取得方針はそのまま）
        review_ids = fetch_list_review_ids(pw, series_id, pages)
        if not review_ids:
            print("[done] parsed 0 reviews (DOM未ロードの可能性あり)")
            return

        # 詳細（ここだけ手法を変更：描画後DOMから取得）
        print(f"[detail] fetching {len(review_ids)} reviews…")
        for i, rid in enumerate(review_ids, 1):
            print(f"  [{i}/{len(review_ids)}] {rid}")
            try:
                fetch_detail_into_cache(pw, rid, cache_dir)
            except PWTimeout:
                print(f"    [timeout] {rid}")
            except Exception as e:
                print(f"    [error] {rid}: {e}")

    # zip化（artifact名は従来通り）
    import shutil
    zipname = f"autohome_reviews_{series_id}"
    shutil.make_archive(zipname, "zip", cache_dir)
    print(f"[done] cached and zipped -> {zipname}.zip")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python tools/koubei_summary_playwright.py <series_id> <pages>")
        sys.exit(1)
    series_id = sys.argv[1].strip()
    pages = int(sys.argv[2])
    main(series_id, pages)
