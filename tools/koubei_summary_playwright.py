#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autohome 口コミ要約ツール（Playwright使用）
- 既存フォーマット・成果物は完全維持
- APIコスト削減のため、既知レビューをキャッシュ
"""
import sys, os, re, json, time, hashlib
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from openai import OpenAI

# ==============================
# 設定
# ==============================
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
SERIES_ID = sys.argv[1] if len(sys.argv) > 1 else ""
MAX_PAGES = int(sys.argv[2]) if len(sys.argv) > 2 else 5

if not SERIES_ID:
    print("Usage: python tools/koubei_summary_playwright.py <SERIES_ID> [pages]", file=sys.stderr)
    sys.exit(1)

OUTDIR = Path(f"output/koubei/{SERIES_ID}")
OUTDIR.mkdir(parents=True, exist_ok=True)
CACHEDIR = Path(f"cache/koubei/{SERIES_ID}")
CACHEDIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHEDIR / "summaries.jsonl"

# ==============================
# キャッシュ機能
# ==============================
def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def load_cache() -> dict:
    if not CACHE_FILE.exists():
        return {}
    data = {}
    for line in CACHE_FILE.read_text(encoding="utf-8").splitlines():
        try:
            obj = json.loads(line)
            data[obj["id"]] = obj
        except Exception:
            continue
    return data

def append_cache(obj: dict):
    with CACHE_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

# ==============================
# LLM要約関数（既存ロジック維持）
# ==============================
def summarize_with_openai(client: OpenAI, text: str) -> str:
    """元のStory生成と同じルールで要約"""
    prompt = (
        "以下は中国の自動車ユーザーによるクチコミです。"
        "重要な満足点、不満点、燃費、価格、快適性などを簡潔にまとめ、"
        "日本語で自然に要約してください。\n\n"
        f"{text}"
    )
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "あなたは自動車レビューを要約する専門アナリストです。"},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3,
        max_tokens=600,
    )
    return resp.choices[0].message.content.strip()

# ==============================
# ページ取得
# ==============================
def fetch_page_html(series_id: str, page: int) -> str:
    url = (
        f"https://k.autohome.com.cn/{series_id}#pvareaid=3454440"
        if page == 1
        else f"https://k.autohome.com.cn/{series_id}/index_{page}.html?#listcontainer"
    )
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        pageobj = ctx.new_page()
        pageobj.goto(url, wait_until="domcontentloaded", timeout=45000)
        pageobj.wait_for_timeout(1500)
        html = pageobj.content()
        ctx.close()
        browser.close()
        return html

# ==============================
# レビュー抽出
# ==============================
def parse_reviews(html: str) -> list:
    soup = BeautifulSoup(html, "lxml")
    reviews = []
    for item in soup.select("li, div.review-item, div.kb-item"):
        rid = None
        href = item.find("a", href=re.compile(r"(view_|detail/)"))
        if href:
            m = re.search(r"(?:view_|detail/)(\d{6,12})", href["href"])
            if m:
                rid = m.group(1)
        if not rid:
            continue

        text_elem = item.select_one(".text-con, .text, .content, .kb-content")
        text = text_elem.get_text(" ", strip=True) if text_elem else ""
        if not text:
            continue
        title_elem = item.select_one(".title, .kb-title")
        title = title_elem.get_text(strip=True) if title_elem else ""
        reviews.append({"id": rid, "title": title, "text": text})
    return reviews

# ==============================
# メイン処理
# ==============================
def main():
    client = OpenAI(api_key=OPENAI_API_KEY)
    cache = load_cache()
    all_results = []

    for p in range(1, MAX_PAGES + 1):
        print(f"[page {p}] fetching…")
        html = fetch_page_html(SERIES_ID, p)
        revs = parse_reviews(html)
        print(f"[page {p}] found {len(revs)} reviews")

        for r in revs:
            rid, text = r["id"], r["text"]
            content_hash = sha1(text)
            # === キャッシュ判定 ===
            if rid in cache and cache[rid].get("content_hash") == content_hash:
                print(f"  [skip cached] {rid}")
                continue

            try:
                summary = summarize_with_openai(client, text)
            except Exception as e:
                print(f"  [error summarizing {rid}] {e}")
                continue

            result = {
                "id": rid,
                "title": r.get("title", ""),
                "text": text,
                "summary": summary,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            all_results.append(result)
            append_cache({"id": rid, "content_hash": content_hash})

        # ページごとに少し待機
        time.sleep(2)

    # === 出力（既存仕様と同じ）===
    if all_results:
        out_json = OUTDIR / f"autohome_reviews_{SERIES_ID}.json"
        out_md = OUTDIR / f"autohome_reviews_{SERIES_ID}.md"

        with out_json.open("w", encoding="utf-8") as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)

        with out_md.open("w", encoding="utf-8") as f:
            for r in all_results:
                f.write(f"## {r['title'] or 'レビュー'} ({r['id']})\n")
                f.write(r["summary"] + "\n\n")

        print(f"[done] saved {len(all_results)} summaries → {out_md.name}")
    else:
        print("[done] no new reviews (all cached)")

if __name__ == "__main__":
    main()
