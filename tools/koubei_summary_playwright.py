#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autohome 口コミ要約ツール（Playwright使用）
- 既存のStory生成/成果物は完全維持
- 追加: 既知レビューをスキップする簡易キャッシュのみ
- 修正: ページの待機/スクロールを強化（0件回避）
"""
import sys, os, re, json, time, hashlib
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from openai import OpenAI

# ====== 既存と同じ前提の設定 ======
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
SERIES_ID = sys.argv[1] if len(sys.argv) > 1 else ""
MAX_PAGES = int(sys.argv[2]) if len(sys.argv) > 2 else 5
if not SERIES_ID:
    print("Usage: python tools/koubei_summary_playwright.py <SERIES_ID> [pages]", file=sys.stderr)
    sys.exit(1)

OUTDIR = Path(f"output/koubei/{SERIES_ID}")
OUTDIR.mkdir(parents=True, exist_ok=True)

# ====== 追加: キャッシュ（ここだけ新規） ======
CACHEDIR = Path(f"cache/koubei/{SERIES_ID}")
CACHEDIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHEDIR / "summaries.jsonl"

def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def _load_cache() -> dict:
    if not CACHE_FILE.exists():
        return {}
    data = {}
    for line in CACHE_FILE.read_text(encoding="utf-8").splitlines():
        try:
            obj = json.loads(line)
            # { "id": "...", "content_hash": "..." }
            if "id" in obj:
                data[obj["id"]] = obj
        except Exception:
            continue
    return data

def _append_cache(entry: dict):
    with CACHE_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

# ====== 既存ロジック（fetch/parse/要約） ======
def summarize_with_openai(client: OpenAI, text: str) -> str:
    """既存のStory生成ルールを維持（モデルやプロンプトは現状どおり）"""
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

# ---- ここを強化（0件の主因だった待機不足を解消） ----
def fetch_page_html(series_id: str, page: int) -> str:
    url = (
        f"https://k.autohome.com.cn/{series_id}#pvareaid=3454440"
        if page == 1
        else f"https://k.autohome.com.cn/{series_id}/index_{page}.html?#listcontainer"
    )
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-gpu"])
        # CNサイト向けにUA/ロケール/タイムゾーンを明示（以前も別スクリプトでこの手の設定あり）
        context = browser.new_context(
            user_agent=os.environ.get("UA_OVERRIDE",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"),
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            viewport={"width": 1280, "height": 2200}
        )
        pageobj = context.new_page()
        try:
            # 1) 最初はDOM読み込み
            pageobj.goto(url, wait_until="domcontentloaded", timeout=45000)
            # 2) XHRによる差し込みを待つ（networkidle）
            pageobj.wait_for_load_state("networkidle", timeout=15000)
            # 3) 口コミDOMが現れるまで待機（いくつかの候補）
            try:
                pageobj.wait_for_selector(".kb-item, .review-item, .text-con, .kb-content", timeout=8000)
            except PWTimeout:
                pass
            # 4) スクロールで追加ロード（高さが安定するまで / 上限回数）
            last_h = 0
            stable_rounds = 0
            for _ in range(12):
                pageobj.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                pageobj.wait_for_load_state("networkidle", timeout=8000)
                time.sleep(0.6)
                h = pageobj.evaluate("document.body.scrollHeight")
                if h == last_h:
                    stable_rounds += 1
                else:
                    stable_rounds = 0
                last_h = h
                if stable_rounds >= 2:
                    break
            # 5) HTML取得
            html = pageobj.content()
        finally:
            context.close()
            browser.close()
        return html
# ---- 強化ここまで ----

def parse_reviews(html: str) -> list:
    """
    aタグ走査 → view_ / detail/ の数値ID抽出
    親要素から title/text を緩く拾う（クラス依存しすぎない）
    """
    soup = BeautifulSoup(html, "lxml")
    items = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        m = re.search(r"(?:view_|detail/)(\d{6,12})", href)
        if not m:
            continue
        rid = m.group(1)

        root = a.find_parent(["li","div"]) or a
        # タイトル候補
        title = ""
        for sel in [".title", ".kb-title"]:
            el = root.select_one(sel)
            if el and el.get_text(strip=True):
                title = el.get_text(strip=True); break
        if not title:
            title = a.get_text(strip=True)

        # 本文候補（断片）
        text = ""
        for sel in [".text-con", ".text", ".content", ".kb-content"]:
            el = root.select_one(sel)
            if el and el.get_text(strip=True):
                text = el.get_text(" ", strip=True); break

        items.append({"id": rid, "title": title, "text": text})

    # ID重複排除
    seen, uniq = set(), []
    for it in items:
        if it["id"] in seen: continue
        seen.add(it["id"]); uniq.append(it)
    return uniq

def main():
    client = OpenAI(api_key=OPENAI_API_KEY)
    cache = _load_cache()

    all_results = []
    parsed_total = 0
    for p in range(1, MAX_PAGES + 1):
        print(f"[page {p}] fetching…")
        html = fetch_page_html(SERIES_ID, p)
        revs = parse_reviews(html)
        parsed_total += len(revs)
        print(f"[page {p}] found {len(revs)} reviews")

        for r in revs:
            rid, text = r["id"], r.get("text","").strip()
            if not text:
                continue  # 空テキストは従来通りスキップ

            content_hash = _sha1(text)

            # 追加: キャッシュ判定（既知はスキップ）
            if rid in cache and cache[rid].get("content_hash") == content_hash:
                print(f"  [skip cached] {rid}")
                continue

            try:
                summary = summarize_with_openai(client, text)
            except Exception as e:
                print(f"  [error summarizing {rid}] {e}")
                continue

            all_results.append({
                "id": rid,
                "title": r.get("title",""),
                "text": text,
                "summary": summary,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            })

            _append_cache({"id": rid, "content_hash": content_hash})

        time.sleep(1)

    # 出力（従来どおり）
    if all_results:
        out_json = OUTDIR / f"autohome_reviews_{SERIES_ID}.json"
        out_md   = OUTDIR / f"autohome_reviews_{SERIES_ID}.md"
        with out_json.open("w", encoding="utf-8") as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)
        with out_md.open("w", encoding="utf-8") as f:
            for r in all_results:
                f.write(f"## {r['title'] or 'レビュー'} ({r['id']})\n")
                f.write(r["summary"] + "\n\n")
        print(f"[done] saved {len(all_results)} summaries → {out_md.name}")
    else:
        if parsed_total == 0:
            print("[done] parsed 0 reviews (page DOM未ロードの可能性)")
        else:
            print("[done] no new reviews (all cached)")

if __name__ == "__main__":
    main()
