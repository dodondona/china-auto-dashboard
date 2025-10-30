#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autohome 口コミ要約ツール（Playwright）
- 既存の成果物（output/…/autohome_reviews_*.json/.md/.csv/.txt）を維持
- キャッシュ（cache/koubei/<series_id>/summaries.jsonl）で既知レビューをスキップ
- 新規0件でも cache から毎回 output を再構成（空出力を防止）
"""
import sys, os, re, json, time, hashlib, csv
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from openai import OpenAI

# ===== 実行引数・出力先 =====
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
CACHE_FILE = CACHEDIR / "summaries.jsonl"   # 1行1レビュー

# ===== キャッシュ =====
def _sha1(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()

def _load_cache() -> dict:
    """
    returns: { review_id: {id, content_hash, title, text, summary, timestamp} }
    """
    data = {}
    if CACHE_FILE.exists():
        for line in CACHE_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line: 
                continue
            try:
                obj = json.loads(line)
                rid = obj.get("id")
                if rid:
                    data[rid] = obj
            except Exception:
                continue
    return data

def _upsert_cache(entry: dict):
    """id キーでアップサート"""
    rows = []
    existed = False
    if CACHE_FILE.exists():
        for line in CACHE_FILE.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s: 
                continue
            try:
                obj = json.loads(s)
                if obj.get("id") == entry.get("id"):
                    rows.append(entry)
                    existed = True
                else:
                    rows.append(obj)
            except Exception:
                continue
    if not existed:
        rows.append(entry)
    CACHE_FILE.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + ("\n" if rows else ""),
        encoding="utf-8"
    )

# ===== LLM（既存の方針を踏襲） =====
def summarize_with_openai(client: OpenAI, text: str) -> str:
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

# ===== 取得（最小の安定化：networkidle＋短いスクロール） =====
def fetch_page_html(series_id: str, page: int) -> str:
    url = (
        f"https://k.autohome.com.cn/{series_id}#pvareaid=3454440"
        if page == 1
        else f"https://k.autohome.com.cn/{series_id}/index_{page}.html?#listcontainer"
    )
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-gpu"])
        context = browser.new_context(
            user_agent=os.environ.get("UA_OVERRIDE",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"),
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            viewport={"width": 1280, "height": 2200}
        )
        pageobj = context.new_page()
        try:
            pageobj.goto(url, wait_until="domcontentloaded", timeout=45000)
            try:
                pageobj.wait_for_load_state("networkidle", timeout=10000)
            except PWTimeout:
                pass
            # 簡易スクロール（遅延ロード対策・控えめ回数）
            for _ in range(4):
                pageobj.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                try:
                    pageobj.wait_for_load_state("networkidle", timeout=3000)
                except PWTimeout:
                    pass
                time.sleep(0.4)
            html = pageobj.content()
        finally:
            context.close()
            browser.close()
        return html

# ===== パース（aタグ走査：view_/detail/ の数値ID） =====
def parse_reviews(html: str) -> list:
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

        # 本文候補
        text = ""
        for sel in [".text-con", ".text", ".content", ".kb-content"]:
            el = root.select_one(sel)
            if el and el.get_text(strip=True):
                text = el.get_text(" ", strip=True); break

        items.append({"id": rid, "title": title, "text": text})

    # 重複除去
    seen, uniq = set(), []
    for it in items:
        if it["id"] in seen: 
            continue
        seen.add(it["id"]); uniq.append(it)
    return uniq

# ===== 出力（毎回：cache＋今回新規 から再構成） =====
def write_outputs_from_cache(cache_map: dict, outdir: Path, series_id: str):
    rows = list(cache_map.values())
    # 新しい順（timestampが無い場合も安定動作）
    rows.sort(key=lambda r: r.get("timestamp",""), reverse=True)

    # JSON
    out_json = outdir / f"autohome_reviews_{series_id}.json"
    with out_json.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    # MD
    out_md = outdir / f"autohome_reviews_{series_id}.md"
    with out_md.open("w", encoding="utf-8") as f:
        for r in rows:
            title = r.get("title") or "レビュー"
            rid = r.get("id")
            summary = r.get("summary","")
            f.write(f"## {title} ({rid})\n{summary}\n\n")

    # CSV
    out_csv = outdir / f"autohome_reviews_{series_id}.csv"
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id","title","text","summary","timestamp"])
        for r in rows:
            w.writerow([r.get("id",""), r.get("title",""), r.get("text",""), r.get("summary",""), r.get("timestamp","")])

    # TXT（必要なら：MDのプレーン版）
    out_txt = outdir / f"autohome_reviews_{series_id}.txt"
    with out_txt.open("w", encoding="utf-8") as f:
        for r in rows:
            title = r.get("title") or "レビュー"
            rid = r.get("id")
            summary = r.get("summary","")
            f.write(f"{title} ({rid})\n{summary}\n\n")

def main():
    client = OpenAI(api_key=OPENAI_API_KEY)
    cache_map = _load_cache()
    parsed_total = 0
    new_count = 0

    for p in range(1, MAX_PAGES + 1):
        print(f"[page {p}] fetching…")
        html = fetch_page_html(SERIES_ID, p)
        revs = parse_reviews(html)
        parsed_total += len(revs)
        print(f"[page {p}] found {len(revs)} reviews")

        for r in revs:
            rid, title = r["id"], r.get("title","")
            text = (r.get("text","") or "").strip()
            if not text:
                continue

            h = _sha1(text)
            ent = cache_map.get(rid)

            # 既知 & 内容同一ならスキップ（既存summaryを保持）
            if ent and ent.get("content_hash") == h:
                continue

            # 新規 or 内容更新 → LLM
            try:
                summary = summarize_with_openai(client, text)
            except Exception as e:
                print(f"  [error summarizing {rid}] {e}")
                continue

            entry = {
                "id": rid,
                "content_hash": h,
                "title": title,
                "text": text,
                "summary": summary,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            cache_map[rid] = entry
            _upsert_cache(entry)
            new_count += 1

        time.sleep(0.4)

    # 新規0件でも必ず output を作成（cache から再構成）
    write_outputs_from_cache(cache_map, OUTDIR, SERIES_ID)

    if parsed_total == 0:
        print("[done] parsed 0 reviews (DOM未ロードの可能性あり)")
    else:
        print(f"[done] new_summaries={new_count} total_cached={len(cache_map)} → outputs written")

if __name__ == "__main__":
    main()
