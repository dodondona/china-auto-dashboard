#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autohome 口コミ要約ツール（Playwright）
- 成果物: output/koubei/<sid>/autohome_reviews_<sid>.{json,md,csv,txt} ※従来どおり
- キャッシュ: cache/koubei/<sid>/summaries.jsonl  ※従来どおり＋既知スキップ
- 修正点:
  1) 反ボット回避 (navigator.webdriver 無効化 / zh-CN ヘッダ)
  2) iframe 内も必ず走査・保存 (0件時デバッグHTML出力)
"""
import sys, os, re, json, time, hashlib, csv
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from openai import OpenAI

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
SERIES_ID = sys.argv[1] if len(sys.argv) > 1 else ""
MAX_PAGES = int(sys.argv[2]) if len(sys.argv) > 2 else 5
if not SERIES_ID:
    print("Usage: python tools/koubei_summary_playwright.py <SERIES_ID> [pages]", file=sys.stderr)
    sys.exit(1)

OUTDIR = Path(f"output/koubei/{SERIES_ID}"); OUTDIR.mkdir(parents=True, exist_ok=True)
CACHEDIR = Path(f"cache/koubei/{SERIES_ID}"); CACHEDIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHEDIR / "summaries.jsonl"

def _sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def _load_cache() -> dict:
    data = {}
    if CACHE_FILE.exists():
        for line in CACHE_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line: continue
            try:
                obj = json.loads(line)
                rid = obj.get("id")
                if rid: data[rid] = obj
            except: pass
    return data

def _upsert_cache(entry: dict):
    rows, existed = [], False
    if CACHE_FILE.exists():
        for line in CACHE_FILE.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s: continue
            try:
                o = json.loads(s)
                if o.get("id") == entry.get("id"):
                    rows.append(entry); existed = True
                else:
                    rows.append(o)
            except: pass
    if not existed: rows.append(entry)
    CACHE_FILE.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + ("\n" if rows else ""), encoding="utf-8")

def summarize_with_openai(client: OpenAI, text: str) -> str:
    prompt = (
        "以下は中国の自動車ユーザーによるクチコミです。"
        "重要な満足点、不満点、燃費、価格、快適性などを簡潔にまとめ、"
        "日本語で自然に要約してください。\n\n" + text
    )
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role":"system","content":"あなたは自動車レビューを要約する専門アナリストです。"},
            {"role":"user","content":prompt}
        ],
        temperature=0.3,
        max_tokens=600,
    )
    return resp.choices[0].message.content.strip()

def _page_url(series_id: str, page: int) -> str:
    return (f"https://k.autohome.com.cn/{series_id}#pvareaid=3454440"
            if page == 1 else
            f"https://k.autohome.com.cn/{series_id}/index_{page}.html?#listcontainer")

def _collect_html_from_page(page, page_idx: int) -> list[str]:
    """メインDOM + すべてのiframeのDOMを収集。0件時の解析に必須。"""
    htmls = []
    # メイン
    htmls.append(page.content())
    # iframe
    frames = page.frames
    for i, fr in enumerate(frames):
        if fr == page.main_frame:  # mainは除外（すでに追加済み）
            continue
        try:
            inner = fr.content()
            htmls.append(inner)
        except Exception:
            # 取得できない frame はスキップ
            continue
    # デバッグ保存（後で人間が見れるように）
    if len(htmls) <= 1:
        (OUTDIR / f"debug_page_{page_idx}_main.html").write_text(htmls[0] if htmls else "", encoding="utf-8")
    else:
        (OUTDIR / f"debug_page_{page_idx}_main.html").write_text(htmls[0], encoding="utf-8")
        for i, h in enumerate(htmls[1:], start=1):
            (OUTDIR / f"debug_page_{page_idx}_frame{i}.html").write_text(h or "", encoding="utf-8")
    return htmls

def fetch_all_pages_html(series_id: str, max_pages: int) -> list[list[str]]:
    """
    1ブラウザ/1コンテキストで全ページ取得。
    返り値は「ページごとに [main, frame1, frame2, ...] のHTMLリスト」。
    """
    all_pages_htmls = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-gpu"])
        context = browser.new_context(
            user_agent=os.environ.get("UA_OVERRIDE",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"),
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            viewport={"width": 1280, "height": 2200},
            extra_http_headers={
                "Referer": f"https://k.autohome.com.cn/{series_id}/",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7",
            },
        )
        # 自動化検知の回避
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = window.chrome || { runtime: {} };
            const originalQuery = window.navigator.permissions && window.navigator.permissions.query;
            if (originalQuery) {
              window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                  Promise.resolve({ state: Notification.permission }) :
                  originalQuery(parameters)
              );
            }
            Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN','zh','en'] });
            Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
        """)

        page = context.new_page()
        for i in range(1, max_pages+1):
            url = _page_url(series_id, i)
            print(f"[page {i}] fetching…")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=45000)
                # XHR/動的挿入を待つ
                try: page.wait_for_load_state("networkidle", timeout=15000)
                except PWTimeout: pass
                # コンテナ/カード類の現れを待つ（存在しなくても続行）
                for sel in ["#listcontainer", ".kb-item", ".review-item", ".text-con", ".kb-content"]:
                    try:
                        page.wait_for_selector(sel, timeout=5000)
                        break
                    except PWTimeout:
                        continue
                # ある程度スクロール（frame 内遅延も促す）
                for _ in range(8):
                    page.evaluate("""
                        (sel)=>{
                          const el = document.querySelector(sel);
                          if(el){ el.scrollTop = el.scrollHeight; }
                          window.scrollTo(0, document.body.scrollHeight);
                        }
                    """, "#listcontainer")
                    try: page.wait_for_load_state("networkidle", timeout=3000)
                    except PWTimeout: pass
                    time.sleep(0.35)

                htmls = _collect_html_from_page(page, i)
                all_pages_htmls.append(htmls)
            except Exception as e:
                print(f"[page {i}] error: {e}")
                all_pages_htmls.append([""])

        context.close(); browser.close()
    return all_pages_htmls

def parse_reviews_from_html(html: str) -> list[dict]:
    soup = BeautifulSoup(html or "", "lxml")
    items = []
    for a in soup.select("a[href]"):
        href = a.get("href","")
        m = re.search(r"(?:view_|detail/)(\d{6,12})", href)
        if not m: continue
        rid = m.group(1)
        root = a.find_parent(["li","div"]) or a
        title = ""
        for sel in [".title",".kb-title"]:
            el = root.select_one(sel)
            if el and el.get_text(strip=True):
                title = el.get_text(strip=True); break
        if not title:
            title = a.get_text(strip=True)
        text = ""
        for sel in [".text-con",".text",".content",".kb-content"]:
            el = root.select_one(sel)
            if el and el.get_text(strip=True):
                text = el.get_text(" ", strip=True); break
        items.append({"id": rid, "title": title, "text": text})
    # 重複排除
    seen, uniq = set(), []
    for it in items:
        if it["id"] in seen: continue
        seen.add(it["id"]); uniq.append(it)
    return uniq

def write_outputs_from_cache(cache_map: dict, outdir: Path, series_id: str):
    rows = list(cache_map.values())
    rows.sort(key=lambda r: r.get("timestamp",""), reverse=True)
    # JSON
    (outdir / f"autohome_reviews_{series_id}.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    # MD
    with (outdir / f"autohome_reviews_{series_id}.md").open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(f"## {(r.get('title') or 'レビュー')} ({r.get('id')})\n{r.get('summary','')}\n\n")
    # CSV
    with (outdir / f"autohome_reviews_{series_id}.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f); w.writerow(["id","title","text","summary","timestamp"])
        for r in rows:
            w.writerow([r.get("id",""), r.get("title",""), r.get("text",""), r.get("summary",""), r.get("timestamp","")])
    # TXT
    with (outdir / f"autohome_reviews_{series_id}.txt").open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(f"{(r.get('title') or 'レビュー')} ({r.get('id')})\n{r.get('summary','')}\n\n")

def main():
    client = OpenAI(api_key=OPENAI_API_KEY)
    cache_map = _load_cache()
    parsed_total, new_count = 0, 0

    pages_htmls = fetch_all_pages_html(SERIES_ID, MAX_PAGES)  # [[main, frame1, ...], ...]
    for idx, html_list in enumerate(pages_htmls, start=1):
        page_revs = []
        for h in html_list:
            page_revs.extend(parse_reviews_from_html(h))
        parsed_total += len(page_revs)
        print(f"[page {idx}] found {len(page_revs)} reviews")

        for r in page_revs:
            rid, title = r["id"], r.get("title","")
            text = (r.get("text","") or "").strip()
            if not text:  # テキスト空は従来通りスキップ
                continue
            h = _sha1(text)
            ent = cache_map.get(rid)
            if ent and ent.get("content_hash") == h:
                continue
            try:
                summary = summarize_with_openai(client, text)
            except Exception as e:
                print(f"  [error summarizing {rid}] {e}")
                continue
            entry = {
                "id": rid, "content_hash": h,
                "title": title, "text": text,
                "summary": summary,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            cache_map[rid] = entry
            _upsert_cache(entry)
            new_count += 1
        time.sleep(0.3)

    # 新規0でも毎回 output を作る（従来仕様）
    write_outputs_from_cache(cache_map, OUTDIR, SERIES_ID)

    if parsed_total == 0:
        print("[done] parsed 0 reviews → debug_page_*_main.html / debug_page_*_frame*.html を確認してください")
    else:
        print(f"[done] new_summaries={new_count} total_cached={len(cache_map)} → outputs written")

if __name__ == "__main__":
    main()
