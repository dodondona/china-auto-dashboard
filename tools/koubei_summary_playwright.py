#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
koubei_summary_playwright.py
Autohome 口碑（レビュー）一覧→詳細を取得して JSONL / cache を生成

■ ポイント（壊れていない部分は維持）
- 一覧は HTML 全体から /detail/view_<id>.html を正規表現で抽出
  + 予備として data-reviewid="…" も拾う（増やすだけ。既存の抽出は壊さない）
- 一覧ページは JS 遅延描画のため networkidle まで待機し、さらに a[href*="/detail/view_"] or li[data-reviewid] が出るまで wait_for_function で待つ
- UA は “extra_http_headers” ではなく “browser.new_context(user_agent=…)” で設定
- 詳細ページは JS 描画後の DOM（page.content()）から Tailwind 構造（tw-whitespace-pre-wrap）優先で本文抽出
  + 取れなければ response.body() をデコードして旧構造のセレクタでもフォールバック
- 出力フォーマット／ファイル名／cache 構造は維持
"""

import sys, re, json, time, shutil
from pathlib import Path
from typing import List, Set
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

DETAIL_URL = "https://k.autohome.com.cn/detail/view_{reviewid}.html"

def build_list_url(series_id: str, page: int) -> str:
    if page == 1:
        return f"https://k.autohome.com.cn/{series_id}#pvareaid=3454440"
    else:
        return f"https://k.autohome.com.cn/{series_id}/index_{page}.html?#listcontainer"

# =========================
# 一覧：review_id 抽出
# =========================
RE_VIEW = re.compile(r"/detail/view_([0-9]+)\.html")

def extract_review_ids_from_list(html: str) -> List[str]:
    ids = list(dict.fromkeys(RE_VIEW.findall(html)))
    if not ids:
        # 予備：モバイル/別テンプレ用
        ids2 = re.findall(r'data-reviewid="([0-9]+)"', html)
        if ids2:
            seen = set(ids)
            for rid in ids2:
                if rid not in seen:
                    ids.append(rid); seen.add(rid)
    return ids

# =========================
# 詳細：本文抽出
# =========================
CHARSET_RE = re.compile(br"charset\s*=\s*([a-zA-Z0-9\-\_]+)", re.I)

def _decode_html_bytes(body: bytes) -> str:
    head = body[:32768]
    enc = ""
    m = CHARSET_RE.search(head)
    if m:
        try:
            enc = m.group(1).decode("ascii", "ignore").lower()
        except Exception:
            enc = ""
    for cand in ([enc] if enc else []) + ["utf-8", "gbk", "gb2312", "gb18030"]:
        if not cand:
            continue
        try:
            return body.decode(cand, errors="strict")
        except Exception:
            continue
    return body.decode("utf-8", errors="ignore")

def _extract_detail_from_rendered_html(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    # タイトル（h1優先、なければ<title>）
    title = ""
    h1 = soup.select_one("h1")
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        t = soup.find("title")
        if t:
            title = re.sub(r"_口碑_汽车之家.*", "", t.get_text(strip=True))

    # 本文（Tailwind 構造優先）
    text = ""
    cont = soup.select_one("div.tw-whitespace-pre-wrap")
    if cont:
        text = cont.get_text("\n", strip=True)

    # 旧構造などのフォールバック
    if not text:
        candidates = [
            "div.content", "section.content", "article", "div#content",
            ".koubei-txt", ".mouthcon-text", ".text-con",
        ]
        for sel in candidates:
            node = soup.select_one(sel)
            if node:
                text = node.get_text("\n", strip=True)
                if text:
                    break
        if not text:
            for psel in [".text-con p", ".koubei-txt p", ".mouthcon-text p"]:
                nodes = soup.select(psel)
                if nodes:
                    text = "\n".join([p.get_text(" ", strip=True) for p in nodes if p.get_text(strip=True)])
                    if text:
                        break

    text = re.sub(r"\s+\n", "\n", text).strip()
    return {"title": title, "text": text}

def fetch_detail_into_cache(context, reviewid: str, cache_dir: Path) -> None:
    cache_file = cache_dir / f"{reviewid}.json"
    if cache_file.exists():
        return

    url = DETAIL_URL.format(reviewid=reviewid)
    print(f"  fetching detail {url}")

    def _once() -> dict | None:
        page = context.new_page()
        try:
            resp = page.goto(url, wait_until="networkidle", timeout=60000)
            try:
                page.get_by_text("展开全文", exact=False).click(timeout=2000)
            except Exception:
                pass

            try:
                page.wait_for_selector("div.tw-whitespace-pre-wrap", timeout=8000)
            except PWTimeout:
                try:
                    page.wait_for_selector(".text-con, .koubei-txt, .mouthcon-text, div.content, section.content, article", timeout=6000)
                except Exception:
                    pass

            html = page.content()
            data = _extract_detail_from_rendered_html(html)

            if (not data.get("text")) and resp:
                try:
                    body = resp.body()
                    html_bytes = _decode_html_bytes(body)
                    fallback = _extract_detail_from_rendered_html(html_bytes)
                    if fallback.get("text"):
                        data = fallback
                except Exception:
                    pass

            return {"id": reviewid, "url": url, "title": data.get("title", ""), "text": data.get("text", "")}
        finally:
            page.close()

    data = None
    try:
        data = _once()
    except Exception:
        data = None
    if (not data) or (not data.get("text")):
        time.sleep(1.0)
        try:
            data = _once()
        except Exception:
            data = None

    if not data:
        data = {"id": reviewid, "url": url, "title": "", "text": ""}

    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# =========================
# メイン
# =========================
def main(series_id: str, pages: int):
    cache_dir = Path("cache") / series_id
    cache_dir.mkdir(parents=True, exist_ok=True)
    out_jsonl = Path(f"autohome_reviews_{series_id}.jsonl")

    all_ids: Set[str] = set()

    with sync_playwright() as p:
        # ▼ 修正点：まず launch、その browser から new_context
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"},
            viewport={"width": 1366, "height": 2200},
        )

        page = context.new_page()
        for i in range(1, pages + 1):
            url = build_list_url(series_id, i)
            print(f"[page {i}] fetching… {url}")
            try:
                page.goto(url, wait_until="networkidle", timeout=60000)
                page.wait_for_function(
                    """() => {
                        return !!(document.querySelector("a[href*='/detail/view_']") ||
                                  document.querySelector("li[data-reviewid]"));
                    }""",
                    timeout=20000
                )
                html = page.content()
                ids = extract_review_ids_from_list(html)
                print(f"[page {i}] found {len(ids)} reviews")
                for rid in ids:
                    all_ids.add(rid)

            except Exception as e:
                print(f"[page {i}] error: {e}")

            time.sleep(0.5)

        page.close()

        print(f"[done] parsed {len(all_ids)} reviews")

        for rid in sorted(all_ids):
            fetch_detail_into_cache(context, rid, cache_dir)
            time.sleep(0.3)

        context.close()
        browser.close()

    with open(out_jsonl, "w", encoding="utf-8") as f:
        for rid in sorted(all_ids):
            cf = cache_dir / f"{rid}.json"
            if cf.exists():
                f.write(cf.read_text(encoding="utf-8").strip() + "\n")

    zipname = f"autohome_reviews_{series_id}"
    shutil.make_archive(zipname, "zip", cache_dir)
    print(f"[done] cached -> {cache_dir}/, zipped -> {zipname}.zip")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python tools/koubei_summary_playwright.py <series_id> <pages>")
        sys.exit(1)
    _series = sys.argv[1].strip()
    _pages = int(sys.argv[2])
    main(_series, _pages)
