#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, re, json, time
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

"""
Usage:
  python tools/koubei_summary_playwright.py <series_id> <pages>

最小方針（余計なことなし）:
- 一覧は「左カラム（.con-left）」内のみから review_id を抽出（右カラムの固定リンクは除外）
- 詳細は Playwright の response.body() を取得して <meta charset> を見てデコード（GBK/GB2312/UTF-8 自動判定）
- 詳細アクセスは domcontentloaded + リトライ1回、timeout やや長め
- 既存のキャッシュ/zip/artifact の流れはそのまま
"""

DETAIL_URL = "https://k.autohome.com.cn/detail/view_{reviewid}.html"

def build_list_url(series_id: str, page: int) -> str:
    if page == 1:
        return f"https://k.autohome.com.cn/{series_id}#pvareaid=3454440"
    else:
        return f"https://k.autohome.com.cn/{series_id}/index_{page}.html?#listcontainer"

# ---------- 一覧: 左カラム限定で review_id 抽出（右カラム除外）、.html なしも救済 ----------
ID_PAT = re.compile(r"/detail/view_([A-Za-z0-9]+)(?:\.html|\.)")

def extract_review_ids_from_list(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    ids: set[str] = set()

    left = soup.select_one(".con-left")
    scope = left if left else soup  # 左が無い場合のみ全体

    def in_right_column(tag) -> bool:
        for anc in tag.parents:
            classes = anc.get("class") or []
            if isinstance(classes, str):
                classes = [classes]
            if "con-right" in classes:
                return True
        return False

    for a in scope.select('a[href*="/detail/view_"]'):
        if in_right_column(a):
            continue
        href = a.get("href") or ""
        m = ID_PAT.search(href)
        if m:
            ids.add(m.group(1))

    # 互換: data-reviewid
    for li in scope.select("li[data-reviewid]"):
        if in_right_column(li):
            continue
        rid = (li.get("data-reviewid") or "").strip()
        if rid:
            ids.add(rid)

    return list(ids)

# ---------- 詳細: body バイトから charset 判定してデコード ----------
CHARSET_RE = re.compile(br"charset\s*=\s*([a-zA-Z0-9\-\_]+)", re.I)

def decode_html(body: bytes) -> str:
    # <meta ... charset=...> を先頭32KBから検出
    head = body[:32768]
    m = CHARSET_RE.search(head)
    enc = (m.group(1).decode("ascii", "ignore").lower() if m else "")
    for cand in ([enc] if enc else []) + ["utf-8", "gbk", "gb2312", "gb18030"]:
        if not cand:
            continue
        try:
            return body.decode(cand, errors="strict")
        except Exception:
            continue
    # 最後の手段
    return body.decode("utf-8", errors="ignore")

# ---------- 本文抽出部のみ修正 ----------
def parse_detail_html_bytes(body: bytes) -> dict:
    html = decode_html(body)
    soup = BeautifulSoup(html, "lxml")

    title = ""
    t = soup.find("title")
    if t:
        title = re.sub(r"_口碑_汽车之家.*", "", t.get_text(strip=True))

    # 新レイアウト対応: p.kb-item-msg 優先
    nodes = soup.select(".kb-con .kb-item .kb-item-msg, .kb-item-msg")
    if nodes:
        text_blocks = [n.get_text(" ", strip=True) for n in nodes]
    else:
        text_blocks = [p.get_text(" ", strip=True) for p in soup.select(".text-con p")]
        if not text_blocks:
            # 旧レイアウトのフォールバック（元のまま）
            for css in [
                ".koubei-txt p",
                ".mouthcon-text p",
                ".text-con",
                ".koubei-txt",
                ".mouthcon-text",
                "article",
            ]:
                nodes = soup.select(css)
                if nodes:
                    if css.endswith(" p"):
                        text_blocks = [p.get_text(" ", strip=True) for p in nodes]
                    else:
                        text_blocks = [" ".join(n.get_text(" ", strip=True) for n in nodes)]
                    break

    text = "\n".join([s for s in text_blocks if s]).strip()
    text = re.sub(r"\s+", " ", text)
    return {"title": title, "text": text}

# ---------- 詳細取得（domcontentloaded・60s・1回リトライ） ----------
def fetch_detail_into_cache(pw, reviewid: str, cache_dir: Path) -> None:
    cache_file = cache_dir / f"{reviewid}.json"
    if cache_file.exists():
        return

    url = DETAIL_URL.format(reviewid=reviewid)
    print(f"  fetching detail {url}")

    def _once() -> bytes | None:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        # UAを固定（ブロック回避のため穏当なデスクトップUA）
        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=60000)
            if not resp:
                return None
            body = resp.body()
            return body
        finally:
            page.close()
            browser.close()

    body = None
    try:
        body = _once()
    except PWTimeout:
        body = None

    if body is None:
        # 1回だけリトライ（軽く待つ）
        time.sleep(2)
        try:
            body = _once()
        except Exception:
            body = None

    if body is None:
        print(f"  !! failed {reviewid}: fetch timeout")
        return

    data = parse_detail_html_bytes(body)
    data["id"] = reviewid
    data["url"] = url
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ---------- main ----------
def main(series_id: str, pages: int):
    cache_dir = Path("cache") / series_id
    cache_dir.mkdir(parents=True, exist_ok=True)

    all_ids: set[str] = set()
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        for i in range(1, pages + 1):
            url = build_list_url(series_id, i)
            print(f"[page {i}] fetching… {url}")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                # 左カラムのリンクが現れるまで待つ（無ければ通常リンク）
                try:
                    page.wait_for_selector(".con-left a[href*='/detail/view_']", timeout=20000)
                except Exception:
                    page.wait_for_selector("a[href*='/detail/view_']", timeout=20000)
            except Exception as e:
                print(f"  !! timeout or load error on page {i}: {e}")
                continue

            html = page.content()
            ids = extract_review_ids_from_list(html)
            print(f"[page {i}] found {len(ids)} reviews")
            all_ids.update(ids)
        page.close()
        browser.close()

        print(f"[total] unique reviews: {len(all_ids)}")
        for rid in sorted(all_ids):
            try:
                fetch_detail_into_cache(pw, rid, cache_dir)
            except Exception as e:
                print(f"  !! failed {rid}: {e}")

    # zip 化（artifact 用）
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
