#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, re, json, time
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

DETAIL_URL = "https://k.autohome.com.cn/detail/view_{reviewid}.html"

def build_list_url(series_id: str, page: int) -> str:
    if page == 1:
        return f"https://k.autohome.com.cn/{series_id}#pvareaid=3454440"
    else:
        return f"https://k.autohome.com.cn/{series_id}/index_{page}.html?#listcontainer"

# ---- 一覧: 左カラム限定で抽出（右カラム除外）、.html なしも救済 ----
ID_PAT = re.compile(r"/detail/view_([A-Za-z0-9]+)(?:\.html|\.)")

def extract_review_ids_from_list(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    ids = set()
    left = soup.select_one(".con-left")
    scope = left if left else soup

    def in_right(tag) -> bool:
        for anc in tag.parents:
            cls = anc.get("class") or []
            if isinstance(cls, str): cls = [cls]
            if "con-right" in cls: return True
        return False

    for a in scope.select('a[href*="/detail/view_"]'):
        if in_right(a): continue
        href = a.get("href") or ""
        m = ID_PAT.search(href)
        if m: ids.add(m.group(1))

    for li in scope.select("li[data-reviewid]"):
        if in_right(li): continue
        rid = (li.get("data-reviewid") or "").strip()
        if rid: ids.add(rid)
    return list(ids)

# ---- 詳細: DOMから直接テキスト取得（=文字化けしない） ----
DETAIL_SELECTORS = [
    ".text-con p", ".koubei-txt p", ".mouthcon-text p",
    ".text-con", ".koubei-txt", ".mouthcon-text", "article"
]

def extract_dom_text(page) -> tuple[str,str]:
    title = page.title().strip()
    title = re.sub(r"_口碑_汽车之家.*", "", title)
    text = ""
    for css in DETAIL_SELECTORS:
        try:
            blocks = page.eval_on_selector_all(
                css if css.endswith(" p") else (css + " p"),
                "els => els.map(e => e.innerText.trim()).filter(Boolean)"
            )
            if not blocks and not css.endswith(" p"):
                blocks = page.eval_on_selector_all(
                    css, "els => els.map(e => e.innerText.trim()).filter(Boolean)"
                )
            if blocks:
                text = "\n".join(blocks)
                break
        except Exception:
            continue
    text = re.sub(r"\s+", " ", (text or "").strip())
    return title, text

# ---- main ----
def main(series_id: str, pages: int):
    cache_dir = Path("cache") / series_id
    cache_dir.mkdir(parents=True, exist_ok=True)

    all_ids = set()
    with sync_playwright() as pw:
        # 1) 共通ブラウザ＆コンテキスト（使い回し）
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36)"),
            viewport={"width": 1280, "height": 720}
        )
        # 2) 重いリソースをブロック（高速化＆安定化）
        context.route("**/*", lambda route: route.abort()
                      if route.request.resource_type in {"image","stylesheet","font","media"} else route.continue_())

        # 一覧用ページ
        list_page = context.new_page()
        for i in range(1, pages + 1):
            url = build_list_url(series_id, i)
            print(f"[page {i}] fetching… {url}")
            try:
                list_page.goto(url, wait_until="domcontentloaded", timeout=60000)
                try:
                    list_page.wait_for_selector(".con-left a[href*='/detail/view_']", timeout=20000)
                except Exception:
                    list_page.wait_for_selector("a[href*='/detail/view_']", timeout=20000)
            except Exception as e:
                print(f"  !! timeout or load error on page {i}: {e}")
                continue
            html = list_page.content()
            ids = extract_review_ids_from_list(html)
            print(f"[page {i}] found {len(ids)} reviews")
            all_ids.update(ids)

        print(f"[total] unique reviews: {len(all_ids)}")

        # 詳細用ページ（1枚を使い回し）
        detail_page = context.new_page()

        for rid in sorted(all_ids):
            cache_file = cache_dir / f"{rid}.json"
            if cache_file.exists():
                continue
            url = DETAIL_URL.format(reviewid=rid)
            print(f"  fetching detail {url}")

            success = False
            for attempt in (1, 2):  # 最大2回
                try:
                    detail_page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    # 本文の出現待ち（まずは p 付き）
                    waited = False
                    for css in DETAIL_SELECTORS:
                        try:
                            to_wait = css if css.endswith(" p") else (css + " p")
                            detail_page.wait_for_selector(to_wait, timeout=8000)
                            waited = True
                            break
                        except Exception:
                            continue
                    title, text = extract_dom_text(detail_page)
                    if not text and not waited:
                        # 最終フォールバック：body 全体
                        text = detail_page.eval_on_selector_all(
                            "body", "els => els.map(e => e.innerText.trim()).filter(Boolean).join('\\n')"
                        ) or ""
                        text = re.sub(r"\s+", " ", text).strip()

                    data = {"id": rid, "url": url, "title": title, "text": text}
                    with open(cache_file, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    success = True
                    break
                except PWTimeout:
                    print(f"  .. retry {attempt} timeout for {rid}")
                    time.sleep(1)
                except Exception as e:
                    print(f"  !! error for {rid}: {e}")
                    break
            if not success:
                print(f"  !! failed {rid}: fetch timeout or empty")

        detail_page.close()
        list_page.close()
        context.close()
        browser.close()

    # zip（artifact用）
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
