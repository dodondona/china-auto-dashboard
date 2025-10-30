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

# ---------- 詳細: DOMから直接テキスト抽出（文字化け不可） ----------
DETAIL_SELECTORS = [
    ".text-con p",          # 現行最有力
    ".koubei-txt p",
    ".mouthcon-text p",
    ".text-con",
    ".koubei-txt",
    ".mouthcon-text",
    "article"
]

def fetch_detail_into_cache(pw, reviewid: str, cache_dir: Path) -> None:
    cache_file = cache_dir / f"{reviewid}.json"
    if cache_file.exists():
        return

    url = DETAIL_URL.format(reviewid=reviewid)
    print(f"  fetching detail {url}")

    def _once() -> dict | None:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)

            # 出現待ち（順番に当てる）— まずは p付きselector
            waited = False
            for css in DETAIL_SELECTORS:
                try:
                    to_wait = css if css.endswith(" p") else (css + " p")
                    page.wait_for_selector(to_wait, timeout=8000)
                    waited = True
                    break
                except Exception:
                    continue
            if not waited:
                # 見出しなどしかない場合でも page.title() だけは拾える
                pass

            # タイトルはブラウザのDOMから（自動で正しいエンコーディング）
            title = page.title().strip()
            title = re.sub(r"_口碑_汽车之家.*", "", title)

            # 本文は DOM から直接 innerText を収集（ブラウザが正しくUnicode化済み）
            text = ""
            for css in DETAIL_SELECTORS:
                try:
                    blocks = page.eval_on_selector_all(
                        css if css.endswith(" p") else (css + " p"),
                        "els => els.map(e => e.innerText.trim()).filter(Boolean)"
                    )
                    if not blocks and not css.endswith(" p"):
                        # p要素が無いタイプ
                        blocks = page.eval_on_selector_all(
                            css, "els => els.map(e => e.innerText.trim()).filter(Boolean)"
                        )
                    if blocks:
                        text = "\n".join(blocks)
                        break
                except Exception:
                    continue

            text = re.sub(r"\s+", " ", (text or "").strip())

            return {"title": title, "text": text}
        finally:
            page.close()
            browser.close()

    data = None
    try:
        data = _once()
    except PWTimeout:
        data = None
    if data is None or not (data.get("title") or data.get("text")):
        # リトライ1回（軽く待つ）
        time.sleep(2)
        try:
            data = _once()
        except Exception:
            data = None

    if not data:
        print(f"  !! failed {reviewid}: fetch timeout or empty")
        return

    # 空テキスト対策：最終フォールバックとしてHTML全体のinnerTextを拾う
    if not data.get("text"):
        # どうしても空なら、ページ全体から
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            data["text"] = page.eval_on_selector_all(
                "body", "els => els.map(e => e.innerText.trim()).filter(Boolean).join('\\n')"
            ) or ""
            data["text"] = re.sub(r"\s+", " ", data["text"]).strip()
        except Exception:
            pass
        finally:
            page.close()
            browser.close()

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
