# tools/autohome_config_to_csv.py
import csv
import os
from playwright.sync_api import sync_playwright

URL = "https://www.autohome.com.cn/config/series/7578.html#pvareaid=3454437"
OUT = "output/autohome/7578/config_7578.csv"

# ---- Helper: lazy-load を確実に終わらせる ----
def scroll_until_stable(page, max_loops=40, dy=1400, wait_ms=600, stable_rounds=4):
    """
    ページ下端までスクロールし、scrollHeight が増えなくなるまで待つ。
    stable_rounds 連続で高さが不変なら終了。
    """
    unchanged = 0
    last_h = page.evaluate("document.scrollingElement.scrollHeight")
    for _ in range(max_loops):
        page.mouse.wheel(0, dy)
        page.wait_for_timeout(wait_ms)
        h = page.evaluate("document.scrollingElement.scrollHeight")
        if h == last_h:
            unchanged += 1
            if unchanged >= stable_rounds:
                break
        else:
            unchanged = 0
            last_h = h
    # 念のため最終待ち
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(3000)

# ---- Helper: セル文字列（●○/• + 本文 + 単位）を合成 ----
def _cell_text_enriched(cell):
    # 基本文字
    base = (cell.inner_text() or "").replace("\u00a0", " ").strip()

    # 1) iconfont（class名で判定）→ 記号
    mark = ""
    try:
        for k in cell.query_selector_all("i, span, em"):
            cls = (k.get_attribute("class") or "")
            if "icon-point-on" in cls:   mark = "●"; break
            if "icon-point-off" in cls:  mark = "○"; break
            if "icon-point-none" in cls: mark = "-"; break
    except Exception:
        pass

    # 2) 疑似要素(::before/::after)や子要素の疑似要素に記号がないか拾う
    def pseudo(el, which):
        try:
            v = el.evaluate(f"el => getComputedStyle(el, '::{which}').content")
            if v and v not in ('none', '""', "''"):
                return v.strip('"').strip("'")
        except Exception:
            pass
        return ""
    if not mark:
        b, a = pseudo(cell, "before"), pseudo(cell, "after")
        for ch in (b, a):
            if ch in ("●", "○", "•", "◦", "● "):  # 代表的な丸印
                mark = "●" if "●" in ch else ("○" if "○" in ch else "•")
                break
    if not mark:
        try:
            for k in cell.query_selector_all("*")[:8]:
                b, a = pseudo(k, "before"), pseudo(k, "after")
                for ch in (b, a):
                    if ch in ("●", "○", "•", "◦"):
                        mark = "●" if "●" in ch else ("○" if "○" in ch else "•")
                        break
                if mark: break
        except Exception:
            pass

    # 3) 単位（.unit / data-unit / class*='unit'）
    unit_txt = ""
    for sel in (".unit", "[data-unit]", "[class*='unit']"):
        try:
            u = cell.query_selector(sel)
            if u:
                t = (u.inner_text() or "").strip()
                if t and t not in ("-", "—"):
                    unit_txt = t
                    break
        except Exception:
            continue

    # 4) 空なら textContent でフォールバック（SVG/フォントアイコンの横文字も拾いやすい）
    if not base:
        try:
            base_alt = (cell.evaluate("el => el.textContent") or "").replace("\u00a0", " ").strip()
            base = base_alt
        except Exception:
            pass

    # 5) 合成：記号は先頭、次に本文、最後に単位
    parts = []
    if mark: parts.append(mark)
    if base: parts.append(base)
    if unit_txt and (not base.endswith(unit_txt)):  # 単位が既に含まれていれば重複回避
        parts.append(unit_txt)
    s = " ".join(parts).strip().replace("－", "-")
    return s

# ---- テーブルを2次元に展開（row/colspan ざっくり展開）----
def extract_matrix(table):
    rows = table.query_selector_all(":scope>thead>tr, :scope>tbody>tr, :scope>tr")
    grid, max_cols = [], 0

    def next_free(ridx):
        c = 0
        while True:
            if c >= len(grid[ridx]):
                grid[ridx].extend([""] * (c - len(grid[ridx]) + 1))
            if grid[ridx][c] == "":
                return c
            c += 1

    for ri, r in enumerate(rows):
        grid.append([])
        for cell in r.query_selector_all("th,td"):
            txt = _cell_text_enriched(cell)
            rs = int(cell.get_attribute("rowspan") or "1")
            cs = int(cell.get_attribute("colspan") or "1")
            col = next_free(ri)
            need = col + cs
            if need > len(grid[ri]):
                grid[ri].extend([""] * (need - len(grid[ri])))
            grid[ri][col] = txt
            if rs > 1:
                for k in range(1, rs):
                    rr = ri + k
                    while rr >= len(grid):
                        grid.append([])
                    if len(grid[rr]) < need:
                        grid[rr].extend([""] * (need - len(grid[rr])))
            max_cols = max(max_cols, need)

    for i in range(len(grid)):
        if len(grid[i]) < max_cols:
            grid[i].extend([""] * (max_cols - len(grid[i])))
    return grid

def save_csv(matrix, outpath):
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with open(outpath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerows(matrix)
    print(f"✅ Saved: {outpath} ({len(matrix)} rows)")

def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            viewport={"width": 1366, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36")
        )
        page = context.new_page()
        print("Loading:", URL)
        page.goto(URL, wait_until="networkidle", timeout=120000)

        # ← 以前うまくいっていた“大量に取れた状態”を再現するため、
        #    高さが安定するまでオートスクロールして遅延描画を完了させる
        scroll_until_stable(page, max_loops=40, dy=1400, wait_ms=700, stable_rounds=4)

        # 余計な絞り込みはせず、可視な <table> をすべて対象にし、最も大きいものを選ぶ
        tables = [t for t in page.query_selector_all("table") if t.is_visible()]
        print(f"Found {len(tables)} table(s)")
        if not tables:
            print("❌ No tables found. Exiting.")
            browser.close()
            return

        best_table, best_score = None, 0
        for t in tables:
            rows = t.query_selector_all(":scope>thead>tr, :scope>tbody>tr, :scope>tr")
            rcount = len(rows)
            ccount = max((len(r.query_selector_all("th,td")) for r in rows), default=0)
            score = rcount * ccount
            if score > best_score:
                best_table, best_score = t, score

        print(f"Selected largest table with score={best_score}")
        matrix = extract_matrix(best_table)
        save_csv(matrix, OUT)

        browser.close()

if __name__ == "__main__":
    main()
