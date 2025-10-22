# tools/autohome_config_to_csv.py
import csv
import os
from playwright.sync_api import sync_playwright

def _cell_text_enriched(cell):
    base = (cell.inner_text() or "").replace("\u00a0"," ").strip()

    # ●○-（iconfont）対応
    try:
        for k in cell.query_selector_all("i, span, em"):
            cls = (k.get_attribute("class") or "")
            if "icon-point-on" in cls:   return "●"
            if "icon-point-off" in cls:  return "○"
            if "icon-point-none" in cls: return "-"
    except Exception:
        pass

    # 単位
    unit = ""
    for sel in (".unit", "[data-unit]", "[class*='unit']"):
        try:
            u = cell.query_selector(sel)
            if u:
                t = (u.inner_text() or "").strip()
                if t and t not in ("-", "—"):
                    unit = t
                    break
        except Exception:
            continue

    if not base:
        try:
            base = (cell.evaluate("el => el.textContent") or "").strip()
        except Exception:
            pass

    parts = [p for p in [base, unit] if p]
    return " ".join(parts)

def extract_matrix(table):
    rows = table.query_selector_all(":scope>thead>tr, :scope>tbody>tr, :scope>tr")
    grid, max_cols = [], 0

    def next_free(ridx):
        c = 0
        while True:
            if c >= len(grid[ridx]):
                grid[ridx].extend([""]*(c - len(grid[ridx]) + 1))
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
                grid[ri].extend([""]*(need - len(grid[ri])))
            grid[ri][col] = txt
            if rs > 1:
                for k in range(1, rs):
                    rr = ri + k
                    while rr >= len(grid):
                        grid.append([])
                    if len(grid[rr]) < need:
                        grid[rr].extend([""]*(need - len(grid[rr])))
            max_cols = max(max_cols, need)

    for i in range(len(grid)):
        if len(grid[i]) < max_cols:
            grid[i].extend([""]*(max_cols - len(grid[i])))
    return grid

def save_csv(matrix, outpath):
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with open(outpath, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerows(matrix)
    print(f"✅ Saved: {outpath} ({len(matrix)} rows)")

def main():
    url = "https://car.autohome.com.cn/config/series/7578.html"  # ← car.autohome に戻す
    out_csv = "output/autohome/7578/config_7578.csv"

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(locale="zh-CN", timezone_id="Asia/Shanghai")
        page = context.new_page()
        print("Loading:", url)
        page.goto(url, wait_until="networkidle", timeout=120000)

        # ページが伸び切るまで自動スクロール（以前の成功時と同じ）
        last_height = 0
        for _ in range(40):
            page.mouse.wheel(0, 1400)
            page.wait_for_timeout(800)
            new_height = page.evaluate("document.scrollingElement.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        page.wait_for_timeout(6000)

        # 可視テーブル全て取得
        tables = [t for t in page.query_selector_all("table") if t.is_visible()]
        print(f"Found {len(tables)} table(s)")
        if not tables:
            print("❌ No tables found. Exiting.")
            browser.close()
            return

        best, best_score = None, 0
        for t in tables:
            rows = t.query_selector_all(":scope>thead>tr, :scope>tbody>tr, :scope>tr")
            score = len(rows) * max((len(r.query_selector_all('th,td')) for r in rows), default=0)
            if score > best_score:
                best, best_score = t, score

        print(f"Selected largest table with score={best_score}")
        matrix = extract_matrix(best)
        save_csv(matrix, out_csv)
        browser.close()

if __name__ == "__main__":
    main()
