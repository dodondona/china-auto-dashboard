# tools/autohome_config_to_csv.py
import csv
import os
from playwright.sync_api import sync_playwright

def _cell_text_enriched(cell):
    """1セルの表示をテキスト化（●○- と単位を補完）"""
    base = (cell.inner_text() or "").replace("\u00a0"," ").strip()

    # iconfont のクラス名で ●/○/- を判定（Autohomeで頻出）
    try:
        for k in cell.query_selector_all("i, span, em"):
            cls = (k.get_attribute("class") or "")
            if "icon-point-on" in cls:
                return "●"
            if "icon-point-off" in cls:
                return "○"
            if "icon-point-none" in cls:
                return "-"
    except Exception:
        pass

    # 単位（.unit / data-unit / class*='unit'）
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

    # テキストが空なら textContent でフォールバック
    if not base:
        try:
            alt = cell.evaluate("el => el.textContent.trim()") or ""
            base = alt.replace("\u00a0", " ").strip()
        except Exception:
            pass

    # 軽く子要素のテキストも結合（過剰にはしない）
    extra = ""
    try:
        parts = []
        for sel in ("span", "i", "em"):
            for n in cell.query_selector_all(sel)[:6]:
                tt = (n.inner_text() or "").strip()
                if tt and tt not in parts:
                    parts.append(tt)
        extra = " ".join(parts)
    except Exception:
        pass

    pieces = [p for p in [base, extra, unit_txt] if p]
    return " ".join(pieces).strip().replace("－", "-")

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
        writer = csv.writer(f)
        writer.writerows(matrix)
    print(f"✅ Saved: {outpath} ({len(matrix)} rows)")

def main():
    # 安定する car.* を使う（見ている www.* と内容は同じ）
    url = "https://car.autohome.com.cn/config/series/7578.html"
    out_csv = "output/autohome/7578/config_7578.csv"

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
        print("Loading:", url)
        page.goto(url, wait_until="networkidle", timeout=120000)

        # ゆっくり深くスクロールして遅延描画を完了させる
        for _ in range(25):
            page.mouse.wheel(0, 1200)
            page.wait_for_timeout(800)
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(5000)

        # 余計な縛りはかけず、ページ内の <table> から最大を採用（以前うまくいっていた方法）
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
            ccount = max((len(r.query_selector_all('th,td')) for r in rows), default=0)
            score = rcount * ccount
            if score > best_score:
                best_table, best_score = t, score

        print(f"Selected largest table with score={best_score}")
        matrix = extract_matrix(best_table)
        save_csv(matrix, out_csv)

        browser.close()

if __name__ == "__main__":
    main()
