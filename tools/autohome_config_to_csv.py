# tools/autohome_config_to_csv.py
import csv
import os
from playwright.sync_api import sync_playwright

def _cell_text_enriched(cell):
    """疑似要素(●/○/-)＋子要素テキスト＋単位を合成して1セル文字列に"""
    base = (cell.inner_text() or "").replace("\u00a0"," ").strip()

    def pseudo(el, which):
        try:
            v = el.evaluate(f"el => getComputedStyle(el, '::{which}').content")
            if v and v not in ('none', '""', "''"):
                return v.strip('"').strip("'")
        except Exception:
            pass
        return ""

    icon_before = pseudo(cell, "before")
    icon_after  = pseudo(cell, "after")

    try:
        for k in cell.query_selector_all("*")[:8]:
            cls = (k.get_attribute("class") or "")
            if any(s in cls for s in ("icon","dot","point","state")):
                ib, ia = pseudo(k, "before"), pseudo(k, "after")
                if ib: icon_before = ib + (" " + icon_before if icon_before else "")
                if ia: icon_after  = (icon_after + " " if icon_after else "") + ia
    except Exception:
        pass

    unit_txt = ""
    for sel in (".unit", "[data-unit]", "[aria-label*='单位']", "[class*='unit']"):
        try:
            u = cell.query_selector(sel)
            if u:
                t = (u.inner_text() or "").strip()
                if t and t not in ("-", "—"):
                    unit_txt = t
                    break
        except Exception:
            continue

    parts = []
    try:
        for sel in ("span", "i", "em"):
            for n in cell.query_selector_all(sel)[:6]:
                tt = (n.inner_text() or "").strip()
                if tt and tt not in parts:
                    parts.append(tt)
    except Exception:
        pass
    extra = " ".join(parts)

    # SVGやフォントアイコンで描画されている場合のフォールバック
    if not base.strip():
        try:
            alt = cell.evaluate("el => el.textContent.trim()") or ""
            base = alt.replace("\u00a0", " ").strip()
        except Exception:
            pass

    pieces = [p for p in [icon_before, base, extra, unit_txt, icon_after] if p]
    s = " ".join(pieces).strip()
    return s.replace("－", "-")

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
    url = "https://car.autohome.com.cn/config/series/7578.html"
    out_csv = "output/autohome/7578/config_7578.csv"

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            viewport={"width": 1366, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        print("Loading:", url)
        page.goto(url, wait_until="networkidle", timeout=90000)

        # ゆっくり全域スクロールして遅延描画を促す
        for _ in range(25):
            page.mouse.wheel(0, 1200)
            page.wait_for_timeout(800)
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(5000)

        tables = page.query_selector_all("table")
        print(f"Found {len(tables)} table(s)")
        if not tables:
            print("❌ No tables found. Exiting.")
            browser.close()
            return

        best_table = None
        best_score = 0
        for t in tables:
            rows = t.query_selector_all(":scope>thead>tr, :scope>tbody>tr, :scope>tr")
            rcount = len(rows)
            ccount = max((len(r.query_selector_all('th,td')) for r in rows), default=0)
            score = rcount * ccount
            if score > best_score:
                best_table = t
                best_score = score

        if not best_table:
            print("❌ No usable table found.")
            browser.close()
            return

        print(f"Selected largest table with score={best_score}")
        matrix = extract_matrix(best_table)
        save_csv(matrix, out_csv)

        browser.close()

if __name__ == "__main__":
    main()
