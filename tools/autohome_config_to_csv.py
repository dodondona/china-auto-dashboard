# tools/autohome_config_to_csv.py
import csv
import os
from pathlib import Path
from playwright.sync_api import sync_playwright

URL = "https://car.autohome.com.cn/config/series/7578.html"
OUTDIR = Path("output/autohome/7578")
OUTDIR.mkdir(parents=True, exist_ok=True)

# ← ここに一度ブラウザで調べたクラス名をマッピングしてください（例）
ICON_MAP = {
    "icon-point-on": "●",     # 標配
    "icon-point-off": "○",    # 選配
    "icon-point-none": "-",    # 無
    # もし他のクラス名なら行を足すだけ（例："icon-yes": "●" など）
}

def _cell_text_enriched(cell):
    """1セルをテキスト化：●○-（iconfont）→記号 / 単位→結合 / 本文→そのまま"""
    base = (cell.inner_text() or "").replace("\u00a0"," ").strip()

    # 1) iconfontのclass名で ●/○/- を判定
    mark = ""
    try:
        for k in cell.query_selector_all("i, span, em"):
            cls = (k.get_attribute("class") or "")
            for key, sym in ICON_MAP.items():
                if key in cls:
                    mark = sym
                    break
            if mark:
                break
    except Exception:
        pass

    # 2) 単位（.unit / data-unit / class*='unit'）
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

    # 3) baseが空なら textContent をフォールバック
    if not base:
        try:
            base = (cell.evaluate("el => el.textContent") or "").replace("\u00a0"," ").strip()
        except Exception:
            pass

    # 4) 最終合成：記号→本文→単位（「整车质保 ● 六年或15万公里」等を再現）
    parts = []
    if mark: parts.append(mark)
    if base: parts.append(base)
    if unit and not base.endswith(unit):
        parts.append(unit)
    return " ".join(parts).strip().replace("－","-")

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
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with open(outpath, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerows(matrix)
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

        # ← 遅延ロード安定化（“大量に取れた回”の再現）
        last_h = 0
        for _ in range(40):
            page.mouse.wheel(0, 1400)
            page.wait_for_timeout(700)
            h = page.evaluate("document.scrollingElement.scrollHeight")
            if h == last_h:
                break
            last_h = h
        page.wait_for_timeout(5000)

        # 余計な絞り込みをせず、可視tableを全部CSVにする（上の方の表も取りこぼさない）
        tables = [t for t in page.query_selector_all("table") if t.is_visible()]
        print(f"Found {len(tables)} table(s)")
        if not tables:
            print("❌ No tables found. Exiting.")
            browser.close()
            return

        # すべて個別保存（index順）。必要なら最大テーブルだけ別名で重ねて保存。
        biggest = (None, 0, -1)
        for idx, t in enumerate(tables, start=1):
            rows = t.query_selector_all(":scope>thead>tr, :scope>tbody>tr, :scope>tr")
            rcount = len(rows)
            ccount = max((len(r.query_selector_all("th,td")) for r in rows), default=0)
            score = rcount * ccount
            mat = extract_matrix(t)
            save_csv(mat, OUTDIR / f"table_{idx:02d}.csv")
            if score > biggest[1]:
                biggest = (mat, score, idx)

        # 最大テーブルは従来互換のファイル名でも保存
        if biggest[0] is not None:
            save_csv(biggest[0], OUTDIR / "config_7578.csv")

        browser.close()

if __name__ == "__main__":
    main()
