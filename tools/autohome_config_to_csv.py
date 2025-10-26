# tools/autohome_config_to_csv.py
import argparse
import csv
import re
from pathlib import Path
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup  # requires: beautifulsoup4

# --------------------------------
# 共通設定
# --------------------------------
PC_URL = "https://www.autohome.com.cn/config/series/{series}.html#pvareaid=3454437"
MOBILE_URL = "https://m.autohome.com.cn/config/series/{series}.html"

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

# --------------------------------
# 旧テーブル(<table>)用：行列展開
# --------------------------------
ICON_MAP_LEGACY = {
    "icon-point-on": "●",
    "icon-point-off": "○",
    "icon-point-none": "-",
}

def _cell_text_enriched(cell):
    """1セルをテキスト化（iconfontやunitを含む）"""
    base = (cell.inner_text() or "").replace("\u00a0"," ").strip()
    mark = ""
    try:
        for k in cell.query_selector_all("i, span, em"):
            cls = (k.get_attribute("class") or "")
            for key, sym in ICON_MAP_LEGACY.items():
                if key in cls:
                    mark = sym
                    break
            if mark:
                break
    except Exception:
        pass

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
            base = (cell.evaluate("el => el.textContent") or "").replace("\u00a0"," ").strip()
        except Exception:
            pass

    parts = []
    if mark: parts.append(mark)
    if base: parts.append(base)
    if unit and not base.endswith(unit):
        parts.append(unit)
    return " ".join(parts).strip().replace("－","-")

def extract_matrix_from_table(table):
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

def save_csv_matrix(matrix, outpath: Path):
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with open(outpath, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerows(matrix)
    print(f"✅ Saved: {outpath} ({len(matrix)} rows)")

# --------------------------------
# 新レイアウト(divベース)用
# --------------------------------
def parse_div_layout_to_wide_csv(html: str):
    soup = BeautifulSoup(html, "html.parser")

    head = soup.select_one('[class*="style_table_head__"]')
    if not head:
        return None

    head_cells = [c for c in head.find_all(recursive=False) if getattr(c, "name", None)]
    def clean_model_name(t):
        t = norm_space(t)
        t = re.sub(r"^\s*钉在左侧\s*", "", t)
        t = re.sub(r"\s*对比\s*$", "", t)
        return norm_space(t)

    model_names = [clean_model_name(c.get_text(" ", strip=True)) for c in head_cells[1:]]
    n_models = len(model_names)

    def find_container_with(head_node):
        p = head_node
        for _ in range(12):
            p = p.parent
            if not p:
                break
            if p.find(class_=re.compile(r"style_table_title__")) and p.find(class_=re.compile(r"style_row__")):
                return p
        return head_node.parent

    container = find_container_with(head)
    if not container:
        return None

    def is_section_title(node):
        cls = " ".join(node.get("class", []))
        return "style_table_title__" in cls

    def get_section_from_title(node):
        sticky = node.find(class_=re.compile(r"table_title_col"))
        sec = norm_space(sticky.get_text(" ", strip=True) if sticky else node.get_text(" ", strip=True))
        sec = re.sub(r"\s*标配.*$", "", sec)
        sec = re.sub(r"\s*选配.*$", "", sec)
        sec = re.sub(r"\s*- 无.*$", "", sec)
        return norm_space(sec)

    def is_data_row(node):
        cls = " ".join(node.get("class", []))
        return "style_row__" in cls

    def cell_value(td):
        is_solid = bool(td.select_one('[class*="style_col_dot_solid__"]'))
        is_outline = bool(td.select_one('[class*="style_col_dot_outline__"]'))
        txt = norm_space(td.get_text(" ", strip=True))
        if is_solid and not is_outline:
            return "●" if txt in ("", "●", "○") else f"● {txt}"
        if is_outline and not is_solid:
            return "○" if txt in ("", "●", "○") else f"○ {txt}"
        return txt if txt else "–"

    records = []
    current_section = ""
    children = [c for c in container.find_all(recursive=False) if getattr(c, "name", None)]

    for ch in children:
        if ch is head:
            continue
        if is_section_title(ch):
            current_section = get_section_from_title(ch)
            continue
        if is_data_row(ch):
            kids = [k for k in ch.find_all(recursive=False) if getattr(k, "name", None)]
            if not kids:
                continue
            left = norm_space(kids[0].get_text(" ", strip=True))
            cells = kids[1:1+n_models]
            if len(cells) < n_models:
                cells = cells + [soup.new_tag("div")] * (n_models - len(cells))
            elif len(cells) > n_models:
                cells = cells[:n_models]
            vals = [cell_value(td) for td in cells]
            records.append([current_section, left] + vals)

    if not records:
        return None

    header = ["セクション", "項目"] + model_names
    return [header] + records

# --------------------------------
# メイン
# --------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", type=str, required=True, help="Autohome series id (e.g., 6814)")
    ap.add_argument("--outdir", type=str, default="output/autohome", help="Output base dir")
    ap.add_argument("--mobile", action="store_true", help="Use mobile site")
    args = ap.parse_args()

    series = args.series.strip()
    outdir = Path(args.outdir) / series
    outdir.mkdir(parents=True, exist_ok=True)
    url = (MOBILE_URL if args.mobile else PC_URL).format(series=series)

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

        # ✅ リトライ1回だけ追加
        try:
            page.goto(url, wait_until="networkidle", timeout=120000)
        except Exception as e:
            print(f"⚠️ page.goto failed once ({e}), retrying...")
            page.goto(url, wait_until="networkidle", timeout=120000)

        last_h = 0
        for _ in range(40):
            page.mouse.wheel(0, 1400)
            page.wait_for_timeout(700)
            h = page.evaluate("document.scrollingElement.scrollHeight")
            if h == last_h:
                break
            last_h = h
        page.wait_for_timeout(5000)

        html = page.content()
        wide_matrix = parse_div_layout_to_wide_csv(html)

        if wide_matrix:
            out_csv = outdir / f"config_{series}.csv"
            with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
                csv.writer(f).writerows(wide_matrix)
            print(f"✅ Saved (div-layout wide): {out_csv} ({len(wide_matrix)-1} rows)")
            browser.close()
            return

        tables = [t for t in page.query_selector_all("table") if t.is_visible()]
        print(f"Found {len(tables)} table(s)")
        if not tables:
            print("❌ No tables found.")
            browser.close()
            return

        biggest = (None, 0, -1)
        for idx, t in enumerate(tables, start=1):
            rows = t.query_selector_all(":scope>thead>tr, :scope>tbody>tr, :scope>tr")
            rcount = len(rows)
            ccount = max((len(r.query_selector_all("th,td")) for r in rows), default=0)
            score = rcount * ccount
            mat = extract_matrix_from_table(t)
            out_csv = outdir / f"table_{idx:02d}.csv"
            save_csv_matrix(mat, out_csv)
            if score > biggest[1]:
                biggest = (mat, score, idx)

        if biggest[0] is not None:
            out_csv_std = outdir / f"config_{series}.csv"
            save_csv_matrix(biggest[0], out_csv_std)

        browser.close()

if __name__ == "__main__":
    main()
