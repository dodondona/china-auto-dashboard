# tools/autohome_config_to_csv.py
# 使い方例:
#   python tools/autohome_config_to_csv.py --series 7578 --outdir output/autohome
# 出力:
#   output/autohome/7578/
#     7578__参数配置__基本参数.csv
#     7578__参数配置__车身.csv
#     ...（複数）...
#     7578__ALL_tables_concat.csv  ← 全テーブル連結1枚

import re, csv, os, argparse, unicodedata
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

PC_TPL = "https://car.autohome.com.cn/config/series/{sid}.html"
M_TPL  = "https://car.m.autohome.com.cn/config/series/{sid}.html"

def slug(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[^\w\u4e00-\u9fff\-]+", "_", s)  # 漢字は残す
    return re.sub(r"_+", "_", s).strip("_")[:40] or "section"

def humanize_context(pw):
    browser = pw.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox","--disable-dev-shm-usage","--disable-gpu",
        ],
    )
    ctx = browser.new_context(
        locale="zh-CN",
        timezone_id="Asia/Shanghai",
        viewport={"width": 1366, "height": 900},
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"),
    )
    ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter){
          if (parameter === 37445) return "Google Inc.";
          if (parameter === 37446) return "ANGLE (Intel, Intel(R) UHD Graphics, D3D11)";
          return getParameter.call(this, parameter);
        };
        Object.defineProperty(Notification, 'permission', { get: () => 'denied' });
    """)
    return browser, ctx

def ensure_param_tab(page):
    # 念のため「参数配置」をクリック
    for txt in ("参数配置", "参数", "配置"):
        try:
            page.get_by_text(txt, exact=False).click(timeout=1500)
            page.wait_for_load_state("networkidle", timeout=8000)
            break
        except Exception:
            pass

def auto_scroll(page, steps=10, dy=1400, wait=400):
    for _ in range(steps):
        page.mouse.wheel(0, dy)
        page.wait_for_timeout(wait)

def table_score(tbl):
    # 小さすぎる表を除外するためのスコア
    rows = tbl.query_selector_all(":scope>thead>tr, :scope>tbody>tr, :scope>tr")
    r = len(rows)
    c = 0
    for tr in rows:
        c = max(c, len(tr.query_selector_all("th,td")))
    # フィルタUIを除去するためのキーワード
    txt = (tbl.inner_text() or "").strip()
    if "筛选条件" in txt or "年代款" in txt:
        return 0  # フィルタ表は除外
    return r * c

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
            txt = (cell.inner_text() or "").replace("\u00a0"," ").strip()
            rs = int(cell.get_attribute("rowspan") or "1")
            cs = int(cell.get_attribute("colspan") or "1")
            col = next_free(ri)
            need = col + cs
            if need > len(grid[ri]): grid[ri].extend([""]*(need-len(grid[ri])))
            grid[ri][col] = txt
            if rs > 1:
                for k in range(1, rs):
                    rr = ri + k
                    while rr >= len(grid): grid.append([])
                    if len(grid[rr]) < need: grid[rr].extend([""]*(need-len(grid[rr])))
            max_cols = max(max_cols, need)
    for i in range(len(grid)):
        if len(grid[i]) < max_cols:
            grid[i].extend([""]*(max_cols-len(grid[i])))
    # 空行の連続は軽く詰める（任意）
    return grid

def section_name_for_table(page, tbl):
    # 直近の見出しを拾ってセクション名にする
    name = "参数配置"
    try:
        # 表の前方にある見出し要素を探索
        heading = tbl.evaluate("""
            (el)=>{
              function prevHead(e){
                while(e && e.previousElementSibling){
                  e = e.previousElementSibling;
                  if(!e) break;
                  const tag = (e.tagName||'').toLowerCase();
                  if(['h1','h2','h3','h4'].includes(tag)) return e.innerText.trim();
                  if(e.className && /title|tit|hd|header|subhead/i.test(e.className)) return e.innerText.trim();
                }
                return null;
              }
              return prevHead(el);
            }
        """)
        if heading and len(heading) <= 40:
            name = heading
        else:
            # 行頭のラベルから推測（例：基本参数/车身/发动机…）
            txt = (tbl.inner_text() or "").splitlines()
            for line in txt[:5]:
                if len(line.strip())>=2 and len(line.strip())<=12:
                    name = line.strip()
                    break
    except Exception:
        pass
    return slug(name)

def write_csv(path: Path, matrix):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        for row in matrix:
            w.writerow(row)

def collect(url, outdir, series):
    with sync_playwright() as pw:
        browser, ctx = humanize_context(pw)
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_load_state("networkidle", timeout=30000)
        ensure_param_tab(page)
        auto_scroll(page, steps=12)

        # 全テーブルを取得し、有用なものだけ残す
        tables = [t for t in page.query_selector_all("table") if t.is_visible()]
        scored = sorted([(table_score(t), t) for t in tables], key=lambda x: x[0], reverse=True)
        # 0スコア（フィルタなど）は除外
        scored = [x for x in scored if x[0] >= 6]  # 行×列が小さすぎる表も落とす

        out_base = Path(outdir) / str(series)
        concat_rows = []

        if not scored:
            raise SystemExit("見出し以外の有効なテーブルが見つかりませんでした。")

        for idx, (_, t) in enumerate(scored, 1):
            sec = section_name_for_table(page, t)
            mat = extract_matrix(t)
            # セクションタイトル行を付けて連結用にも保存
            concat_rows.append([f"[{sec}]"])
            concat_rows.extend(mat)
            concat_rows.append([])

            fname = f"{series}__参数配置__{sec}.csv"
            write_csv(out_base / fname, mat)
            print(f"Saved: {out_base/fname}")

        # 連結版も保存（レビューしやすい）
        write_csv(out_base / f"{series}__ALL_tables_concat.csv", concat_rows)
        print(f"Saved: {out_base/(str(series)+'__ALL_tables_concat.csv')}")

        ctx.close(); browser.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", type=int, required=True, help="Autohome series id, e.g., 7578")
    ap.add_argument("--outdir", type=str, required=True, help="Output directory, e.g., output/autohome")
    ap.add_argument("--mobile", action="store_true", help="Use mobile site")
    args = ap.parse_args()

    url = (M_TPL if args.mobile else PC_TPL).format(sid=args.series)
    collect(url, args.outdir, args.series)

if __name__ == "__main__":
    main()
