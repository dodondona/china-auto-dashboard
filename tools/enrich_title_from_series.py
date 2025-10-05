
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
enrich_title_from_series.py
- Input: CSV that contains a 'series_url' column (and other columns such as rank, count, etc.)
- For each row, open the series_url with Playwright and read <title>.
- Parse brand and model from the title and write them back to CSV columns 'brand' and 'model'.
- No LLM. Headless-friendly. Retries for robustness.

Usage:
  python tools/enrich_title_from_series.py --input data/autohome_raw_2025-09_with_series.csv --output data/autohome_raw_2025-09_with_brand.csv
"""

import csv, re, argparse, time, random
from typing import List, Dict, Tuple
from playwright.sync_api import sync_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")

GENERIC = set([
    "汽车之家","参数配置","图片","口碑","论坛","资讯","新车","报价","经销商","视频","车型","首页"
])

SEP = r"[ _\-\–\—\|\｜]+"  # token separators

def read_csv_rows(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv_rows(path: str, rows: List[Dict[str, str]]) -> None:
    if not rows: return
    fields = list(rows[0].keys())
    for c in ["brand", "model", "title_raw"]:
        if c not in fields:
            fields.append(c)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)

def normalize_token(tok: str) -> str:
    s = (tok or "").strip()
    s = re.sub(r"[«»“”\"'《》〖〗\[\]（）\(\)]+", "", s)
    return s

def parse_title(title: str) -> Tuple[str, str]:
    """
    Heuristics:
      - Remove trailing ' - 汽车之家' or similar
      - Split by separators; drop generic tokens; keep Chinese/alnum tokens
      - Choose 2 most informative tokens; prefer shorter as brand, longer as model
    """
    if not title:
        return ("未知", "未知")
    t = title.strip()
    # Drop trailing site suffix
    t = re.sub(r"\s*[-–—\|｜]\s*汽车之家.*$", "", t)
    t = re.sub(r"汽车之家\s*[-–—\|｜]\s*", "", t)

    # Tokenize
    tokens = [normalize_token(x) for x in re.split(SEP, t) if x.strip()]
    tokens = [x for x in tokens if x and x not in GENERIC and not re.fullmatch(r"[^\w\u4e00-\u9fff]+", x)]
    if not tokens:
        return ("未知", "未知")

    # If tokens include both brand & series, usually 2~3 tokens; pick top2 by uniqueness/length
    # Prefer first two tokens
    cands = tokens[:3]
    if len(cands) == 1:
        return ("未知", cands[0])

    # Decide brand/model by length: brand tends to be shorter than series
    a, b = cands[0], cands[1]
    brand, model = (a, b) if len(a) <= len(b) else (b, a)

    # Guardrails
    if not brand: brand = "未知"
    if not model: model = "未知"
    return (brand, model)

def get_title(page, url: str, tries: int = 2) -> str:
    last_err = None
    for i in range(tries):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=20000)
            # If page has heavy scripts, wait a tad
            page.wait_for_timeout(300)
            return page.title()
        except Exception as e:
            last_err = e
            page.wait_for_timeout(int(500 + 400*random.random()))
    return ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--url-col", default="series_url")
    args = ap.parse_args()

    rows = read_csv_rows(args.input)
    if not rows:
        print("入力CSVが空です。"); return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=UA, viewport={"width":1280,"height":1200})
        page = context.new_page()

        for r in rows:
            url = r.get(args.url_col, "")
            if not url:
                continue
            title = get_title(page, url, tries=2)
            r["title_raw"] = title
            b, m = parse_title(title)
            if "brand" not in r or not r["brand"]:
                r["brand"] = b
            else:
                r["brand"] = r["brand"] or b
            # Always prefer title model over previous OCR model
            r["model"] = m

        context.close(); browser.close()

    write_csv_rows(args.output, rows)
    print(f"✅ brand/model 追記: {args.output}  ({len(rows)} 行)")

if __name__ == "__main__":
    main()
