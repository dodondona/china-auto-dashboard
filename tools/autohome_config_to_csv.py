#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autohome CONFIG → CSV
Playwright crawler for each series page
"""

import re
import os
import sys
import csv
import json
import time
import argparse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


def extract_config_json(html: str):
    """HTMLから window.CONFIG のJSONを抽出"""
    m = re.search(r"window\.CONFIG\s*=\s*(\{.*?\})\s*;", html, re.S)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    return None


def write_csv(path, rows):
    """CSV出力（空でも必ず生成）"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        if rows:
            writer.writerows(rows)
        else:
            writer.writerow(["empty"])


def goto_and_get_config(page, url: str):
    """PlaywrightでCONFIG抽出（networkidleは使わない）"""
    for attempt in range(3):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=180000)
            html = page.content()
            cfg = extract_config_json(html)
            if cfg:
                return cfg

            # JS評価でも試す
            js = page.evaluate(
                "() => window.CONFIG ? JSON.stringify(window.CONFIG) : null"
            )
            if js:
                return json.loads(js)
        except PWTimeoutError:
            print(f"[WARN] Timeout at attempt {attempt+1}, retrying...", flush=True)
            time.sleep(2 + attempt * 3)
            continue
        except Exception as e:
            print(f"[ERROR] {e}", flush=True)
            continue
    return None


def main():
    parser = argparse.ArgumentParser(description="Autohome CONFIG to CSV")
    parser.add_argument("series_pos", nargs="*", help="series id(s)")
    parser.add_argument("--series", dest="series_opt", nargs="+")
    args, _unknown = parser.parse_known_args()

    series_list = []
    if args.series_opt:
        series_list.extend(args.series_opt)
    if args.series_pos:
        series_list.extend(args.series_pos)

    if not series_list:
        print("No series specified.", flush=True)
        sys.exit(0)

    print("Processing series:", " ".join(series_list), flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for sid in series_list:
            url = f"https://www.autohome.com.cn/config/series/{sid}.html#pvareaid=3454437"
            print(f"Loading: {url}", flush=True)
            outdir = os.path.join("output", "autohome", sid)
            os.makedirs(outdir, exist_ok=True)

            cfg = goto_and_get_config(page, url)
            if not cfg:
                print(f"No config found for {sid}", flush=True)
                write_csv(os.path.join(outdir, "config.csv"), [])
                write_csv(os.path.join(outdir, f"config_{sid}.csv"), [])
                continue

            # 仮変換: 各モデル名だけ抜き出す
            headers = ["Model", "Value"]
            rows = [headers]
            try:
                for item in cfg.get("result", {}).get("paramtypeitems", []):
                    rows.append([item.get("name", ""), json.dumps(item, ensure_ascii=False)])
            except Exception:
                rows = [["raw_json", json.dumps(cfg, ensure_ascii=False)]]

            write_csv(os.path.join(outdir, "config.csv"), rows)
            write_csv(os.path.join(outdir, f"config_{sid}.csv"), rows)

            print("output:")
            print("autohome")
            print("output/autohome:")
            print(sid)
            print(f"output/autohome/{sid}:", flush=True)

        browser.close()


if __name__ == "__main__":
    main()
