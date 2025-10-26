#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Autohome series CONFIG -> CSV extractor (no-API version)

- 入力: シリーズID (例: 5213) もしくは config ページURL
- 出力: output/autohome/<series_id>/ 以下に
    - config.raw.json
    - config.csv
    - config_<series_id>.csv   ← YAMLを変えずに済むよう追設（互換）
- Playwright を用いてページを開き、`window.CONFIG` を取得。
- `networkidle` は使わず、`domcontentloaded` 待ち + 直取り/評価の二段構え（3回リトライ）。
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PWTimeoutError

# ---- 環境変数で調整可能なパラメータ -----------------------------

NAV_TIMEOUT_MS = int(os.getenv("NAV_TIMEOUT_MS", "180000"))  # 既定 180 秒
GOTO_WAIT_UNTIL = os.getenv("GOTO_WAIT_UNTIL", "domcontentloaded")  # networkidle は使用しない
OUTPUT_BASE = os.getenv("OUTPUT_BASE", "output/autohome")

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
)

BLOCK_HOSTS = tuple(
    h.strip()
    for h in os.getenv(
        "BLOCK_HOSTS",
        "googletagmanager.com,google-analytics.com,hm.baidu.com,baidu.com",
    ).split(",")
    if h.strip()
)

# -----------------------------------------------------------------

def log(*args: Any) -> None:
    print(*args, flush=True)

def is_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")

def build_series_url(arg: str) -> str:
    """引数がIDならURLに、URLならそのまま返す"""
    if is_url(arg):
        return arg
    series_id = str(int(arg))
    return f"https://www.autohome.com.cn/config/series/{series_id}.html#pvareaid=3454437"

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def extract_config_from_text(html_text: str) -> Optional[Dict[str, Any]]:
    """ページ全体テキストから window.CONFIG = {...}; を素直に抜く"""
    m = re.search(r"window\.CONFIG\s*=\s*(\{.*?\})\s*;", html_text, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None

def goto_and_get_config(page, url: str) -> Optional[Dict[str, Any]]:
    """
    - domcontentloaded まで待機
    - script:has-text("CONFIG") があれば page.content() から直取り
    - ダメなら page.evaluate で window.CONFIG を見る
    - 3回までリトライ
    """
    for attempt in range(3):
        try:
            page.goto(url, wait_until=GOTO_WAIT_UNTIL, timeout=NAV_TIMEOUT_MS)
            try:
                page.wait_for_selector('script:has-text("CONFIG")', timeout=5000)
            except PWTimeoutError:
                pass

            html = page.content()
            cfg = extract_config_from_text(html)
            if cfg:
                return cfg

            try:
                js = page.evaluate("() => (window.CONFIG ? JSON.stringify(window.CONFIG) : null)")
                if js:
                    return json.loads(js)
            except Exception:
                pass

        except PWTimeoutError:
            time.sleep(2 + attempt * 3)
            continue
    return None

def flatten_to_rows(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Autohome CONFIG から、主要な列をフラット化
    - model_name（车型名）
    - 厂商指导价（MSRP）
    - 经销商报价/经销商参考价（ディーラー価格）
    """
    result = cfg.get("result") or cfg

    specs: List[Dict[str, Any]] = result.get("specs") or result.get("speclist") or []
    spec_ids: List[str] = []
    spec_names: List[str] = []

    for s in specs:
        sid = str(s.get("id") or s.get("specid") or "")
        name = str(s.get("name") or s.get("specname") or "").strip()
        if sid:
            spec_ids.append(sid)
            spec_names.append(name)

    price_msrp_per_spec: Dict[int, str] = {}
    price_dealer_per_spec: Dict[int, str] = {}

    def assign_param_values(param_name: str, dest: Dict[int, str], items: List[Dict[str, Any]]):
        for param in items:
            name = str(param.get("name") or "").strip()
            if name != param_name:
                continue
            vals = param.get("valueitems") or []
            for idx, vi in enumerate(vals):
                v = str(vi.get("value") or "").strip()
                if v:
                    dest[idx] = v

    paramtypeitems = result.get("paramtypeitems") or []
    for group in paramtypeitems:
        items = group.get("paramitems") or []
        assign_param_values("厂商指导价", price_msrp_per_spec, items)
        assign_param_values("经销商报价", price_dealer_per_spec, items)
        assign_param_values("经销商参考价", price_dealer_per_spec, items)

    rows: List[Dict[str, Any]] = []
    count = max(len(spec_ids), len(spec_names))
    for i in range(count):
        row = {
            "spec_id": spec_ids[i] if i < len(spec_ids) else "",
            "model_name": spec_names[i] if i < len(spec_names) else "",
            "厂商指导价": price_msrp_per_spec.get(i, ""),
            "经销商价": price_dealer_per_spec.get(i, ""),
        }
        rows.append(row)

    return rows

def write_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        rows = [{"spec_id": "", "model_name": "", "厂商指导价": "", "经销商价": ""}]
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def get_series_id_from_arg(arg: str) -> str:
    if is_url(arg):
        m = re.search(r"/series/(\d+)\.html", arg)
        if m:
            return m.group(1)
        return re.sub(r"\W+", "_", arg)[:32]
    return str(int(arg))

def process_one_series(arg: str) -> None:
    url = build_series_url(arg)
    series_id = get_series_id_from_arg(arg)
    outdir = os.path.join(OUTPUT_BASE, series_id)
    ensure_dir(outdir)

    log(f"Processing series: {series_id}")
    log(f"Loading: {url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-gpu"])
        context = browser.new_context(
            user_agent=USER_AGENT,
            locale="zh-CN",
            extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"},
        )
        page = context.new_page()

        def _route_handler(route):
            req_url = route.request.url
            if any(h in req_url for h in BLOCK_HOSTS):
                return route.abort()
            return route.continue_()

        page.route("**/*", _route_handler)

        cfg = goto_and_get_config(page, url)

        if not cfg:
            log(f"No config found for {series_id}")
            write_json(os.path.join(outdir, "config.raw.json"), {"error": "CONFIG not found"})
            # 下流互換のため「両方のCSV名」を空で出す
            write_csv(os.path.join(outdir, "config.csv"), [])
            write_csv(os.path.join(outdir, f"config_{series_id}.csv"), [])
            context.close()
            browser.close()
            return

        write_json(os.path.join(outdir, "config.raw.json"), cfg)

        rows = flatten_to_rows(cfg)
        # 互換のため、2つのファイル名で書き出し
        write_csv(os.path.join(outdir, "config.csv"), rows)
        write_csv(os.path.join(outdir, f"config_{series_id}.csv"), rows)

        context.close()
        browser.close()

    # あなたのログ風の確認出力
    top = OUTPUT_BASE
    log("output:")
    for root in sorted({top, os.path.join(top, series_id)}):
        log(root + ":" if root.endswith(series_id) else root)
        if root.endswith(series_id):
            for name in sorted(os.listdir(root)):
                path = os.path.join(root, name)
                log(f"{path}:" if os.path.isdir(path) else f"{path}")

def parse_args() -> List[str]:
    """
    YMLを変更せずに使えるように、以下の両方を受け付ける:
      - 位置引数:  python autohome_config_to_csv.py 5714 8042
      - オプション: python autohome_config_to_csv.py --series 5714 8042
    """
    parser = argparse.ArgumentParser(description="Autohome CONFIG -> CSV")
    parser.add_argument("series_positional", nargs="*", help="シリーズID もしくは config URL（複数可）")
    parser.add_argument("--series", dest="series_opt", nargs="+", help="(互換) シリーズID/URL（複数可）")
    args = parser.parse_args()

    series_list: List[str] = []
    if args.series_opt:
        series_list.extend(args.series_opt)
    if args.series_positional:
        series_list.extend(args.series_positional)

    if not series_list:
        parser.error("series が指定されていません（位置引数 または --series を使ってください）")
    return series_list

def main() -> None:
    series_list = parse_args()
    for s in series_list:
        try:
            process_one_series(s)
        except Exception as e:
            log(f"[ERROR] series={s}: {e}")
            series_id = get_series_id_from_arg(s)
            outdir = os.path.join(OUTPUT_BASE, series_id)
            ensure_dir(outdir)
            write_json(os.path.join(outdir, "config.raw.json"), {"error": str(e)})
            write_csv(os.path.join(outdir, "config.csv"), [])
            write_csv(os.path.join(outdir, f"config_{series_id}.csv"), [])

if __name__ == "__main__":
    main()
