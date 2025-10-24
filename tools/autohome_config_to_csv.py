#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import re
import json
import csv
from typing import List, Dict, Any

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from openai import OpenAI


# ==== ユーティリティ =========================================================

def read_series_ids_from_stdin_or_arg() -> List[str]:
    """
    使い方:
      1) 単体指定: python tools/autohome_config_to_csv.py --series 6674
      2) 複数指定: echo -e "6674\n7806\n7538" | python tools/autohome_config_to_csv.py
    """
    if len(sys.argv) > 2 and sys.argv[1] == "--series":
        return [sys.argv[2].strip()]
    series_ids = []
    for line in sys.stdin:
        line = line.strip()
        if line:
            series_ids.append(line)
    return series_ids


def extract_config_json(html: str) -> Dict[str, Any]:
    """
    Autohome 構成ページの window.CONFIG JSON を抽出して dict で返す。
    """
    m = re.search(r"window\.CONFIG\s*=\s*(\{.*?\});", html, re.S)
    if not m:
        raise ValueError("window.CONFIG not found")
    return json.loads(m.group(1))


def ensure_dirs():
    os.makedirs("public/config_csv", exist_ok=True)


# ==== ページ取得 (最小変更) ====================================================

def fetch_config_html(series_id: str) -> str:
    """
    series_id から構成ページを開き、HTML を返す。
    【最小変更のみ】networkidle → domcontentloaded に変更し、
    直後に window.CONFIG の存在を 1 行待機追加。
    """
    url = f"https://www.autohome.com.cn/config/series/{series_id}.html#pvareaid=3454437"
    print(f"[INFO] Fetching: {url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="zh-CN")  # 既存踏襲: ロケールだけ
        page = context.new_page()

        # 既存の軽量化（もし元スクリプトに無ければ削ってください）
        # 画像・フォントは読み込み不要なのでブロック
        try:
            page.route(
                "**/*",
                lambda r: r.abort()
                if r.request.resource_type in ("image", "font")
                else r.continue_(),
            )
        except Exception:
            # 既存と差異が出ないよう握りつぶし（あればそのまま通す）
            pass

        try:
            # ★最小変更ポイント①: wait_until を networkidle → domcontentloaded
            page.goto(url, wait_until="domcontentloaded", timeout=120000)
            # ★最小変更ポイント②: CONFIG 出現のみ 1 行で待つ
            page.wait_for_function("Boolean(window.CONFIG)", timeout=60000)
            html = page.content()
        except PlaywrightTimeoutError as e:
            browser.close()
            raise RuntimeError(f"Timeout while loading {url}: {e}") from e
        except Exception as e:
            browser.close()
            raise
        browser.close()
    return html


# ==== 翻訳 (既存踏襲) =========================================================

def translate_text_with_openai(text: str) -> str:
    """
    ChatGPT（OpenAI API）で日本語化。
    既存の挙動踏襲: OPENAI_API_KEY が無ければ翻訳スキップ。
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("[WARN] OPENAI_API_KEY not set. Skipping translation.")
        return text

    client = OpenAI(api_key=api_key)
    prompt = (
        "以下の自動車仕様情報(JSON)のラベル・値を自然な日本語に翻訳してください。"
        "意味は変えず、省略せず、機械的に置き換えないでください。\n\n"
        f"{text}"
    )
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"[WARN] Translation failed: {e}")
        return text


# ==== CSV への保存（既存踏襲・最小変更） =====================================

def save_csv_original_and_translated(config: Dict[str, Any], series_id: str) -> None:
    """
    既存の「CONFIG(原文)＋翻訳」を CSV に保存。
    既存フォーマットから列名/構造を変えると壊れる場合は、必要に応じてここを調整してください。
    """
    ensure_dirs()
    outfile = os.path.join("public", "config_csv", f"{series_id}.csv")

    # 原文を JSON テキスト化
    orig_text = json.dumps(config, ensure_ascii=False, separators=(",", ":"))

    # 翻訳
    translated_text = translate_text_with_openai(orig_text)

    # 既存踏襲: 2 カラム（original_json / translated_json）
    # ※もしあなたの元スクリプトで列名が異なる場合は、列名だけ合わせてください。
    with open(outfile, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["original_json", "translated_json"])
        w.writerow([orig_text, translated_text])

    print(f"[OK] Saved CSV: {outfile}")


# ==== メイン ================================================================

def main():
    series_ids = read_series_ids_from_stdin_or_arg()
    if not series_ids:
        print("[ERROR] No series_id provided (use --series <id> or pipe IDs via stdin).")
        sys.exit(1)

    failed: List[str] = []

    for sid in series_ids:
        try:
            html = fetch_config_html(sid)
            cfg = extract_config_json(html)
            save_csv_original_and_translated(cfg, sid)
        except Exception as e:
            print(f"[ERROR] series {sid} failed: {e}")
            failed.append(sid)
            # 既存挙動を壊さないため、ここで終了させず次へ

    if failed:
        print(f"[SUMMARY] Failed series: {failed}")
        # 既存の終了コードを変えたくない場合は 0 のまま。
        # pipeline を止めたいなら次行をコメントアウト解除:
        # sys.exit(1)
    else:
        print("[SUMMARY] All series completed successfully.")


if __name__ == "__main__":
    main()
