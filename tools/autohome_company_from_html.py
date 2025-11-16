# -*- coding: utf-8 -*-
# tools/autohome_company_from_html.py
#
# SingleFile で保存した Autohome ランキング HTML を直接パースして
# output/company 以下へ CSV と画像を保存する。

import os
import re
import csv
import base64
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime

# ====== 読み込む HTML（あなたの添付ファイル名に書き換えてください） ======
INPUT_HTML = "58591aab-07e9-4d7d-9b5d-28defcb24a22.htm"

# ====== 出力フォルダ ======
BASE_DIR = Path("output/company")
IMG_DIR = BASE_DIR / "images"
CSV_PATH = BASE_DIR / "autohome_company_ranking.csv"

BASE_DIR.mkdir(parents=True, exist_ok=True)
IMG_DIR.mkdir(parents=True, exist_ok=True)


def sanitize_filename(s: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", (s or "company").strip())
    return s[:80].strip("_") or "company"


def save_base64_image(data_url: str, rank: int, manufacturer: str):
    """
    <img src="data:image/xxx;base64,AAAA..."> を PNG として保存
    """
    if not data_url.startswith("data:image"):
        return ""

    try:
        header, b64 = data_url.split(",", 1)
        img_bytes = base64.b64decode(b64)
        fname = f"{rank:03d}_{sanitize_filename(manufacturer)}.png"
        outpath = IMG_DIR / fname
        with open(outpath, "wb") as f:
            f.write(img_bytes)
        return str(outpath)
    except Exception:
        return ""


def parse_delta(card):
    """
    SVG の fill 色 ＋ テキストの数字から
    +2 / -1 / → / NEW を判定する。
    """
    # SVG 探す
    svg = card.find("svg")
    if not svg:
        return "NEW"

    # fill="#xxxxxx"
    svg_html = str(svg)
    fills = re.findall(r'fill="(#?[0-9a-fA-F]{3,6})"', svg_html)
    fills = {f.lower() for f in fills}

    # 数字探す（svg の直近 innerText）
    text = svg.get_text(strip=True)
    m = re.search(r"\d+", text)
    num = m.group(0) if m else None

    if not num:
        return "→"

    # 上昇 (F60 / FF6600)
    if any(x in fills for x in {"#f60", "#ff6600"}):
        return f"+{num}"

    # 下降 (1CCD99)
    if any(x in fills for x in {"#1ccd99"}):
        return f"-{num}"

    # 方向不明 → numberだけ
    return num


def parse_units(card):
    """
    カード全体のテキストから「台数っぽい大きな数字」だけ抜く。
    """
    text = card.get_text(" ", strip=True)
    candidates = re.findall(r"\d{4,7}", text)
    if not candidates:
        return None
    return int(candidates[-1])


def extract_one_card(card):
    """
    個々の <div data-rank-num> から全項目を抽出
    """
    rank_num = int(card.get("data-rank-num"))

    # メーカー名
    name_el = card.select_one(".tw-text-lg.tw-font-medium")
    manufacturer = name_el.get_text(strip=True) if name_el else ""

    # 台数
    units = parse_units(card)

    # 変動
    delta = parse_delta(card)

    # 画像（base64）
    img_tag = card.find("img")
    img_src = img_tag["src"] if img_tag else ""
    img_path = ""
    if img_src.startswith("data:image"):
        img_path = save_base64_image(img_src, rank_num, manufacturer)

    return {
        "rank": rank_num,
        "manufacturer": manufacturer,
        "units": units,
        "delta": delta,
        "image": img_path,
    }


def main():
    with open(INPUT_HTML, "r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f, "lxml")

    cards = soup.find_all("div", attrs={"data-rank-num": True})

    rows = [extract_one_card(card) for card in cards]
    rows = sorted(rows, key=lambda x: x["rank"])

    with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "manufacturer", "units", "delta", "image"])
        for r in rows:
            writer.writerow([
                r["rank"],
                r["manufacturer"],
                r["units"],
                r["delta"],
                r["image"],
            ])

    print(f"CSV saved → {CSV_PATH}")
    print(f"Images saved → {len(list(IMG_DIR.glob('*.png')))} files")


if __name__ == "__main__":
    main()
