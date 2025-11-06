#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autohome config HTML → CSV 抽出（●/○をサブ項目ごとに維持、改行区切り）
- 外観つきクラス名はバージョンで変わるため、「style_col_dot_」の接頭辞で判定
- solid ⇒ ● , outline ⇒ ○
- サブ項目の直後に（xxxx元）等の価格があれば同じ行に付与
- 日本語CSVでは『セクション』『項目』列を削る（_ja 列のみ残す）

使い方:
  python .github/scripts/autohome_config_html_to_csv.py \
      --html /path/to/series_18.html \
      --csv-ja out/config_18.ja.csv

備考:
- 抽出の基本単位は「行（項目）」→「各グレード列のセル」。
- サブ項目は <i class="style_col_dot__... ..._solid__.../ ..._outline__..."> の直近テキストを拾う。
"""
import re
import sys
import csv
import argparse
from pathlib import Path
from bs4 import BeautifulSoup

DOT_BASE = "style_col_dot_"
DOT_SOLID = "solid"
DOT_OUTLINE = "outline"

def is_dot_icon(tag):
    if tag.name != "i":
        return False
    cls = " ".join(tag.get("class", []))
    return DOT_BASE in cls

def dot_kind_from_classes(classes):
    s = " ".join(classes)
    # 固定プレフィックス + 末尾の種別で判定（ハッシュは都度変化するため無視）
    if f"{DOT_BASE}{DOT_SOLID}" in s:
        return "●"
    if f"{DOT_BASE}{DOT_OUTLINE}" in s:
        return "○"
    # ハッシュの途中に "solid__" / "outline__" が入るケースも拾う
    if re.search(r"solid__", s):
        return "●"
    if re.search(r"outline__", s):
        return "○"
    return ""  # 不明（基本ここには来ない）

def extract_label_with_price(icon_tag):
    """
    アイコン直後のテキストをラベルとして取得し、可能なら直後の（xxxx元）等の括弧表記も連結。
    HTML構造は多様なので、兄弟ノード・親内の次要素などを幅広く探索。
    """
    # 候補: 直後のテキスト or span/div のテキスト
    txt_parts = []

    # 兄弟のテキスト・要素を少しだけ先読み
    cur = icon_tag.next_sibling
    steps = 0
    while cur and steps < 6:
        steps += 1
        if isinstance(cur, str):
            t = cur.strip()
            if t:
                txt_parts.append(t)
                break
        elif hasattr(cur, "get_text"):
            t = cur.get_text(" ", strip=True)
            if t:
                txt_parts.append(t)
                break
        cur = cur.next_sibling

    # 取れなかったら親側も少し見る
    if not txt_parts:
        parent = icon_tag.parent
        if parent:
            t = parent.get_text(" ", strip=True)
            # 親の先頭側に別要素が多い場合、アイコン以降っぽい部分を素直に採れないことがある。
            # 応急的に、全体文から前方の余計な空白を削って使う。
            if t:
                # アイコンのすぐ右に出る語を優先したいので、スペースで分割して先頭～2語程度にする
                txt_parts.append(t.split(" ")[0])

    label = txt_parts[0] if txt_parts else ""

    # 直後の価格（（））を拾う
    price = ""
    # 価格はラベルに一緒に含まれているケースもある
    m = re.search(r"（[^）]*元[^）]*）|\([^)]*元[^)]*\)", label)
    if m:
        # 既にラベルに含まれていればそのまま
        return label
    # 含まれていなければ、アイコン以降にある括弧表記を少し探索
    cur = icon_tag.next_sibling
    steps = 0
    while cur and steps < 8:
        steps += 1
        s = ""
        if isinstance(cur, str):
            s = cur.strip()
        elif hasattr(cur, "get_text"):
            s = cur.get_text(" ", strip=True)
        if s:
            mp = re.search(r"(（[^）]*元[^）]*）|\([^)]*元[^)]*\))", s)
            if mp:
                price = mp.group(1)
                break
        cur = cur.next_sibling

    return (label + (price if price else "")).strip()

def parse_rows(soup):
    """
    行（セクション/項目）を見つけ、各グレード列のセルを抽出する。
    Autohomeの構造は頻繁にclass名が変わるため、semanticな手掛かりで広めに探す。
    - 左側に「項目名」的なセル（タイトル）があり、右に複数列の値セルが並ぶ構造。
    """
    rows = []

    # 代表的な行コンテナ（例: style_row__***）に近いものを広めに
    for row in soup.find_all(lambda t: t.name in ("div","li","tr") and t.find(string=re.compile(r".+")) and t.find(is_dot_icon) or t.find("i")):
        # セクション名/項目名候補（左側タイトル）を拾う
        # タイトルは左側に strong/span などで置かれることが多い
        title_text = ""
        title_node = None

        # “参数”、“配置”テーブルの一般的なラベル領域を推測（太字/タイトルらしさ）
        for cand in row.find_all(["strong","h3","h4","h5","span","div"], limit=6):
            t = cand.get_text(" ", strip=True)
            if t and len(t) <= 30:
                title_text = t
                title_node = cand
                break

        # セル群を推測（i.dot を含む要素をセルと見なして集める）
        cell_candidates = []
        for c in row.find_all(lambda t: t.name in ("div","td","li") and t.find(is_dot_icon)):
            cell_candidates.append(c)

        if not cell_candidates:
            continue

        # セクション/項目の二段構造に対応できるよう，暫定的にタイトルを分割
        section_ja = title_text  # 日本語化は後段（既存の翻訳処理）に委譲
        item_ja = title_text

        # 各セル内で「サブ項目ごとに ●/○＋ラベル(+価格)」を改行で連結
        parsed_cells = []
        for cell in cell_candidates:
            lines = []
            for ico in cell.find_all(is_dot_icon):
                mark = dot_kind_from_classes(ico.get("class", []))
                if not mark:
                    continue
                label = extract_label_with_price(ico)
                if not label:
                    continue
                lines.append(f"{mark} {label}")
            if lines:
                parsed_cells.append("\n".join(lines))
            else:
                # アイコンが取れないセルは素のテキストを置き換えで救済
                t = cell.get_text(" ", strip=True)
                parsed_cells.append(t)

        if parsed_cells:
            rows.append((section_ja, item_ja, parsed_cells))

    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--html", required=True)
    ap.add_argument("--csv-ja", required=True, help="出力: 日本語列のみ（セクション/項目の原語は出さない）")
    args = ap.parse_args()

    html_path = Path(args.html)
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8", errors="ignore"), "lxml")

    rows = parse_rows(soup)

    # 出力列は「セクション_ja」「項目_ja」に加え、右側のトリム列（可変本数）
    # トリム列名は HTML から厳密に取るのが安全だが、既存パイプラインに合わせ「col1,col2,...」で仮置き。
    max_cols = max((len(cells) for _,_,cells in rows), default=0)
    fieldnames = ["セクション_ja", "項目_ja"] + [f"col{i+1}" for i in range(max_cols)]

    with open(args.csv_ja, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for sec_ja, item_ja, cells in rows:
            row = {"セクション_ja": sec_ja, "項目_ja": item_ja}
            for i, v in enumerate(cells):
                row[f"col{i+1}"] = v
            w.writerow(row)

if __name__ == "__main__":
    main()
