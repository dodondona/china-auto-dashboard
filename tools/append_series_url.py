#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_from_html.py

保存した Autohome ランキング HTML から seriesid / seriesname を抽出し、
既存CSVの各行に series_url を追記して別名で保存します。

特徴:
- Playwright不要（クリック/待ち時間ゼロで爆速）
- "seriesid":"7806","seriesname":"星愿" のような埋め込みを正規表現で抽出
- さらに保険として appスキーム/autohomeリンク等からも seriesid を抽出
- CSVとの対応付けは ①名前突合 → ②順番突合 の二段構え
- 既存列は不変更。末尾に 'series_url' を追加

使い方:
python tools/append_series_url_from_html.py \
  --html data/2025最新汽车之家销量榜排行榜-2025年08月-车系月销榜前十名-汽车之家.html \
  --input data/autohome_raw_2025-09.csv \
  --output data/autohome_raw_2025-09_with_series.csv \
  --name-col model_text
"""

import re, csv, argparse, sys
from typing import List, Dict, Tuple, Optional
from pathlib import Path

def read_text(path: str) -> str:
    p = Path(path)
    data = p.read_bytes()
    # 自動判定（UTF-8前提、BOM許容）
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return data.decode("utf-8-sig")
        except UnicodeDecodeError:
            return data.decode("gb18030", errors="ignore")

def read_csv_rows(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv_rows(path: str, rows: List[Dict[str, str]]) -> None:
    if not rows: return
    fields = list(rows[0].keys())
    if "series_url" not in fields:
        fields.append("series_url")
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)

# ---------- HTML 解析 ----------

# 埋め込みJSON様データ: ..."seriesid":"7806","seriesname":"星愿"...
RE_SERIES_PAIR = re.compile(r'"seriesid"\s*:\s*"(\d{3,7})"\s*,\s*"seriesname"\s*:\s*"([^"]+)"')
# appスキーム等: autohome://car/seriesmain?seriesid=5769
RE_SERIES_APP = re.compile(r'seriesid\s*=\s*(\d{3,7})', re.I)
# href 等からの保険: /5769/ や /5769?xxx
RE_SERIES_PATH = re.compile(r'/(\d{3,7})(?:/|[?#]|")')

def normalize(s: str) -> str:
    return re.sub(r'\s+', '', (s or "")).lower()

def to_series_url(series_id: str) -> str:
    return f"https://www.autohome.com.cn/{series_id}/"

def extract_series_from_html(html: str) -> List[Tuple[str, str]]:
    """
    HTMLから (series_id, series_name) をページ出現順で返す。
    まず RE_SERIES_PAIR を使い、見つからない分は他のパターンで補完。
    """
    pairs: List[Tuple[str, str]] = []

    # 1) メイン: seriesid/seriesname のペア
    for sid, sname in RE_SERIES_PAIR.findall(html):
        pairs.append((sid, sname))

    # 2) もし0件/少数なら、保険: seriesid=1234 / /1234/ から推定（nameは空）
    if not pairs:
        # seriesid= の方が確度高いので先
        sids = RE_SERIES_APP.findall(html)
        if not sids:
            sids = RE_SERIES_PATH.findall(html)
        # 重複除去・順序維持
        seen = set()
        for sid in sids:
            if sid not in seen:
                seen.add(sid)
                pairs.append((sid, ""))

    # 去重（同一 series_id を最初の出現だけ残す）
    seen2 = set()
    uniq: List[Tuple[str, str]] = []
    for sid, sname in pairs:
        if sid not in seen2:
            seen2.add(sid)
            uniq.append((sid, sname))
    return uniq

# ---------- 突合ロジック ----------

def attach_by_name_then_order(
    csv_rows: List[Dict[str, str]],
    pairs: List[Tuple[str, str]],
    name_col: str
) -> None:
    """
    1) 名前完全/部分一致で series_url を付与
    2) 付かなかった行はページ順で埋める
    """
    # name -> index map (正規化)
    page_names = [normalize(n) for _, n in pairs]
    page_urls = [to_series_url(sid) for sid, _ in pairs]

    # 1) 名前一致（完全→部分）
    used = set()
    for i, row in enumerate(csv_rows):
        name = normalize(row.get(name_col, ""))
        url = ""
        if not name:
            csv_rows[i]["series_url"] = ""
            continue
        # 完全一致
        for j, pn in enumerate(page_names):
            if j in used: continue
            if pn and pn == name:
                url = page_urls[j]; used.add(j); break
        # 部分一致
        if not url:
            for j, pn in enumerate(page_names):
                if j in used: continue
                if pn and (pn in name or name in pn):
                    url = page_urls[j]; used.add(j); break
        csv_rows[i]["series_url"] = url  # いったんセット（空の場合も）

    # 2) 順番埋め（残り）
    k = 0
    for i, row in enumerate(csv_rows):
        if row.get("series_url"): continue
        while k < len(page_urls) and k in used:
            k += 1
        if k < len(page_urls):
            csv_rows[i]["series_url"] = page_urls[k]
            used.add(k); k += 1
        else:
            csv_rows[i]["series_url"] = ""

# ---------- メイン ----------

def detect_name_col(fieldnames: List[str], preferred: Optional[str]) -> str:
    if preferred and preferred in fieldnames:
        return preferred
    for c in ["model_text", "model", "name", "car", "series_name", "title"]:
        if c in fieldnames: return c
    return fieldnames[0]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--html", required=True, help="保存したランキングHTMLのパス")
    ap.add_argument("--input", required=True, help="既存CSV（追記元）")
    ap.add_argument("--output", required=True, help="出力CSV（series_url追加）")
    ap.add_argument("--name-col", default=None, help="CSVの車名列（未指定なら自動判定）")
    args = ap.parse_args()

    # 入力読み込み
    rows = read_csv_rows(args.input)
    if not rows:
        print("入力CSVが空です。", file=sys.stderr); sys.exit(1)
    name_col = detect_name_col(list(rows[0].keys()), args.name_col)

    # HTML解析
    html = read_text(args.html)
    pairs = extract_series_from_html(html)
    if not pairs:
        print("HTMLから series 情報が抽出できませんでした。", file=sys.stderr); sys.exit(2)

    # 突合（名前→順序）
    attach_by_name_then_order(rows, pairs, name_col=name_col)

    # 出力
    write_csv_rows(args.output, rows)
    print(f"✔ 出力: {args.output}  （{len(rows)}行 / HTML抽出 {len(pairs)}件）")
    print(f"  使用した車名列: {name_col}")

if __name__ == "__main__":
    main()
