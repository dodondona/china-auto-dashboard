#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
autohome_reviews_XXXX_story.txt → CSV化
  車両ID / 全体サマリー / ポジティブ / ネガティブ を抽出して1行出力
"""
import os, re, csv, glob, argparse

def detect_txt(vehicle_id: str|None):
    if vehicle_id:
        p = f"autohome_reviews_{vehicle_id}_story.txt"
        if not os.path.exists(p):
            raise FileNotFoundError(p)
        return p
    txts = sorted(glob.glob("autohome_reviews_*_story.txt"), key=os.path.getmtime, reverse=True)
    if not txts:
        raise FileNotFoundError("story.txt が見つかりません")
    return txts[0]

def parse_story_text(text: str):
    # 各セクションを正規表現で抜き出す
    summary = re.search(r"(?:全体|総評|まとめ)[:：]?\s*(.+)", text)
    pos = re.search(r"(?:ポジティブ|良い点|長所|優点)[:：]?\s*(.+?)(?:ネガティブ|短所|欠点|弱点|$)", text, re.S)
    neg = re.search(r"(?:ネガティブ|短所|欠点|弱点)[:：]?\s*(.+)", text, re.S)

    clean = lambda s: re.sub(r"\s+", " ", s.strip()) if s else ""
    return clean(summary.group(1) if summary else ""), clean(pos.group(1) if pos else ""), clean(neg.group(1) if neg else "")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("vehicle_id", nargs="?", help="Autohome vehicle id（例: 7806）")
    args = ap.parse_args()

    txt_path = detect_txt(args.vehicle_id)
    vid = re.search(r"autohome_reviews_(\d+)_story\.txt$", os.path.basename(txt_path))
    vid = vid.group(1) if vid else (args.vehicle_id or "unknown")

    with open(txt_path, "r", encoding="utf-8") as f:
        text = f.read()

    summary, pos, neg = parse_story_text(text)

    out_csv = f"autohome_reviews_{vid}_story.csv"
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["vehicle_id", "summary", "positive", "negative"])
        writer.writerow([vid, summary, pos, neg])

    print(f"✅ {out_csv} に書き出しました")

if __name__ == "__main__":
    main()
