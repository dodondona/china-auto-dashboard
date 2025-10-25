#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
既存の _story.txt を核として残しつつ、ポジ/ネガの厚みと具体例を追記する“補強”工程。
API不要。CSVから代表コメントを拾って自然な追記を行う。

使い方:
  python tools/story_enricher.py 7806 --pos 3 --neg 3 --quotes 2
"""

import os, re, glob, argparse, random
import pandas as pd

def detect_story(vehicle_id: str|None):
    if vehicle_id:
        p = f"autohome_reviews_{vehicle_id}_story.txt"
        if not os.path.exists(p): raise FileNotFoundError(p)
        return p
    txts = sorted(glob.glob("autohome_reviews_*_story.txt"), key=os.path.getmtime, reverse=True)
    if not txts: raise FileNotFoundError("*_story.txt が見つかりません")
    return txts[0]

def detect_csv(vehicle_id: str|None):
    if vehicle_id:
        p = f"autohome_reviews_{vehicle_id}.csv"
        if not os.path.exists(p):
            alt = f"output/autohome/{vehicle_id}/autohome_reviews_{vehicle_id}.csv"
            if os.path.exists(alt): return alt
            raise FileNotFoundError(p)
        return p
    csvs = sorted(glob.glob("autohome_reviews_*.csv"), key=os.path.getmtime, reverse=True)
    if not csvs: raise FileNotFoundError("autohome_reviews_*.csv が見つかりません")
    return csvs[0]

def detect_cols(df: pd.DataFrame):
    # 優先: 日本語 → 中国語 → 旧形式
    if {"pros_ja","cons_ja"}.issubset(df.columns): return "pros_ja","cons_ja"
    if {"pros_zh","cons_zh"}.issubset(df.columns): return "pros_zh","cons_zh"
    if {"pros","cons"}.issubset(df.columns):       return "pros","cons"
    # どれもない場合は空扱い
    return None, None

def split_terms(series):
    s = series.dropna().astype(str).str.split(" / ").explode().str.strip()
    s = s[s!=""]
    return s

def top_k_terms(df, col, k=3):
    if col is None or col not in df.columns: return []
    vc = split_terms(df[col]).value_counts().head(k)
    return [(str(term), int(cnt)) for term, cnt in vc.items()]

def sample_quotes(df, cols, match_terms, each=2, max_len=120):
    """用語を含む行から代表フレーズを抽出し、引用っぽく整形"""
    quotes = []
    cols = [c for c in cols if c and c in df.columns]
    if not cols: return quotes
    used = set()
    for term in match_terms:
        # 対象行のインデックス候補
        idxs = []
        for c in cols:
            hit = df[c].dropna().astype(str)
            idxs += list(hit[hit.str.contains(re.escape(term))].index)
        random.shuffle(idxs)
        picked = 0
        for i in idxs:
            if i in used: continue
            # その行のコメント素材を合成
            frags = []
            for c in cols:
                val = df.at[i, c] if c in df.columns else ""
                if isinstance(val, (str, int)) or (isinstance(val, float) and not pd.isna(val)):
                    v = str(val).strip()
                    if v: frags.append(v)
            if not frags: continue
            text = " / ".join(frags)
            # 長すぎる場合は整形
            text = re.sub(r"\s+", " ", text)
            if len(text) > max_len:
                text = text[:max_len-1] + "…"
            quotes.append(f"> 「{text}」")
            used.add(i)
            picked += 1
            if picked >= each: break
    return quotes

def tidy_japanese(s: str) -> str:
    """軽い日本語整形（記号・全角空白・重複助詞などの最小限）"""
    if not s: return s
    t = s
    # 全角スペース → 半角、重複空白の圧縮
    t = t.replace("\u3000", " ")
    t = re.sub(r"[ \t]+", " ", t)
    # 句読点の連続の簡易整形
    t = re.sub(r"[。、]{3,}", "。", t)
    # 語尾の連続「です。です。」の簡易抑制
    t = re.sub(r"(です。)(\s*です。)+", r"\1", t)
    t = re.sub(r"(ます。)(\s*ます。)+", r"\1", t)
    # 読点のダブり
    t = re.sub(r"、、+", "、", t)
    return t.strip()

def build_reinforcement_block(title, top_items, quotes, tone="positive"):
    """
    「さっきの良かった文」を壊さず、補強だけを下に足す。
    top_items: [(term, cnt), ...]
    quotes: ["> 「…」", ...]
    """
    lines = [f"### {title}（補強）"]
    if top_items:
        # 2〜3項目をまとめて“厚み”の段落に
        names = [t for t,_ in top_items[:3]]
        if tone == "positive":
            core = "、".join(names[:-1]) + ("、" if len(names)>1 else "") + names[-1] if len(names)>=2 else names[0]
            lines.append(f"{core}といった項目で評価が厚く、日常利用での満足度を底上げしています。")
            lines.append("装備や操作性のこなれ感に言及する声も多く、価格に対する納得感が全体のポジティブさを支えています。")
        else:
            core = "、".join(names[:-1]) + ("、" if len(names)>1 else "") + names[-1] if len(names)>=2 else names[0]
            lines.append(f"{core}のほか、体感差の出やすい領域に改善余地を指摘する声がまとまっています。")
            lines.append("長距離・季節要因・路面状況など条件依存の指摘も見られ、購入前の試乗や仕様確認が推奨されます。")
    else:
        lines.append("補強する論点は限定的でした。")

    if quotes:
        lines.append("")
        lines.append("#### 代表コメント（一部抜粋）")
        lines.extend(quotes[:6])
    return "\n".join(lines)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("vehicle_id", nargs="?", help="Autohome vehicle id（例: 7806）")
    ap.add_argument("--pos", type=int, default=3, help="補強に使うポジ項目の数")
    ap.add_argument("--neg", type=int, default=3, help="補強に使うネガ項目の数")
    ap.add_argument("--quotes", type=int, default=2, help="各項目から拾う代表コメント数")
    args = ap.parse_args()

    story_path = detect_story(args.vehicle_id)
    csv_path   = detect_csv(args.vehicle_id)

    # 既存ストーリー（核）読み込み
    with open(story_path, "r", encoding="utf-8") as f:
        base_story = f.read()

    # CSVから“厚み”用の材料を作る
    df = pd.read_csv(csv_path)
    pros_col, cons_col = detect_cols(df)
    # TOP項目
    pos_items = top_k_terms(df, pros_col, k=max(2, min(args.pos, 5)))
    neg_items = top_k_terms(df, cons_col, k=max(2, min(args.neg, 5)))
    # 代表コメント
    pos_quotes = sample_quotes(df, [pros_col, cons_col], [t for t,_ in pos_items], each=max(1, min(args.quotes, 3)))
    neg_quotes = sample_quotes(df, [cons_col, pros_col], [t for t,_ in neg_items], each=max(1, min(args.quotes, 3)))

    # 補強ブロック生成（核を壊さず、下に追加）
    reinf_pos = build_reinforcement_block("ポジティブ面", pos_items, pos_quotes, tone="positive")
    reinf_neg = build_reinforcement_block("ネガティブ面", neg_items, neg_quotes, tone="negative")

    vid_match = re.search(r"autohome_reviews_(\d+)_story\.txt$", os.path.basename(story_path))
    vid = vid_match.group(1) if vid_match else (args.vehicle_id or "unknown")

    # 仕上げ：軽い日本語整形
    final_text = tidy_japanese(base_story.strip()) + "\n\n" + reinf_pos + "\n\n" + reinf_neg + "\n"
    final_md   = "# 口コミ 要約（拡張版）\n\n" + final_text

    out_txt = f"autohome_reviews_{vid}_story_plus.txt"
    out_md  = f"autohome_reviews_{vid}_story_plus.md"
    with open(out_txt, "w", encoding="utf-8") as f: f.write(final_text)
    with open(out_md,  "w", encoding="utf-8") as f: f.write(final_md)

    print(f"✅ enriched story generated: {out_txt}, {out_md}")

if __name__ == "__main__":
    main()
