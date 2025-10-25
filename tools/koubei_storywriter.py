#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
“羅列 → 日本語の読みやすい要約文”に仕上げる最終ライター工程。
- 既存 CSV (pros_ja/cons_ja/sentiment もしくは zh/legacy) を読み込み
- TOP項目と代表コメントを抽出
- OpenAI に1回だけ投げて、日本語の自然文レポートを生成
- API未設定時はテンプレ（ルール）で文章を生成してフォールバック

使い方:
  export OPENAI_API_KEY=sk-xxxx   # APIを使う場合
  python tools/koubei_storywriter.py 5714 --pros 5 --cons 4 --quotes 2 --style executive
"""
import os, sys, re, glob, argparse, random
import pandas as pd
from collections import Counter
from openai import OpenAI

# ---------- 探索ユーティリティ ----------
def detect_csv(vehicle_id: str|None):
    if vehicle_id:
        path = f"autohome_reviews_{vehicle_id}.csv"
        if not os.path.exists(path):
            raise FileNotFoundError(f"CSVが見つかりません: {path}")
        return path
    csvs = sorted(glob.glob("autohome_reviews_*.csv"), key=os.path.getmtime, reverse=True)
    if not csvs:
        raise FileNotFoundError("CSVが見つかりません。先に要約処理を実行してください。")
    return csvs[0]

def sentiment_counts(df):
    if "sentiment" not in df.columns: return (0,0,0,0)
    s = df["sentiment"].astype(str).str.lower()
    pos = int((s=="positive").sum())
    mix = int((s=="mixed").sum())
    neg = int((s=="negative").sum())
    total = len(df)
    return pos, mix, neg, total

def split_terms(series):
    s = series.dropna().astype(str).str.split(" / ").explode().str.strip()
    return s[s!=""]

def top_k(series, k):
    if series is None: return Counter()
    vc = split_terms(series).value_counts()
    return Counter(dict(vc.head(k)))

def choose_representatives(df, col, terms, max_each=2):
    """各上位termにつき、その語を含む行から代表コメントを抽出（重複回避）"""
    reps = []
    used_idx = set()
    col = df[col].fillna("").astype(str)

    def row_source(i):
        parts = []
        # 各候補列を確認
        for c in ["pros_ja","pros_zh","pros","cons_ja","cons_zh","cons"]:
            if c in df.columns:
                val = df.loc[i, c]
                if isinstance(val, (str, int)) or not pd.isna(val):
                    parts.append(str(val))
        return " / ".join(p.strip() for p in parts if p).strip()

    for term in terms:
        picks = []
        cand_idx = [i for i, v in col.items() if term in v]
        random.shuffle(cand_idx)
        for i in cand_idx:
            if i in used_idx: continue
            src = row_source(i)
            if not src: continue
            picks.append(src)
            used_idx.add(i)
            if len(picks) >= max_each: break
        if picks:
            reps.append((term, picks))
    return reps

def detect_cols(df):
    if {"pros_ja","cons_ja"}.issubset(df.columns):
        return "ja","pros_ja","cons_ja"
    if {"pros_zh","cons_zh"}.issubset(df.columns):
        return "zh","pros_zh","cons_zh"
    if {"pros","cons"}.issubset(df.columns):
        text = " ".join(df["pros"].dropna().astype(str).head(40).tolist())
        return ("ja" if re.search(r"[ぁ-ゟ゠-ヿ]", text) else "zh"), "pros","cons"
    return "ja", None, None

def ratio(n, total):
    return 0.0 if total<=0 else round(n/total*100,1)

# ---------- OpenAI ----------
def get_client_or_none():
    key = os.environ.get("OPENAI_API_KEY")
    if not key: return None
    try:
        return OpenAI(api_key=key)
    except Exception:
        return None

def build_prompt(payload, style):
    tone = {
        "executive": "簡潔・客観・要点先出し",
        "friendly": "やわらかい語り口・読みやすさ重視",
        "neutral": "中立で事実に忠実",
    }.get(style, "簡潔・客観・要点先出し")

    pros_lines = [f"- {term}｜{cnt}件（{pct}%）" for term, cnt, pct in payload["pros_top"]]
    cons_lines = [f"- {term}｜{cnt}件（{pct}%）" for term, cnt, pct in payload["cons_top"]]
    rep_lines = []
    for t, quotes in payload["representatives"]:
        for q in quotes:
            if len(q) > 180: q = q[:178] + "…"
            rep_lines.append(f"- [{t}] {q}")

    meta = (
        f"対象ID: {payload['vehicle_id']}\n"
        f"件数: {payload['total']}  "
        f"内訳: Positive {payload['pos']}（{payload['pos_pct']}%） / "
        f"Mixed {payload['mix']}（{payload['mix_pct']}%） / "
        f"Negative {payload['neg']}（{payload['neg_pct']}%）\n"
    )
    pros_block = "\n".join(pros_lines) or "(なし)"
    cons_block = "\n".join(cons_lines) or "(なし)"
    reps_block = "\n".join(rep_lines) or "(代表コメントなし)"

    system = (
        "あなたは自動車ユーザーレビューを日本語で要約する編集者です。"
        "与えられた統計と短い代表コメントから、読み手に伝わる自然な日本語の要約本文を作成してください。"
        "文体は報告書向け。箇条書きは許容。数値は過度に羅列せず要点で示す。"
        "過度な誇張や断定は避け、データの範囲内で表現すること。"
    )
    user = (
        f"文体ガイド: {tone}\n"
        "出力要件:\n"
        "1) 導入の1段落（全体傾向：肯定/否定のバランスを1〜2文）\n"
        "2) ポジティブの要点（2〜4点）\n"
        "3) ネガティブの要点（2〜4点）\n"
        "4) 向いているユーザー像と、購入時の注意点を1段落\n"
        "5) 最後に但し書き（サンプル範囲・時期により変動）\n"
        "6) すべて日本語。適度に接続詞を入れて自然に。\n"
        "7) 代表コメントは必要に応じて“例：〜”の形で軽く引用可。\n\n"
        f"メタ情報:\n{meta}\n"
        f"ポジティブ上位:\n{pros_block}\n\n"
        f"ネガティブ上位:\n{cons_block}\n\n"
        f"代表コメント（必要に応じて活用）:\n{reps_block}\n"
    )
    return system, user

def ask_model(client, system, user):
    comp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"system","content":system},{"role":"user","content":user}],
        temperature=0.4,
        max_tokens=900,
    )
    return comp.choices[0].message.content.strip()

# ---------- フォールバック ----------
def rule_based_narrative(payload):
    pos_pct = payload["pos_pct"]; mix_pct = payload["mix_pct"]; neg_pct = payload["neg_pct"]
    if payload["pos"] >= max(payload["mix"], payload["neg"]):
        intro = f"全体としては好意的な声が比較的多く（Positive {pos_pct}%）、続いてMixed、Negativeが続く傾向でした。"
    elif payload["neg"] >= max(payload["mix"], payload["pos"]):
        intro = f"全体としては否定的な声がやや目立ち（Negative {neg_pct}%）、MixedとPositiveが続く構成でした。"
    else:
        intro = f"全体としては評価が分かれる印象で（Mixed {mix_pct}%）、PositiveとNegativeが拮抗しています。"

    def bullets(items, heading):
        if not items: return f"{heading}\n- （該当データなし）"
        lines = [heading]
        for term, cnt, pct in items:
            lines.append(f"- {term}（{pct}%）")
        return "\n".join(lines)

    pos_block = bullets(payload["pros_top"], "【ポジティブ】")
    cons_block = bullets(payload["cons_top"], "【ネガティブ】")

    who = []
    pros_terms = [t for t,_,_ in payload["pros_top"]]
    cons_terms = [t for t,_,_ in payload["cons_top"]]
    if any("取り回し" in t or "駐車" in t or "小さ" in t for t in pros_terms):
        who.append("都市部での取り回しや駐車のしやすさを重視する人")
    if any("燃費" in t or "維持費" in t or "コスパ" in t or "価格" in t for t in pros_terms):
        who.append("日常の維持費や購入コストを抑えたい人")
    suit = "、".join(who) if who else "明確な対象像は限定されません"
    warn = []
    if any("航続" in t or "距離" in t for t in cons_terms):
        warn.append("長距離走行の頻度が高い場合は航続面を要確認")
    if any("加速" in t or "パワー" in t for t in cons_terms):
        warn.append("動力性能は試乗での確認がおすすめ")
    if any("静粛" in t or "内装" in t for t in cons_terms):
        warn.append("質感や静粛性はグレード差に注意")
    warning = "・".join(warn) if warn else "特段の注意点は上位には表れていません"

    body = (
        f"{intro}\n\n"
        f"{pos_block}\n\n"
        f"{cons_block}\n\n"
        f"【向いているユーザー／注意点】\n"
        f"- 向いている人：{suit}\n"
        f"- 注意点：{warning}\n\n"
        f"※ 本結果は取得範囲のレビューに基づくため、時期やページ数により変動します。"
    )
    return body

# ---------- メイン ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("vehicle_id", nargs="?", help="Autohome vehicle id（例: 5714）")
    ap.add_argument("--pros", type=int, default=5)
    ap.add_argument("--cons", type=int, default=4)
    ap.add_argument("--quotes", type=int, default=2)
    ap.add_argument("--style", default="executive", choices=["executive","friendly","neutral"])
    args = ap.parse_args()

    csv_path = detect_csv(args.vehicle_id)
    df = pd.read_csv(csv_path)
    lang, pros_col, cons_col = detect_cols(df)

    pos, mix, neg, total = sentiment_counts(df)
    pos_pct, mix_pct, neg_pct = (
        round(pos/total*100,1) if total else 0.0,
        round(mix/total*100,1) if total else 0.0,
        round(neg/total*100,1) if total else 0.0,
    )

    pros_top_c = top_k(df[pros_col], args.pros) if pros_col else Counter()
    cons_top_c = top_k(df[cons_col], args.cons) if cons_col else Counter()

    reps = []
    if pros_col:
        reps += choose_representatives(df, pros_col, list(pros_top_c.keys()), max_each=args.quotes)
    if cons_col:
        reps += choose_representatives(df, cons_col, list(cons_top_c.keys()), max_each=args.quotes)

    vid_match = re.search(r"autohome_reviews_(\d+)\.csv$", os.path.basename(csv_path))
    vid = vid_match.group(1) if vid_match else (args.vehicle_id or "unknown")
    payload = {
        "vehicle_id": vid,
        "total": total, "pos": pos, "mix": mix, "neg": neg,
        "pos_pct": pos_pct, "mix_pct": mix_pct, "neg_pct": neg_pct,
        "pros_top": [(t, c, round(c/total*100,1) if total else 0.0) for t,c in pros_top_c.items()],
        "cons_top": [(t, c, round(c/total*100,1) if total else 0.0) for t,c in cons_top_c.items()],
        "representatives": reps[:8],
    }

    client = get_client_or_none()
    if client:
        system, user = build_prompt(payload, args.style)
        try:
            body = ask_model(client, system, user)
        except Exception:
            body = rule_based_narrative(payload)
    else:
        body = rule_based_narrative(payload)

    header = f"【車両ID: {vid}】口コミ 要約（ストーリー版）"
    txt = header + "\n\n" + body + "\n"
    md  = f"# {header}\n\n{body}\n"

    out_txt = f"autohome_reviews_{vid}_story.txt"
    out_md  = f"autohome_reviews_{vid}_story.md"
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write(txt)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"✅ story generated: {out_txt}, {out_md}")

if __name__ == "__main__":
    main()
