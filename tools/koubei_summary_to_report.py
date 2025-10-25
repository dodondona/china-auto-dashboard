#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV/TXT（pros/cons/sentiment）→ “読める”日本語レポートを自動生成
・API呼び出しなし（高速）
・割合・件数を明示
・エグゼクティブサマリー/推奨読者/注意点まで自動
使い方:
  python tools/koubei_summary_to_report.py 7806
  （ID省略可：最新ファイルを自動検出）
"""
import os, sys, re, glob
import pandas as pd

# ---------- 基本ユーティリティ ----------
def detect_files(vehicle_id: str | None):
    """
    指定の vehicle_id があれば、その ID の CSV/TXT を優先的に探す。
    なければ最新ペアを自動検出。
    探索順：
      1) ./autohome_reviews_{id}.csv / _summary.txt
      2) ./output/autohome/{id}/autohome_reviews_{id}.csv / _summary.txt
      3) **/autohome_reviews_{id}.csv / _summary.txt （広域グロブ）
    """
    def try_pair(csv_path: str, txt_path: str) -> tuple[str, str] | None:
        if os.path.exists(csv_path) and os.path.exists(txt_path):
            return csv_path, txt_path
        return None

    if vehicle_id:
        # 1) 直下
        c1 = f"autohome_reviews_{vehicle_id}.csv"
        t1 = f"autohome_reviews_{vehicle_id}_summary.txt"
        pair = try_pair(c1, t1)
        if pair: return pair

        # 2) output/autohome/{id}/
        c2 = f"output/autohome/{vehicle_id}/autohome_reviews_{vehicle_id}.csv"
        t2 = f"output/autohome/{vehicle_id}/autohome_reviews_{vehicle_id}_summary.txt"
        pair = try_pair(c2, t2)
        if pair: return pair

        # 3) 広域グロブ（最後の保険）
        cands_csv = sorted(glob.glob(f"**/autohome_reviews_{vehicle_id}.csv", recursive=True), key=os.path.getmtime, reverse=True)
        cands_txt = sorted(glob.glob(f"**/autohome_reviews_{vehicle_id}_summary.txt", recursive=True), key=os.path.getmtime, reverse=True)
        if cands_csv and cands_txt:
            # 同一ディレクトリの組を優先
            for c in cands_csv:
                base = os.path.dirname(c)
                t = os.path.join(base, f"autohome_reviews_{vehicle_id}_summary.txt")
                if os.path.exists(t):
                    return c, t
            return cands_csv[0], cands_txt[0]

        tried = [c1, t1, c2, t2, f"**/autohome_reviews_{vehicle_id}.csv", f"**/autohome_reviews_{vehicle_id}_summary.txt"]
        raise FileNotFoundError(f"必要ファイルが見つかりません: {tried}")

    # vehicle_id 未指定：最新ペア検出
    csvs = sorted(glob.glob("autohome_reviews_*.csv"), key=os.path.getmtime, reverse=True)
    txts = sorted(glob.glob("autohome_reviews_*_summary.txt"), key=os.path.getmtime, reverse=True)

    # 直下に無ければ output/autohome/* も見る
    if not csvs:
        csvs = sorted(glob.glob("output/autohome/*/autohome_reviews_*.csv"), key=os.path.getmtime, reverse=True)
    if not txts:
        txts = sorted(glob.glob("output/autohome/*/autohome_reviews_*_summary.txt"), key=os.path.getmtime, reverse=True)

    if not csvs or not txts:
        # 広域
        csvs = sorted(glob.glob("**/autohome_reviews_*.csv", recursive=True), key=os.path.getmtime, reverse=True)
        txts = sorted(glob.glob("**/autohome_reviews_*_summary.txt", recursive=True), key=os.path.getmtime, reverse=True)
        if not csvs or not txts:
            raise FileNotFoundError("入力CSV/TXTが見つかりません。")

    # 同じIDのペアを優先
    for c in csvs:
        m = re.search(r"autohome_reviews_(\d+)\.csv$", c)
        if not m:
            continue
        vid = m.group(1)
        # 同ディレクトリの txt を最優先
        base = os.path.dirname(c)
        t_same_dir = os.path.join(base, f"autohome_reviews_{vid}_summary.txt")
        if os.path.exists(t_same_dir):
            return c, t_same_dir
        # リスト内にあればそれを返す
        t = f"autohome_reviews_{vid}_summary.txt"
        if t in txts or os.path.exists(t):
            return c, t

    # それでもマッチしなければ最上位の候補を返す
    return csvs[0], txts[0]


def ratio(n, total):
    return 0.0 if total <= 0 else round(n / total * 100, 1)


def split_terms(series):
    s = series.dropna().astype(str).str.split(" / ").explode().str.strip()
    return s[s != ""]


def top_k(series, k=5):
    if series is None:
        return pd.Series(dtype=int)
    return split_terms(series).value_counts().head(k)


def col_exists(df, *cols):
    return all(c in df.columns for c in cols)


# ---------- 本体 ----------
def main():
    vehicle_id = sys.argv[1].strip() if len(sys.argv) >= 2 else None
    csv_path, txt_path = detect_files(vehicle_id)
    df = pd.read_csv(csv_path)

    # どの列を使うか自動判定
    if col_exists(df, "pros_ja", "cons_ja", "sentiment"):
        pros_col, cons_col = "pros_ja", "cons_ja"
        lang = "ja"
    elif col_exists(df, "pros_zh", "cons_zh", "sentiment"):
        pros_col, cons_col = "pros_zh", "cons_zh"
        lang = "zh"
    elif col_exists(df, "pros", "cons", "sentiment"):
        pros_col, cons_col = "pros", "cons"
        text = " ".join(df["pros"].dropna().astype(str).head(40).tolist())
        lang = "ja" if re.search(r"[ぁ-ゟ゠-ヿ]", text) else "zh"
    else:
        pros_col = cons_col = None
        lang = "ja"

    total = len(df)
    pos = int((df["sentiment"].astype(str).str.lower() == "positive").sum()) if "sentiment" in df.columns else 0
    mix = int((df["sentiment"].astype(str).str.lower() == "mixed").sum()) if "sentiment" in df.columns else 0
    neg = int((df["sentiment"].astype(str).str.lower() == "negative").sum()) if "sentiment" in df.columns else 0

    pros_top = top_k(df[pros_col]) if pros_col else pd.Series(dtype=int)
    cons_top = top_k(df[cons_col]) if cons_col else pd.Series(dtype=int)

    # 文面用ヘルパ
    def join_items(vc):
        items = [f"「{t}」（{cnt}件・{ratio(cnt, total)}%）" for t, cnt in vc.items()]
        if not items:
            return ""
        if len(items) == 1:
            return items[0]
        return "、".join(items)

    # エグゼクティブサマリー
    if total == 0:
        senti_line = "レビュー件数が極めて少なく、全体傾向は不明です。"
    else:
        max_tag = max([("positive", pos), ("mixed", mix), ("negative", neg)], key=lambda x: x[1])[0]
        if max_tag == "positive":
            senti_line = f"全体として**好意的な声が多く**（Positive {pos}件・{ratio(pos, total)}%）、次いでMixed {mix}件（{ratio(mix, total)}%）、Negative {neg}件（{ratio(neg, total)}%）でした。"
        elif max_tag == "mixed":
            senti_line = f"全体として**評価はやや分かれ**（Mixed {mix}件・{ratio(mix, total)}%）、Positive {pos}件（{ratio(pos, total)}%）、Negative {neg}件（{ratio(neg, total)}%）が続きました。"
        else:
            senti_line = f"全体として**否定的な声が目立ち**（Negative {neg}件・{ratio(neg, total)}%）、Mixed {mix}件（{ratio(mix, total)}%）、Positive {pos}件（{ratio(pos, total)}%）が続きました。"

    pros_line = "（該当データなし）" if pros_top.empty else join_items(pros_top)
    cons_line = "（該当データなし）" if cons_top.empty else join_items(cons_top)

    # “誰に向く/どこが注意”を定型で補う
    who_line = ""
    if not pros_top.empty:
        if any("取り回し" in t or "駐車" in t or "小さ" in t for t in pros_top.index.astype(str)):
            who_line += "都市部の短距離移動や駐車頻度が高い人には向いています。"
        if any("燃費" in t or "維持費" in t or "コスパ" in t or "価格" in t for t in pros_top.index.astype(str)):
            who_line += "コスト重視のユーザーから支持されています。"
    warn_line = ""
    if not cons_top.empty:
        if any("航続" in t or "距離" in t for t in cons_top.index.astype(str)):
            warn_line += "長距離走行の頻度が高い人は航続面を要確認。"
        if any("加速" in t or "パワー" in t for t in cons_top.index.astype(str)):
            warn_line += "動力性能は試乗での確認がおすすめ。"
        if any("静粛" in t or "内装" in t for t in cons_top.index.astype(str)):
            warn_line += "質感や静粛性はグレード差に注意。"

    # vid抽出（安全版）
    vid_match = re.search(r"autohome_reviews_(\d+)\.csv$", os.path.basename(csv_path))
    vid = vid_match.group(1) if vid_match else (vehicle_id or "unknown")

    # レポート本文
    header = f"【車両ID: {vid}】口コミサマリー（文章版）"
    body_lines = [
        "■ エグゼクティブサマリー",
        senti_line,
        "",
        "■ よく挙がったポジティブ要素",
        pros_line,
        "",
        "■ よく挙がったネガティブ要素",
        cons_line,
        "",
        "■ こんな人に向いています",
        (who_line or "（特記なし）"),
        "",
        "■ 注意したいポイント",
        (warn_line or "（特記なし）"),
        "",
        "※ 比率は本レポート対象の要約行を母数に算出。取得ページや時期により変動します。"
    ]
    txt = header + "\n\n" + "\n".join(body_lines) + "\n"
    md = f"# {header}\n\n" + "\n".join(body_lines) + "\n"

    report_txt = f"autohome_reviews_{vid}_report.txt"
    report_md = f"autohome_reviews_{vid}_report.md"
    with open(report_txt, "w", encoding="utf-8") as f:
        f.write(txt)
    with open(report_md, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"✅ narrative report generated: {report_txt}")

if __name__ == "__main__":
    main()
