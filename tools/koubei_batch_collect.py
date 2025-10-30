#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, os, json, time, re
import pandas as pd
from openai import OpenAI

"""
Usage:
  OPENAI_API_KEY=sk-... python tools/koubei_batch_collect.py <batch_id> <vehicle_id> [mode: ja|zh]

Outputs:
  - autohome_reviews_<ID>.csv
  - autohome_reviews_<ID>_summary.txt
"""

BATCH_ID = sys.argv[1].strip() if len(sys.argv)>=2 else ""
VEHICLE_ID = sys.argv[2].strip() if len(sys.argv)>=3 else ""
MODE = (sys.argv[3].strip().lower() if len(sys.argv)>=4 else "ja")
if MODE not in ("ja","zh"): MODE="ja"

OUTDIR = os.path.join(os.path.dirname(__file__), "..")
CSV_PATH = os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}.csv")
TXT_PATH = os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}_summary.txt")

def extract_json_loose(s:str):
    if not s: return None
    s = re.sub(r"```json\s*|\s*```", "", s, flags=re.I).strip()
    try:
        return json.loads(s)
    except Exception:
        # {} or [] の外側ゴミ除去を軽く試みる
        m = re.search(r"(\[.*\]|\{.*\})", s, flags=re.S)
        if m:
            try: return json.loads(m.group(1))
            except: return None
        return None

def main():
    assert BATCH_ID and VEHICLE_ID, "batch_id and vehicle_id are required"
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    # 状態確認（長時間待たない運用を想定：ここは手動実行 or 後続ワークフローで実行）
    b = client.batches.retrieve(BATCH_ID)
    if b.status != "completed":
        print(f"STATUS={b.status} (not completed). Try later.")
        return

    # 出力ファイルを取得
    out_file_id = b.output_file_id
    content = client.files.content(out_file_id).text

    rows=[]
    for line in content.splitlines():
        if not line.strip(): continue
        obj = json.loads(line)
        # 失敗行もあるのでケア
        if "error" in obj:
            continue
        body = obj.get("response",{}).get("body",{})
        choices = body.get("choices", [])
        if not choices: 
            continue
        msg = choices[0].get("message",{}).get("content","")
        data = extract_json_loose(msg) or []
        # data は [{pros:[..], cons:[..], sentiment:".."}] 想定
        if isinstance(data, dict): data=[data]
        for r in data:
            pros = " / ".join(r.get("pros", [])) if isinstance(r.get("pros"), list) else str(r.get("pros",""))
            cons = " / ".join(r.get("cons", [])) if isinstance(r.get("cons"), list) else str(r.get("cons",""))
            if MODE=="ja":
                rows.append({"pros_ja":pros, "cons_ja":cons, "sentiment": r.get("sentiment","mixed")})
            else:
                rows.append({"pros_zh":pros, "cons_zh":cons, "sentiment": r.get("sentiment","mixed")})

    if MODE=="ja":
        df = pd.DataFrame(rows, columns=["pros_ja","cons_ja","sentiment"]).fillna("")
    else:
        df = pd.DataFrame(rows, columns=["pros_zh","cons_zh","sentiment"]).fillna("")
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    # 簡易サマリ（上位語）
    def head_counts(series):
        s = series.dropna().astype(str).str.split(" / ").explode().str.strip()
        s = s[s!=""]
        return s.value_counts().head(15)

    if MODE=="ja":
        top_pros = head_counts(df["pros_ja"]); top_cons = head_counts(df["cons_ja"])
    else:
        top_pros = head_counts(df["pros_zh"]); top_cons = head_counts(df["cons_zh"])
    senti = df["sentiment"].value_counts()

    with open(TXT_PATH, "w", encoding="utf-8") as f:
        f.write(f"【車両ID】{VEHICLE_ID}\n")
        f.write("=== ポジティブTOP ===\n"); f.write(top_pros.to_string() if not top_pros.empty else "(なし)")
        f.write("\n\n=== ネガティブTOP ===\n"); f.write(top_cons.to_string() if not top_cons.empty else "(なし)")
        f.write("\n\n=== センチメント比 ===\n"); f.write(senti.to_string() if not senti.empty else "(なし)")
        f.write("\n")

    print(f"✅ Done: {CSV_PATH} / {TXT_PATH}")

if __name__ == "__main__":
    main()
