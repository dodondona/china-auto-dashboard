#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, os, json, re
import pandas as pd
from openai import OpenAI

"""
Usage:
  OPENAI_API_KEY=sk-... python tools/koubei_batch_collect.py <batch_id> <vehicle_id> [mode: ja|zh]

Outputs:
  - autohome_reviews_<ID>.csv
  - autohome_reviews_<ID>_summary.txt
  - autohome_reviews_<ID>.batch.result.json   ← 新規保存（Batch行ごとJSON）
  - autohome_reviews_<ID>_story.txt          ← 新規保存（Batch内 'story-full' から抽出）
"""

BATCH_ID    = sys.argv[1].strip() if len(sys.argv)>=2 else ""
VEHICLE_ID  = sys.argv[2].strip() if len(sys.argv)>=3 else ""
MODE        = (sys.argv[3].strip().lower() if len(sys.argv)>=4 else "ja")
if MODE not in ("ja","zh"): MODE="ja"

OUTDIR = os.path.join(os.path.dirname(__file__), "..")
CSV_PATH = os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}.csv")
TXT_PATH = os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}_summary.txt")
RAW_JSON = os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}.batch.result.json")
STORY_TXT= os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}_story.txt")

def extract_json_loose(s:str):
    if not s: return None
    s = re.sub(r"```json\s*|\s*```", "", s, flags=re.I).strip()
    try:
        return json.loads(s)
    except Exception:
        m = re.search(r"(\[.*\]|\{.*\})", s, flags=re.S)
        if m:
            try: return json.loads(m.group(1))
            except: return None
        return None

def main():
    assert BATCH_ID and VEHICLE_ID, "batch_id and vehicle_id are required"
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    # 状態確認
    b = client.batches.retrieve(BATCH_ID)
    if b.status != "completed":
        print(f"STATUS={b.status} (not completed). Try later.")
        return

    # 出力ファイル取得（NDJSON）
    out_file_id = b.output_file_id
    ndjson_text = client.files.content(out_file_id).text

    # 行ごとJSON化して保存（検証用にフル保存）
    results=[]
    rows_for_csv=[]
    story_text=None

    for line in ndjson_text.splitlines():
        if not line.strip(): 
            continue
        obj = json.loads(line)
        results.append(obj)

        if "error" in obj:
            continue

        body = obj.get("response",{}).get("body",{})
        custom_id = obj.get("custom_id") or ""
        choices = body.get("choices", [])
        if not choices:
            continue
        msg = choices[0].get("message",{}).get("content","")

        # 口コミ要約（rev-xxxxx）
        if custom_id.startswith("rev-"):
            data = extract_json_loose(msg) or []
            if isinstance(data, dict): data=[data]
            for r in data:
                pros = " / ".join(r.get("pros", [])) if isinstance(r.get("pros"), list) else str(r.get("pros",""))
                cons = " / ".join(r.get("cons", [])) if isinstance(r.get("cons"), list) else str(r.get("cons",""))
                if MODE=="ja":
                    rows_for_csv.append({"pros_ja":pros, "cons_ja":cons, "sentiment": r.get("sentiment","mixed")})
                else:
                    rows_for_csv.append({"pros_zh":pros, "cons_zh":cons, "sentiment": r.get("sentiment","mixed")})

        # Story抽出（story-full）
        if custom_id == "story-full" and not story_text:
            story_text = (msg or "").strip()

    # CSV出力（従来互換）
    if MODE=="ja":
        df = pd.DataFrame(rows_for_csv, columns=["pros_ja","cons_ja","sentiment"]).fillna("")
    else:
        df = pd.DataFrame(rows_for_csv, columns=["pros_zh","cons_zh","sentiment"]).fillna("")
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    # 簡易集計テキスト（従来互換）
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

    # Batchの生結果をJSON保存（検証・再利用用）
    with open(RAW_JSON, "w", encoding="utf-8") as f:
        json.dump({"batch_id": BATCH_ID, "vehicle_id": VEHICLE_ID, "results": results}, f, ensure_ascii=False, indent=2)

    # Story保存（4段落フォーマット：submit側で厳しく指示済み）
    if story_text:
        with open(STORY_TXT, "w", encoding="utf-8") as f:
            f.write(story_text)
        print(f"✅ Story saved: {STORY_TXT}")
    else:
        print("⚠️ Story not found in batch result.")

    print(f"✅ Done: {CSV_PATH} / {TXT_PATH} / {RAW_JSON}")

if __name__ == "__main__":
    main()
