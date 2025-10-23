# tools/translate_columns.py
import os, json, pandas as pd, time
from pathlib import Path
from openai import OpenAI

SRC = Path(os.environ.get("CSV_IN", "output/autohome/7578/config_7578.csv"))
DST = Path(os.environ.get("CSV_OUT", "output/autohome/7578/config_7578_ja.csv"))
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

def translate_batch(texts):
    if not texts:
        return {}
    try:
        messages = [
            {"role": "system", "content": "あなたは自動車仕様表の専門翻訳者です。中国語を日本語に自然かつ簡潔に翻訳してください。"},
            {"role": "user", "content": "以下のリストをJSON形式で翻訳してください。各要素は {\"cn\":\"原文\",\"ja\":\"翻訳\"} の形です。\n\n" + json.dumps(texts, ensure_ascii=False)}
        ]
        response = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            temperature=0,
            response_format={"type":"json_object"}
        )
        content = response.choices[0].message.content
        data = json.loads(content)
        out = {d["cn"]: d["ja"] for d in data.get("translations", [])}
        print(f"✅ Translated {len(out)}/{len(texts)} terms")
        return out
    except Exception as e:
        print("⚠️ Translation error:", e)
        return {t: t for t in texts}  # 原文fallback

def main():
    df = pd.read_csv(SRC, encoding="utf-8-sig")
    unique_sections = sorted(set(df["セクション"].dropna().astype(str)))
    unique_items = sorted(set(df["項目"].dropna().astype(str)))

    print(f"Translating {len(unique_sections)} sections + {len(unique_items)} items using {MODEL}...")

    sec_map = translate_batch(unique_sections)
    item_map = translate_batch(unique_items)

    df.insert(1, "セクション_ja", df["セクション"].map(sec_map))
    df.insert(3, "項目_ja", df["項目"].map(item_map))

    DST.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(DST, index=False, encoding="utf-8-sig")
    print(f"✅ Saved translated file: {DST}")

if __name__ == "__main__":
    main()
