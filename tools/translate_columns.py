from pathlib import Path
import os, json, time
import pandas as pd
from openai import OpenAI

SRC = Path(os.environ.get("CSV_IN","output/autohome/7578/config_7578.csv"))
DST = Path(os.environ.get("CSV_OUT","output/autohome/7578/config_7578_ja.csv"))
GLOSS = Path(os.environ.get("GLOSS","tools/glossary_cn_ja.csv"))
MODEL = os.environ.get("OPENAI_MODEL","gpt-4.1-mini")  # 品質寄りは gpt-4.1

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

def load_glossary():
    if not GLOSS.exists(): return {}
    g = pd.read_csv(GLOSS)
    out = {}
    for _,r in g.iterrows():
        cn = str(r["cn"]).strip()
        out[cn] = {"ja": str(r["ja"]).strip(), "policy": str(r.get("policy","translate")).strip()}
    return out

def batch_translate(unique_terms, glossary):
    # 既知は即時反映
    out = {}
    to_translate = []
    for t in unique_terms:
        if t in glossary:
            pol = glossary[t]["policy"]
            if pol == "do_not_translate":
                out[t] = t  # 原文維持
            else:
                out[t] = glossary[t]["ja"]
        else:
            to_translate.append(t)

    if not to_translate: return out

    # LLM翻訳（用語固定＋非翻訳指示）
    sys = (
        "あなたは自動車仕様表の用語を中国語→日本語に翻訳します。"
        "以下のルールを厳守してください：\n"
        "1) JSON配列で返す（各要素は {\"src\":\"…\",\"ja\":\"…\"}）。\n"
        "2) 用語集にある語はその訳語を使い、policy=do_not_translate は原文を維持。\n"
        "3) 固有名詞・ブランド・グレードは意訳せず、訳すなと指示されたものはそのまま。\n"
        "4) 文体は簡潔、末尾の句読点は不要。"
    )
    # 用語集をプロンプトに埋め込み
    gloss_snippets = [{"cn":k, "ja":v["ja"], "policy":v["policy"]} for k,v in glossary.items()]
    user = {
        "instruction": "次の用語を翻訳してください。",
        "glossary": gloss_snippets[:300],  # 大きすぎる場合は分割
        "terms": to_translate
    }

    resp = client.chat.completions.create(
        model=MODEL,
        temperature=0,
        top_p=0,
        response_format={"type":"json_object"},
        messages=[
            {"role":"system","content":sys},
            {"role":"user","content":json.dumps(user, ensure_ascii=False)}
        ]
    )
    data = json.loads(resp.choices[0].message.content)
    # 期待形式: {"translations":[{"src":"…","ja":"…"}, ...]}
    for pair in data.get("translations", []):
        out[pair["src"]] = pair["ja"]
    # 念のため穴埋め
    for t in to_translate:
        out.setdefault(t, t)
    return out

def main():
    df = pd.read_csv(SRC, encoding="utf-8-sig")
    gloss = load_glossary()

    uniq_sec = sorted({str(x).strip() for x in df["セクション"].tolist()})
    uniq_item = sorted({str(x).strip() for x in df["項目"].tolist()})

    sec_map = batch_translate(uniq_sec, gloss)
    item_map = batch_translate(uniq_item, gloss)

    # 列追加（CNの右にJAを挿入）
    out = df.copy()
    out.insert(1, "セクション_ja", out["セクション"].map(sec_map))
    out.insert(3, "項目_ja", out["項目"].map(item_map))

    DST.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST, index=False, encoding="utf-8-sig")
    print(f"✅ saved: {DST}")

if __name__ == "__main__":
    main()
