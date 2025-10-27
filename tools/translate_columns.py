from __future__ import annotations
import os, json, time, re
from pathlib import Path
import pandas as pd
from openai import OpenAI

SERIES_ID = os.environ.get("SERIES_ID", "").strip()

def resolve_src_dst():
    csv_in  = os.environ.get("CSV_IN", "").strip()
    csv_out = os.environ.get("CSV_OUT", "").strip()
    def guess_paths_from_series(sid: str):
        if not sid: return None, None
        base = f"output/autohome/{sid}/config_{sid}"
        return Path(f"{base}.csv"), Path(f"{base}.ja.csv")
    s2, d2 = guess_paths_from_series(SERIES_ID)
    return Path(csv_in or s2), Path(csv_out or d2)

SRC, DST_PRIMARY = resolve_src_dst()
DST_SECONDARY = Path(str(DST_PRIMARY).replace(".ja.csv","_ja.csv"))

MODEL   = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
API_KEY = os.environ.get("OPENAI_API_KEY")
CACHE_REPO_DIR = os.environ.get("CACHE_REPO_DIR","cache").strip()
TRANSLATE_VALUES = True
EXRATE_CNY_TO_JPY = 21.0
BATCH_SIZE, RETRIES, SLEEP_BASE = 60, 3, 1.2

PRICE_ITEM_MSRP_CN={"厂商指导价"}
PRICE_ITEM_DEALER_CN={"经销商参考价","经销商报价","经销商"}

def clean_any_noise(s): return re.sub(r"\s+"," ",str(s or "")).strip()
def uniq(seq): s=set(); out=[]; [out.append(x) for x in seq if not(x in s or s.add(x))]; return out

# ---------- 安全なJSONパーサ ----------
def parse_json_relaxed(content:str, terms:list[str]):
    try:
        d=json.loads(content)
        if isinstance(d,dict) and "translations" in d:
            return {t["cn"]:t.get("ja",t["cn"]) for t in d["translations"] if t.get("cn")}
    except Exception:
        pass
    # 正規表現fallback
    pairs=re.findall(r'"cn"\s*:\s*"([^"]+)"\s*,\s*"ja"\s*:\s*"([^"]+)"', content)
    if pairs:
        return {cn:ja for cn,ja in pairs}
    return {t:t for t in terms}

# ---------- Translator ----------
class Translator:
    def __init__(self, model:str, api_key:str):
        self.client=OpenAI(api_key=api_key)
        self.model=model
        self.system=("あなたは自動車仕様表の専門翻訳者です。"
                     "中国語の語句を自然な日本語に翻訳し、数値や単位は保持してください。"
                     "出力はJSON形式で {\"translations\":[{\"cn\":\"原文\",\"ja\":\"訳文\"}]} のみ。")
    def translate_batch(self, terms:list[str]):
        if not terms: return {}
        msgs=[{"role":"system","content":self.system},
              {"role":"user","content":json.dumps({"terms":terms},ensure_ascii=False)}]
        for attempt in range(RETRIES):
            try:
                resp=self.client.chat.completions.create(model=self.model,messages=msgs,temperature=0)
                content=resp.choices[0].message.content or ""
                result=parse_json_relaxed(content,terms)
                if result: return result
            except Exception as e:
                print(f"❌ translate_batch error {e}")
                time.sleep(SLEEP_BASE*(attempt+1))
        return {t:t for t in terms}

    def translate_unique(self, terms:list[str]):
        out={}
        for i in range(0,len(terms),BATCH_SIZE):
            out.update(self.translate_batch(terms[i:i+BATCH_SIZE]))
        return out

def main():
    print(f"🔎 SRC: {SRC}")
    df=pd.read_csv(SRC,encoding="utf-8-sig").map(clean_any_noise)
    out=df.copy()

    cn_path=Path(CACHE_REPO_DIR)/SERIES_ID/"cn.csv"
    ja_path=Path(CACHE_REPO_DIR)/SERIES_ID/"ja.csv"
    prev_cn=pd.read_csv(cn_path,encoding="utf-8-sig").map(clean_any_noise) if cn_path.exists() else None
    prev_ja=pd.read_csv(ja_path,encoding="utf-8-sig") if ja_path.exists() else None
    reuse=(prev_cn is not None and prev_ja is not None and prev_cn.shape==df.shape)
    print(f"♻️ reuse={reuse}")

    tr=Translator(MODEL,API_KEY)
    numeric_like=re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
    values_to_translate=[]; coords_to_update=[]
    for i in range(len(df)):
        is_price=df.at[i,"項目"] in PRICE_ITEM_MSRP_CN or df.at[i,"項目"] in PRICE_ITEM_DEALER_CN
        for j in range(4,len(df.columns)):
            cur=str(df.iat[i,j]).strip()
            if is_price or not cur or cur in {"●","○","–","-","—"} or numeric_like.fullmatch(cur):
                continue
            if reuse:
                old=str(prev_cn.iat[i,j]).strip()
                if cur==old:
                    out.iat[i,j]=prev_ja.iat[i,j]; continue
            values_to_translate.append(cur)
            coords_to_update.append((i,j))

    uniq_vals=uniq(values_to_translate)
    print(f"🌐 to_translate={len(uniq_vals)}")
    val_map=tr.translate_unique(uniq_vals) if uniq_vals else {}
    for (i,j) in coords_to_update:
        s=str(df.iat[i,j]).strip()
        out.iat[i,j]=val_map.get(s,s)

    DST_PRIMARY.parent.mkdir(parents=True,exist_ok=True)
    out.to_csv(DST_PRIMARY,index=False,encoding="utf-8-sig")
    out.to_csv(DST_SECONDARY,index=False,encoding="utf-8-sig")
    cn_path.parent.mkdir(parents=True,exist_ok=True)
    df.to_csv(cn_path,index=False,encoding="utf-8-sig")
    out.to_csv(ja_path,index=False,encoding="utf-8-sig")
    print(f"✅ Saved: {DST_PRIMARY}")

if __name__=="__main__":
    main()
