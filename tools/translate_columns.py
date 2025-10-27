from __future__ import annotations
import os, json, time, re
from pathlib import Path
import pandas as pd
from openai import OpenAI

# ====== 入出力 ======
SERIES_ID = os.environ.get("SERIES_ID", "").strip()

def resolve_src_dst():
    csv_in  = os.environ.get("CSV_IN", "").strip()
    csv_out = os.environ.get("CSV_OUT", "").strip()

    def guess_paths_from_series(sid: str):
        if not sid:
            return None, None
        base = f"output/autohome/{sid}/config_{sid}"
        return Path(f"{base}.csv"), Path(f"{base}.ja.csv")

    default_in  = Path("output/autohome/7578/config_7578.csv")
    default_out = Path("output/autohome/7578/config_7578.ja.csv")

    src = Path(csv_in)  if csv_in  else None
    dst = Path(csv_out) if csv_out else None
    if src is None or dst is None:
        s2, d2 = guess_paths_from_series(SERIES_ID)
        src = src or s2
        dst = dst or d2
    src = src or default_in
    dst = dst or default_out
    return src, dst

SRC, DST_PRIMARY = resolve_src_dst()
DST_SECONDARY = DST_PRIMARY.parent / DST_PRIMARY.name.replace(".ja.csv", "_ja.csv")

# ====== 設定 ======
MODEL   = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
API_KEY = os.environ.get("OPENAI_API_KEY")
CACHE_REPO_DIR = os.environ.get("CACHE_REPO_DIR", "cache").strip()
BATCH_SIZE, RETRIES, SLEEP_BASE = 60, 3, 1.2

# ====== LLM ======
class Translator:
    def __init__(self, model: str, api_key: str):
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY missing")
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セル値』の配列です。"
            "自然で簡潔な日本語に翻訳してください。"
            "数値・記号・年式はそのまま保持し、JSONで返してください。"
            "出力は{'translations':[{'cn':'原文','ja':'訳文'}]}のみ。"
        )
        print(f"🟢 Translator ready: model={self.model}")

    def translate_batch(self, terms:list[str])->dict[str,str]:
        if not terms: return {}
        msgs=[
            {"role":"system","content":self.system},
            {"role":"user","content":json.dumps({"terms":terms},ensure_ascii=False)},
        ]
        try:
            resp=self.client.chat.completions.create(
                model=self.model,messages=msgs,temperature=0,
                response_format={"type":"json_object"},
            )
            content=resp.choices[0].message.content or ""
            data=json.loads(content)
            return {t["cn"]:t["ja"] for t in data.get("translations",[]) if t.get("cn")}
        except Exception as e:
            print("❌ API error:",repr(e))
            return {t:t for t in terms}

    def translate_unique(self, terms:list[str])->dict[str,str]:
        out={}
        for chunk_i in range(0,len(terms),BATCH_SIZE):
            chunk=terms[chunk_i:chunk_i+BATCH_SIZE]
            for attempt in range(1,RETRIES+1):
                try:
                    out.update(self.translate_batch(chunk))
                    break
                except Exception as e:
                    print(f"retry {attempt}",repr(e))
                    time.sleep(SLEEP_BASE*attempt)
        return out

# ====== 共通 ======
def clean_any_noise(s:str)->str:
    s=str(s) if s is not None else ""
    s=re.sub(r"\s+"," ",s)
    return s.strip(" 　-—–")

def uniq(seq):
    seen=set();out=[]
    for x in seq:
        if x not in seen:
            seen.add(x);out.append(x)
    return out

# ====== main ======
def main():
    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    if not SRC.exists():
        raise FileNotFoundError(SRC)

    df=pd.read_csv(SRC,encoding="utf-8-sig").map(clean_any_noise)

    cache_cn=Path(CACHE_REPO_DIR)/f"{SERIES_ID}/cn.csv"
    cache_ja=Path(CACHE_REPO_DIR)/f"{SERIES_ID}/ja.csv"
    cache_cn.parent.mkdir(parents=True,exist_ok=True)
    reuse=cache_cn.exists() and cache_ja.exists()

    prev_cn=prev_ja=None
    if reuse:
        prev_cn=pd.read_csv(cache_cn,encoding="utf-8-sig")
        prev_ja=pd.read_csv(cache_ja,encoding="utf-8-sig")
    print(f"♻️ reuse={reuse}")

    tr=Translator(MODEL,API_KEY)
    out=df.copy()

    values_to_translate=[]
    if reuse and prev_cn is not None:
        for i in range(len(df)):
            for j in range(len(df.columns)):
                a,b=str(df.iat[i,j]).strip(),str(prev_cn.iat[i,j]).strip()
                if a!=b:
                    values_to_translate.append(a)
                    # 同位置更新対象
                    out.iat[i,j]=a
    else:
        for i in range(len(df)):
            for j in range(len(df.columns)):
                values_to_translate.append(str(df.iat[i,j]).strip())

    uniq_vals=uniq([v for v in values_to_translate if v and v not in {"","●","○","–","-","—"}])
    print(f"🌐 to_translate={len(uniq_vals)}")

    val_map=tr.translate_unique(uniq_vals) if uniq_vals else {}

    for i in range(len(out)):
        for j in range(len(out.columns)):
            s=str(out.iat[i,j]).strip()
            if s in val_map:
                out.iat[i,j]=val_map[s]

    # 出力：セクション,項目は削除（_jaは残す）
    out_save=out.drop(columns=["セクション","項目"],errors="ignore")
    DST_PRIMARY.parent.mkdir(parents=True,exist_ok=True)
    out_save.to_csv(DST_PRIMARY,index=False,encoding="utf-8-sig")
    out_save.to_csv(DST_SECONDARY,index=False,encoding="utf-8-sig")

    # キャッシュ更新
    df.to_csv(cache_cn,index=False,encoding="utf-8-sig")
    out.to_csv(cache_ja,index=False,encoding="utf-8-sig")

    print(f"✅ Saved: {DST_PRIMARY}")
    print(f"📦 cache CN: {cache_cn}")
    print(f"📦 cache JA: {cache_ja}")

if __name__=="__main__":
    main()
