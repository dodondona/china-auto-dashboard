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

def make_secondary(dst: Path) -> Path:
    s = dst.name
    if s.endswith(".ja.csv"):
        s2 = s.replace(".ja.csv", "_ja.csv")
    elif s.endswith("_ja.csv"):
        s2 = s.replace("_ja.csv", ".ja.csv")
    else:
        s2 = dst.stem + ".ja.csv"
    return dst.parent / s2

DST_SECONDARY = make_secondary(DST_PRIMARY)

# ====== 設定 ======
MODEL   = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
API_KEY = os.environ.get("OPENAI_API_KEY")

TRANSLATE_VALUES   = os.environ.get("TRANSLATE_VALUES", "true").lower() == "true"
STRIP_GRADE_PREFIX = os.environ.get("STRIP_GRADE_PREFIX", "true").lower() == "true"
EXRATE_CNY_TO_JPY  = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))
CACHE_REPO_DIR     = os.environ.get("CACHE_REPO_DIR", "cache").strip()

BATCH_SIZE, RETRIES, SLEEP_BASE = 60, 3, 1.2

# ====== 基本辞書 ======
PRICE_ITEM_MSRP_CN={"厂商指导价"}
PRICE_ITEM_DEALER_CN={"经销商参考价","经销商报价","经销商"}

def clean_any_noise(s:str)->str:
    if s is None: return ""
    return re.sub(r"\s+"," ",str(s)).strip()

def uniq(seq):
    s, out = set(), []
    for x in seq:
        if x not in s:
            s.add(x); out.append(x)
    return out

# ====== LLM ======
class Translator:
    def __init__(self, model: str, api_key: str):
        if not (api_key and api_key.strip()):
            raise RuntimeError("OPENAI_API_KEY is not set")
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セクション名/項目名/セル値』の配列です。"
            "自然で簡潔な日本語に翻訳し、単位・記号は保持してください。"
            "出力は JSON（{'translations':[{'cn':'原文','ja':'訳文'}]}）のみ。"
        )

    def translate_batch(self, terms: list[str]) -> dict[str,str]:
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
            return {t["cn"]:t.get("ja",t["cn"]) for t in data.get("translations",[]) if t.get("cn")}
        except Exception as e:
            print("❌ OpenAI error:",repr(e))
            return {t:t for t in terms}

    def translate_unique(self, terms:list[str])->dict[str,str]:
        out={}
        for chunk in [terms[i:i+BATCH_SIZE] for i in range(0,len(terms),BATCH_SIZE)]:
            out.update(self.translate_batch(chunk))
        return out

# ====== main ======
def main():
    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    df=pd.read_csv(SRC,encoding="utf-8-sig").map(clean_any_noise)
    cn_path=Path(CACHE_REPO_DIR)/SERIES_ID/"cn.csv"
    ja_path=Path(CACHE_REPO_DIR)/SERIES_ID/"ja.csv"

    prev_cn=pd.read_csv(cn_path,encoding="utf-8-sig").map(clean_any_noise) if cn_path.exists() else None
    prev_ja=pd.read_csv(ja_path,encoding="utf-8-sig") if ja_path.exists() else None
    reuse=(prev_cn is not None and prev_ja is not None and prev_cn.shape==df.shape)
    print(f"♻️ reuse={reuse}")

    tr=Translator(MODEL,API_KEY)
    out=df.copy()

    if TRANSLATE_VALUES:
        numeric_like=re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
        values_to_translate=[]
        coords_to_update=[]

        for i in range(len(df)):
            is_price_row = df.at[i,"項目"] in PRICE_ITEM_MSRP_CN or df.at[i,"項目"] in PRICE_ITEM_DEALER_CN
            for j in range(4,len(df.columns)):
                cur=str(df.iat[i,j]).strip()
                if is_price_row or cur in {"","●","○","–","-","—"} or numeric_like.fullmatch(cur):
                    continue

                if reuse:
                    old=str(prev_cn.iat[i,j]).strip()
                    if cur==old:
                        out.iat[i,j]=prev_ja.iat[i,j]
                        continue

                values_to_translate.append(cur)
                coords_to_update.append((i,j))

        uniq_vals=uniq(values_to_translate)
        print(f"🌐 to_translate={len(uniq_vals)}")
        val_map=tr.translate_unique(uniq_vals) if uniq_vals else {}

        for (i,j) in coords_to_update:
            s=str(df.iat[i,j]).strip()
            if s:
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
