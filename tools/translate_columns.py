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
TRANSLATE_COLNAMES = os.environ.get("TRANSLATE_COLNAMES", "true").lower() == "true"
STRIP_GRADE_PREFIX = os.environ.get("STRIP_GRADE_PREFIX", "true").lower() == "true"
SERIES_PREFIX_RE   = os.environ.get("SERIES_PREFIX", "").strip()
EXRATE_CNY_TO_JPY  = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))

CACHE_REPO_DIR     = os.environ.get("CACHE_REPO_DIR", "cache").strip()

BATCH_SIZE, RETRIES, SLEEP_BASE = 60, 3, 1.2

# ====== クリーニング ======
NOISE_ANY = ["对比","参数","图片","配置","详情"]
def clean_any_noise(s:str)->str:
    s=str(s) if s is not None else ""
    for w in NOISE_ANY:
        s=s.replace(w,"")
    return re.sub(r"\s+"," ",s).strip(" 　-—–")

def uniq(seq):
    s, out = set(), []
    for x in seq:
        if x not in s:
            s.add(x); out.append(x)
    return out

def chunked(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

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
            "自然で簡潔な日本語へ翻訳してください。"
            "数値・年式・排量・AT/MT等の記号は保持。"
            "出力は JSON（{'translations':[{'cn':'原文','ja':'訳文'}]}）のみ。"
        )
        print(f"🟢 Translator ready: model={self.model}")

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
            d=json.loads(content)
            return {t["cn"]:t["ja"] for t in d.get("translations",[]) if t.get("cn")}
        except Exception as e:
            print("❌ OpenAI error:", repr(e))
            return {t:t for t in terms}

    def translate_unique(self, unique_terms:list[str])->dict[str,str]:
        out={}
        for chunk in chunked(unique_terms,BATCH_SIZE):
            for attempt in range(1,RETRIES+1):
                try:
                    out.update(self.translate_batch(chunk))
                    break
                except Exception as e:
                    print(f"retry {attempt} {e}")
                    time.sleep(SLEEP_BASE*attempt)
        return out

# ====== main ======
def main():
    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    if not Path(SRC).exists():
        raise FileNotFoundError(f"入力CSVが見つかりません: {SRC}")

    df = pd.read_csv(SRC, encoding="utf-8-sig").map(clean_any_noise)

    cn_snap_path = Path(CACHE_REPO_DIR) / f"{SERIES_ID}/cn.csv"
    ja_prev_path = Path(CACHE_REPO_DIR) / f"{SERIES_ID}/ja.csv"

    cn_exists = cn_snap_path.exists()
    ja_exists = ja_prev_path.exists()
    reuse = cn_exists and ja_exists
    print(f"♻️ reuse={reuse}")

    prev_cn, prev_ja = None, None
    if reuse:
        prev_cn = pd.read_csv(cn_snap_path, encoding="utf-8-sig")
        prev_ja = pd.read_csv(ja_prev_path, encoding="utf-8-sig")

    tr = Translator(MODEL, API_KEY)

    out = df.copy()
    to_translate = []
    if reuse and prev_cn is not None:
        for i in range(len(df)):
            for j in range(len(df.columns)):
                if j<4: continue
                a,b=str(df.iat[i,j]).strip(), str(prev_cn.iat[i,j]).strip()
                if a!=b: to_translate.append(a)
    else:
        for j in range(len(df.columns)):
            if j<4: continue
            for i in range(len(df)):
                s=str(df.iat[i,j]).strip()
                if s: to_translate.append(s)

    uniq_vals = uniq(to_translate)
    print(f"🌐 to_translate={len(uniq_vals)}")
    val_map = tr.translate_unique(uniq_vals) if uniq_vals else {}

    for i in range(len(df)):
        for j in range(len(df.columns)):
            if j<4: continue
            s=str(df.iat[i,j]).strip()
            if s: out.iat[i,j] = val_map.get(s,s)

    # ===== 出力 =====
    DST_PRIMARY.parent.mkdir(parents=True, exist_ok=True)
    out_save = out.drop(columns=["セクション","項目"], errors="ignore")

    out_save.to_csv(DST_PRIMARY, index=False, encoding="utf-8-sig")
    out_save.to_csv(DST_SECONDARY, index=False, encoding="utf-8-sig")

    cn_snap_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(cn_snap_path, index=False, encoding="utf-8-sig")
    out.to_csv(ja_prev_path, index=False, encoding="utf-8-sig")

    print(f"✅ Saved: {DST_PRIMARY}")
    print(f"📦 Repo cache CN: {cn_snap_path}")
    print(f"📦 Repo cache JA: {ja_prev_path}")

if __name__ == "__main__":
    main()
