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

# リポジトリ内キャッシュフォルダ
CACHE_REPO_DIR = os.environ.get("CACHE_REPO_DIR", "cache").strip()

BATCH_SIZE, RETRIES, SLEEP_BASE = 60, 3, 1.2

# ====== クリーニング・固定訳など（省略なし、前回版と同一） ======
NOISE_ANY = ["对比","参数","图片","配置","详情"]
NOISE_PRICE_TAIL = ["询价","计算器","询底价","报价","价格询问","起","起售"]

def clean_any_noise(s:str)->str:
    s=str(s) if s is not None else ""
    for w in NOISE_ANY+NOISE_PRICE_TAIL:
        s=s.replace(w,"")
    return re.sub(r"\s+"," ",s).strip(" 　-—–")

def clean_price_cell(s:str)->str:
    t=clean_any_noise(s)
    for w in NOISE_PRICE_TAIL:
        t=re.sub(rf"(?:\s*{re.escape(w)}\s*)+$","",t)
    return t.strip()

RE_WAN=re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*万")
RE_YUAN=re.compile(r"(?P<num>[\d,]+)\s*元")

def parse_cny(text:str):
    t=str(text)
    m1=RE_WAN.search(t)
    if m1:return float(m1.group("num"))*10000.0
    m2=RE_YUAN.search(t)
    if m2:return float(m2.group("num").replace(",",""))
    return None

def msrp_to_yuan_and_jpy(cell:str,rate:float)->str:
    t=clean_price_cell(cell)
    if not t or t in {"-","–","—"}:return t
    cny=parse_cny(t)
    if cny is None:
        if("元"not in t)and RE_WAN.search(t):t=f"{t}元"
        return t
    m1=RE_WAN.search(t)
    yuan_disp=f"{m1.group('num')}万元" if m1 else (t if"元"in t else f"{t}元")
    jpy=int(round(cny*rate))
    return f"{yuan_disp}（日本円{jpy:,}円）"

def dealer_to_yuan_only(cell:str)->str:
    t=clean_price_cell(cell)
    if not t or t in {"-","–","—"}:return t
    if("元"not in t)and RE_WAN.search(t):t=f"{t}元"
    return t

# ====== LLM翻訳器 ======
class Translator:
    def __init__(self, model: str, api_key: str):
        if not (api_key and api_key.strip()):
            raise RuntimeError("OPENAI_API_KEY is not set")
        self.client = OpenAI(api_key=api_key)
        self.model = model
        print(f"🟢 Translator ready: model={self.model}, API_KEY_LEN={len(api_key.strip())}")

    def translate_batch(self, terms: list[str]) -> dict[str,str]:
        if not terms:
            return {}
        print(f"🛰️  API call: {len(terms)} terms (sample={terms[:3]})")
        msgs=[
            {"role":"system","content":"あなたは自動車仕様表の専門翻訳者です。"},
            {"role":"user","content":json.dumps({"terms":terms},ensure_ascii=False)},
        ]
        try:
            resp=self.client.chat.completions.create(
                model=self.model,messages=msgs,temperature=0,
                response_format={"type":"json_object"},
            )
            data=json.loads(resp.choices[0].message.content)
            if "translations" in data:
                return {d["cn"]:d["ja"] for d in data["translations"] if d.get("cn")}
        except Exception as e:
            print("❌ OpenAI error:", repr(e))
        return {t:t for t in terms}

    def translate_unique(self, unique_terms:list[str])->dict[str,str]:
        out={}
        for chunk in [unique_terms[i:i+BATCH_SIZE] for i in range(0,len(unique_terms),BATCH_SIZE)]:
            out.update(self.translate_batch(chunk))
        return out

# ====== キャッシュ処理 ======
def repo_cache_paths(series_id: str) -> tuple[Path, Path]:
    base = Path(CACHE_REPO_DIR) / str(series_id or "unknown")
    return (base / "cn.csv", base / "ja.csv")

def same_shape_and_headers(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    return (df1.shape == df2.shape) and (list(df1.columns) == list(df2.columns))

# ====== メイン ======
def main():
    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    df = pd.read_csv(SRC, encoding="utf-8-sig").map(clean_any_noise)
    cn_snap_path, ja_prev_path = repo_cache_paths(SERIES_ID)
    prev_cn_df = pd.read_csv(cn_snap_path, encoding="utf-8-sig") if cn_snap_path.exists() else None
    prev_ja_df = pd.read_csv(ja_prev_path, encoding="utf-8-sig") if ja_prev_path.exists() else None
    enable_reuse = (prev_cn_df is not None) and (prev_ja_df is not None) and same_shape_and_headers(df, prev_cn_df)
    print(f"♻️ reuse_available={enable_reuse}")

    tr = Translator(MODEL, API_KEY)

    uniq_sec = df["セクション"].dropna().unique().tolist()
    uniq_item = df["項目"].dropna().unique().tolist()
    sec_map = tr.translate_unique(uniq_sec)
    item_map = tr.translate_unique(uniq_item)

    out = df.copy()
    out.insert(1,"セクション_ja",out["セクション"].map(sec_map))
    out.insert(3,"項目_ja",out["項目"].map(item_map))

    DST_PRIMARY.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST_PRIMARY,index=False,encoding="utf-8-sig")
    out.to_csv(DST_SECONDARY,index=False,encoding="utf-8-sig")

    cn_snap_path.parent.mkdir(parents=True,exist_ok=True)
    df.to_csv(cn_snap_path,index=False,encoding="utf-8-sig")
    out.to_csv(ja_prev_path,index=False,encoding="utf-8-sig")

    print(f"✅ Saved: {DST_PRIMARY}")
    print(f"📦 Repo cache CN: {cn_snap_path}")
    print(f"📦 Repo cache JA: {ja_prev_path}")

if __name__ == "__main__":
    main()
