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
    if s.endswith(".ja.csv"): s2 = s.replace(".ja.csv", "_ja.csv")
    elif s.endswith("_ja.csv"): s2 = s.replace("_ja.csv", ".ja.csv")
    else: s2 = dst.stem + ".ja.csv"
    return dst.parent / s2
DST_SECONDARY = make_secondary(DST_PRIMARY)

# ====== 設定 ======
MODEL   = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
API_KEY = os.environ.get("OPENAI_API_KEY")
FORCE_RETRANSLATE = os.environ.get("FORCE_RETRANSLATE", "false").lower() == "true"
TRANSLATE_VALUES   = os.environ.get("TRANSLATE_VALUES", "true").lower() == "true"
TRANSLATE_COLNAMES = os.environ.get("TRANSLATE_COLNAMES", "true").lower() == "true"
STRIP_GRADE_PREFIX = os.environ.get("STRIP_GRADE_PREFIX", "true").lower() == "true"
SERIES_PREFIX_RE   = os.environ.get("SERIES_PREFIX", "").strip()
EXRATE_CNY_TO_JPY  = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))
CACHE_REPO_DIR     = os.environ.get("CACHE_REPO_DIR", "cache").strip()
BATCH_SIZE, RETRIES, SLEEP_BASE = 60, 3, 1.2

# ====== ノイズ除去等 ======
NOISE_ANY = ["对比","参数","图片","配置","详情"]
def clean_any_noise(s:str)->str:
    s=str(s) if s is not None else ""
    for w in NOISE_ANY: s=s.replace(w,"")
    return re.sub(r"\s+"," ",s).strip(" 　-—–")

BRAND_MAP={
    "BYD":"BYD","比亚迪":"BYD",
    "奔驰":"メルセデス・ベンツ","梅赛德斯-奔驰":"メルセデス・ベンツ",
}
FIX_JA_ITEMS={
    "厂商指导价":"メーカー希望小売価格",
    "经销商参考价":"ディーラー販売価格（元）",
    "经销商报价":"ディーラー販売価格（元）",
    "经销商":"ディーラー販売価格（元）",
    "被动安全":"衝突安全",
}
FIX_JA_SECTIONS={"被动安全":"衝突安全"}

# ====== ユーティリティ ======
def uniq(seq):
    s, out = set(), []
    for x in seq:
        if x not in s:
            s.add(x); out.append(x)
    return out

def chunked(xs, n):
    for i in range(0, len(xs), n): yield xs[i:i+n]

def parse_json_relaxed(content:str,terms:list[str])->dict[str,str]:
    try:
        d=json.loads(content)
        if isinstance(d,dict)and"translations"in d:
            return {str(t["cn"]).strip():str(t.get("ja",t["cn"])).strip() for t in d["translations"] if t.get("cn")}
    except Exception: pass
    pairs=re.findall(r'"cn"\s*:\s*"([^"]+)"\s*,\s*"ja"\s*:\s*"([^"]*)"', content)
    if pairs: return {cn.strip():ja.strip() for cn,ja in pairs}
    return {t:t for t in terms}

# ====== LLM ======
class Translator:
    def __init__(self, model: str, api_key: str):
        if not (api_key and api_key.strip()): raise RuntimeError("OPENAI_API_KEY is not set")
        self.client = OpenAI(api_key=api_key); self.model = model
        self.system = ("あなたは自動車仕様表の専門翻訳者です。"
                       "入力は中国語の『セクション名/項目名/モデル名/セル値』の配列です。"
                       "自然で簡潔な日本語へ翻訳してください。数値・年式・排量・AT/MT等の記号は保持。"
                       "出力は JSON（{'translations':[{'cn':'原文','ja':'訳文'}]}）のみ。")
    def translate_batch(self, terms: list[str]) -> dict[str,str]:
        if not terms: return {}
        msgs=[{"role":"system","content":self.system},
              {"role":"user","content":json.dumps({"terms":terms},ensure_ascii=False)}]
        try:
            resp=self.client.chat.completions.create(
                model=self.model,messages=msgs,temperature=0,
                response_format={"type":"json_object"})
            content=resp.choices[0].message.content or ""
            return parse_json_relaxed(content, terms)
        except Exception as e:
            print("❌ OpenAI error:", repr(e)); return {t:t for t in terms}
    def translate_unique(self, unique_terms: list[str]) -> dict[str,str]:
        out={}; 
        for chunk in chunked(unique_terms, BATCH_SIZE):
            for attempt in range(1, RETRIES+1):
                try: out.update(self.translate_batch(chunk)); break
                except Exception as e:
                    print(f"❌ translate_unique error attempt={attempt}:", repr(e))
                    if attempt==RETRIES:
                        for t in chunk: out.setdefault(t, t)
                    time.sleep(SLEEP_BASE*attempt)
        return out

# ====== キャッシュ ======
def repo_cache_paths(series_id: str) -> tuple[Path, Path]:
    base = Path(CACHE_REPO_DIR) / str(series_id or "unknown")
    return (base / "cn.csv", base / "ja.csv")

def same_shape_and_headers(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    return (df1.shape == df2.shape) and (list(df1.columns) == list(df2.columns))

def norm_cn_cell(s: str) -> str: return clean_any_noise(str(s)).strip()

# ====== main ======
def main():
    print(f"🔎 SRC: {SRC}")
    df = pd.read_csv(SRC, encoding="utf-8-sig").map(clean_any_noise)
    df.columns = [BRAND_MAP.get(c,c) for c in df.columns]

    cn_path, ja_path = repo_cache_paths(SERIES_ID)
    prev_cn = pd.read_csv(cn_path, encoding="utf-8-sig").map(clean_any_noise) if cn_path.exists() else None
    prev_ja = pd.read_csv(ja_path, encoding="utf-8-sig") if ja_path.exists() else None
    enable_reuse = (not FORCE_RETRANSLATE) and (prev_cn is not None) and (prev_ja is not None) and same_shape_and_headers(df, prev_cn)
    print(f"♻️ reuse={enable_reuse}")

    tr = Translator(MODEL, API_KEY)

    # ------- セクション/項目 翻訳 -------
    uniq_sec  = uniq([str(x).strip() for x in df["セクション"].fillna("") if str(x).strip()])
    uniq_item = uniq([str(x).strip() for x in df["項目"].fillna("") if str(x).strip()])
    sec_map = tr.translate_unique(uniq_sec)
    item_map = tr.translate_unique(uniq_item)
    sec_map.update(FIX_JA_SECTIONS); item_map.update(FIX_JA_ITEMS)

    out_full = df.copy()
    out_full.insert(1,"セクション_ja",out_full["セクション"].map(lambda s:sec_map.get(str(s).strip(),str(s).strip())))
    out_full.insert(3,"項目_ja",out_full["項目"].map(lambda s:item_map.get(str(s).strip(),str(s).strip())))

    # ------- 値セル 翻訳 -------
    if TRANSLATE_VALUES:
        numeric_like=re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
        values=[]; coords=[]
        for i in range(len(df)):
            for j in range(4,len(df.columns)):
                v=str(df.iat[i,j]).strip()
                if v in {"","●","○","–","-","—"}: continue
                if numeric_like.fullmatch(v): continue
                values.append(v); coords.append((i,j))
        uniq_vals=uniq(values)
        print(f"🌐 to_translate: values={len(uniq_vals)}")
        val_map=tr.translate_unique(uniq_vals)
        for (i,j) in coords:
            s=str(df.iat[i,j]).strip()
            out_full.iat[i,j]=val_map.get(s,s)

    # ------- 出力 -------
    grade_cols=[c for c in out_full.columns if c not in("セクション","項目")]
    final_out=pd.concat(
        [out_full.loc[:,["セクション_ja","項目_ja"]],
         out_full.loc[:,grade_cols[4:]]],
        axis=1
    )

    DST_PRIMARY.parent.mkdir(parents=True,exist_ok=True)
    final_out.to_csv(DST_PRIMARY,index=False,encoding="utf-8-sig")
    final_out.to_csv(DST_SECONDARY,index=False,encoding="utf-8-sig")

    cn_path.parent.mkdir(parents=True,exist_ok=True)
    pd.read_csv(SRC,encoding="utf-8-sig").to_csv(cn_path,index=False,encoding="utf-8-sig")
    out_full.to_csv(ja_path,index=False,encoding="utf-8-sig")

    print(f"✅ Saved: {DST_PRIMARY}")
    print(f"📦 cache CN: {cn_path}")
    print(f"📦 cache JA: {ja_path}")

if __name__=="__main__":
    main()
