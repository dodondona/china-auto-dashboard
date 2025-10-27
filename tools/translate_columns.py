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

# ====== 固定訳・整形 ======
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

RE_PAREN_ANY_YEN=re.compile(r"（[^）]*(?:日本円|JPY|[¥￥]|円)[^）]*）")
RE_ANY_YEN_TOKEN=re.compile(r"(日本円|JPY|[¥￥]|円)")
def strip_any_yen_tokens(s:str)->str:
    t=str(s)
    t=RE_PAREN_ANY_YEN.sub("",t)
    t=RE_ANY_YEN_TOKEN.sub("",t)
    return re.sub(r"\s+"," ",t).strip()

BRAND_MAP={"BYD":"BYD","比亚迪":"BYD"}
FIX_JA_ITEMS={
    "厂商指导价":"メーカー希望小売価格",
    "经销商参考价":"ディーラー販売価格（元）",
    "经销商报价":"ディーラー販売価格（元）",
    "经销商":"ディーラー販売価格（元）",
    "被动安全":"衝突安全",
}
FIX_JA_SECTIONS={"被动安全":"衝突安全"}

PRICE_ITEM_MSRP_CN={"厂商指导价"}
PRICE_ITEM_MSRP_JA={"メーカー希望小売価格"}
PRICE_ITEM_DEALER_CN={"经销商参考价","经销商报价","经销商"}
PRICE_ITEM_DEALER_JA={"ディーラー販売価格（元）"}

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
    t=strip_any_yen_tokens(clean_price_cell(cell))
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
    t=strip_any_yen_tokens(clean_price_cell(cell))
    if not t or t in {"-","–","—"}:return t
    if("元"not in t)and RE_WAN.search(t):t=f"{t}元"
    return t

# ====== Utility ======
def uniq(seq):
    s,out=set(),[]
    for x in seq:
        if x not in s:s.add(x);out.append(x)
    return out

def chunked(xs,n):
    for i in range(0,len(xs),n):
        yield xs[i:i+n]

def parse_json_relaxed(content:str,terms:list[str])->dict[str,str]:
    try:
        d=json.loads(content)
        if isinstance(d,dict)and"translations"in d:
            return {t["cn"]:t["ja"] for t in d["translations"] if t.get("cn")}
    except Exception:pass
    return {t:t for t in terms}

# ====== Translator ======
class Translator:
    def __init__(self,model:str,api_key:str):
        if not(api_key and api_key.strip()):
            raise RuntimeError("OPENAI_API_KEY missing")
        self.client=OpenAI(api_key=api_key)
        self.model=model
        self.system=("あなたは自動車仕様表の専門翻訳者です。"
                     "入力は中国語の『セクション名/項目名/モデル名/セル値』配列です。"
                     "自然で簡潔な日本語に翻訳してください。JSONで返すこと。")
    def translate_unique(self,terms:list[str])->dict[str,str]:
        out={}
        for chunk in chunked(terms,BATCH_SIZE):
            msgs=[{"role":"system","content":self.system},
                  {"role":"user","content":json.dumps({"terms":chunk},ensure_ascii=False)}]
            try:
                r=self.client.chat.completions.create(
                    model=self.model,messages=msgs,temperature=0,
                    response_format={"type":"json_object"})
                c=r.choices[0].message.content or ""
                out.update(parse_json_relaxed(c,chunk))
            except Exception as e:
                print("❌",e)
                for t in chunk:out.setdefault(t,t)
        return out

# ====== Cache ======
def repo_cache_paths(series_id:str)->tuple[Path,Path]:
    base=Path(CACHE_REPO_DIR)/str(series_id or "unknown")
    return base/"cn.csv", base/"ja.csv"

def same_shape_and_headers(df1,df2):
    return (df1.shape==df2.shape) and (list(df1.columns)==list(df2.columns))

def norm_cn_cell(s:str)->str:
    return clean_any_noise(str(s)).strip()

# ====== main ======
def main():
    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    df=pd.read_csv(SRC,encoding="utf-8-sig").map(clean_any_noise)
    df.columns=[BRAND_MAP.get(c,c) for c in df.columns]

    cn_snap,ja_prev=repo_cache_paths(SERIES_ID)
    cn_exist,ja_exist=cn_snap.exists(),ja_prev.exists()
    prev_cn=pd.read_csv(cn_snap,encoding="utf-8-sig").map(clean_any_noise) if cn_exist else None
    prev_ja=pd.read_csv(ja_prev,encoding="utf-8-sig") if ja_exist else None
    enable=(prev_cn is not None and prev_ja is not None and same_shape_and_headers(df,prev_cn))
    print("♻️ reuse=",enable)

    tr=Translator(MODEL,API_KEY)

    uniq_sec=uniq(df["セクション"].dropna().astype(str))
    uniq_item=uniq(df["項目"].dropna().astype(str))
    sec_chg,item_chg=set(),set()
    if enable:
        for c,o in zip(df["セクション"],prev_cn["セクション"]):
            if norm_cn_cell(c)!=norm_cn_cell(o):sec_chg.add(c)
        for c,o in zip(df["項目"],prev_cn["項目"]):
            if norm_cn_cell(c)!=norm_cn_cell(o):item_chg.add(c)
    sec_map=tr.translate_unique(list(sec_chg)) if sec_chg else {}
    item_map=tr.translate_unique(list(item_chg)) if item_chg else {}
    sec_map.update(FIX_JA_SECTIONS);item_map.update(FIX_JA_ITEMS)

    out=df.copy()
    out.insert(1,"セクション_ja",out["セクション"].map(lambda s:sec_map.get(s,s)))
    out.insert(3,"項目_ja",out["項目"].map(lambda s:item_map.get(s,s)))

    MSRP_RE=re.compile(r"^メーカー希望小売価格$")
    DEALER_RE=re.compile(r"^ディーラー販売価格（元）$")
    is_msrp=out["項目"].isin(PRICE_ITEM_MSRP_CN)|out["項目_ja"].str.match(MSRP_RE)
    is_dealer=out["項目"].isin(PRICE_ITEM_DEALER_CN)|out["項目_ja"].str.match(DEALER_RE)

    for col in out.columns[4:]:
        out.loc[is_msrp,col]=out.loc[is_msrp,col].map(lambda s:msrp_to_yuan_and_jpy(s,EXRATE_CNY_TO_JPY))
        out.loc[is_dealer,col]=out.loc[is_dealer,col].map(lambda s:dealer_to_yuan_only(s))

    # ---- 値セル ----
    if TRANSLATE_VALUES:
        numeric_like=re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
        non_price_mask=~(is_msrp|is_dealer)
        vals_to_tr=[]
        if enable:
            for col in out.columns[4:]:
                cur=df[col].astype(str).map(norm_cn_cell)
                old=prev_cn[col].astype(str).map(norm_cn_cell)
                chg=(cur!=old)
                m_copy=non_price_mask & (~chg)
                out.loc[m_copy,col]=prev_ja.loc[m_copy,col]
                for i in out.index:
                    if not(non_price_mask[i] and chg[i]):continue
                    v=str(out.at[i,col]).strip()
                    if v in {"","●","○","–","-","—"}:continue
                    if numeric_like.fullmatch(v):continue
                    vals_to_tr.append(v)
        else:
            for col in out.columns[4:]:
                for v in out.loc[non_price_mask,col].astype(str):
                    v=v.strip()
                    if v in {"","●","○","–","-","—"}:continue
                    if numeric_like.fullmatch(v):continue
                    vals_to_tr.append(v)
        uniq_vals=uniq(vals_to_tr)
        val_map=tr.translate_unique(uniq_vals) if uniq_vals else {}
        for col in out.columns[4:]:
            for i in out.index:
                if not non_price_mask[i]:continue
                s=str(out.at[i,col]).strip()
                out.at[i,col]=val_map.get(s,s)

    # ---- 出力 ----
    DST_PRIMARY.parent.mkdir(parents=True,exist_ok=True)
    out.to_csv(DST_PRIMARY,index=False,encoding="utf-8-sig")
    out.to_csv(DST_SECONDARY,index=False,encoding="utf-8-sig")
    cn_snap.parent.mkdir(parents=True,exist_ok=True)
    df.to_csv(cn_snap,index=False,encoding="utf-8-sig")
    out.to_csv(ja_prev,index=False,encoding="utf-8-sig")
    print("✅ done",DST_PRIMARY)

if __name__=="__main__":
    main()
