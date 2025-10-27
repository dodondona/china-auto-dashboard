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

# リポジトリに保存するスナップショット（編集可）
CACHE_REPO_DIR     = os.environ.get("CACHE_REPO_DIR", "cache").strip()

BATCH_SIZE, RETRIES, SLEEP_BASE = 60, 3, 1.2

# ====== クリーニング・固定訳 ======
NOISE_ANY = ["对比","参数","图片","配置","详情"]
NOISE_PRICE_TAIL = ["询价","计算器","询底价","报价","价格询问","起","起售","到店","经销商"]

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

BRAND_MAP={"BYD":"BYD","比亚迪":"BYD","奔驰":"メルセデス・ベンツ","梅赛德斯-奔驰":"メルセデス・ベンツ"}
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

# ====== 金額整形 ======
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

# ====== ユーティリティ ======
def uniq(seq):
    s, out = set(), []
    for x in seq:
        if x not in s:
            s.add(x); out.append(x)
    return out

def chunked(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def parse_json_relaxed(content:str,terms:list[str])->dict[str,str]:
    try:
        d=json.loads(content)
        if isinstance(d,dict)and"translations"in d:
            return {str(t["cn"]).strip():str(t.get("ja",t["cn"])).strip()
                    for t in d["translations"] if t.get("cn")}
    except Exception:
        pass
    pairs=re.findall(r'"cn"\s*:\s*"([^"]+)"\s*,\s*"ja"\s*:\s*"([^"]*)"', content)
    if pairs:
        return {cn.strip():ja.strip() for cn,ja in pairs}
    return {t:t for t in terms}

# ====== LLM ======
class Translator:
    def __init__(self, model: str, api_key: str):
        if not (api_key and api_key.strip()):
            raise RuntimeError("OPENAI_API_KEY is not set")
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セクション名/項目名/モデル名/セル値』の配列です。"
            "自然で簡潔な日本語へ翻訳してください。数値・年式・排量・AT/MT等の記号は保持。"
            "出力は JSON（{'translations':[{'cn':'原文','ja':'訳文'}]}）のみ。"
        )
        print(f"🟢 Translator ready: model={self.model}")

    def translate_batch(self, terms: list[str]) -> dict[str,str]:
        if not terms:
            return {}
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
            return parse_json_relaxed(content, terms)
        except Exception as e:
            print("❌ OpenAI error:", repr(e))
            return {t: t for t in terms}

    def translate_unique(self, unique_terms: list[str]) -> dict[str,str]:
        out={}
        for chunk in chunked(unique_terms, BATCH_SIZE):
            for attempt in range(1, RETRIES+1):
                try:
                    out.update(self.translate_batch(chunk))
                    break
                except Exception as e:
                    print(f"❌ translate_unique error attempt={attempt}:", repr(e))
                    if attempt==RETRIES:
                        for t in chunk: out.setdefault(t, t)
                    time.sleep(SLEEP_BASE*attempt)
        return out

# ====== 列名用：先頭車名のカット ======
YEAR_TOKEN_RE=re.compile(r"(?:20\d{2}|19\d{2})|(?:\d{2}款|[上中下]市|改款|年款)")
LEADING_TOKEN_RE=re.compile(r"^[\u4e00-\u9fffA-Za-z][\u4e00-\u9fffA-Za-z0-9\- ]{1,40}")
def cut_before_year_or_kuan(s:str)->str|None:
    s=s.strip()
    m=YEAR_TOKEN_RE.search(s)
    if m:return s[:m.start()].strip()
    kuan=re.search(r"款",s)
    if kuan:return s[:kuan.start()].strip()
    m2=LEADING_TOKEN_RE.match(s)
    return m2.group(0).strip() if m2 else None

def detect_common_series_prefix(cols:list[str])->str|None:
    cand=[]
    for c in cols:
        p=cut_before_year_or_kuan(str(c))
        if p and len(p)>=2:cand.append(p)
    if not cand:return None
    from collections import Counter
    top,ct=Counter(cand).most_common(1)[0]
    return re.escape(top) if ct>=max(1,int(0.6*len(cols))) else None

def strip_series_prefix_from_grades(grade_cols:list[str])->list[str]:
    if not grade_cols or not STRIP_GRADE_PREFIX:return grade_cols
    pattern=SERIES_PREFIX_RE or detect_common_series_prefix(grade_cols)
    if not pattern:return grade_cols
    regex=re.compile(rf"^\s*(?:{pattern})\s*[-:：/ ]*\s*",re.IGNORECASE)
    return [regex.sub("",str(c)).strip() or c for c in grade_cols]

# ====== リポジトリ内キャッシュ（編集可） ======
def repo_cache_paths(series_id: str) -> tuple[Path, Path]:
    base = Path(CACHE_REPO_DIR) / str(series_id or "unknown")
    return (base / "cn.csv", base / "ja.csv")

def same_shape_and_headers(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    return (df1.shape == df2.shape) and (list(df1.columns) == list(df2.columns))

def norm_cn_cell(s: str) -> str:
    return clean_any_noise(str(s)).strip()

# ====== main ======
def main():
    print(f"CSV_IN check: {SRC}")
    print(f"Pre-run cache listing:")
    os.system(f"ls -l {CACHE_REPO_DIR}/{SERIES_ID} || echo '(no cache dir)'")

    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    if not Path(SRC).exists():
        raise FileNotFoundError(f"入力CSVが見つかりません: {SRC}")

    # 原文（CN）読込・ノイズ掃除
    df = pd.read_csv(SRC, encoding="utf-8-sig").map(clean_any_noise)
    # 列ヘッダのブランド正規化
    df.columns = [BRAND_MAP.get(c, c) for c in df.columns]

    # 前回 CN/JA をリポジトリ内から読込
    cn_snap_path, ja_prev_path = repo_cache_paths(SERIES_ID)
    prev_cn_df = pd.read_csv(cn_snap_path, encoding="utf-8-sig").map(clean_any_noise) if cn_snap_path.exists() else None
    prev_ja_df = pd.read_csv(ja_prev_path, encoding="utf-8-sig") if ja_prev_path.exists() else None
    enable_reuse = (prev_cn_df is not None) and (prev_ja_df is not None) and same_shape_and_headers(df, prev_cn_df)
    print(f"♻️ reuse={enable_reuse}")

    # 翻訳器
    tr = Translator(MODEL, API_KEY)

    # ------- セクション/項目：差分検出 -------
    uniq_sec  = uniq([str(x).strip() for x in df["セクション"].fillna("") if str(x).strip()])
    uniq_item = uniq([str(x).strip() for x in df["項目"].fillna("")    if str(x).strip()])
    print(f"🔢 uniq_sec={len(uniq_sec)}, uniq_item={len(uniq_item)}")

    sec_changed, item_changed = set(), set()
    if enable_reuse:
        for cur, old in zip(df["セクション"].astype(str), prev_cn_df["セクション"].astype(str)):
            if norm_cn_cell(cur) != norm_cn_cell(old):
                sec_changed.add(str(cur).strip())
        for cur, old in zip(df["項目"].astype(str), prev_cn_df["項目"].astype(str)):
            if norm_cn_cell(cur) != norm_cn_cell(old):
                item_changed.add(str(cur).strip())
    print(f"🧮 changed: sections={len(sec_changed)} items={len(item_changed)} (reuse={enable_reuse})")

    sec_to_translate  = [x for x in uniq_sec  if (not enable_reuse) or (x in sec_changed)]
    item_to_translate = [x for x in uniq_item if (not enable_reuse) or (x in item_changed)]
    print(f"🌐 to_translate: sec={len(sec_to_translate)} item={len(item_to_translate)}")

    # 未変更は前回JAからコピー、変更のみAPI
    sec_map_old, item_map_old = {}, {}
    if enable_reuse:
        if "セクション_ja" in prev_ja_df.columns:
            for cur, old_cn, old_ja in zip(df["セクション"].astype(str), prev_cn_df["セクション"].astype(str), prev_ja_df["セクション_ja"].astype(str)):
                if norm_cn_cell(cur) == norm_cn_cell(old_cn):
                    sec_map_old[str(cur).strip()] = str(old_ja).strip() or str(cur).strip()
        if "項目_ja" in prev_ja_df.columns:
            for cur, old_cn, old_ja in zip(df["項目"].astype(str), prev_cn_df["項目"].astype(str), prev_ja_df["項目_ja"].astype(str)):
                if norm_cn_cell(cur) == norm_cn_cell(old_cn):
                    item_map_old[str(cur).strip()] = str(old_ja).strip() or str(cur).strip()

    sec_map_new  = tr.translate_unique(sec_to_translate) if sec_to_translate else {}
    item_map_new = tr.translate_unique(item_to_translate) if item_to_translate else {}

    sec_map  = {**sec_map_old, **sec_map_new}
    item_map = {**item_map_old, **item_map_new}
    sec_map.update(FIX_JA_SECTIONS)
    item_map.update(FIX_JA_ITEMS)

    out_full = df.copy()
    out_full.insert(1, "セクション_ja", out_full["セクション"].map(lambda s: sec_map.get(str(s).strip(), str(s).strip())))
    out_full.insert(3, "項目_ja",     out_full["項目"].map(lambda s: item_map.get(str(s).strip(), str(s).strip())))

    # 見出し(項目_ja)の統一
    PAREN_CURR_RE=re.compile(r"（\s*(?:円|元|人民元|CNY|RMB|JPY)[^）]*）")
    out_full["項目_ja"]=out_full["項目_ja"].astype(str).str.replace(PAREN_CURR_RE,"",regex=True).str.strip()
    out_full.loc[out_full["項目_ja"].str.match(r"^メーカー希望小売価格.*$",na=False),"項目_ja"]="メーカー希望小売価格"
    out_full.loc[out_full["項目_ja"].str.contains(r"ディーラー販売価格",na=False),"項目_ja"]="ディーラー販売価格（元）"

    # ------- 列ヘッダ（グレード） -------
    if TRANSLATE_COLNAMES:
        orig_cols=list(out_full.columns); fixed=orig_cols[:4]; grades=orig_cols[4:]
        grades_norm=[BRAND_MAP.get(c,c) for c in grades]
        grades_stripped=strip_series_prefix_from_grades(grades_norm)

        reuse_headers=False
        if enable_reuse:
            reuse_headers = list(prev_cn_df.columns[4:]) == list(df.columns[4:])
        print(f"🧾 headers_reuse={reuse_headers}")
        if reuse_headers and prev_ja_df is not None and list(prev_ja_df.columns[:4])==list(out_full.columns[:4]):
            out_full.columns = list(prev_ja_df.columns)
        else:
            uniq_grades=uniq([str(c).strip() for c in grades_stripped])
            grade_map=tr.translate_unique(uniq_grades) if uniq_grades else {}
            translated=[grade_map.get(g,g) for g in grades_stripped]
            out_full.columns=fixed+translated
    else:
        if STRIP_GRADE_PREFIX:
            orig_cols=list(out_full.columns); fixed=orig_cols[:4]; grades=orig_cols[4:]
            out_full.columns=fixed+strip_series_prefix_from_grades(grades)

    # ------- 価格セル 整形 -------
    MSRP_JA_RE=re.compile(r"^メーカー希望小売価格$")
    DEALER_JA_RE=re.compile(r"^ディーラー販売価格（元）$")
    is_msrp  = out_full["項目"].isin(PRICE_ITEM_MSRP_CN)  | out_full["項目_ja"].str.match(MSRP_JA_RE,na=False)
    is_dealer= out_full["項目"].isin(PRICE_ITEM_DEALER_CN)| out_full["項目_ja"].str.match(DEALER_JA_RE,na=False)
    for col in out_full.columns[4:]:
        out_full.loc[is_msrp,  col]=out_full.loc[is_msrp,  col].map(lambda s:msrp_to_yuan_and_jpy(s,EXRATE_CNY_TO_JPY))
        out_full.loc[is_dealer,col]=out_full.loc[is_dealer,col].map(lambda s:dealer_to_yuan_only(s))

    # ------- 値セル：変更セルのみ翻訳 -------
    if TRANSLATE_VALUES:
        numeric_like = re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
        non_price_mask = ~(is_msrp | is_dealer)

        values_to_translate=[]
        coords_to_update=[]

        if enable_reuse and (prev_cn_df is not None) and (prev_ja_df is not None):
            if prev_cn_df.shape == df.shape and list(prev_cn_df.columns)==list(df.columns):
                diff_mask=(df!=prev_cn_df)
                for i in range(len(df)):
                    if not non_price_mask.iloc[i]:
                        continue
                    for j in range(4, len(df.columns)):
                        cur=str(df.iat[i,j]).strip()
                        if not diff_mask.iat[i,j]:
                            # 未変更は前回JAをコピー
                            out_full.iat[i,j]=prev_ja_df.iat[i,j]
                            continue
                        if cur in {"","●","○","–","-","—"}: continue
                        if numeric_like.fullmatch(cur): continue
                        values_to_translate.append(cur)
                        coords_to_update.append((i,j))
            else:
                print("⚠️ shape/headers mismatch: full translate for non-price rows")
                for i in range(len(df)):
                    if not non_price_mask.iloc[i]:
                        continue
                    for j in range(4,len(df.columns)):
                        v=str(df.iat[i,j]).strip()
                        if v in {"","●","○","–","-","—"}: continue
                        if numeric_like.fullmatch(v): continue
                        values_to_translate.append(v)
                        coords_to_update.append((i,j))
        else:
            for i in range(len(df)):
                if not non_price_mask.iloc[i]:
                    continue
                for j in range(4,len(df.columns)):
                    v=str(df.iat[i,j]).strip()
                    if v in {"","●","○","–","-","—"}: continue
                    if numeric_like.fullmatch(v): continue
                    values_to_translate.append(v)
                    coords_to_update.append((i,j))

        print(f"🗂️  values candidates before-uniq = {len(values_to_translate)}")
        uniq_vals=uniq(values_to_translate)
        print(f"🌐 to_translate: values={len(uniq_vals)}")
        val_map=tr.translate_unique(uniq_vals) if uniq_vals else {}
        for (i, j) in coords_to_update:
            s=str(df.iat[i,j]).strip()
            if not s: continue
            out_full.iat[i,j]=val_map.get(s,s)

    # ------- 出力 -------
    # 出力は最初から CN 列を含めない（位置ベースで確実に JA＋グレードのみ）
    ja_core = out_full.iloc[:, [1, 3]]  # セクション_ja, 項目_ja
    grades  = out_full.iloc[:, 4:]
    final_out = pd.concat([ja_core, grades], axis=1)

    DST_PRIMARY.parent.mkdir(parents=True, exist_ok=True)
    final_out.to_csv(DST_PRIMARY,   index=False, encoding="utf-8-sig")
    final_out.to_csv(DST_SECONDARY, index=False, encoding="utf-8-sig")

    # リポジトリ内キャッシュは毎回「上書き保存」（削除はしない）
    cn_snap_path.parent.mkdir(parents=True, exist_ok=True)
    pd.read_csv(SRC, encoding="utf-8-sig").to_csv(cn_snap_path, index=False, encoding="utf-8-sig")
    out_full.to_csv(ja_prev_path, index=False, encoding="utf-8-sig")

    print(f"✅ Saved (no CN cols in output): {DST_PRIMARY}")
    print(f"📦 Repo cache CN: {cn_snap_path}")
    print(f"📦 Repo cache JA: {ja_prev_path}")

if __name__ == "__main__":
    main()
