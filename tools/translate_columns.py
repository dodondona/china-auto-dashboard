import os
import re
import time
from pathlib import Path
import pandas as pd
from openai import OpenAI
import json

# ===== 設定 =====
CACHE_DIR = Path("cache_repo/series")         # リポジトリ内キャッシュ（シリーズごとに CN/JA を丸ごと保存）
OUTPUT_DIR = Path("output/autohome")          # 出力先
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

RETRIES = 3
SLEEP_BASE = 1.2
BATCH_SIZE = 60
EXRATE = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))

# ===== 判定 =====
RE_NUMERIC_LIKE = re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
RE_WAN = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*万")
RE_YUAN = re.compile(r"(?P<num>[\d,]+)\s*元")

def is_blank_or_symbol(x) -> bool:
    s = str(x).strip()
    return s in {"", "-", "—", "—-", "●", "○"}

def is_numeric_like(x) -> bool:
    return bool(RE_NUMERIC_LIKE.fullmatch(str(x).strip()))

def parse_cny(text: str):
    t = str(text)
    m1 = RE_WAN.search(t)
    if m1:
        return float(m1.group("num")) * 10000.0
    m2 = RE_YUAN.search(t)
    if m2:
        return float(m2.group("num").replace(",", ""))
    return None

def msrp_to_yuan_and_jpy(cell: str, rate: float) -> str:
    t = str(cell).strip()
    if not t or t in {"-", "–", "—"}:
        return t
    cny = parse_cny(t)
    if cny is None:
        if ("元" not in t) and RE_WAN.search(t):
            t = f"{t}元"
        return t
    m1 = RE_WAN.search(t)
    yuan_disp = f"{m1.group('num')}万元" if m1 else (t if "元" in t else f"{t}元")
    jpy = int(round(cny * rate))
    return f"{yuan_disp}（日本円{jpy:,}円）"

def dealer_to_yuan_only(cell: str) -> str:
    t = str(cell).strip()
    if not t or t in {"-", "–", "—"}:
        return t
    if ("元" not in t) and RE_WAN.search(t):
        t = f"{t}元"
    return t

# ===== 文字化け防止CSV読込 =====
def safe_read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)

# ===== 小物 =====
def uniq(seq):
    seen, out = set(), []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def chunked(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def same_shape_and_headers(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    return (df1.shape == df2.shape) and (list(df1.columns) == list(df2.columns))

# ===== バッチ翻訳（新規に必要な分だけ） =====
class Translator:
    def __init__(self, client: OpenAI, model: str, retries=3, sleep_base=1.2, batch_size=60):
        self.client = client
        self.model = model
        self.retries = retries
        self.sleep_base = sleep_base
        self.batch_size = batch_size

    def _translate_chunk(self, terms):
        cleaned = []
        passthrough = {}
        for t in terms:
            if t is None:
                passthrough[""] = ""
                continue
            s = str(t)
            if is_blank_or_symbol(s) or is_numeric_like(s):
                passthrough[s] = s
            else:
                cleaned.append(s)

        out = dict(passthrough)
        if not cleaned:
            return out

        msgs = [
            {"role":"system","content":"あなたは中国語→日本語の専門翻訳者です。配列の各要素を自然な日本語に翻訳してください。出力は JSON のみ。各要素は {\"cn\": 原文, \"ja\": 訳} の配列で返してください。"},
            {"role":"user","content":json.dumps({"terms":cleaned}, ensure_ascii=False)}
        ]
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=msgs,
            temperature=0,
            response_format={"type":"json_object"},
        )
        content = resp.choices[0].message.content or "{}"
        try:
            data = json.loads(content)
            if isinstance(data, dict) and "translations" in data:
                for item in data["translations"]:
                    cn = str(item.get("cn",""))
                    ja = str(item.get("ja","") or cn)
                    out[cn] = ja
        except Exception:
            for s in cleaned:
                out.setdefault(s, s)
        return out

    def translate_unique(self, unique_terms):
        out = {}
        terms = [str(t) for t in unique_terms if str(t) not in out]
        for chunk in chunked(terms, self.batch_size):
            for attempt in range(1, self.retries+1):
                try:
                    out.update(self._translate_chunk(chunk))
                    break
                except Exception as e:
                    print(f"⚠ バッチ翻訳失敗 ({attempt}/{self.retries}) {e}")
                    if attempt == self.retries:
                        for t in chunk: out.setdefault(str(t), str(t))
                    time.sleep(self.sleep_base * attempt)
        return out

# ===== メイン =====
def main():
    series_id = (os.environ.get("SERIES_ID") or "unknown").strip()
    csv_in_env = (os.environ.get("CSV_IN") or "").strip()

    # 入力：CSV_IN優先、無ければ output/autohome/<id>/config_<id>.csv
    if csv_in_env and Path(csv_in_env).exists():
        SRC = Path(csv_in_env)
    else:
        SRC = OUTPUT_DIR / series_id / f"config_{series_id}.csv"

    # 出力（448と同じ）
    CN_OUT = OUTPUT_DIR / series_id / f"config_{series_id}.csv"           # 原文をそのまま
    JA_OUT = OUTPUT_DIR / series_id / f"config_{series_id}.ja.csv"        # 訳（_ja列含む）
    JA_OUT_COMPAT = OUTPUT_DIR / series_id / f"config_{series_id}_ja.csv" # 互換名

    # キャッシュ（シリーズ毎に丸ごと保存）
    CACHE_DIR_SERIES = CACHE_DIR / series_id
    CACHE_DIR_SERIES.mkdir(parents=True, exist_ok=True)  # ← 必ず先に作る
    CACHE_CN = CACHE_DIR_SERIES / "cn.csv"
    CACHE_JA = CACHE_DIR_SERIES / "ja.csv"

    print(f"🔎 SRC: {SRC}")
    print(f"📝 CN:  {CN_OUT}")
    print(f"📝 JA:  {JA_OUT}")

    if not SRC.exists():
        print(f"⚠ 入力CSVが見つかりません（スキップ）: {SRC}")
        return

    df_cn = safe_read_csv(SRC)
    if df_cn.empty:
        print("⚠ 入力CSVが空です。スキップします。")
        return

    # 原文（CN）を保存（BOM）
    CN_OUT.parent.mkdir(parents=True, exist_ok=True)
    df_cn.to_csv(CN_OUT, index=False, encoding="utf-8-sig")

    # 前回キャッシュ（丸ごと）読込
    df_cn_prev = safe_read_csv(CACHE_CN) if CACHE_CN.exists() else None
    df_ja_prev = safe_read_csv(CACHE_JA) if CACHE_JA.exists() else None
    can_reuse = (df_cn_prev is not None) and (df_ja_prev is not None) and same_shape_and_headers(df_cn, df_cn_prev)

    client = OpenAI(api_key=OPENAI_API_KEY)
    tr = Translator(client, OPENAI_MODEL, retries=RETRIES, sleep_base=SLEEP_BASE, batch_size=BATCH_SIZE)

    # 出力JAフレーム（CNコピー＋_ja列追加）
    out = df_cn.copy()
    if "セクション_ja" not in out.columns:
        out.insert(1, "セクション_ja", "")
    if "項目_ja" not in out.columns:
        out.insert(3, "項目_ja", "")

    # ===== セクション/項目（特別扱いしない：セル位置で差分判定） =====
    #   前回CNと同じ位置・値なら前回JAを再利用。違えば翻訳キューへ。
    sec_terms, itm_terms = [], []
    if can_reuse:
        # 再利用
        reuse_mask_sec = df_cn["セクション"].astype(str).str.strip().values == df_cn_prev["セクション"].astype(str).str.strip().values
        reuse_mask_itm = df_cn["項目"].astype(str).str.strip().values == df_cn_prev["項目"].astype(str).str.strip().values
        out.loc[reuse_mask_sec, "セクション_ja"] = df_ja_prev.loc[reuse_mask_sec, "セクション_ja"].astype(str).values
        out.loc[reuse_mask_itm, "項目_ja"]     = df_ja_prev.loc[reuse_mask_itm, "項目_ja"].astype(str).values
        # 変更だけ翻訳候補へ
        sec_terms = [str(s).strip() for s, used in zip(out["セクション"], out["セクション_ja"].astype(str).eq("")) if used and str(s).strip()]
        itm_terms = [str(s).strip() for s, used in zip(out["項目"],     out["項目_ja"].astype(str).eq("")) if used and str(s).strip()]
    else:
        sec_terms = [str(s).strip() for s in out["セクション"] if str(s).strip()]
        itm_terms = [str(s).strip() for s in out["項目"]     if str(s).strip()]

    sec_terms = uniq(sec_terms)
    itm_terms = uniq(itm_terms)
    if sec_terms:
        sec_map = tr.translate_unique(sec_terms)
        out.loc[out["セクション_ja"].eq(""), "セクション_ja"] = out.loc[out["セクション_ja"].eq(""), "セクション"].map(lambda s: sec_map.get(str(s).strip(), str(s).strip()))
    if itm_terms:
        itm_map = tr.translate_unique(itm_terms)
        out.loc[out["項目_ja"].eq(""),     "項目_ja"]     = out.loc[out["項目_ja"].eq(""),     "項目"].map(lambda s: itm_map.get(str(s).strip(), str(s).strip()))

    # ===== 列ヘッダ（グレード名など） =====
    fixed = list(out.columns[:4])
    cur_grades = list(out.columns[4:])
    if can_reuse and list(df_cn_prev.columns) == list(df_cn.columns):
        # 列配列が完全一致なら、前回JAの列名（グレード名）をそのまま使う
        out.columns = list(df_ja_prev.columns)
    else:
        grade_terms = uniq([str(c) for c in cur_grades if str(c).strip()])
        if grade_terms:
            grade_map = tr.translate_unique(grade_terms)
            out.columns = fixed + [grade_map.get(str(c), str(c)) for c in cur_grades]
        else:
            out.columns = fixed + cur_grades

    # ===== 値セル =====
    MSRP_CN = {"厂商指导价(元)", "厂商指导价", "厂商指导价（元）"}
    DEALER_CN = {"经销商报价", "经销商参考价", "经销商"}
    is_msrp   = out["項目"].isin(MSRP_CN) | out["項目_ja"].astype(str).str.contains("メーカー希望小売価格", na=False)
    is_dealer = out["項目"].isin(DEALER_CN) | out["項目_ja"].astype(str).str.contains("ディーラー販売価格", na=False)

    # 価格整形（まず価格行を整形、非価格は後で翻訳）
    for col in out.columns[4:]:
        out.loc[is_msrp, col]   = out.loc[is_msrp, col].map(lambda s: msrp_to_yuan_and_jpy(s, EXRATE))
        out.loc[is_dealer, col] = out.loc[is_dealer, col].map(lambda s: dealer_to_yuan_only(s))

    # 非価格セルの差分だけ翻訳対象に（セル位置・値で比較）
    terms_val = []
    if can_reuse and list(df_cn_prev.columns) == list(df_cn.columns):
        for col in out.columns[4:]:
            cn_col = col if can_reuse and (col in df_cn_prev.columns) else None
            if cn_col is None:
                # 新しい列は全部翻訳候補へ
                vals = out[col].astype(str)
                for v in vals:
                    s = v.strip()
                    if s and (not is_blank_or_symbol(s)) and (not is_numeric_like(s)):
                        terms_val.append(s)
                continue
            cur = df_cn[cn_col].astype(str).str.strip()
            old = df_cn_prev[cn_col].astype(str).str.strip()
            changed = (cur != old)
            for i in out.index:
                if changed.iat[i] and (not is_msrp.iat[i]) and (not is_dealer.iat[i]):
                    v = str(out.iat[i, out.columns.get_loc(col)]).strip()
                    if v and (not is_blank_or_symbol(v)) and (not is_numeric_like(v)):
                        terms_val.append(v)
    else:
        for col in out.columns[4:]:
            for v in out[col].astype(str):
                s = v.strip()
                if s and (not is_blank_or_symbol(s)) and (not is_numeric_like(s)):
                    terms_val.append(s)

    terms_val = uniq(terms_val)
    if terms_val:
        val_map = tr.translate_unique(terms_val)
        for col in out.columns[4:]:
            non_price_mask = ~(is_msrp | is_dealer)
            out.loc[non_price_mask, col] = out.loc[non_price_mask, col].astype(str).map(lambda s: val_map.get(s.strip(), s.strip()))

    # ===== 出力（BOM） =====
    out.to_csv(JA_OUT, index=False, encoding="utf-8-sig")
    out.to_csv(JA_OUT_COMPAT, index=False, encoding="utf-8-sig")

    # ===== キャッシュ保存（シリーズ丸ごと／BOM） =====
    try:
        df_cn.to_csv(CACHE_CN, index=False, encoding="utf-8-sig")
        out.to_csv(CACHE_JA, index=False, encoding="utf-8-sig")
        print("💾 リポジトリ内キャッシュ更新完了")
    except Exception as e:
        print(f"⚠ キャッシュ保存中にエラー: {e}")

    print("✅ 完了：セル単位の差分のみ翻訳（セクション_ja/項目_jaも特別扱いなし）、前回と一致セルは全面再利用")

if __name__ == "__main__":
    main()
