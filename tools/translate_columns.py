from __future__ import annotations
import os, json, time, re
from pathlib import Path
import pandas as pd
from openai import OpenAI

# 入出力パス
SRC = Path(os.environ.get("CSV_IN",  "output/autohome/7578/config_7578.csv"))
DST = Path(os.environ.get("CSV_OUT", "output/autohome/7578/config_7578_ja.csv"))

# OpenAI
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
API_KEY = os.environ.get("OPENAI_API_KEY")

# 挙動スイッチ
TRANSLATE_VALUES    = os.environ.get("TRANSLATE_VALUES", "true").lower() == "true"
TRANSLATE_COLNAMES  = os.environ.get("TRANSLATE_COLNAMES", "true").lower() == "true"

# グレード列の先頭車名を削除する（既定オン）
STRIP_GRADE_PREFIX  = os.environ.get("STRIP_GRADE_PREFIX", "true").lower() == "true"
# 明示指定（例: "駆逐艦05|驱逐舰05|Destroyer 05"）
SERIES_PREFIX_RE    = os.environ.get("SERIES_PREFIX", "").strip()

# 為替（CNY→JPY）。必要に応じて Actions の env で上書き
EXRATE_CNY_TO_JPY   = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))

BATCH_SIZE = 60
RETRIES    = 3
SLEEP_BASE = 1.2

# -----------------------
# ノイズ＆辞書
# -----------------------
NOISE_ANY = ["对比", "参数", "图片", "配置", "详情"]
NOISE_PRICE_TAIL = ["询价", "计算器", "询底价", "报价", "价格询问", "価格問い合わせ"]

def clean_any_noise(s: str) -> str:
    s = str(s) if s is not None else ""
    for w in NOISE_ANY + NOISE_PRICE_TAIL:
        s = s.replace(w, "")
    s = re.sub(r"\s+", " ", s).strip(" 　-—–")
    return s

def clean_price_cell(s: str) -> str:
    t = clean_any_noise(s)
    for w in NOISE_PRICE_TAIL:
        t = re.sub(rf"(?:\s*{re.escape(w)}\s*)+$", "", t)
    return t.strip()

# BYD は翻訳しない（比亚迪→BYD に正規化）
BRAND_MAP = {
    "BYD": "BYD",
    "比亚迪": "BYD",
}

# 最優先の固定訳
# ★ 価格見出しは「（元）」に統一（※ご要望どおり）
FIX_JA_ITEMS = {
    "厂商指导价": "メーカー希望小売価格（元）",
    "经销商参考价": "ディーラー販売価格（元）",
    "经销商报价":   "ディーラー販売価格（元）",
    "被动安全":     "衝突安全",
}
FIX_JA_SECTIONS = {"被动安全": "衝突安全"}

PRICE_ITEM_CN = {"厂商指导价", "经销商参考价", "经销商报价"}
PRICE_ITEM_JA = {"メーカー希望小売価格（元）", "ディーラー販売価格（元）"}

# 価格併記（CNY→JPY）
RE_WAN  = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*万")
RE_YUAN = re.compile(r"(?P<num>[\d,]+)\s*元")

def append_jpy_with_yuan_label(s: str, rate: float) -> str:
    """
    値セルに円概算を併記しつつ、通貨ラベルは「元」で統一。
    例: "11.98万" -> "11.98万元（約¥251,580）"
        "129,800元" -> "129,800元（約¥2,725,800）"
    """
    t = str(s).strip()
    if not t or t in {"-", "–", "—"}:
        return t

    # CNY抽出
    m1 = RE_WAN.search(t)
    m2 = RE_YUAN.search(t)
    cny = None
    if m1:
        cny = float(m1.group("num")) * 10000.0
    elif m2:
        cny = float(m2.group("num").replace(",", ""))

    # 「元」を明記（“中国元”ではなく“元”）
    if cny is not None and "元" not in t:
        # "11.98万" のように“万”のみなら "万元" とする
        if "万" in t:
            t = f"{t}元"
        else:
            t = f"{t}元"

    if cny is None:
        return t

    jpy = int(round(cny * rate))
    jpy_fmt = f"{jpy:,}"
    if "（約¥" in t or "(約¥" in t:
        return t
    return f"{t}（約¥{jpy_fmt}）"

# -----------------------
# LLM ユーティリティ
# -----------------------
def uniq(seq):
    s, out = set(), []
    for x in seq:
        if x not in s:
            s.add(x); out.append(x)
    return out

def chunked(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def parse_json_relaxed(content: str, terms: list[str]) -> dict[str, str]:
    try:
        data = json.loads(content)
        if isinstance(data, dict) and "translations" in data:
            m = {}
            for d in data["translations"]:
                cn = str(d.get("cn", "")).strip()
                ja = str(d.get("ja", "")).strip()
                if cn:
                    m[cn] = ja or cn
            if m:
                return m
    except Exception:
        pass
    mjson = re.search(r"\{[\s\S]*\}", content)
    if mjson:
        try:
            data = json.loads(mjson.group(0))
            if isinstance(data, dict) and "translations" in data:
                m = {}
                for d in data["translations"]:
                    cn = str(d.get("cn", "")).strip()
                    ja = str(d.get("ja", "")).strip()
                    if cn:
                        m[cn] = ja or cn
                if m:
                    return m
        except Exception:
            pass
    m = {}
    for line in content.splitlines():
        if "\t" in line:
            cn, ja = line.split("\t", 1)
            cn = cn.strip(); ja = ja.strip()
            if cn:
                m[cn] = ja or cn
    for t in terms:
        m.setdefault(t, t)
    return m

class Translator:
    def __init__(self, model: str, api_key: str):
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セクション名/項目名/モデル名/セル値』の配列です。"
            "自然で簡潔な日本語へ翻訳してください。数値・年式・排量・AT/MT等の記号は保持。"
            "出力は JSON（{'translations':[{'cn':'原文','ja':'訳文'}]}）のみ。"
        )

    def translate_batch(self, terms: list[str]) -> dict[str, str]:
        msgs = [
            {"role": "system", "content": self.system},
            {"role": "user", "content": json.dumps({"terms": terms}, ensure_ascii=False)},
        ]
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=msgs,
            temperature=0,
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content or ""
        return parse_json_relaxed(content, terms)

    def translate_unique(self, unique_terms: list[str]) -> dict[str, str]:
        out = {}
        for chunk in chunked(unique_terms, BATCH_SIZE):
            for attempt in range(1, RETRIES+1):
                try:
                    out.update(self.translate_batch(chunk))
                    break
                except Exception:
                    if attempt == RETRIES:
                        for t in chunk:
                            out.setdefault(t, t)
                    time.sleep(SLEEP_BASE * attempt)
        return out

# -----------------------
# 列ヘッダ（グレード）先頭の車名を削除
# -----------------------
# 自動検出: グレード列の先頭に共通して現れる「漢字/ラテン文字＋数字等」のプレフィックスを推定
RE_SERIES_TOKEN = re.compile(r"^[\u4e00-\u9fffA-Za-z][\u4e00-\u9fffA-Za-z0-9\- ]{1,20}")

def detect_common_prefix(candidates: list[str]) -> str | None:
    """
    候補列（グレード列）から共通プレフィックスを推定。
    - 先頭トークン（上記正規表現でマッチ）を抽出
    - 出現上位1件で、全体の80%以上に現れ、かつ長さ>=2 の場合に採用
    """
    tokens = []
    for c in candidates:
        m = RE_SERIES_TOKEN.match(c.strip())
        if m:
            tokens.append(m.group(0).strip())
    if not tokens:
        return None
    # 最頻値
    from collections import Counter
    cnt = Counter(tokens)
    top, n = cnt.most_common(1)[0]
    if n >= max(1, int(0.8 * len(candidates))) and len(top) >= 2:
        return re.escape(top)
    return None

def strip_series_prefix_from_grades(grade_cols: list[str]) -> list[str]:
    if not grade_cols or not STRIP_GRADE_PREFIX:
        return grade_cols

    # 1) ユーザー指定があればそれを優先
    pattern = None
    if SERIES_PREFIX_RE:
        pattern = SERIES_PREFIX_RE  # そのまま正規表現として使用（"A|B|C" など）
    else:
        # 2) 自動検出
        auto = detect_common_prefix(grade_cols)
        if auto:
            pattern = auto

    if not pattern:
        return grade_cols

    # 先頭の series + 区切り（スペース/ハイフン/スラッシュ/コロン等）を一発で除去
    # 例: "驱逐舰05 2024款DM-i 豪华型" -> "2024款DM-i 豪华型"
    #     "駆逐艦05-2024年式…"        -> "2024年式…"
    regex = re.compile(rf"^\s*(?:{pattern})\s*[-:：/ ]*\s*", re.IGNORECASE)
    cleaned = [regex.sub("", c).strip() or c for c in grade_cols]
    return cleaned

# -----------------------
# main
# -----------------------
def main():
    df = pd.read_csv(SRC, encoding="utf-8-sig")
    df = df.map(clean_any_noise)

    # 列ヘッダのブランド正規化（BYD）
    new_cols = [BRAND_MAP.get(c, c) for c in df.columns]
    df.columns = new_cols

    # —— セクション/項目の翻訳 —— #
    uniq_sec  = uniq([str(x).strip() for x in df["セクション"].fillna("").tolist() if str(x).strip()])
    uniq_item = uniq([str(x).strip() for x in df["項目"].fillna("").tolist() if str(x).strip()])

    tr = Translator(MODEL, API_KEY)
    sec_map  = tr.translate_unique(uniq_sec)
    item_map = tr.translate_unique(uniq_item)

    # 固定訳で上書き
    sec_map.update(FIX_JA_SECTIONS)
    item_map.update(FIX_JA_ITEMS)

    out = df.copy()
    out.insert(1, "セクション_ja", out["セクション"].map(lambda s: sec_map.get(str(s).strip(), str(s).strip())))
    out.insert(3, "項目_ja",     out["項目"].map(lambda s: item_map.get(str(s).strip(), str(s).strip())))

    # —— 列ヘッダ（グレード名など）の翻訳＆先頭車名削除 —— #
    if TRANSLATE_COLNAMES:
        orig_cols   = list(out.columns)
        fixed_cols  = orig_cols[:4]
        grade_cols  = orig_cols[4:]

        # まずはブランド正規化済み
        grade_cols_norm = [BRAND_MAP.get(c, c) for c in grade_cols]

        # 先頭車名の除去（自動 or 指定）
        grade_cols_stripped = strip_series_prefix_from_grades(grade_cols_norm)

        # LLM翻訳（重複削除→復元）
        uniq_grades = uniq([str(c).strip() for c in grade_cols_stripped])
        grade_map   = tr.translate_unique(uniq_grades)
        translated  = [grade_map.get(g, g) or g for g in grade_cols_stripped]

        out.columns = fixed_cols + translated
    else:
        # 翻訳しない場合でも、車名除去だけ適用したいときはここで
        if STRIP_GRADE_PREFIX:
            orig_cols   = list(out.columns)
            fixed_cols  = orig_cols[:4]
            grade_cols  = orig_cols[4:]
            out.columns = fixed_cols + strip_series_prefix_from_grades(grade_cols)

    # —— 価格セル：ノイズ除去 → 「元」明記 + 円併記 —— #
    is_price_row = out["項目"].isin(list(PRICE_ITEM_CN)) | out["項目_ja"].isin(list(PRICE_ITEM_JA))
    for col in out.columns[4:]:
        out.loc[is_price_row, col] = out.loc[is_price_row, col].map(
            lambda s: append_jpy_with_yuan_label(clean_price_cell(s), EXRATE_CNY_TO_JPY)
        )

    # —— 値セルの翻訳（価格行は対象外のまま） —— #
    if TRANSLATE_VALUES:
        values = []
        numeric_like = re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
        for col in out.columns[4:]:
            for v in out[col].astype(str).tolist():
                vv = v.strip()
                if vv in {"", "●", "○", "–", "-", "—"}:
                    continue
                if numeric_like.fullmatch(vv):
                    continue
                values.append(vv)
        uniq_vals = uniq(values)
        val_map = tr.translate_unique(uniq_vals)

        non_price_mask = ~is_price_row
        for col in out.columns[4:]:
            out.loc[non_price_mask, col] = out.loc[non_price_mask, col].map(
                lambda s: val_map.get(str(s).strip(), str(s).strip())
            )

    DST.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST, index=False, encoding="utf-8-sig")
    print(f"✅ Saved translated file: {DST}")

if __name__ == "__main__":
    main()
