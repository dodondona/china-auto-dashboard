# tools/translate_columns.py
from __future__ import annotations
import os, json, time, re
from pathlib import Path
import pandas as pd
from openai import OpenAI

SRC = Path(os.environ.get("CSV_IN",  "output/autohome/7578/config_7578.csv"))
DST = Path(os.environ.get("CSV_OUT", "output/autohome/7578/config_7578_ja.csv"))
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
API_KEY = os.environ.get("OPENAI_API_KEY")
TRANSLATE_VALUES = os.environ.get("TRANSLATE_VALUES", "true").lower() == "true"

# 為替（CNY→JPY）。例として21.0。必要に応じて Actions の env で上書きしてください。
EXRATE_CNY_TO_JPY = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))

BATCH_SIZE = 60
RETRIES = 3
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
    # ほかは触らない（余計な変更を避ける）
}

# 最優先の固定訳（上書き）
FIX_JA_ITEMS = {
    "厂商指导价": "メーカー希望小売価格（元）",
    "经销商参考价": "ディーラー販売価格（元）",
    "经销商报价": "ディーラー販売価格（元）",
    "被动安全": "衝突安全",
}
FIX_JA_SECTIONS = {"被动安全": "衝突安全"}

PRICE_ITEM_CN = {"厂商指导价", "经销商参考价", "经销商报价"}
PRICE_ITEM_JA = {"メーカー希望小売価格（元）", "ディーラー販売価格（元）"}

# 価格併記
RE_WAN  = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*万")
RE_YUAN = re.compile(r"(?P<num>[\d,]+)\s*元")

def append_jpy(s: str, rate: float) -> str:
    t = str(s).strip()
    if not t or t in {"-", "–", "—"}:
        return t
    m1 = RE_WAN.search(t)
    m2 = RE_YUAN.search(t)
    cny = None
    if m1:
        cny = float(m1.group("num")) * 10000.0
    elif m2:
        cny = float(m2.group("num").replace(",", ""))
    # “元”の明記が無ければ付与（例: 11.98万 → 11.98万中国元）
    if cny is not None and "元" not in t:
        t = f"{t}中国元"
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
    """常に dict を返す。JSON崩れ時にフォールバック。"""
    # 1) 期待通りの JSON
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
    # 2) コードブロック内のJSONを拾う
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
    # 3) タブ区切り "cn\tja" の列を拾う
    m = {}
    for line in content.splitlines():
        if "\t" in line:
            cn, ja = line.split("\t", 1)
            cn = cn.strip(); ja = ja.strip()
            if cn:
                m[cn] = ja or cn
    # 4) 穴埋め（恒等）
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
            "自然で簡潔な日本語へ翻訳してください。数値・単位は保持。"
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
        m = parse_json_relaxed(content, terms)
        if all(m.get(t, t) == t for t in terms):
            print("⚠️ zero translation; raw head:", content[:400])
        return m

    def translate_unique(self, unique_terms: list[str]) -> dict[str, str]:
        out = {}
        for chunk in chunked(unique_terms, BATCH_SIZE):
            for attempt in range(1, RETRIES+1):
                try:
                    part = self.translate_batch(chunk)
                    out.update(part)
                    break
                except Exception as e:
                    if attempt == RETRIES:
                        for t in chunk:
                            out.setdefault(t, t)
                    time.sleep(SLEEP_BASE * attempt)
        return out

# -----------------------
# main
# -----------------------
def main():
    df = pd.read_csv(SRC, encoding="utf-8-sig")

    # ノイズ除去（原文からUI残滓を除く）
    df = df.map(clean_any_noise)

    # 列ヘッダ（モデル名など）にブランド辞書適用（BYD固定）
    df.columns = [BRAND_MAP.get(c, c) for c in df.columns]

    # —— 縦列（セクション/項目）の翻訳 —— #
    uniq_sec  = uniq([str(x).strip() for x in df["セクション"].fillna("").tolist() if str(x).strip()])
    uniq_item = uniq([str(x).strip() for x in df["項目"].fillna("").tolist() if str(x).strip()])

    tr = Translator(MODEL, API_KEY)
    sec_map  = tr.translate_unique(uniq_sec)
    item_map = tr.translate_unique(uniq_item)

    # 最優先の固定訳で上書き
    sec_map.update(FIX_JA_SECTIONS)
    item_map.update(FIX_JA_ITEMS)

    out = df.copy()
    out.insert(1, "セクション_ja", out["セクション"].map(lambda s: sec_map.get(str(s).strip(), str(s).strip())))
    out.insert(3, "項目_ja",     out["項目"].map(lambda s: item_map.get(str(s).strip(), str(s).strip())))

    # 列ヘッダ（モデル名）も LLM で翻訳したい場合はここで適用（今回は余計な変更回避のため未変更）

    # —— 価格セル：ノイズ除去 → CNY→JPY 併記 —— #
    is_price_row = out["項目"].isin(list(PRICE_ITEM_CN)) | out["項目_ja"].isin(list(PRICE_ITEM_JA))
    for col in out.columns[4:]:
        out.loc[is_price_row, col] = out.loc[is_price_row, col].map(lambda s: append_jpy(clean_price_cell(s), EXRATE_CNY_TO_JPY))

    # —— 値セルの翻訳（元の仕様どおり有効化されていれば実行） —— #
    if TRANSLATE_VALUES:
        # 翻訳対象（記号・純数値のみは除外）
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
            out.loc[non_price_mask, col] = out.loc[non_price_mask, col].map(lambda s: val_map.get(str(s).strip(), str(s).strip()))

    DST.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST, index=False, encoding="utf-8-sig")
    print(f"✅ Saved translated file: {DST}")

if __name__ == "__main__":
    main()
