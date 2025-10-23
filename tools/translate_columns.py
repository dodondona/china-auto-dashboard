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
EXRATE_CNY_TO_JPY = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))

BATCH_SIZE = 60
RETRIES = 3

# --- ノイズ除去 ---
NOISE_WORDS_ANY = ["计算器", "询价", "询底价", "报价", "对比", "图片", "配置", "参数", "详情", "价格询问"]
NOISE_PRICE_TAIL = ["询价", "计算器", "询底价", "报价"]

def clean_any_noise(s: str) -> str:
    s = str(s).strip()
    for w in NOISE_WORDS_ANY:
        s = s.replace(w, "")
    return re.sub(r"\s+", " ", s).strip(" -　")

def clean_price_cell(s: str) -> str:
    s = clean_any_noise(s)
    for w in NOISE_PRICE_TAIL:
        s = re.sub(rf"(?:\s*{re.escape(w)}\s*)+$", "", s)
    return s.strip()

# --- ブランド辞書（英語維持） ---
BRAND_MAP = {
    "BYD": "BYD", "比亚迪": "BYD",
    "NIO": "NIO", "蔚来": "NIO",
    "XPeng": "XPeng", "小鹏": "XPeng",
    "Geely": "Geely", "吉利": "Geely",
    "Changan": "Changan", "长安": "Changan",
    "Chery": "Chery", "奇瑞": "Chery",
    "Li Auto": "Li Auto", "理想": "Li Auto",
    "AITO": "AITO", "问界": "AITO",
    "Wuling": "Wuling", "五菱": "Wuling",
    "Ora": "Ora", "欧拉": "Ora",
    "Zeekr": "Zeekr", "极氪": "Zeekr",
    "Lynk & Co": "Lynk & Co", "领克": "Lynk & Co",
}

# --- 固定訳 ---
FIX_JA_ITEMS = {
    "厂商指导价": "メーカー希望小売価格（元）",
    "经销商参考价": "ディーラー販売価格（元）",
    "经销商报价": "ディーラー販売価格（元）",
    "被动安全": "衝突安全",
    "语音助手唤醒词": "音声アシスタント起動ワード",
    "后排出风口": "後席送風口",
    "后座出风口": "後席送風口",
}
FIX_JA_SECTIONS = {"被动安全": "衝突安全"}

PRICE_ITEM_CN = {"厂商指导价", "经销商参考价", "经销商报价"}
PRICE_ITEM_JA = {"メーカー希望小売価格（元）", "ディーラー販売価格（元）"}

# --- 価格正規表現（改良版）---
RE_WAN = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*万(?:元)?")
RE_YUAN = re.compile(r"(?P<num>[\d,]+)\s*元")

def append_jpy(s: str, rate: float) -> str:
    t = str(s)
    m1 = RE_WAN.search(t)
    m2 = RE_YUAN.search(t)
    cny = None
    if m1:
        cny = float(m1.group("num")) * 10000.0
    elif m2:
        cny = float(m2.group("num").replace(",", ""))
    if cny is None:
        return t
    jpy = int(round(cny * rate))
    jpy_fmt = f"{jpy:,}"
    # すでに日本円が併記されていればスキップ
    if "¥" in t and "約" in t:
        return t
    # 「起」など語尾は括弧の外に残す
    suffix = ""
    if t.endswith("起"):
        t = t[:-1]
        suffix = "起"
    return f"{t}中国元（約¥{jpy_fmt}）{suffix}"

# --- LLM補助 ---
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
    mapp = {}
    try:
        data = json.loads(content)
        if isinstance(data, dict) and "translations" in data:
            for d in data["translations"]:
                cn = str(d.get("cn", "")).strip()
                ja = str(d.get("ja", "")).strip()
                if cn:
                    mapp[cn] = ja or cn
            return mapp
    except Exception:
        pass
    return {t: t for t in terms}

class Translator:
    def __init__(self, model: str, api_key: str):
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "中国語の項目やセクション名を自然な日本語に翻訳してください。"
            "JSON形式で {\"translations\":[{\"cn\":\"原文\",\"ja\":\"訳文\"}]} で返してください。"
        )
    def translate_batch(self, terms: list[str]) -> dict[str, str]:
        messages = [
            {"role": "system", "content": self.system},
            {"role": "user", "content": json.dumps({"terms": terms}, ensure_ascii=False)},
        ]
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0,
            response_format={"type": "json_object"},
        )
        return parse_json_relaxed(resp.choices[0].message.content or "", terms)

    def translate_unique(self, terms: list[str]) -> dict[str, str]:
        out = {}
        for chunk in chunked(terms, BATCH_SIZE):
            for _ in range(RETRIES):
                try:
                    part = self.translate_batch(chunk)
                    out.update(part)
                    break
                except Exception as e:
                    print("⚠️ retry:", e)
                    time.sleep(1)
        return out

# --- メイン ---
def main():
    df = pd.read_csv(SRC, encoding="utf-8-sig")
    df = df.map(clean_any_noise)
    df.columns = [BRAND_MAP.get(c, c) for c in df.columns]

    uniq_sec  = uniq(df["セクション"].dropna().astype(str))
    uniq_item = uniq(df["項目"].dropna().astype(str))
    tr = Translator(MODEL, API_KEY)
    sec_map  = tr.translate_unique(uniq_sec)
    item_map = tr.translate_unique(uniq_item)

    sec_map.update(FIX_JA_SECTIONS)
    item_map.update(FIX_JA_ITEMS)

    out = df.copy()
    out.insert(1, "セクション_ja", out["セクション"].map(lambda s: sec_map.get(s, s)))
    out.insert(3, "項目_ja", out["項目"].map(lambda s: item_map.get(s, s)))

    # 価格セル変換
    is_price = out["項目"].isin(PRICE_ITEM_CN) | out["項目_ja"].isin(PRICE_ITEM_JA)
    for col in out.columns[4:]:
        out.loc[is_price, col] = out.loc[is_price, col].map(clean_price_cell)
        out.loc[is_price, col] = out.loc[is_price, col].map(lambda s: append_jpy(s, EXRATE_CNY_TO_JPY))

    DST.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST, index=False, encoding="utf-8-sig")
    print(f"✅ Saved translated file: {DST}")

if __name__ == "__main__":
    main()
