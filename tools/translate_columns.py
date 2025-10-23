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

BATCH_SIZE = 60
RETRIES = 3

# --- ノイズ除去（Autohome固有のUI文字） ---
NOISE_WORDS = ["计算器", "询价", "对比", "图片", "配置", "参数", "详情", "报价"]

# --- ブランド名辞書（固定訳） ---
BRAND_MAP = {
    "BYD": "比亜迪",
    "比亚迪": "比亜迪",
    "NIO": "蔚来",
    "XPeng": "小鵬",
    "Xpeng": "小鵬",
    "Geely": "吉利",
    "Changan": "長安",
    "Chery": "奇瑞",
    "Li Auto": "理想",
    "AITO": "問界",
    "Wuling": "五菱",
    "Ora": "欧拉",
    "Zeekr": "極氪",
    "Lynk & Co": "領克",
}

# --- 共通正規表現 ---
def clean_text(s: str) -> str:
    s = str(s).strip()
    for n in NOISE_WORDS:
        s = s.replace(n, "")
    s = re.sub(r"\s+", " ", s)
    return s.strip(" -　")

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
            if mapp: return mapp
    except Exception:
        pass
    m = re.search(r"\{[\s\S]*\}", content)
    if m:
        try:
            data = json.loads(m.group(0))
            if isinstance(data, dict) and "translations" in data:
                for d in data["translations"]:
                    cn = str(d.get("cn", "")).strip()
                    ja = str(d.get("ja", "")).strip()
                    if cn:
                        mapp[cn] = ja or cn
                if mapp: return mapp
        except Exception:
            pass
    for line in content.splitlines():
        if "\t" in line:
            cn, ja = line.split("\t", 1)
            mapp[cn.strip()] = ja.strip()
    for t in terms:
        mapp.setdefault(t, t)
    return mapp

class Translator:
    def __init__(self, model: str, api_key: str):
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セクション名/項目名/モデル名/セル値』の配列です。"
            "自然で簡潔な日本語へ翻訳してください。"
            "数値・単位は保持し、車名やブランドは辞書に従うこと。"
            "JSONで {'translations':[{'cn':'原文','ja':'訳文'}]} の形式で返してください。"
        )
        self.jargon = (
            "車身→車体, 外观→外観, 灯光→照明, 方向盘→ステアリング, 后视镜→ミラー, "
            "座椅→シート, 底盘→シャシー, 转向→ステアリング, 制动→ブレーキ, "
            "多媒体→マルチメディア, 电机→電動機, 电池→バッテリー, 发动机→エンジン, "
            "智能→スマート, 主动安全→予防安全, 被动安全→受動安全"
        )

    def translate_batch(self, terms: list[str]) -> dict[str, str]:
        messages = [
            {"role": "system", "content": self.system},
            {"role": "user", "content": self.jargon},
            {"role": "user", "content": json.dumps({"terms": terms}, ensure_ascii=False)},
        ]
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0,
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content or ""
        mapp = parse_json_relaxed(content, terms)
        if sum(1 for t in terms if mapp.get(t, "") != t) == 0:
            print("⚠️ zero translation; raw head:", content[:200])
        return mapp

    def translate_unique(self, terms: list[str]) -> dict[str, str]:
        out = {}
        for chunk in chunked(terms, BATCH_SIZE):
            part = self.translate_batch(chunk)
            out.update(part)
        return out

def main():
    df = pd.read_csv(SRC, encoding="utf-8-sig")
    df = df.map(clean_text)  # ノイズ除去

    # ブランド辞書適用（列ヘッダなど）
    df.columns = [BRAND_MAP.get(c, c) for c in df.columns]

    uniq_sec  = uniq(df["セクション"].dropna().astype(str))
    uniq_item = uniq(df["項目"].dropna().astype(str))
    model_headers = [c for c in df.columns[2:]]  # モデル名も翻訳対象へ
    tr = Translator(MODEL, API_KEY)

    sec_map  = tr.translate_unique(uniq_sec)
    item_map = tr.translate_unique(uniq_item)
    model_map = tr.translate_unique(model_headers)

    out = df.copy()
    out.insert(1, "セクション_ja", out["セクション"].map(sec_map))
    out.insert(3, "項目_ja", out["項目"].map(item_map))
    out.columns = [model_map.get(c, c) for c in out.columns]  # モデル列名の翻訳

    # セル本文翻訳（オプション）
    if TRANSLATE_VALUES:
        values = []
        for col in out.columns[4:]:
            values += [v for v in out[col].dropna().astype(str) if not re.fullmatch(r"[●○–\-0-9\.\s]+", v)]
        uniq_vals = uniq(values)
        val_map = tr.translate_unique(uniq_vals)
        for col in out.columns[4:]:
            out[col] = out[col].map(lambda s: val_map.get(str(s).strip(), str(s).strip()))

    DST.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST, index=False, encoding="utf-8-sig")
    print(f"✅ Saved translated file: {DST}")

if __name__ == "__main__":
    main()
