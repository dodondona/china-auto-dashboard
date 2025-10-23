# tools/translate_columns.py
from __future__ import annotations
import os, json, time, math, re
from pathlib import Path
import pandas as pd

# OpenAI SDK (Responses API)
from openai import OpenAI

SRC = Path(os.environ.get("CSV_IN",  "output/autohome/7578/config_7578.csv"))
DST = Path(os.environ.get("CSV_OUT", "output/autohome/7578/config_7578_ja.csv"))
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")  # 品質寄りは gpt-4.1
API_KEY = os.environ.get("OPENAI_API_KEY")

# ===== Utilities =====

def uniq_list(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def chunked(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

# ===== Translator =====

class LLMTranslator:
    def __init__(self, model: str, api_key: str):
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set.")
        self.client = OpenAI(api_key=api_key)
        self.model = model

        # JSON schema to FORCE the structure: {"translations":[{"cn": "...", "ja":"..."}]}
        self.schema = {
            "name": "cn_ja_translations",
            "schema": {
                "type": "object",
                "properties": {
                    "translations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "cn": {"type": "string"},
                                "ja": {"type": "string"}
                            },
                            "required": ["cn", "ja"],
                            "additionalProperties": False
                        }
                    }
                },
                "required": ["translations"],
                "additionalProperties": False
            },
            "strict": True,
        }

        # System instruction focuses on glossary-like behavior & non-translation policy
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セクション名/項目名』の配列です。"
            "自然で簡潔な日本語へ翻訳してください。固有名詞・グレード名・車名は意訳しないでください。"
            "出力は JSON で、{'translations': [{'cn':'原文','ja':'訳文'}, ...]} の形式だけで返してください。"
        )

        # Jargon hinting for better automotive terms
        self.jargon_hint = (
            "用語指針: 车身→車体, 外观→外観, 灯光→照明, 方向盘→ステアリング, 后视镜→ミラー, "
            "座椅→シート, 底盘→シャシー, 转向→ステアリング, 制动→ブレーキ, 多媒体→マルチメディア, "
            "电动机/电机→電動機, 电池→バッテリー, 充电→充電, 发动机→エンジン, 智能→スマート, "
            "主动安全→予防安全, 被动安全→受動安全。句読点は不要。"
        )

    def _responses_api(self, terms: list[str]) -> dict[str, str]:
        """
        Primary path: Responses API with json_schema to enforce structure strictly.
        """
        prompt = {
            "input": json.dumps({"terms": terms}, ensure_ascii=False),
        }
        resp = self.client.responses.create(
            model=self.model,
            temperature=0,
            response_format={"type": "json_schema", "json_schema": self.schema},
            input=[
                {"role": "system", "content": self.system},
                {"role": "user", "content": self.jargon_hint},
                {"role": "user", "content": prompt["input"]},
            ],
        )
        # Responses API: aggregated text lives in resp.output_text, but we asked for JSON schema
        # so we should pull from the JSON structure.
        try:
            # Some SDK versions provide .output[0].content[0].text
            content = resp.output_text  # already a JSON string as per response_format
        except Exception:
            # fallback: serialize entire object and try to fish out JSON
            content = json.dumps(resp.model_dump(), ensure_ascii=False)

        return self._parse_any_json(content, terms)

    def _parse_any_json(self, content: str, terms: list[str]) -> dict[str, str]:
        """
        Be liberal in what we accept: handle object with 'translations', array of dicts,
        or even line-based 'cn\tja' formats (last resort).
        """
        mapp = {}

        # 1) try strict {"translations":[{"cn":..,"ja":..},...]}
        try:
            data = json.loads(content)
            if isinstance(data, dict) and "translations" in data:
                for d in data["translations"]:
                    cn = str(d.get("cn", "")).strip()
                    ja = str(d.get("ja", "")).strip()
                    if cn:
                        mapp[cn] = ja or cn
                if mapp:
                    return mapp
        except Exception:
            pass

        # 2) try it's a list of dicts
        try:
            data = json.loads(content)
            if isinstance(data, list):
                for d in data:
                    if isinstance(d, dict):
                        cn = str(d.get("cn", "")).strip()
                        ja = str(d.get("ja", "")).strip()
                        if cn:
                            mapp[cn] = ja or cn
            if mapp:
                return mapp
        except Exception:
            pass

        # 3) try to find JSON object in a code block
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
                    if mapp:
                        return mapp
            except Exception:
                pass

        # 4) line-based fallback: cn \t ja
        lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
        if len(lines) >= len(terms) // 2:  # crude sanity
            for ln in lines:
                if "\t" in ln:
                    cn, ja = ln.split("\t", 1)
                    mapp[cn.strip()] = ja.strip()
        # Ensure all mapped
        for t in terms:
            mapp.setdefault(t, t)
        return mapp

    def translate_unique(self, unique_terms: list[str], batch_size=100, sleep=0.5) -> dict[str, str]:
        out = {}
        for chunk in chunked(unique_terms, batch_size):
            tries = 0
            while True:
                tries += 1
                try:
                    result = self._responses_api(chunk)
                    out.update(result)
                    print(f"✅ Translated {sum(1 for k in chunk if k in result)}/{len(chunk)} terms (acc={len(out)}/{len(unique_terms)})")
                    break
                except Exception as e:
                    if tries >= 3:
                        print("⚠️ 3 retries failed:", e)
                        # fallback: identity mapping
                        for t in chunk:
                            out.setdefault(t, t)
                        break
                    time.sleep(1.5 * tries + sleep)
        return out

def main():
    if not SRC.exists():
        raise FileNotFoundError(f"CSV_IN not found: {SRC}")

    df = pd.read_csv(SRC, encoding="utf-8-sig")

    uniq_sec = uniq_list([str(x).strip() for x in df["セクション"].fillna("").tolist() if str(x).strip()])
    uniq_item = uniq_list([str(x).strip() for x in df["項目"].fillna("").tolist() if str(x).strip()])

    print(f"Translating {len(uniq_sec)} sections + {len(uniq_item)} items using {MODEL}...")

    tr = LLMTranslator(MODEL, API_KEY)

    sec_map = tr.translate_unique(uniq_sec, batch_size=60)
    item_map = tr.translate_unique(uniq_item, batch_size=60)

    df_out = df.copy()
    # Insert Japanese columns immediately right of CN columns
    df_out.insert(1, "セクション_ja", df_out["セクション"].map(lambda x: sec_map.get(str(x).strip(), str(x).strip())))
    df_out.insert(3, "項目_ja", df_out["項目"].map(lambda x: item_map.get(str(x).strip(), str(x).strip())))

    DST.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(DST, index=False, encoding="utf-8-sig")
    print(f"✅ Saved translated file: {DST}")

if __name__ == "__main__":
    main()
