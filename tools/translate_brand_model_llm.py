#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
translate_brand_model_llm.py
最小変更版：OpenAI/Anthropic(Claude) を切替可能にするだけ。既存仕様は保持。

- 入力CSV: --input
- 出力CSV: --output
- 列名: --brand-col, --model-col, --brand-ja-col, --model-ja-col
- モデル指定: --model（OpenAI用）, --anthropic-model（Claude用）
- Provider切替: --provider [openai|anthropic]（既定: openai）
- キャッシュは毎回クリア（従来通り）

必要な環境変数:
- OpenAI: OPENAI_API_KEY
- Anthropic(Claude): ANTHROPIC_API_KEY
"""

import os
import sys
import json
import time
import argparse
import hashlib
from typing import Dict, Tuple, Optional

import pandas as pd

# ====== 依存クライアント（存在する環境のみimport） ======
_OPENAI_AVAILABLE = False
_ANTHROPIC_AVAILABLE = False
try:
    from openai import OpenAI
    _OPENAI_AVAILABLE = True
except Exception:
    pass

try:
    import anthropic
    _ANTHROPIC_AVAILABLE = True
except Exception:
    pass

# ====== 引数 ======
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--brand-col", default="brand")
    p.add_argument("--model-col", default="model")
    p.add_argument("--brand-ja-col", default="brand_ja")
    p.add_argument("--model-ja-col", default="model_ja")
    p.add_argument("--model", default="gpt-4o-mini", help="OpenAI model name")
    p.add_argument("--anthropic-model", default="claude-3-5-sonnet-latest", help="Claude model name")
    p.add_argument("--provider", choices=["openai", "anthropic"], default="openai")
    p.add_argument("--max-retries", type=int, default=3)
    p.add_argument("--sleep", type=float, default=0.7, help="sleep between calls")
    # キャッシュは従来通り都度削除
    p.add_argument("--cache-dir", default=".cache_brand_model_llm")
    return p.parse_args()

# ====== キャッシュ（従来通り：起動時にクリア） ======
def ensure_fresh_cache(cache_dir: str):
    import shutil
    if os.path.isdir(cache_dir):
        shutil.rmtree(cache_dir, ignore_errors=True)
    os.makedirs(cache_dir, exist_ok=True)

def cache_key(brand: str, model: str) -> str:
    return hashlib.sha256(f"{brand}|||{model}".encode("utf-8")).hexdigest()

def cache_get(cache_dir: str, key: str) -> Optional[Tuple[str, str]]:
    path = os.path.join(cache_dir, key + ".json")
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d.get("brand_ja"), d.get("model_ja")
    except Exception:
        return None

def cache_put(cache_dir: str, key: str, brand_ja: str, model_ja: str):
    path = os.path.join(cache_dir, key + ".json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"brand_ja": brand_ja, "model_ja": model_ja}, f, ensure_ascii=False)

# ====== プロンプト（既存ルールを尊重：ここは触らない想定） ======
SYSTEM_PROMPT = """あなたは自動車の中国名→日本語＋グローバル名の正規化アシスタントです。
出力は JSON: {"brand_ja":"...", "model_ja":"..."} のみ。
【厳守ルール】
- 余計な語句や注釈は入れない。必ずJSONのみ返す。
- ブランドは日本で通用する表記（例：フォルクスワーゲン、トヨタ、ホンダ、メルセデス・ベンツ、アウディ、テスラ、BYD など）。
- 中国ブランドは方針に合わせる（例：吉利=Geely、五菱=Wuling、奇瑞=Chery、紅旗=紅旗、長安=Changan、AITO=AITO、零跑=Leapmotor など）。
- モデルは下記の優先順位で：
  1) 公式のグローバル英名が存在 → その英名を優先（例：元PLUS=Atto 3、海豹=Seal、海鸥=Seagull、海豚=Dolphin、朗逸=Lavida、速腾=Sagitar、探岳=Tayron、途岳=Tharu、帕萨特=Passat、迈腾=Magotan 等）
  2) グローバル名が無い場合、中国語の漢字は日本語の新字体へ自然置換し、ピンインは（括弧）併記（例：宏光（Hongguang）MINIEV、星願（Xingyuan）など）
- 「宋/Song」「秦/Qin」のようにシリーズ名＋サフィックス（PLUS, Pro, L など）は、漢字（ピンイン）＋サフィックスの順にする（例：宋（Song）PLUS、秦（Qin）L）
- 吉利银河はブランドは "Geely Galaxy" とし、"银河A7" の model_ja は "A7" のようにグローバル名相当を優先
- 明らかな誤表記はしない（Frontlander/Corolla Cross/Tiguan/Lavida 等）
- 出力は必ず JSON 一行のみ
"""

USER_PROMPT_TEMPLATE = """ブランド: {brand}
モデル: {model}
期待する返答: JSON一行（brand_ja, model_ja）"""

# ====== LLM呼び出しを provider 非依存にする薄いラッパ ======
class LLMClient:
    def __init__(self, provider: str, openai_model: str, anthropic_model: str):
        self.provider = provider
        self.openai_model = openai_model
        self.anthropic_model = anthropic_model

        if provider == "openai":
            if not _OPENAI_AVAILABLE:
                raise RuntimeError("openai パッケージが見つかりません。")
            if not os.getenv("OPENAI_API_KEY"):
                raise RuntimeError("環境変数 OPENAI_API_KEY が未設定です。")
            self._client = OpenAI()
        else:
            if not _ANTHROPIC_AVAILABLE:
                raise RuntimeError("anthropic パッケージが見つかりません。")
            if not os.getenv("ANTHROPIC_API_KEY"):
                raise RuntimeError("環境変数 ANTHROPIC_API_KEY が未設定です。")
            self._client = anthropic.Anthropic()

    def chat_once(self, system_prompt: str, user_prompt: str) -> str:
        if self.provider == "openai":
            # OpenAI
            resp = self._client.chat.completions.create(
                model=self.openai_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=200,
            )
            return resp.choices[0].message.content.strip()
        else:
            # Anthropic(Claude)
            resp = self._client.messages.create(
                model=self.anthropic_model,
                system=system_prompt,
                max_tokens=200,
                temperature=0.1,
                messages=[{"role": "user", "content": user_prompt}],
            )
            # Claudeは contentがリスト。テキストを連結
            chunks = []
            for b in resp.content:
                if getattr(b, "type", None) == "text":
                    chunks.append(b.text)
            return "".join(chunks).strip()

# ====== 行ごと処理 ======
def translate_one(llm: LLMClient, brand: str, model: str, retries: int = 3, sleep_sec: float = 0.7) -> Tuple[str, str]:
    user_prompt = USER_PROMPT_TEMPLATE.format(brand=brand, model=model)
    last_err = None
    for _ in range(retries):
        try:
            raw = llm.chat_once(SYSTEM_PROMPT, user_prompt)
            # 必ずJSON一行の想定
            data = json.loads(raw)
            b = (data.get("brand_ja") or "").strip()
            m = (data.get("model_ja") or "").strip()
            if b and m:
                return b, m
        except Exception as e:
            last_err = e
        time.sleep(sleep_sec)
    # 失敗時はフォールバック：そのまま返す（余計な変更はしないため）
    sys.stderr.write(f"[warn] LLM変換失敗: brand={brand}, model={model}, err={last_err}\n")
    return brand, model

def main():
    args = parse_args()
    ensure_fresh_cache(args.cache_dir)  # 従来通り：起動毎にクリア

    # LLM クライアント初期化
    llm = LLMClient(provider=args.provider, openai_model=args.model, anthropic_model=args.anthropic_model)

    df = pd.read_csv(args.input)
    # 必要列が無ければそのまま出す（余計な変更を避ける）
    for col in (args.brand_col, args.model_col):
        if col not in df.columns:
            raise SystemExit(f"必要列がありません: {col}")

    # 既存列を尊重し、無ければ作る
    if args.brand_ja_col not in df.columns:
        df[args.brand_ja_col] = ""
    if args.model_ja_col not in df.columns:
        df[args.model_ja_col] = ""

    for idx, row in df.iterrows():
        brand = str(row[args.brand_col]) if pd.notna(row[args.brand_col]) else ""
        model = str(row[args.model_col]) if pd.notna(row[args.model_col]) else ""
        if not brand and not model:
            continue

        # 既に値が入っていたら尊重（余計な変更なし）
        if str(row.get(args.brand_ja_col, "")).strip() and str(row.get(args.model_ja_col, "")).strip():
            continue

        key = cache_key(brand, model)
        c = cache_get(args.cache_dir, key)
        if c:
            bja, mja = c
        else:
            bja, mja = translate_one(llm, brand, model, retries=args.max_retries, sleep_sec=args.sleep)
            cache_put(args.cache_dir, key, bja, mja)

        df.at[idx, args.brand_ja_col] = bja
        df.at[idx, args.model_ja_col] = mja

    # 出力
    df.to_csv(args.output, index=False, encoding="utf-8")

if __name__ == "__main__":
    main()
