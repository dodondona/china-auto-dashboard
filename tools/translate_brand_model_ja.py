#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
translate_brand_model_ja.py
- 入力CSV（brand, model, title_raw がある前提）を読み込み
- brand と model を日本語訳し、brand_ja / model_ja を列追加して保存
- まずルールで「英字はそのまま」「記号は保持」、それ以外は LLM 翻訳
- 低コスト化のためのキャッシュあり（tools/.cache_ja.json）

使い方:
  python tools/translate_brand_model_ja.py \
    --input  data/autohome_raw_2025-09_with_brand.csv \
    --output data/autohome_raw_2025-09_with_brand_ja.csv \
    --model  gpt-4o-mini
"""

import os, re, json, time, argparse
from pathlib import Path
import pandas as pd

CACHE_PATH = Path("tools/.cache_ja.json")

# --- ちょいルール -------------------------------------------------------------

def looks_latin_or_mixed(s: str) -> bool:
    """英数字/記号メインなら True（Tesla, Model Y, SU7 などはそのまま）"""
    if not s:
        return False
    # 中/日/韓の文字が無い or ほぼ英数字なら True
    return not re.search(r"[\u4e00-\u9fff\u3040-\u30ff]", s) or re.fullmatch(r"[A-Za-z0-9\-\s_+./]+", s) is not None

# 一部ブランドの簡易固定訳（必要最低限・好みで拡張可）
FIXED_BRAND = {
    "特斯拉": "テスラ",
    "丰田": "トヨタ",
    "本田": "ホンダ",
    "日产": "日産",
    "大众": "フォルクスワーゲン",
    "奥迪": "アウディ",
    "宝马": "BMW",
    "奔驰": "メルセデス・ベンツ",
    "比亚迪": "BYD",
    "吉利": "ジーリー",
    "吉利汽车": "ジーリー",
    "五菱": "ウーリン",
    "上汽大众": "SAIC-VW",
    "广汽丰田": "GACトヨタ",
    "长安": "長安",
    "奇瑞": "奇瑞",
    "小米": "シャオミ",
    "红旗": "紅旗",
    "别克": "ビュイック",
    "别克汽车": "ビュイック",
}

# --- LLM ----------------------------------------------------------------------

def llm_translate_pairs(pairs, model="gpt-4o-mini"):
    """
    pairs: [{"brand": "...", "model": "...", "title": "..."} ...]
    まとめて1回で投げ、JSON配列で返させる（出力を厳密JSONに限定）
    """
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # プロンプト：自動車の文脈で自然な日本語に。英字は維持、数字/記号も保持。
    sys = (
        "あなたは自動車名の翻訳器です。入力は中国語中心のブランド名と車種名の組です。"
        "以下のルールで日本語へ変換してください：\n"
        "1) 英字や数字、型番（例: Model Y, SU7, A6L）はそのまま残す\n"
        "2) ブランドは既知の日本語表記があればそれを使う。なければカタカナ音訳\n"
        "3) 車種は自然な日本語（多くはカタカナ、ただし '星越L' の L など英字は保持）\n"
        "4) 出力は厳密なJSON配列。各要素は {\"brand_ja\":\"…\",\"model_ja\":\"…\"} のみ\n"
        "5) 余計な説明やコメントは禁止"
    )
    user_lines = []
    for i, p in enumerate(pairs, 1):
        user_lines.append(f"{i}. brand={p['brand']} | model={p['model']} | title={p.get('title','')}")
    user = "\n".join(user_lines)

    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        messages=[
            {"role": "system", "content": sys},
            {"role": "user", "content": user},
        ],
    )
    txt = (resp.choices[0].message.content or "").strip()
    m = re.search(r"\[.*\]", txt, re.S)
    if not m:
        return []
    try:
        arr = json.loads(m.group(0))
        return arr if isinstance(arr, list) else []
    except Exception:
        return []

# --- メイン --------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")
    ap.add_argument("--batch-size", type=int, default=10)
    ap.add_argument("--sleep-ms", type=int, default=150)
    args = ap.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY が未設定です。secrets に設定してください。")

    df = pd.read_csv(args.input)

    # キャッシュ
    cache = {}
    if CACHE_PATH.exists():
        try:
            cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            cache = {}
    changed = False

    out_brand_ja, out_model_ja = [], []

    batch = []
    idx_map = []  # バッチ→行番号対応
    for i, row in df.iterrows():
        b = str(row.get("brand", "")).strip()
        m = str(row.get("model", "")).strip()
        t = str(row.get("title_raw", "")).strip()

        key = f"{b}|{m}"
        trans_b, trans_m = None, None

        # 1) 固定訳・英字優先のルール
        if looks_latin_or_mixed(b):
            trans_b = b
        elif b in FIXED_BRAND:
            trans_b = FIXED_BRAND[b]

        if looks_latin_or_mixed(m):
            trans_m = m

        # 2) キャッシュ
        if key in cache:
            cached = cache[key]
            # ルールで決まっていない側だけキャッシュから補う
            if trans_b is None:
                trans_b = cached.get("brand_ja") or trans_b
            if trans_m is None:
                trans_m = cached.get("model_ja") or trans_m

        # 3) まだ欠けている場合は後でLLMへ
        if trans_b is None or trans_m is None:
            batch.append({"brand": b, "model": m, "title": t})
            idx_map.append(i)
            # すぐ埋めるのは後で
            out_brand_ja.append(None)
            out_model_ja.append(None)
        else:
            out_brand_ja.append(trans_b)
            out_model_ja.append(trans_m)

        # バッチ投げ
        if len(batch) >= args.batch_size:
            res = llm_translate_pairs(batch, model=args.model)
            for k, ridx in enumerate(idx_map):
                if k < len(res):
                    tb = res[k].get("brand_ja", "") or ""
                    tm = res[k].get("model_ja", "") or ""
                    out_brand_ja[ridx] = out_brand_ja[ridx] or tb
                    out_model_ja[ridx] = out_model_ja[ridx] or tm
                    cache_key = f"{df.at[ridx,'brand']}|{df.at[ridx,'model']}"
                    cache[cache_key] = {"brand_ja": out_brand_ja[ridx], "model_ja": out_model_ja[ridx]}
                    changed = True
            batch.clear()
            idx_map.clear()
            time.sleep(args.sleep_ms/1000.0)

    # 最終バッチ
    if batch:
        res = llm_translate_pairs(batch, model=args.model)
        for k, ridx in enumerate(idx_map):
            if k < len(res):
                tb = res[k].get("brand_ja", "") or ""
                tm = res[k].get("model_ja", "") or ""
                out_brand_ja[ridx] = out_brand_ja[ridx] or tb
                out_model_ja[ridx] = out_model_ja[ridx] or tm
                cache_key = f"{df.at[ridx,'brand']}|{df.at[ridx,'model']}"
                cache[cache_key] = {"brand_ja": out_brand_ja[ridx], "model_ja": out_model_ja[ridx]}
                changed = True
        batch.clear()
        idx_map.clear()

    # 欠けていれば最後に埋める（安全策）
    for i in range(len(df)):
        if out_brand_ja[i] is None:
            out_brand_ja[i] = str(df.at[i, "brand"])
        if out_model_ja[i] is None:
            out_model_ja[i] = str(df.at[i, "model"])

    df_out = df.copy()
    df_out["brand_ja"] = out_brand_ja
    df_out["model_ja"] = out_model_ja
    df_out.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"✅ 保存: {args.output}")

    if changed:
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"🗂️ キャッシュ更新: {CACHE_PATH}")

if __name__ == "__main__":
    main()
