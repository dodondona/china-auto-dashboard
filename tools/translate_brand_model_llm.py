import os
import sys
import time
import json
import argparse
import pandas as pd
import google.generativeai as genai

# --- Gemini API セットアップ ---
api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    raise ValueError("APIキーが設定されていません。環境変数 'GEMINI_API_KEY' を確認してください。")

genai.configure(api_key=api_key)
# モデルを最新・高速・低コストの 'gemini-1.5-flash' にアップグレード
model = genai.GenerativeModel('gemini-1.5-flash-latest')

def load_cache(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_cache(path, cache):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def translate_with_gemini(text, cache):
    if not text or pd.isna(text):
        return ""
    if text in cache:
        return cache[text]
    
    try:
        prompt = f'Translate the following Chinese car brand or model name to its global English name. Return only the single most common English name and nothing else. Chinese name: "{text}"'
        response = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(temperature=0.0)
        )
        translated_text = response.text.strip().replace('"', '').replace("'", "")
        
        print(f"Gemini 変換: {text} -> {translated_text}", flush=True)
        cache[text] = translated_text
        time.sleep(1) # APIレート制限のための待機
        return translated_text
        
    except Exception as e:
        print(f"Gemini APIエラー ({text}): {e}", flush=True)
        return "" # エラー時は空文字を返す

def main():
    parser = argparse.ArgumentParser(description="Translate brand/model names using Gemini.")
    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--output", required=True, help="Output CSV file path")
    parser.add_argument("--brand-col", required=True, help="Column name for brand")
    parser.add_argument("--model-col", required=True, help="Column name for model")
    parser.add_argument("--brand-ja-col", required=True, help="Column name for translated brand")
    parser.add_argument("--model-ja-col", required=True, help="Column name for translated model")
    parser.add_argument("--cache", help="Cache file path for translations")
    
    # ymlから渡される未知の引数(modelなど)を無視して、定義済みの引数だけを解析する
    args, _ = parser.parse_known_args()

    df = pd.read_csv(args.input)
    cache = load_cache(args.cache) if args.cache else {}

    df[args.brand_ja_col] = df[args.brand_col].apply(lambda x: translate_with_gemini(x, cache))
    df[args.model_ja_col] = df[args.model_col].apply(lambda x: translate_with_gemini(x, cache))

    if args.cache:
        save_cache(args.cache, cache)

    df.to_csv(args.output, index=False)
    print(f"翻訳完了。出力ファイル: {args.output}")

if __name__ == "__main__":
    main()
