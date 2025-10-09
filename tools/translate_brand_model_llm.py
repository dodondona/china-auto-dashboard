import os
import sys
import time
import google.generativeai as genai
import pandas as pd

# --- ここから変更 ---

# GitHub ActionsのSecretsからAPIキーを設定
api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    raise ValueError("APIキーが設定されていません。環境変数 'GEMINI_API_KEY' を確認してください。")

genai.configure(api_key=api_key)
model = genai.GenerativeModel('gemini-pro')

def get_eng_name_gemini(cn_name: str) -> str:
    """
    Gemini APIを使用して中国語の名前を英語名に変換する。
    """
    if not cn_name or pd.isna(cn_name):
        return ""
    
    try:
        # よりシンプルで安定した結果を得るためのプロンプト
        prompt = f'Translate the following Chinese car brand or model name to its global English name. Return only the single most common English name and nothing else. Chinese name: "{cn_name}"'
        
        response = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                temperature=0.0 # 創造性を抑え、最も可能性の高い単語を返させる
            )
        )
        
        eng_name = response.text.strip().replace('"', '').replace("'", "")
        print(f"Gemini 変換: {cn_name} -> {eng_name}", flush=True)
        return eng_name
        
    except Exception as e:
        print(f"Gemini APIエラー ({cn_name}): {e}", flush=True)
        # エラー時は元の名前を返すか、空文字を返すか選択できます。
        # ここでは空文字を返して後続処理で対応できるようにします。
        return ""

# --- ここまで変更 ---

def translate_and_enrich_csv(input_path, output_path):
    df = pd.read_csv(input_path)

    if 'brand_en' not in df.columns:
        df['brand_en'] = ''
    if 'model_en' not in df.columns:
        df['model_en'] = ''

    for index, row in df.iterrows():
        # 既に翻訳済みの場合はスキップ
        if pd.isna(row['brand_en']) or row['brand_en'] == '':
            df.loc[index, 'brand_en'] = get_eng_name_gemini(row['brand'])
            time.sleep(1)  # APIレート制限のための待機
        
        if pd.isna(row['model_en']) or row['model_en'] == '':
            df.loc[index, 'model_en'] = get_eng_name_gemini(row['model'])
            time.sleep(1)

    df.to_csv(output_path, index=False)
    print(f"翻訳・拡充済みのファイルを保存しました: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("使い方: python translate_brand_model_llm.py <入力CSVパス> <出力CSVパス>")
        sys.exit(1)
    
    input_csv_path = sys.argv[1]
    output_csv_path = sys.argv[2]
    
    translate_and_enrich_csv(input_csv_path, output_csv_path)
