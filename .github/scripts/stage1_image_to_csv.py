import os, io, base64
from PIL import Image
from openai import OpenAI

# === 設定 ===
MODEL = "gpt-4o-mini"  # または "gpt-4o"
IN_DIR = "captures"
OUT_DIR = "csv"
PROMPT = """
この画像は汽车之家（autohome.com.cn）の参数配置比较表です。
表の行・列構造を保持して、CSV形式に変換してください。
出力はUTF-8のカンマ区切り（,）とし、1行目に列ヘッダを入れてください。
中国語の項目名・数値・記号はそのまま保持し、順序も画像通りにしてください。
"""

# === 処理 ===
os.makedirs(OUT_DIR, exist_ok=True)
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def encode_image(path: str) -> str:
    with Image.open(path) as img:
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()

for file in sorted(os.listdir(IN_DIR)):
    if not file.lower().endswith(".png"): 
        continue
    img_path = os.path.join(IN_DIR, file)
    base = os.path.splitext(file)[0]
    out_csv = os.path.join(OUT_DIR, f"{base}.csv")

    print(f"[+] Converting {file} -> {out_csv}")

    resp = client.responses.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": "You are an expert in table recognition and CSV conversion."},
            {"role": "user", "content": [
                {"type": "input_text", "text": PROMPT},
                {"type": "input_image", "image_url": encode_image(img_path)}
            ]}
        ],
    )
    csv_text = resp.output_text.strip()
    with open(out_csv, "w", encoding="utf-8-sig") as f:
        f.write(csv_text)
    print(f"✅ saved {out_csv}")
