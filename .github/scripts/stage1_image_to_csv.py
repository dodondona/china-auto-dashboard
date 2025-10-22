import os, base64

# pip install openai==1.* でOK
from openai import OpenAI

MODEL  = "gpt-4o-mini"       # 必要なら gpt-4o に変更可
IN_DIR = "captures"
OUT_DIR = "csv"

PROMPT = (
    "この画像は汽车之家（autohome.com.cn）の参数配置比较表です。"
    "表の行・列構造を保持してCSVにしてください。"
    "出力はUTF-8のカンマ区切り（,）、1行目は列ヘッダ。"
    "中国語の項目名・数値・記号（●/○/—）はそのまま保持し、順序も画像通りに。"
)

def data_uri_from_path(path: str) -> str:
    # PILは使わず、PNGバイトをそのままbase64化してデータURIに
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    # ここではPNG前提。必要なら拡張子で切り替え可
    return f"data:image/png;base64,{b64}"

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    files = sorted([f for f in os.listdir(IN_DIR) if f.lower().endswith(".png")])
    if not files:
        print("No PNG files in captures/")
        return

    for fname in files:
        in_path  = os.path.join(IN_DIR, fname)
        base     = os.path.splitext(fname)[0]
        out_csv  = os.path.join(OUT_DIR, f"{base}.csv")
        print(f"[+] Converting {fname} -> {out_csv}")

        image_data_uri = data_uri_from_path(in_path)

        # ✅ chat.completions API を使用（messages引数OK）
        resp = client.chat.completions.create(
            model=MODEL,
            temperature=0,
            messages=[
                {"role": "system", "content": "You convert tables in screenshots into clean CSV text."},
                {"role": "user", "content": [
                    {"type": "text", "text": PROMPT},
                    {"type": "image_url", "image_url": {"url": image_data_uri}}
                ]}
            ],
        )

        csv_text = resp.choices[0].message.content.strip()

        with open(out_csv, "w", encoding="utf-8-sig") as f:
            f.write(csv_text)

        print(f"✅ saved {out_csv}")

if __name__ == "__main__":
    main()
