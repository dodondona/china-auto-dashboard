import os, base64
from openai import OpenAI

MODEL = "gpt-4o"          # mini が失敗する場合はこちらを推奨
IN_DIR = "captures"
OUT_DIR = "csv"

PROMPT = (
    "这是一张汽车之家（autohome.com.cn）的车型参数配置对比表截图。"
    "请识别表格结构并输出CSV格式，保留中文字段、数值和符号（●/○/—）。"
    "输出为UTF-8编码的逗号分隔文本，第一行为表头。不要输出任何解释文字。"
)

def data_uri_from_path(path: str) -> str:
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    return f"data:image/png;base64,{b64}"

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    files = [f for f in sorted(os.listdir(IN_DIR)) if f.lower().endswith(".png")]
    if not files:
        print("No PNG files in captures/")
        return

    for fname in files:
        in_path = os.path.join(IN_DIR, fname)
        base = os.path.splitext(fname)[0]
        out_csv = os.path.join(OUT_DIR, f"{base}.csv")
        print(f"[+] Converting {fname} -> {out_csv}")

        img_b64 = data_uri_from_path(in_path)

        # ✅ 正しい chat.completions API フォーマット
        resp = client.chat.completions.create(
            model=MODEL,
            temperature=0,
            messages=[
                {"role": "system", "content": "You are a highly accurate table recognizer."},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": PROMPT},
                        {"type": "image_url", "image_url": {"url": img_b64}},
                    ],
                },
            ],
        )

        csv_text = resp.choices[0].message.content.strip()
        with open(out_csv, "w", encoding="utf-8-sig") as f:
            f.write(csv_text)
        print(f"✅ Saved {out_csv}")

if __name__ == "__main__":
    main()
