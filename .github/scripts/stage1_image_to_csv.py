import os, base64
from openai import OpenAI

MODEL = "gpt-4o"  # miniではなく本体推奨
IN_DIR = "captures"
OUT_DIR = "csv"

PROMPT = (
    "这是一张汽车之家（autohome.com.cn）的车型参数配置对比表截图。"
    "请识别表格结构并输出CSV格式，保留中文字段、数字和符号（●/○/—）。"
    "输出为UTF-8编码的逗号分隔文本，第一行为表头。"
    "不要输出任何解释文字或说明。"
)

def data_uri_from_path(path):
    with open(path, "rb") as f:
        return "data:image/png;base64," + base64.b64encode(f.read()).decode()

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    for fname in sorted(os.listdir(IN_DIR)):
        if not fname.lower().endswith(".png"):
            continue
        path = os.path.join(IN_DIR, fname)
        out_csv = os.path.join(OUT_DIR, os.path.splitext(fname)[0] + ".csv")
        print(f"[+] {fname} → {out_csv}")

        img_b64 = data_uri_from_path(path)

        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You are a precise table recognizer."},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": PROMPT},
                        {"type": "image_url", "image_url": {"url": img_b64}},
                    ],
                },
            ],
            temperature=0,
        )

        csv_text = resp.choices[0].message.content.strip()
        with open(out_csv, "w", encoding="utf-8-sig") as f:
            f.write(csv_text)
        print(f"✅ Saved {out_csv}")

if __name__ == "__main__":
    main()
