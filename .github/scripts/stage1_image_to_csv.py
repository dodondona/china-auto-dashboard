import os, base64
from openai import OpenAI

MODEL = "gpt-4o"  # ← miniではなく本体を使う
IN_DIR = "captures"
OUT_DIR = "csv"

PROMPT = (
    "这是一张汽车之家（autohome.com.cn）的车型参数配置对比表截图。"
    "请识别表格结构并输出CSV格式。保留所有中文、数字和符号（●/○/—）。"
    "不要输出任何说明文字，只输出纯CSV内容。"
)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    for fname in sorted(os.listdir(IN_DIR)):
        if not fname.lower().endswith(".png"):
            continue
        path = os.path.join(IN_DIR, fname)
        out_csv = os.path.join(OUT_DIR, os.path.splitext(fname)[0] + ".csv")
        print(f"[+] {fname} → {out_csv}")

        # 画像をファイルオブジェクトとして渡す（これが最も確実）
        with open(path, "rb") as f:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": "You are an expert in table extraction."},
                    {"role": "user", "content": [
                        {"type": "text", "text": PROMPT},
                        {"type": "image_url", "image_url": {"url": "attachment://image"}}
                    ]},
                ],
                attachments=[{"name": "image", "data": f.read()}],  # ← ここが重要
                temperature=0,
            )

        csv_text = resp.choices[0].message.content.strip()
        with open(out_csv, "w", encoding="utf-8-sig") as f:
            f.write(csv_text)
        print(f"✅ saved {out_csv}")

if __name__ == "__main__":
    main()
