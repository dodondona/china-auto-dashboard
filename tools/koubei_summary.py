import sys, os, re, time, json, requests
from bs4 import BeautifulSoup
import pandas as pd
from openai import OpenAI

# -----------------------------
# 引数: 車両ID（例: 5714）
# -----------------------------
if len(sys.argv) < 2:
    print("Usage: python tools/koubei_summary.py <vehicle_id>")
    sys.exit(1)

VEHICLE_ID = sys.argv[1]
BASE_URL = f"https://k.autohome.com.cn/{VEHICLE_ID}/index_{{page}}.html?#listcontainer"
PAGES = 5  # 最大ページ数（必要に応じて変更可）

# -----------------------------
# ページHTML取得
# -----------------------------
def fetch_html(page):
    url = BASE_URL.format(page=page)
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text

# -----------------------------
# レビュー本文抽出
# -----------------------------
def parse_reviews(html):
    soup = BeautifulSoup(html, "html.parser")
    reviews = []
    for block in soup.select(".mouthcon, .mouthcon-cont, .text-con"):
        txt = re.sub(r"\s+", " ", block.get_text(" ", strip=True))
        if len(txt) > 50:
            reviews.append(txt)
    return reviews

# -----------------------------
# OpenAI要約API呼び出し
# -----------------------------
def summarize_reviews(reviews):
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    chunks = [reviews[i:i+8] for i in range(0, len(reviews), 8)]
    all_results = []

    for idx, chunk in enumerate(chunks):
        prompt_text = (
            "以下は中国語の車レビューです。各レビューから『良い点』『悪い点』を日本語で簡潔に抽出し、"
            "JSON形式で出力してください。形式: "
            "[{\"pros\": [...], \"cons\": [...], \"sentiment\": \"positive|mixed|negative\"}, ...]\n\n"
            + "\n\n".join(f"[{i+1}] {t}" for i, t in enumerate(chunk))
        )
        try:
            resp = client.responses.create(
                model="gpt-4o-mini",
                input=[{"role": "user", "content": [{"type": "text", "text": prompt_text}]}],
                response_format={"type": "json_object"},
            )
            data = json.loads(resp.output_text)
            all_results.extend(data if isinstance(data, list) else data.get("results", []))
        except Exception as e:
            print(f"⚠️ batch {idx+1} failed: {e}")
        time.sleep(1.2)

    return all_results

# -----------------------------
# メイン処理
# -----------------------------
def main():
    all_reviews = []
    for p in range(1, PAGES + 1):
        try:
            html = fetch_html(p)
            reviews = parse_reviews(html)
            print(f"✅ Page {p}: {len(reviews)} reviews")
            all_reviews.extend(reviews)
        except Exception as e:
            print(f"❌ Page {p} error: {e}")

    if not all_reviews:
        print("No reviews found.")
        return

    summarized = summarize_reviews(all_reviews)
    rows = [
        {
            "pros": " / ".join(x.get("pros", [])),
            "cons": " / ".join(x.get("cons", [])),
            "sentiment": x.get("sentiment", ""),
        }
        for x in summarized
    ]
    df = pd.DataFrame(rows)
    csv_name = f"autohome_reviews_{VEHICLE_ID}.csv"
    df.to_csv(csv_name, index=False, encoding="utf-8-sig")

    # 集計
    def count_terms(col):
        s = df[col].dropna().astype(str).str.split(" / ").explode().str.strip()
        return s[s != ""].value_counts().head(10)

    top_pros = count_terms("pros")
    top_cons = count_terms("cons")
    senti = df["sentiment"].value_counts()

    txt_name = f"autohome_reviews_{VEHICLE_ID}_summary.txt"
    with open(txt_name, "w", encoding="utf-8") as f:
        f.write(f"【車両ID】{VEHICLE_ID}\n")
        f.write("=== ポジティブTOP ===\n")
        f.write(top_pros.to_string())
        f.write("\n\n=== ネガティブTOP ===\n")
        f.write(top_cons.to_string())
        f.write("\n\n=== センチメント比 ===\n")
        f.write(senti.to_string())
        f.write("\n")

    print(f"\n✅ 出力完了: {csv_name}, {txt_name}")

if __name__ == "__main__":
    main()
