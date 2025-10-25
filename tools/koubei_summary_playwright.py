import sys, os, re, time, json
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from openai import OpenAI

# ---- 引数（車両ID、ページ数） ----
if len(sys.argv) < 2:
    print("Usage: python tools/koubei_summary_playwright.py <vehicle_id> [pages]")
    sys.exit(1)

VEHICLE_ID = sys.argv[1].strip()
PAGES = int(sys.argv[2]) if len(sys.argv) >= 3 else 5

BASE_URL = f"https://k.autohome.com.cn/{VEHICLE_ID}/index_{{page}}.html?#listcontainer"

OUTDIR = os.path.join(os.path.dirname(__file__), "..")  # リポジトリ直下へ出力

# ---- Playwrightでレンダ後のHTMLを取得（遅延読み込み対策付き）----
def fetch_rendered_html(page_index: int) -> str:
    url = BASE_URL.format(page=page_index)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = browser.new_context()
        pg = ctx.new_page()
        pg.set_default_timeout(60000)
        pg.goto(url, wait_until="domcontentloaded")
        # 追加ロードを待機（レビューがJSで差し込まれるため）
        # 代表的な候補セレクタに対して待機。ヒットしなくてもタイムアウトは短めに。
        candidates = [
            ".mouthcon", ".mouthcon-cont", ".text-con",
            ".comment-content", ".koubei-item", ".koubei-content",
            ".review-item", ".review-content",
            '[data-type="koubei"]', '[data-mark*="koubei"]'
        ]
        # スクロールして遅延ロードを誘発
        for _ in range(6):
            pg.mouse.wheel(0, 2000)
            pg.wait_for_timeout(500)
        # どれかが現れるのを軽く待つ（最大3秒）
        try:
            pg.wait_for_selector(",".join(candidates), timeout=3000)
        except:
            pass
        html = pg.content()
        browser.close()
        return html

# ---- レビュー本文抽出（複数セレクタを総当り）----
def parse_reviews(html: str):
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        ".mouthcon", ".mouthcon-cont", ".text-con",
        ".comment-content", ".koubei-item", ".koubei-content",
        ".review-item", ".review-content",
        # 汎用フォールバック：'优点' '缺点' '最满意' 等の見出し近傍
    ]
    reviews = []

    # 1) 既知クラス優先
    for sel in selectors:
        for blk in soup.select(sel):
            txt = " ".join(blk.get_text(" ", strip=True).split())
            if len(txt) >= 50:
                reviews.append(txt)

    # 2) キーワード近傍フォールバック
    if not reviews:
        # キーワードを含む親ブロックを抽出
        keywords = ["优点", "缺点", "最满意", "最不满意", "不足", "槽点", "评价"]
        for kw in keywords:
            for hit in soup.find_all(string=re.compile(kw)):
                blk = hit.find_parent()
                if blk:
                    txt = " ".join(blk.get_text(" ", strip=True).split())
                    if len(txt) >= 50:
                        reviews.append(txt)

    # 3) 重複除去（先頭120文字でキー化）
    uniq, seen = [], set()
    for t in reviews:
        k = t[:120]
        if k not in seen:
            seen.add(k)
            uniq.append(t)
    return uniq

# ---- OpenAI でバッチ要約（JSON構造化）----
def summarize_batch(texts):
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    client = OpenAI(api_key=api_key)

    prompt = {
        "role": "user",
        "content": [
            {
                "type": "text",
                "text": (
                    "以下は同一車種の中国語ユーザー口コミです。"
                    "各レビューから『良い点(Pros)』『悪い点(Cons)』を日本語で箇条書き抽出し、"
                    "JSON配列で返してください。各要素は "
                    "{\"pros\":[...],\"cons\":[...],\"sentiment\":\"positive|mixed|negative\"} "
                    "の形式にしてください。\n\n"
                    + "\n\n---\n".join(f"[{i+1}] {t}" for i,t in enumerate(texts))
                ),
            }
        ],
    }
    resp = client.responses.create(
        model="gpt-4o-mini",
        input=[prompt],
        response_format={"type": "json_object"},
    )
    data = json.loads(resp.output_text)
    return data if isinstance(data, list) else data.get("results", [])

def main():
    all_reviews = []
    for p in range(1, PAGES + 1):
        try:
            html = fetch_rendered_html(p)
            revs = parse_reviews(html)
            print(f"✅ Page {p}: {len(revs)} reviews")
            all_reviews.extend(revs)
        except Exception as e:
            print(f"❌ Page {p} error: {e}")

    if not all_reviews:
        print("No reviews found.")
        return

    # OpenAIへは8件ずつバッチ投入
    rows = []
    chunk = 8
    for i in range(0, len(all_reviews), chunk):
        batch = all_reviews[i:i+chunk]
        try:
            results = summarize_batch(batch)
            for r in results:
                rows.append({
                    "pros": " / ".join(r.get("pros", [])),
                    "cons": " / ".join(r.get("cons", [])),
                    "sentiment": r.get("sentiment", "mixed"),
                })
        except Exception as e:
            print("batch failed:", e)
        time.sleep(1.0)

    df = pd.DataFrame(rows)

    # 出力
    csv_path = os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}.csv")
    txt_path = os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}_summary.txt")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    # 簡易集計
    def head_counts(col):
        s = df[col].dropna().astype(str).str.split(" / ").explode().str.strip()
        s = s[s != ""]
        return s.value_counts().head(15)

    top_pros = head_counts("pros")
    top_cons = head_counts("cons")
    senti = df["sentiment"].value_counts()

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"【車両ID】{VEHICLE_ID}\n")
        f.write("=== ポジティブTOP ===\n")
        f.write(top_pros.to_string()); f.write("\n\n")
        f.write("=== ネガティブTOP ===\n")
        f.write(top_cons.to_string()); f.write("\n\n")
        f.write("=== センチメント比 ===\n")
        f.write(senti.to_string()); f.write("\n")

    print(f"\n✅ 出力完了: {csv_path}, {txt_path}")

if __name__ == "__main__":
    main()
