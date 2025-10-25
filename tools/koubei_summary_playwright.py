#!/usr/bin/env python3
import sys, os, re, time, json
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from openai import OpenAI

"""
使い方:
  export OPENAI_API_KEY=sk-xxxx
  python tools/koubei_summary_playwright.py 7806 5
出力:
  リポジトリ直下に autohome_reviews_<ID>.csv と _summary.txt
"""

# -------- 引数 --------
if len(sys.argv) < 2:
    print("Usage: python tools/koubei_summary_playwright.py <vehicle_id> [pages]")
    sys.exit(1)

VEHICLE_ID = sys.argv[1].strip()
PAGES = int(sys.argv[2]) if len(sys.argv) >= 3 and sys.argv[2].strip().isdigit() else 5

BASE_URL = f"https://k.autohome.com.cn/{VEHICLE_ID}/index_{{page}}.html?#listcontainer"
OUTDIR = os.path.join(os.path.dirname(__file__), "..")  # リポジトリ直下
CSV_PATH = os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}.csv")
TXT_PATH = os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}_summary.txt")

# -------- Playwright: レンダ後HTML取得 --------
def fetch_rendered_html(page_index: int) -> str:
    url = BASE_URL.format(page=page_index)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = browser.new_context()
        pg = ctx.new_page()
        pg.set_default_timeout(60000)
        pg.goto(url, wait_until="domcontentloaded")

        # 遅延読み込み対策: 下へ数回スクロール
        for _ in range(8):
            pg.mouse.wheel(0, 2200)
            pg.wait_for_timeout(400)

        # 代表セレクタ登場を軽く待つ（失敗しても続行）
        selectors = [
            ".mouthcon", ".mouthcon-cont", ".text-con",
            ".comment-content", ".koubei-item", ".koubei-content",
            ".review-item", ".review-content",
            '[data-type="koubei"]', '[data-mark*="koubei"]'
        ]
        try:
            pg.wait_for_selector(",".join(selectors), timeout=3000)
        except Exception:
            pass

        html = pg.content()
        browser.close()
        return html

# -------- レビュー抽出（総当り + 近傍） --------
def parse_reviews(html: str):
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        ".mouthcon", ".mouthcon-cont", ".text-con",
        ".comment-content", ".koubei-item", ".koubei-content",
        ".review-item", ".review-content"
    ]
    reviews = []

    # 1) 既知クラス候補を総当り
    for sel in selectors:
        for blk in soup.select(sel):
            txt = " ".join(blk.get_text(" ", strip=True).split())
            if len(txt) >= 50:
                reviews.append(txt)

    # 2) フォールバック: キーワード近傍（优点/缺点/最满意/最不满意 など）
    if not reviews:
        keywords = ["优点", "缺点", "最满意", "最不满意", "不足", "槽点", "评价", "口碑"]
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

# -------- OpenAI: JSON構造で要約 --------
def _extract_json_loose(s: str):
    """コードフェンスや前後テキストが混じってもJSONを抽出する"""
    if not s:
        return None
    # ```json ... ``` を除去
    s = re.sub(r"```json\s*|\s*```", "", s, flags=re.I)
    s = s.strip()
    # 先頭/末尾にゴミがある場合、最初の [ または { から最後の対応括弧まで抜く
    m = re.search(r"(\[.*\]|\{.*\})", s, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None

def summarize_batch(texts):
    """
    Chat Completions(JSONモード)でJSON配列のみを返させる。
    失敗時は緩い抽出を試み、なお空なら [] を返す。
    """
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    client = OpenAI(api_key=api_key)

    sys_prompt = (
        "あなたはレビューテキストのアナリストです。入力は中国語の車ユーザー口コミです。"
        "各レビューから『良い点(Pros)』『悪い点(Cons)』を日本語で短く抽出し、"
        "overall感情を positive/mixed/negative のいずれかで判断してください。"
        "出力は必ず JSON 配列（各要素: {\"pros\":[..],\"cons\":[..],\"sentiment\":\"...\"}）のみ。"
        "前置き・後書き・説明文は一切出力しないこと。"
    )
    user_text = "\n\n".join(f"[{i+1}] {t}" for i, t in enumerate(texts))

    content = None
    try:
        comp = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_text},
            ],
            temperature=0.2,
        )
        content = comp.choices[0].message.content
        data = _extract_json_loose(content)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "results" in data and isinstance(data["results"], list):
            return data["results"]
    except Exception as e:
        # JSONモード非対応/一時障害時フォールバック
        try:
            comp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": user_text},
                ],
                temperature=0.2,
            )
            content = comp.choices[0].message.content
            data = _extract_json_loose(content)
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and "results" in data and isinstance(data["results"], list):
                return data["results"]
        except Exception as e2:
            print("OpenAI error:", e2)

    # ここまで来たら空→デバッグ出力（先頭200文字だけ）
    if content:
        snip = content.replace("\n", " ")[:200]
        print("LLM raw (head):", snip)
    return []

# -------- 簡易ヒューリスティック（最終フォールバック） --------
def heuristic_extract(review_text: str):
    """
    LLMが空返しの場合に最低限の情報を埋める。
    '最满意/优点' と '最不满意/缺点' の前後を粗く抽出。
    """
    pros_keys = ["最满意", "优点", "优點"]
    cons_keys = ["最不满意", "缺点", "缺點", "不足", "槽点"]
    pros = []
    cons = []

    # pros
    for k in pros_keys:
        m = re.search(k + r"[:： ]?(.*?)(?=(最不满意|缺点|不足|槽点|$))", review_text)
        if m:
            pros.append(m.group(1).strip())
            break
    # cons
    for k in cons_keys:
        m = re.search(k + r"[:： ]?(.*?)(?=$)", review_text)
        if m:
            cons.append(m.group(1).strip())
            break

    return {
        "pros": [p for p in [re.sub(r"\s+", " ", x) for x in pros] if p],
        "cons": [c for c in [re.sub(r"\s+", " ", x) for x in cons] if c],
        "sentiment": "mixed"
    }

# -------- メイン --------
def main():
    print(f"Vehicle: {VEHICLE_ID}  Pages: {PAGES}")
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
        pd.DataFrame(columns=["pros","cons","sentiment"]).to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
        with open(TXT_PATH, "w", encoding="utf-8") as f:
            f.write(f"【車両ID】{VEHICLE_ID}\nレビューが取得できませんでした。\n")
        print(f"✅ 出力完了（空）: {CSV_PATH}, {TXT_PATH}")
        return

    # OpenAIへは8件ずつバッチ投入
    rows = []
    chunk = 8
    for i in range(0, len(all_reviews), chunk):
        batch = all_reviews[i:i+chunk]
        results = summarize_batch(batch)

        # LLMが空返しなら、ヒューリスティックで埋める
        if not results:
            print("batch returned empty; use heuristic")
            for t in batch:
                r = heuristic_extract(t)
                rows.append({
                    "pros": " / ".join(r.get("pros", [])),
                    "cons": " / ".join(r.get("cons", [])),
                    "sentiment": r.get("sentiment", "mixed"),
                })
            continue

        for r in results:
            rows.append({
                "pros": " / ".join(r.get("pros", [])) if isinstance(r.get("pros", []), list) else str(r.get("pros", "")),
                "cons": " / ".join(r.get("cons", [])) if isinstance(r.get("cons", []), list) else str(r.get("cons", "")),
                "sentiment": r.get("sentiment", "mixed"),
            })
        time.sleep(0.8)

    # DataFrame 化（空でもカラムは用意）
    df = pd.DataFrame(rows, columns=["pros","cons","sentiment"])
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    # 集計（防御的：カラム欠損時も落ちない）
    def head_counts(col):
        if col not in df.columns or df.empty:
            return pd.Series(dtype=int)
        s = df[col].dropna().astype(str).str.split(" / ").explode().str.strip()
        s = s[s != ""]
        return s.value_counts().head(15)

    top_pros = head_counts("pros")
    top_cons = head_counts("cons")
    senti = df["sentiment"].value_counts() if "sentiment" in df.columns else pd.Series(dtype=int)

    with open(TXT_PATH, "w", encoding="utf-8") as f:
        f.write(f"【車両ID】{VEHICLE_ID}\n")
        f.write("=== ポジティブTOP ===\n")
        f.write(top_pros.to_string() if not top_pros.empty else "(なし)")
        f.write("\n\n=== ネガティブTOP ===\n")
        f.write(top_cons.to_string() if not top_cons.empty else "(なし)")
        f.write("\n\n=== センチメント比 ===\n")
        f.write(senti.to_string() if not senti.empty else "(なし)")
        f.write("\n")

    print(f"\n✅ 出力完了: {CSV_PATH}, {TXT_PATH}")

if __name__ == "__main__":
    main()
