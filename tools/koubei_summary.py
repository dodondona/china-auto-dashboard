#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, os, re, time, json, hashlib, statistics
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from openai import OpenAI

# ========= 引数 =========
if len(sys.argv) < 2:
    print("Usage: python tools/koubei_summary.py <vehicle_id> [pages] [mode: ja|zh]")
    sys.exit(1)

VEHICLE_ID = sys.argv[1].strip()
PAGES = int(sys.argv[2]) if len(sys.argv) >= 3 and sys.argv[2].strip().isdigit() else 5
MODE = (sys.argv[3].strip().lower() if len(sys.argv) >= 4 else "ja")
if MODE not in ("ja", "zh"):
    MODE = "ja"

BASE_URL = f"https://k.autohome.com.cn/{VEHICLE_ID}/index_{{page}}.html?#listcontainer"
OUTDIR = os.path.join(os.path.dirname(__file__), "..")
CSV_PATH = os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}.csv")
TXT_PATH = os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}_summary.txt")
TIMING_JSON = os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}_timing.json")
TIMING_TXT  = os.path.join(OUTDIR, f"autohome_reviews_{VEHICLE_ID}_timing.txt")

# ========= メトリクス器 =========
T = {
    "fetch_pages": [],   # per page seconds
    "parse_pages": [],   # per page seconds
    "llm_batches": [],   # per batch seconds
    "trans_batches": [], # translation batch seconds (ja only)
    "io": []             # save/load seconds
}

def tick(fn, bucket):
    t0 = time.time()
    res = fn()
    T[bucket].append(time.time() - t0)
    return res

def print_summary_and_save():
    def s(lst):
        if not lst: return "0.00s (0)"
        return f"{sum(lst):.2f}s total  | avg {statistics.mean(lst):.2f}s × {len(lst)}"
    lines = []
    lines.append(f"[Timing] Vehicle {VEHICLE_ID} mode={MODE} pages={PAGES}")
    lines.append(f"  fetch_pages   : {s(T['fetch_pages'])}")
    lines.append(f"  parse_pages   : {s(T['parse_pages'])}")
    lines.append(f"  llm_batches   : {s(T['llm_batches'])}")
    lines.append(f"  trans_batches : {s(T['trans_batches'])}")
    lines.append(f"  io            : {s(T['io'])}")
    txt = "\n".join(lines)
    print("\n" + txt + "\n")
    with open(TIMING_TXT, "w", encoding="utf-8") as f:
        f.write(txt + "\n")
    with open(TIMING_JSON, "w", encoding="utf-8") as f:
        json.dump(T, f, ensure_ascii=False, indent=2)

# ========= 共通ユーティリティ =========
def text_hash(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:12]

def looks_japanese(s: str) -> bool:
    return bool(s and re.search(r"[ぁ-ゟ゠-ヿ]", s))

def normalize_list(x):
    if not x:
        return []
    if isinstance(x, list):
        return [str(i).strip() for i in x if str(i).strip()]
    return [str(x).strip()]

def extract_json_loose(s: str):
    if not s:
        return None
    s = re.sub(r"```json\s*|\s*```", "", s, flags=re.I).strip()
    m = re.search(r"(\[.*\]|\{.*\})", s, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None

def get_client():
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    return OpenAI(api_key=api_key)

# ========= Playwright =========
def _fetch_rendered_html(page_index: int) -> str:
    url = BASE_URL.format(page=page_index)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = browser.new_context(
            viewport={"width": 1200, "height": 1600},
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
        )
        def _route(route):
            t = route.request.resource_type
            if t in ("image", "media", "font", "stylesheet", "other"):
                return route.abort()
            return route.continue_()
        ctx.route("**/*", _route)
        pg = ctx.new_page()
        pg.set_default_timeout(30000)
        pg.goto(url, wait_until="networkidle")
        for _ in range(2):
            pg.mouse.wheel(0, 2000)
            pg.wait_for_timeout(250)
        html = pg.content()
        browser.close()
        return html

def fetch_rendered_html(page_index: int) -> str:
    return tick(lambda: _fetch_rendered_html(page_index), "fetch_pages")

# ========= 解析 =========
def _parse_reviews(html: str):
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        "#listcontainer .mouthcon",
        "#listcontainer .mouthcon-cont",
        "#listcontainer .text-con",
        "#listcontainer .comment-content",
        "#listcontainer .koubei-item",
        "#listcontainer .koubei-content",
        "#listcontainer .review-item",
        "#listcontainer .review-content",
        ".mouthcon", ".mouthcon-cont", ".text-con",
        ".comment-content", ".koubei-item", ".koubei-content",
        ".review-item", ".review-content",
    ]
    reviews, seen = [], set()
    for sel in selectors:
        for blk in soup.select(sel):
            txt = " ".join(blk.get_text(" ", strip=True).split())
            if len(txt) >= 50:
                h = text_hash(txt[:300])
                if h not in seen:
                    seen.add(h)
                    reviews.append(txt)
    if not reviews:
        keywords = ["优点", "缺点", "最满意", "最不满意", "不足", "槽点", "评价", "口碑"]
        for kw in keywords:
            for hit in soup.find_all(string=re.compile(kw)):
                blk = hit.find_parent()
                if blk:
                    txt = " ".join(blk.get_text(" ", strip=True).split())
                    if len(txt) >= 50:
                        h = text_hash(txt[:300])
                        if h not in seen:
                            seen.add(h)
                            reviews.append(txt)
    return reviews

def parse_reviews(html: str):
    return tick(lambda: _parse_reviews(html), "parse_pages")

# ========= LLM =========
def summarize_batch_ja(texts, client: OpenAI):
    sys_prompt = (
        "あなたはレビューテキストのアナリストです。入力は中国語の車ユーザー口コミです。"
        "各レビューから『良い点(Pros)』『悪い点(Cons)』を**日本語**で短く抽出し、"
        "overall感情を positive/mixed/negative のいずれかで判断してください。"
        "出力は**必ず JSON 配列**（各要素: {\"pros\":[..],\"cons\":[..],\"sentiment\":\"...\"}）。"
        "前置き・後書き・説明文は一切出力しない。値は短い日本語フレーズにする。"
    )
    user_text = "\n\n".join(f"[{i+1}] {t}" for i, t in enumerate(texts))

    def _call():
        comp = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[{"role":"system","content":sys_prompt},{"role":"user","content":user_text}],
            temperature=0.0,
        )
        data = extract_json_loose(comp.choices[0].message.content)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            return data["results"]
        return []
    return tick(_call, "llm_batches")

def summarize_batch_zh(texts, client: OpenAI):
    sys_prompt = (
        "你是汽车用户口碑的分析助手。请从每条中文评论中，提取“优点(Pros)”“缺点(Cons)”并给出情感"
        "（positive/mixed/negative）。输出**JSON数组**，每个元素形如："
        "{\"pros\":[...],\"cons\":[...],\"sentiment\":\"...\"}。不要任何解释。"
    )
    user_text = "\n\n".join(f"[{i+1}] {t}" for i, t in enumerate(texts))
    def _call():
        comp = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[{"role":"system","content":sys_prompt},{"role":"user","content":user_text}],
            temperature=0.0,
        )
        data = extract_json_loose(comp.choices[0].message.content)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            return data["results"]
        return []
    return tick(_call, "llm_batches")

def translate_to_ja_unique(phrases, client: OpenAI):
    need = [p for p in phrases if p and not looks_japanese(p)]
    if not need:
        return {p: p for p in phrases}
    mapping = {}
    sys_prompt = (
        "あなたはプロの翻訳者です。与えられた短いフレーズ群を**自然な日本語**に翻訳し、"
        "JSONの {原文: 日本語訳} の辞書で返してください。説明は不要。"
    )
    batch_size = 200
    for i in range(0, len(need), batch_size):
        chunk = need[i:i+batch_size]
        user_text = "\n".join(f"- {t}" for t in chunk)
        def _call():
            comp = client.chat.completions.create(
                model="gpt-4o-mini",
                response_format={"type": "json_object"},
                messages=[{"role":"system","content":sys_prompt},{"role":"user","content":user_text}],
                temperature=0.0,
            )
            data = extract_json_loose(comp.choices[0].message.content) or {}
            if isinstance(data, dict):
                for k, v in data.items():
                    mapping[str(k).strip()] = str(v).strip()
        tick(_call, "trans_batches")
        time.sleep(0.2)
    for p in phrases:
        mapping.setdefault(p, p)
    return mapping

# ========= フォールバック =========
def heuristic_extract(review_text: str):
    pros_keys = ["最满意", "优点", "优點"]
    cons_keys = ["最不满意", "缺点", "缺點", "不足", "槽点"]
    pros = []
    cons = []
    for k in pros_keys:
        m = re.search(k + r"[:： ]?(.*?)(?=(最不满意|缺点|不足|槽点|$))", review_text)
        if m:
            pros.append(m.group(1).strip()); break
    for k in cons_keys:
        m = re.search(k + r"[:： ]?(.*?)(?=$)", review_text)
        if m:
            cons.append(m.group(1).strip()); break
    return {"pros": normalize_list(pros), "cons": normalize_list(cons), "sentiment": "mixed"}

# ========= メイン =========
def main():
    print(f"[Start] Vehicle={VEHICLE_ID} mode={MODE} pages={PAGES}")
    all_reviews = []
    for p in range(1, PAGES + 1):
        try:
            html = fetch_rendered_html(p)
            revs = parse_reviews(html)
            print(f"  page {p:>2}: fetched {len(revs)} reviews")
            all_reviews.extend(revs)
        except Exception as e:
            print(f"  page {p:>2}: ERROR {e}")

    if not all_reviews:
        print("No reviews found.")
        cols = ["pros_ja","cons_ja","sentiment"] if MODE=="ja" else ["pros_zh","cons_zh","sentiment"]
        def _save_empty():
            pd.DataFrame(columns=cols).to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
            with open(TXT_PATH, "w", encoding="utf-8") as f:
                f.write(f"【車両ID】{VEHICLE_ID}\nレビューが取得できませんでした。\n")
        tick(_save_empty, "io")
        print_summary_and_save()
        return

    client = get_client()

    rows = []
    chunk = 16
    for i in range(0, len(all_reviews), chunk):
        batch = all_reviews[i:i+chunk]
        results = summarize_batch_ja(batch, client) if MODE=="ja" else summarize_batch_zh(batch, client)
        if not results:
            # LLM空返し → 簡易抽出
            for t in batch:
                h = heuristic_extract(t)
                if MODE=="ja":
                    rows.append({"pros_raw":" / ".join(h["pros"]), "cons_raw":" / ".join(h["cons"]), "sentiment":h["sentiment"]})
                else:
                    rows.append({"pros_zh":" / ".join(h["pros"]), "cons_zh":" / ".join(h["cons"]), "sentiment":h["sentiment"]})
            continue
        for r in results:
            pros = " / ".join(normalize_list(r.get("pros", [])))
            cons = " / ".join(normalize_list(r.get("cons", [])))
            if MODE=="ja":
                rows.append({"pros_raw":pros, "cons_raw":cons, "sentiment":r.get("sentiment","mixed")})
            else:
                rows.append({"pros_zh":pros, "cons_zh":cons, "sentiment":r.get("sentiment","mixed")})
        time.sleep(0.2)

    if MODE=="ja":
        df = pd.DataFrame(rows, columns=["pros_raw","cons_raw","sentiment"]).fillna("")
        pros_terms = set(t.strip() for t in df["pros_raw"].str.split(" / ").explode().dropna() if t.strip())
        cons_terms = set(t.strip() for t in df["cons_raw"].str.split(" / ").explode().dropna() if t.strip())
        pros_map = translate_to_ja_unique(pros_terms, client)
        cons_map = translate_to_ja_unique(cons_terms, client)

        def _join_map(series, mapping):
            out = []
            for cell in series.fillna(""):
                terms = [t.strip() for t in cell.split(" / ") if t.strip()]
                out.append(" / ".join(mapping.get(t, t) for t in terms))
            return out

        df["pros_ja"] = _join_map(df["pros_raw"], pros_map)
        df["cons_ja"] = _join_map(df["cons_raw"], cons_map)

        def _save_ja():
            df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
            # サマリーTXT
            def head_counts(col):
                s = df[col].dropna().astype(str).str.split(" / ").explode().str.strip()
                s = s[s != ""]
                return s.value_counts().head(15)
            top_pros = head_counts("pros_ja") if not df.empty else pd.Series(dtype=int)
            top_cons = head_counts("cons_ja") if not df.empty else pd.Series(dtype=int)
            senti = df["sentiment"].value_counts() if "sentiment" in df.columns else pd.Series(dtype=int)
            with open(TXT_PATH, "w", encoding="utf-8") as f:
                f.write(f"【車両ID】{VEHICLE_ID}\n")
                f.write("=== ポジティブTOP（日本語） ===\n")
                f.write(top_pros.to_string() if not top_pros.empty else "(なし)")
                f.write("\n\n=== ネガティブTOP（日本語） ===\n")
                f.write(top_cons.to_string() if not top_cons.empty else "(なし)")
                f.write("\n\n=== センチメント比 ===\n")
                f.write(senti.to_string() if not senti.empty else "(なし)")
                f.write("\n")
        tick(_save_ja, "io")

    else:
        df = pd.DataFrame(rows, columns=["pros_zh","cons_zh","sentiment"]).fillna("")
        def _save_zh():
            df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
            def head_counts(col):
                s = df[col].dropna().astype(str).str.split(" / ").explode().str.strip()
                s = s[s != ""]
                return s.value_counts().head(15)
            top_pros = head_counts("pros_zh") if not df.empty else pd.Series(dtype=int)
            top_cons = head_counts("cons_zh") if not df.empty else pd.Series(dtype=int)
            senti = df["sentiment"].value_counts() if "sentiment" in df.columns else pd.Series(dtype=int)
            with open(TXT_PATH, "w", encoding="utf-8") as f:
                f.write(f"【车系ID】{VEHICLE_ID}\n")
                f.write("=== 优点TOP ===\n")
                f.write(top_pros.to_string() if not top_pros.empty else "(无)")
                f.write("\n\n=== 缺点TOP ===\n")
                f.write(top_cons.to_string() if not top_cons.empty else "(无)")
                f.write("\n\n=== 情感比例 ===\n")
                f.write(senti.to_string() if not senti.empty else "(无)")
                f.write("\n")
        tick(_save_zh, "io")

    print_summary_and_save()
    print(f"[Done] CSV={CSV_PATH}  TXT={TXT_PATH}  TIMING=({TIMING_TXT}, {TIMING_JSON})")

if __name__ == "__main__":
    main()
