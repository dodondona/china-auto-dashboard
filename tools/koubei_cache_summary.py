# tools/koubei_cache_summary.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, re, json, time, hashlib, sys
from pathlib import Path
from typing import Dict, Any, Iterable, List, Tuple, Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ===== 設定 =====
SERIES_ID = os.environ.get("SERIES_ID", "").strip() or (sys.argv[1] if len(sys.argv) > 1 else "")
OUTDIR = Path(os.environ.get("OUTDIR", f"output/koubei/{SERIES_ID}"))
CACHEDIR = Path(os.environ.get("CACHEDIR", f"cache/koubei/{SERIES_ID}"))
OUTDIR.mkdir(parents=True, exist_ok=True)
CACHEDIR.mkdir(parents=True, exist_ok=True)

# フォーマットの意図的変更時だけ上げる（=キャッシュ無効化のスイッチ）
SUMMARY_FMT_VER = os.environ.get("SUMMARY_FMT_VER", "v1")

# LLMの最終ポリッシュを使うか（0/1）
USE_LLM = (os.environ.get("SKIP_LLM", "0") != "1")

# LLMモデル（短文で十分）
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-nano")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# 既知IDを何件連続で見つけたらクロール終了にするか（打ち切りの閾値）
STOP_AFTER_CONSEC_KNOWN = int(os.environ.get("STOP_AFTER_CONSEC_KNOWN", "50"))

# ====== ユーティリティ ======
def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists(): return []
    return [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]

def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def upsert_jsonl_by_key(path: Path, obj: Dict[str, Any], key: str) -> None:
    rows = load_jsonl(path)
    idx = next((i for i,r in enumerate(rows) if r.get(key) == obj.get(key)), None)
    if idx is None:
        rows.append(obj)
    else:
        rows[idx] = obj
    path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + ("\n" if rows else ""), encoding="utf-8")

# ====== HTML 取得 & パース ======
K_PAGE = "https://k.autohome.com.cn/{series}/index_{p}.html?#listcontainer"
K_FIRST = "https://k.autohome.com.cn/{series}#pvareaid=3454440"

def fetch_html_series_page(series_id: str, page: int) -> str:
    url = K_FIRST.format(series=series_id) if page == 1 else K_PAGE.format(series=series_id, p=page)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        pageobj = ctx.new_page()
        pageobj.goto(url, wait_until="domcontentloaded", timeout=45000)
        # 簡易ロード待ち
        pageobj.wait_for_timeout(1500)
        html = pageobj.content()
        ctx.close(); browser.close()
        return html

def parse_reviews(html: str) -> List[Dict[str, Any]]:
    """
    Autohome KoubeiのDOMは時期で変わるので、汎用的に拾う：
    - レビュー詳細リンク（/detail/.. または view_ 数字）から review_id を抽出
    - タイトル、本文断片、投稿日、スコアなど、取れる範囲で
    """
    soup = BeautifulSoup(html, "lxml")
    items: List[Dict[str, Any]] = []
    for a in soup.select("a[href]"):
        href = a.get("href","")
        # 例: .../detail/view_1234567_... または .../detail/1234567/...
        m = re.search(r"(?:view_)?(\d{6,12})", href)
        if not m: 
            continue
        rid = m.group(1)
        root = a.find_parent(["li","div"]) or a
        title = (root.select_one(".title") or a).get_text(strip=True)
        # 本文候補（断片）
        content = ""
        cands = [
            root.select_one(".text-con,.text,.content,.kb-content"),
            root.find(class_=re.compile("(text|content|con)")),
        ]
        for c in cands:
            if c and c.get_text(strip=True):
                content = c.get_text(" ", strip=True)
                break
        # 日付/スコアの推測
        pub = ""
        for d in root.find_all(["span","p","div"], string=re.compile(r"\d{4}-\d{2}-\d{2}")):
            pub = d.get_text(strip=True); break
        score = ""
        s_el = root.find(string=re.compile(r"[0-5]\.?\d?分")) or ""
        score = s_el.strip() if isinstance(s_el, str) else ""
        items.append({
            "review_id": rid,
            "title": title,
            "content": content,
            "pub_date": pub,
            "score": score,
        })
    # 重複除去
    seen=set(); uniq=[]
    for it in items:
        if it["review_id"] in seen: continue
        seen.add(it["review_id"]); uniq.append(it)
    return uniq

# ====== 構造抽出（LLM不使用） ======
def rule_extract_fields(text: str) -> Dict[str, Any]:
    """
    よくある見出しを素朴抽出。サイトの項目見出しに合わせ適宜追加可能。
    """
    fields = {}
    # 例：「最满意」「不满意」「油耗」「空间」「舒适性」などを素朴に拾う
    patterns = {
        "pros": r"(最满意|优点|喜欢)[：:\s]\s*(.+)",
        "cons": r"(不满意|缺点|吐槽)[：:\s]\s*(.+)",
        "fuel": r"(油耗|百公里油耗)[：:\s]\s*([0-9\.]+\s*[Ll/]\s*100km|[0-9\.]+\s*L)",
        "mileage": r"(行驶|里程|公里)[：:\s]\s*([0-9,\.]+)\s*公里",
        "model": r"(购买车型|车型|配置)[：:\s]\s*(.+)",
        "price": r"(裸车价|落地价|成交价)[：:\s]\s*([0-9\.]+)\s*万?元?",
    }
    for k, pat in patterns.items():
        m = re.search(pat, text)
        if m:
            fields[k] = m.group(len(m.groups()))
    return fields

def jinja_like_compose(title: str, fields: Dict[str,Any], score: str, text: str) -> str:
    parts = []
    if title: parts.append(f"■ タイトル: {title}")
    if score: parts.append(f"■ 総合評価: {score}")
    if "model" in fields: parts.append(f"■ 購入モデル: {fields['model']}")
    if "price" in fields: parts.append(f"■ 価格: {fields['price']}")
    if "mileage" in fields: parts.append(f"■ 走行距離: {fields['mileage']}")
    if "fuel" in fields: parts.append(f"■ 燃費: {fields['fuel']}")
    if "pros" in fields: parts.append(f"■ 良かった点: {fields['pros']}")
    if "cons" in fields: parts.append(f"■ 気になった点: {fields['cons']}")
    # 末尾に短い要約
    base = "\n".join(parts)
    base += "\n—\n" + (text[:300] + ("…" if len(text) > 300 else ""))
    return base

# ====== LLM（最終ポリッシュ：任意） ======
def polish_with_llm(text_ja: str) -> str:
    if not USE_LLM:
        return text_ja
    if not OPENAI_API_KEY:
        return text_ja
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        sys_prompt = (
            "あなたは日本語の校正者です。文体を簡潔で読みやすく整え、"
            "事実を追加せず、敬体で200〜400字程度にまとめ直してください。"
        )
        user = f"以下の箇条書きと短い抜粋を、過度に脚色せず自然な要約に整えてください。\n\n{text_ja}"
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role":"system","content":sys_prompt},{"role":"user","content":user}],
            temperature=0.2,
            max_tokens=450,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        # 失敗したら素のまま返す（コスト最小＆落ちない）
        return text_ja

# ====== キャッシュ判定 & 保存 ======
SUM_PATH = CACHEDIR / "summaries.jsonl"
RAW_PATH = CACHEDIR / "raw_reviews.jsonl"

def is_already_summarized(review_id: str, content_hash: str, fmt: str, model: str) -> bool:
    for row in load_jsonl(SUM_PATH):
        if (row.get("review_id")==review_id and 
            row.get("content_sha1")==content_hash and 
            row.get("summary_fmt_ver")==fmt and 
            row.get("model")==model):
            return True
    return False

def upsert_summary(row: Dict[str,Any]) -> None:
    row["key"] = f"{row['review_id']}:{row['content_sha1']}:{row['summary_fmt_ver']}:{row['model']}"
    upsert_jsonl_by_key(SUM_PATH, row, key="key")

def upsert_raw(row: Dict[str,Any]) -> None:
    row["key"] = row["review_id"]
    upsert_jsonl_by_key(RAW_PATH, row, key="key")

# ====== メイン：増分取得→要約 ======
def run(series_id: str) -> None:
    seen_consec_known = 0
    page = 1
    new_summaries = 0
    total_seen = 0
    while True:
        html = fetch_html_series_page(series_id, page)
        revs = parse_reviews(html)
        if not revs:
            break
        page_new_any = False
        for r in revs:
            rid = r["review_id"]
            content = (r.get("content") or "").strip()
            # 内容が短すぎる場合、次ページで詳細抽出を別途してもよい
            chash = sha1(content or r.get("title",""))
            total_seen += 1

            already = is_already_summarized(rid, chash, SUMMARY_FMT_VER, OPENAI_MODEL if USE_LLM else "no-llm")
            if already:
                seen_consec_known += 1
                continue

            # 新規 or 更新検知（本文が変わっている）
            seen_consec_known = 0
            page_new_any = True

            fields = rule_extract_fields(content)
            base = jinja_like_compose(r.get("title",""), fields, r.get("score",""), content)
            final = polish_with_llm(base)

            # 保存（raw / summary）
            upsert_raw({
                "review_id": rid,
                "title": r.get("title",""),
                "content": content,
                "pub_date": r.get("pub_date",""),
                "score": r.get("score",""),
                "content_sha1": chash,
                "last_seen_at": int(time.time()),
            })
            upsert_summary({
                "review_id": rid,
                "content_sha1": chash,
                "summary_fmt_ver": SUMMARY_FMT_VER,
                "model": OPENAI_MODEL if USE_LLM else "no-llm",
                "lang": "ja",
                "summary": final,
                "created_at": int(time.time()),
            })
            new_summaries += 1

        # 既知連続閾値で終了
        if seen_consec_known >= STOP_AFTER_CONSEC_KNOWN:
            break
        # そのページで何も新規が無かったら加算しつつ次ページ
        page += 1
        # 安全ガード：最大ページ数を環境変数で制御したい場合に対応
        max_pages = int(os.environ.get("MAX_PAGES", "20"))
        if page > max_pages:
            break

    # 集計をCSV/MDにも出す（任意）
    out_md = OUTDIR / f"{series_id}_koubei_summary.md"
    rows = load_jsonl(SUM_PATH)
    rows = [r for r in rows if r.get("summary_fmt_ver")==SUMMARY_FMT_VER]
    rows.sort(key=lambda x: x.get("created_at",0), reverse=True)
    md = [f"# シリーズ {series_id} 口コミサマリー（{SUMMARY_FMT_VER}）", ""]
    for r in rows[: int(os.environ.get("MAX_EXPORT", "200"))]:
        md.append(f"## ID {r['review_id']}")
        md.append(r["summary"])
        md.append("")
    out_md.write_text("\n".join(md), encoding="utf-8")

    print(f"[series {series_id}] total_seen={total_seen} new_summaries={new_summaries} consec_known_stop={seen_consec_known>=STOP_AFTER_CONSEC_KNOWN}")

if __name__ == "__main__":
    if not SERIES_ID:
        print("Usage: python tools/koubei_cache_summary.py <SERIES_ID>", file=sys.stderr)
        sys.exit(2)
    run(SERIES_ID)
