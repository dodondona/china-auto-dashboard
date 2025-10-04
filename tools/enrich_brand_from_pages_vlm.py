#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
enrich_brand_from_pages_vlm.py
ランキングCSV（rank_seq,rank,name,count, url）を入力として、各「車系（シリーズ）」ページの
フルページスクショをVLM（AI目視）に読ませ、ブランド名（厂商/ブランド）を抽出してCSVに付与します。

- HTMLのDOM解析や正規表現に頼りません（動的でもOK）。
- Autohomeの「面包屑/パンくず」「厂商/品牌」欄、「<ブランド>-<车系>」表記を優先的に読むようプロンプト指示。
- OpenAI（gpt-4o-mini 既定）または Gemini を選択可。
- 進捗ごとにCSVへ追記保存（クラッシュ耐性）。

使い方（例）:
  # 事前準備（初回のみ）
  pip install -U openai pillow playwright google-generativeai tqdm tenacity
  playwright install chromium

  # 実行例（OpenAIを使う場合）
  setx OPENAI_API_KEY "sk-xxxx"          # Windows PowerShell の場合（または環境変数で設定）
  python enrich_brand_from_pages_vlm.py \
    --input data/autohome_raw_2025-08.csv \
    --output data/autohome_rank_2025-08.csv \
    --engine openai --model gpt-4o-mini

  # 実行例（Geminiを使う場合）
  setx GEMINI_API_KEY "xxxx"
  python enrich_brand_from_pages_vlm.py \
    --input data/autohome_raw_2025-08.csv \
    --output data/autohome_rank_2025-08.csv \
    --engine gemini --model gemini-1.5-flash

CSV 入出力:
- 入力CSV: ヘッダに {rank_seq, rank, name, count} があることを想定。モデル名列は name。車系ページURL列が `url` にあればページへ直接アクセス。無い場合は name をもとに自動検索（Autohome内部検索）。
- 出力CSV: 既存列に加え brand 列を追加。既に brand がある行はスキップ（--force で上書き）。

注意:
- 連続アクセスで画像/広告などが重くなる場合があるため、リトライ・待機・軽量化を実装。
- どうしても特定できない場合は brand="未知" を返す。
"""

from __future__ import annotations
import os, io, re, csv, json, time, base64, argparse, random, string
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any, Optional

from PIL import Image
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm

# Playwright
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# OpenAI / Gemini（必要に応じて）
try:
    from openai import OpenAI
except Exception:
    OpenAI = None

try:
    import google.generativeai as genai
except Exception:
    genai = None


# -------------------------- 設定 --------------------------

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
HEADLESS_DEFAULT = True
SHOT_DIR = Path("shots_series")
SHOT_DIR.mkdir(parents=True, exist_ok=True)

PROMPT_RULES_ZH = """你是一名中国汽车“车系页面”AI目视助手。请只根据提供的页面截图（必要时再结合提供的页面标题/少量文本）准确识别车辆的【品牌/厂商】与【车系名】。
必须遵循：
1) 该页面来自汽车之家车系页面，页面常见位置：“面包屑导航”“顶部Logo/标题区”“参数配置页的字段”。优先查找“厂商/品牌”字样，或“<品牌/厂商>-<车系>”格式。
2) 只输出 JSON（不要多余文字）。结构：{"brand":"<string>","series":"<string>","confidence":<0-1>,"evidence":"<≤40字>"}
3) 命名规则：
   - 不要把完整车系名硬拆为品牌。例如“宏光MINIEV”“秦PLUS”“宋PLUS”“汉L”等，“上汽通用五菱/比亚迪”才是品牌/厂商。
   - 若页面显示“吉利银河-星愿”，brand=“吉利银河”，series=“星愿”。
   - 实在无法确定时，brand 填“未知”。
"""

PROMPT_RULES_JA = """あなたは中国の自動車「車系ページ」AI目視アシスタントです。提供するページのスクリーンショット（必要ならタイトル/テキスト）から、【ブランド/メーカー（厂商）】と【車系名】を特定してください。
必須ルール：
1) 汽车之家の車系ページでは、パンくず（面包屑）や見出し、仕様欄に「厂商/品牌」や「<ブランド/厂商>-<车系>」の表記があります。そこを最優先で見てください。
2) 出力は JSON のみ。{"brand":"<string>","series":"<string>","confidence":<0-1>,"evidence":"<40字以内>"}
3) ネーミング：
   - “宏光MINIEV”“秦PLUS”“宋PLUS”“汉L”などは完全な車系名で、先頭語をブランドと誤解しないこと。
   - 例：「吉利银河-星愿」なら brand="吉利银河", series="星愿"。
   - どうしても不明な場合は brand を「未知」とする。
"""

SYSTEM_PROMPT = PROMPT_RULES_ZH + "\n\n" + PROMPT_RULES_JA


# -------------------------- ユーティリティ --------------------------

def b64_image(path: Path) -> str:
    with path.open("rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

def sanitize_filename(s: str) -> str:
    t = re.sub(r"[^\w\-]+", "_", s.strip())[:80]
    return t or "shot"

def ensure_dir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        rows = [dict(x) for x in r]
    return rows

def write_csv(path: Path, rows: List[Dict[str, Any]]):
    ensure_dir(path)
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


# -------------------------- スクリーンショット --------------------------

def render_and_shoot(play, url: str, out_png: Path, timeout_ms: int = 180_000) -> Path:
    """Autohomeの車系ページを開いてフルページスクショを保存。"""
    browser = play.chromium.launch(headless=HEADLESS_DEFAULT)
    ctx = browser.new_context(
        user_agent=UA,
        viewport={"width": 1366, "height": 900},
        java_script_enabled=True,
        locale="zh-CN",
        color_scheme="light",
        bypass_csp=True,
    )
    page = ctx.new_page()

    # 軽量化：不要リソースをブロック
    def route_block(route):
        req = route.request
        if req.resource_type in {"image", "media", "font"} and "autohome.com.cn" not in req.url:
            return route.abort()
        return route.continue_()
    page.route("**/*", route_block)

    # ページの安定化
    page.set_default_timeout(timeout_ms)
    page.add_init_script("""
      try{
        // 中国語フォントパッチ（文字化け防止）
        const style = document.createElement('style');
        style.setAttribute('data-screenshot-font-patch','1');
        style.textContent = `* { font-family:
          "Noto Sans CJK SC","WenQuanYi Zen Hei","Noto Sans CJK JP",
          "Noto Sans","Microsoft YaHei","PingFang SC",sans-serif !important; }`;
        document.documentElement.appendChild(style);
      }catch(e){}
    """)

    try:
        page.goto(url, wait_until="domcontentloaded")
        # 緩く本文待ち（画面の主要部が描画されるまで）
        targets = ["text=厂商", "text=品牌", "text=参数", "nav", "header", "body"]
        for sel in targets:
            try:
                page.wait_for_selector(sel, state="visible", timeout=8000)
                break
            except PWTimeout:
                pass

        # 追加待機（広告/画像ロード落ち着くまで）
        page.wait_for_timeout(1200)

        # フルページショット
        ensure_dir(out_png)
        page.screenshot(path=str(out_png), full_page=True, animations="disabled", caret="hide")

    finally:
        ctx.close()
        browser.close()

    return out_png


# -------------------------- LLM 呼び出し --------------------------

class BrandDetectorError(Exception):
    pass


def choose_engine(engine: str):
    if engine == "openai":
        if OpenAI is None:
            raise BrandDetectorError("openai パッケージが見つかりません。pip install openai>=1.40.0 を実行してください。")
        if not os.getenv("OPENAI_API_KEY"):
            raise BrandDetectorError("OPENAI_API_KEY が未設定です。環境変数に設定してください。")
    elif engine == "gemini":
        if genai is None:
            raise BrandDetectorError("google-generativeai パッケージが見つかりません。pip install google-generativeai を実行してください。")
        if not os.getenv("GEMINI_API_KEY"):
            raise BrandDetectorError("GEMINI_API_KEY が未設定です。環境変数に設定してください。")
    else:
        raise BrandDetectorError(f"未知の engine: {engine}")


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1.5, min=1, max=10),
       retry=retry_if_exception_type(BrandDetectorError))
def detect_brand_openai(image_path: Path, title: str, text_hint: str, model: str = "gpt-4o-mini") -> Dict[str, Any]:
    client = OpenAI()
    b64 = b64_image(image_path)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": [
            {"type": "input_text", "text": f"ページタイトル: {title[:180]}"},
            {"type": "input_text", "text": f"テキストヒント（任意）: {text_hint[:1200]}"},
            {"type": "input_image", "image_url": {"url": f"data:image/png;base64,{b64}"}},
            {"type": "input_text", "text": "上記に基づき JSON を1つだけ出力。"}
        ]}
    ]
    try:
        resp = client.responses.create(model=model, messages=messages, temperature=0.2)
        out = (resp.output_text or "").strip()
    except Exception as e:
        raise BrandDetectorError(str(e))

    m = re.search(r"\{.*\}", out, flags=re.S)
    if not m:
        raise BrandDetectorError(f"JSON が見つかりません: {out[:200]}")
    try:
        data = json.loads(m.group(0))
    except Exception as e:
        raise BrandDetectorError(f"JSON 解析に失敗: {e}; raw={out[:200]}")

    brand = (data.get("brand") or "").strip()
    series = (data.get("series") or "").strip()
    confidence = float(data.get("confidence") or 0.0)
    evidence = (data.get("evidence") or "").strip()
    if not brand:
        brand = "未知"
    return {"brand": brand, "series": series, "confidence": confidence, "evidence": evidence}


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1.5, min=1, max=10),
       retry=retry_if_exception_type(BrandDetectorError))
def detect_brand_gemini(image_path: Path, title: str, text_hint: str, model: str = "gemini-1.5-flash") -> Dict[str, Any]:
    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    img = Image.open(image_path)
    prompt = SYSTEM_PROMPT + "\n" + f"ページタイトル: {title[:180]}\nテキストヒント（任意）: {text_hint[:1200]}\nJSONのみを1つ出力。"
    try:
        model_obj = genai.GenerativeModel(model)
        resp = model_obj.generate_content([prompt, img], request_options={"timeout": 60})
        out = (resp.text or "").strip()
    except Exception as e:
        raise BrandDetectorError(str(e))

    m = re.search(r"\{.*\}", out, flags=re.S)
    if not m:
        raise BrandDetectorError(f"JSON が見つかりません: {out[:200]}")
    try:
        data = json.loads(m.group(0))
    except Exception as e:
        raise BrandDetectorError(f"JSON 解析に失敗: {e}; raw={out[:200]}")

    brand = (data.get("brand") or "").strip()
    series = (data.get("series") or "").strip()
    confidence = float(data.get("confidence") or 0.0)
    evidence = (data.get("evidence") or "").strip()
    if not brand:
        brand = "未知"
    return {"brand": brand, "series": series, "confidence": confidence, "evidence": evidence}


def detect_brand(engine: str, image_path: Path, title: str, text_hint: str, model: str) -> Dict[str, Any]:
    if engine == "openai":
        return detect_brand_openai(image_path, title, text_hint, model=model)
    else:
        return detect_brand_gemini(image_path, title, text_hint, model=model)


# -------------------------- メイン処理 --------------------------

def pick_url(row: Dict[str, str]) -> Optional[str]:
    # 優先: url カラム
    for k in ["url", "link", "series_url", "page_url"]:
        if row.get(k):
            return row[k]
    return None

def normalize_title(t: str) -> str:
    t = (t or "").strip()
    t = re.sub(r"\s+", " ", t)
    return t[:200]

def truncate_text(s: str, n=2000) -> str:
    s = re.sub(r"\s+", " ", (s or ""))
    return s[:n]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="rank/raw CSV のパス（rank_seq,rank,name,count[,url]）")
    ap.add_argument("--output", required=True, help="出力CSVのパス（brand 列を付与）")
    ap.add_argument("--engine", choices=["openai","gemini"], default="openai")
    ap.add_argument("--model", default="gpt-4o-mini", help="OpenAI: gpt-4o-mini / gpt-4o, Gemini: gemini-1.5-flash 等")
    ap.add_argument("--force", action="store_true", help="既存brandがあっても再取得する")
    ap.add_argument("--delay", type=float, default=0.8, help="1レコード間の待機（秒）")
    ap.add_argument("--timeout", type=int, default=180, help="ページ読み込みタイムアウト（秒）")
    args = ap.parse_args()

    choose_engine(args.engine)

    in_path = Path(args.input)
    out_path = Path(args.output)
    rows = read_csv(in_path)

    out_rows: List[Dict[str, Any]] = []
    # 既存出力があれば読み込んで再開（idempotent）
    if out_path.is_file():
        try:
            out_rows = read_csv(out_path)
        except Exception:
            out_rows = []

    # 既存 brand をスキップ
    done_keys = set()
    for r in out_rows:
        key = (r.get("rank_seq",""), r.get("name",""))
        done_keys.add(key)

    with sync_playwright() as play:
        for r in tqdm(rows, desc="brands"):
            rank_seq = str(r.get("rank_seq","")).strip()
            name     = (r.get("name") or r.get("model") or "").strip()
            key = (rank_seq, name)
            if not args.force and key in done_keys:
                continue

            # URL 決定（無ければスキップ：別途URL生成ロジックを入れてもOK）
            url = pick_url(r)
            if not url:
                # ここで name から Autohome の検索→最有力の “/series/” ページを拾うロジックを追加してもよいが、
                # ユーザー環境依存が強いため、この版では URL が無い行は保留とする。
                out_rows.append({
                    "rank_seq": r.get("rank_seq",""),
                    "rank":     r.get("rank",""),
                    "name":     name,
                    "brand":    r.get("brand","") or "未知",
                    "count":    r.get("count",""),
                    "note":     "URLなし（スキップ）",
                })
                continue

            # スクショのファイル名
            shot_name = sanitize_filename(f"{rank_seq}_{name}")
            shot_path = SHOT_DIR / f"{shot_name}.png"

            # レンダリング & 撮影
            try:
                render_and_shoot(play, url, shot_path, timeout_ms=args.timeout*1000)
            except Exception as e:
                out_rows.append({
                    "rank_seq": r.get("rank_seq",""),
                    "rank":     r.get("rank",""),
                    "name":     name,
                    "brand":    "未知",
                    "count":    r.get("count",""),
                    "note":     f"shoot_error: {e}",
                })
                write_csv(out_path, out_rows)
                time.sleep(args.delay)
                continue

            # タイトル・テキスト（ヒント）。※解析ではなく “ヒント” として添えるだけ
            title_hint = normalize_title(name)  # 入力の name 自体も念のためヒントに
            text_hint  = f"series_url={url}"

            # VLM 呼び出し
            try:
                info = detect_brand(args.engine, shot_path, title_hint, text_hint, model=args.model)
                brand = info.get("brand","未知")
                series = info.get("series","")
                conf = info.get("confidence", 0.0)
                note = f"series:{series} conf:{conf:.2f} {info.get('evidence','')}"
            except Exception as e:
                brand, note = "未知", f"vlm_error: {e}"

            out_rows.append({
                "rank_seq": r.get("rank_seq",""),
                "rank":     r.get("rank",""),
                "name":     name,
                "brand":    brand,
                "count":    r.get("count",""),
                "url":      url,
                "shot":     str(shot_path),
                "note":     note,
            })

            # 毎行 flush
            write_csv(out_path, out_rows)
            time.sleep(args.delay)

    # 完了
    write_csv(out_path, out_rows)
    print(f"[OK] {len(out_rows)} rows -> {out_path}")

if __name__ == "__main__":
    main()
