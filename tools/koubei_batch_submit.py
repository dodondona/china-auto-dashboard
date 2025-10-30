#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, os, re, time, json, io
from pathlib import Path
from typing import List, Dict
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from openai import OpenAI

"""
Usage:
  OPENAI_API_KEY=sk-... python tools/koubei_batch_submit.py <vehicle_id> [pages] [mode: ja|zh]

Outputs:
  - autohome_reviews_<ID>.batch.input.jsonl
  - autohome_reviews_<ID>.batch.submit.json
  - 標準出力に batch_id を表示
"""

VEHICLE_ID = sys.argv[1].strip() if len(sys.argv) >= 2 else ""
PAGES = int(sys.argv[2]) if len(sys.argv) >= 3 and sys.argv[2].isdigit() else 5
MODE = (sys.argv[3].strip().lower() if len(sys.argv) >= 4 else "ja")
if MODE not in ("ja", "zh"):
    MODE = "ja"

OUTDIR = Path(__file__).resolve().parent.parent
INPUT_JSONL = OUTDIR / f"autohome_reviews_{VEHICLE_ID}.batch.input.jsonl"
SUBMIT_JSON = OUTDIR / f"autohome_reviews_{VEHICLE_ID}.batch.submit.json"
BASE_URL = f"https://k.autohome.com.cn/{VEHICLE_ID}/index_{{page}}.html?#listcontainer"

MODEL = os.environ.get("BATCH_MODEL", "gpt-4.1-nano") if MODE == "ja" else os.environ.get("BATCH_MODEL", "gpt-4o-mini")
COMPLETION_WINDOW = os.environ.get("BATCH_COMPLETION_WINDOW", "6h")  # デフォルト6hに短縮

def fetch_html(page_no: int) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = browser.new_context(device_scale_factor=1.0, viewport={"width": 1280, "height": 2000})
        page = ctx.new_page()
        url = BASE_URL.format(page=page_no)
        page.goto(url, wait_until="networkidle", timeout=60_000)
        page.evaluate("""() => new Promise(res => {
            let h=0; let i=0; let id=setInterval(()=>{window.scrollBy(0,1200); i++;
                if(document.body.scrollHeight>h){h=document.body.scrollHeight}else{clearInterval(id);res();}
                if(i>20){clearInterval(id);res();}
            },300);
        })""")
        html = page.content()
        ctx.close()
        browser.close()
        return html

def parse_reviews(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    blocks = soup.select("#listcontainer .mouthcon-cont-left, #listcontainer .mouthcon")
    reviews = []
    for b in blocks:
        text = re.sub(r"\s+", " ", b.get_text(" ", strip=True))
        if text:
            reviews.append(text)
    return reviews

def sys_prompt(mode: str) -> str:
    if mode == "ja":
        return (
            "あなたはレビューテキストのアナリストです。入力は中国語の車ユーザー口コミです。"
            "各レビューから『良い点(Pros)』『悪い点(Cons)』を**日本語**で短く抽出し、"
            "overall感情を positive/mixed/negative のいずれかで判断してください。"
            "出力は**必ず JSON 配列**（各要素: {\"pros\":[..],\"cons\":[..],\"sentiment\":\"...\"}）。"
            "前置き・後書き・説明文は一切出力しない。値は短い日本語フレーズにする。"
        )
    else:
        return (
            "你是点评文本分析师。输入是中文的汽车用户口碑。"
            "请从每条点评中提取『优点(Pros)』『缺点(Cons)』的简短中文短语，并判定 overall 情感为 positive/mixed/negative。"
            "必须只输出 JSON 数组（每个元素形如 {\"pros\":[..],\"cons\":[..],\"sentiment\":\"...\"}），不要前后说明文字。"
        )

def build_jsonl(items: List[str], mode: str) -> List[str]:
    sys_msg = sys_prompt(mode)
    lines = []
    for i, text in enumerate(items, start=1):
        body = {
            "model": MODEL,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": sys_msg},
                {"role": "user", "content": f"[{i}] {text}"}
            ],
            "temperature": 0.0
        }
        lines.append(json.dumps({
            "custom_id": f"rev-{i:05d}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": body
        }, ensure_ascii=False))

    # === 追加: 口コミ全体を要約する軽量サマリジョブ ===
    summary_body = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": "次の口コミ全体の傾向を日本語で簡潔にまとめてください。"},
            {"role": "user", "content": "\n".join(items[:30])}
        ],
        "temperature": 0.3,
        "max_tokens": 400
    }
    lines.append(json.dumps({
        "custom_id": "summary-report",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": summary_body
    }, ensure_ascii=False))

    return lines

def main():
    assert VEHICLE_ID, "vehicle_id is required"
    all_texts = []
    for p in range(1, PAGES + 1):
        html = fetch_html(p)
        revs = parse_reviews(html)
        all_texts.extend(revs)
    if not all_texts:
        print("WARN: no reviews found; nothing to submit")
        return

    INPUT_JSONL.write_text("\n".join(build_jsonl(all_texts, MODE)), encoding="utf-8")

    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    up = client.files.create(file=io.BytesIO(INPUT_JSONL.read_bytes()), purpose="batch", filename=INPUT_JSONL.name)
    batch = client.batches.create(
        input_file_id=up.id,
        endpoint="/v1/chat/completions",
        completion_window="6h",
    )
    SUBMIT_JSON.write_text(json.dumps({
        "batch_id": batch.id,
        "input_file_id": up.id,
        "vehicle_id": VEHICLE_ID,
        "pages": PAGES,
        "mode": MODE,
        "model": MODEL
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(batch.id)

if __name__ == "__main__":
    main()
