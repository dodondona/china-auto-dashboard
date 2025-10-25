# tools/koubei_summary.py
from __future__ import annotations
import os
import time
import math
import random
import textwrap
from typing import List, Dict, Any, Iterable

import httpx
from openai import OpenAI
from openai import APIConnectionError, RateLimitError, APITimeoutError, APIStatusError

# =========================
# 設定（環境変数で上書き可）
# =========================
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
LLM_BATCH = int(os.getenv("LLM_BATCH", "10"))        # LLMに投げる件数（小さめが安定）
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "45"))  # 秒
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "7"))     # 自前リトライ回数（指数バックオフ）

# =========================
# OpenAI クライアント（HTTP/2無効）
# =========================
def make_client() -> OpenAI:
    # GitHub Actions で稀に出る httpx.RemoteProtocolError 回避のため HTTP/2 を明示的にOFF
    http_client = httpx.Client(http2=False, timeout=HTTP_TIMEOUT)
    # SDK 内の軽い自動リトライ + 我々の重めの自前リトライを併用
    return OpenAI(http_client=http_client, max_retries=3)

client = make_client()

# =========================
# 汎用：指数バックオフ付き呼び出し
# =========================
def call_chat_with_retries(
    *,
    model: str,
    messages: List[Dict[str, Any]],
    response_format: Dict[str, Any] | None = None,
    max_attempts: int = MAX_RETRIES,
    base_delay: float = 1.0,
):
    """
    RemoteProtocolError / ConnectionError / 429 / 5xx を粘って再試行。
    1,2,4,8,...秒 + ジッタ(0〜300ms)。
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0,
                response_format=response_format or {"type": "text"},
            )
        except (APIConnectionError, APITimeoutError) as e:
            if attempt == max_attempts:
                raise
        except APIStatusError as e:
            # レートや一時的なサーバーエラーのみ再試行対象
            if e.status_code not in (429, 500, 502, 503, 504):
                raise
            if attempt == max_attempts:
                raise
        # バックオフ
        sleep = (base_delay * (2 ** (attempt - 1))) + random.uniform(0, 0.3)
        time.sleep(sleep)

# =========================
# 進捗表示ヘルパ
# =========================
def tick(fn, label: str):
    """
    ログで段階名を示すヘルパ。あなたのログの ‘tick(_call, "llm_batches")’ に合わせています。
    """
    print(f"{label} ...", flush=True)
    res = fn()
    print(f"{label} done", flush=True)
    return res

# =========================
# LLM 呼び出しの薄いラッパ（元の _call 名に合わせる）
# =========================
def _call(messages: List[Dict[str, Any]]):
    comp = call_chat_with_retries(
        model=OPENAI_MODEL,
        messages=messages,
        response_format={"type": "text"},
    )
    return comp.choices[0].message.content or ""

# =========================
# チャンク分割
# =========================
def batched(iterable: Iterable[Any], n: int) -> Iterable[list[Any]]:
    batch: list[Any] = []
    for x in iterable:
        batch.append(x)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch

# =========================
# 要約プロンプト
# =========================
SYSTEM_JA = (
    "あなたは中国語のクルマのユーザーレビューを要約する日本語アナリストです。"
    "入力は複数件のレビュー本文（中国語）です。重複や宣伝は無視し、"
    "長所/短所/気づき（品質・乗り心地・電費/燃費・価格・内外装・装備・不具合）を簡潔に日本語で箇条書きにしてください。"
)
SYSTEM_ZH = (
    "你是汽车用户口碑的中文分析师。请对多条原文（中文）做去重摘要，"
    "按优点/缺点/其他洞见分点列出，语言简洁。"
)

def build_messages_ja(reviews: List[str]) -> List[Dict[str, Any]]:
    joined = "\n\n---\n\n".join(r.strip() for r in reviews if str(r).strip())
    user_content = (
        "以下のレビューをまとめて要約してください（日本語）。\n"
        "フォーマット：\n"
        "【長所】\n"
        "・...\n"
        "【短所】\n"
        "・...\n"
        "【気づき】\n"
        "・...\n\n"
        f"レビュー本文:\n{joined}"
    )
    return [{"role": "system", "content": SYSTEM_JA},
            {"role": "user", "content": user_content}]

def build_messages_zh(reviews: List[str]) -> List[Dict[str, Any]]:
    joined = "\n\n---\n\n".join(r.strip() for r in reviews if str(r).strip())
    user_content = (
        "请对以下多条中文用户口碑进行去重摘要，用简洁的中文按“优点/缺点/其他发现”列点：\n\n"
        f"{joined}"
    )
    return [{"role": "system", "content": SYSTEM_ZH},
            {"role": "user", "content": user_content}]

# =========================
# バッチ要約（日本語 / 中国語）
# =========================
def summarize_batch_ja(reviews: List[str]) -> List[str]:
    """
    reviews: 中国語レビューの配列
    return: それぞれのバッチの要約テキスト（長文1本/バッチ）
    """
    outputs: List[str] = []
    for i, chunk in enumerate(batched(reviews, LLM_BATCH), start=1):
        # 混雑回避のため、バッチ間に少し隙を入れる
        if i > 1:
            time.sleep(0.2)
        def work():
            msgs = build_messages_ja(chunk)
            return _call(msgs)
        text = tick(work, "llm_batches")
        outputs.append(text)
        print(f"batch {i}: summarized {len(chunk)} reviews", flush=True)
    return outputs

def summarize_batch_zh(reviews: List[str]) -> List[str]:
    outputs: List[str] = []
    for i, chunk in enumerate(batched(reviews, LLM_BATCH), start=1):
        if i > 1:
            time.sleep(0.2)
        def work():
            msgs = build_messages_zh(chunk)
            return _call(msgs)
        text = tick(work, "llm_batches")
        outputs.append(text)
        print(f"batch {i}: summarized {len(chunk)} reviews", flush=True)
    return outputs

# =========================
# 参考：ページング取得のログ出し（存在するなら呼び出し側で使ってください）
# =========================
def log_fetched_page(page_index: int, n: int):
    print(f"page {page_index:>2}: fetched {n} reviews", flush=True)

# =========================
# メイン（既存の呼び出しと互換：MODE, INPUT を環境変数で渡す場合のみ）
# =========================
def main():
    """
    既存ログの呼び出しは `results = summarize_batch_ja(batch, client) if MODE=="ja" else summarize_batch_zh(batch, client)`
    のようでした。本スクリプトでも互換で動かせるよう、環境変数 INPUT_PATH が与えられた時のみ
    そこで与えたテキスト一覧（1行1件）を読み込み、サマリーを標準出力へ吐きます。
    Actions 側の既存フローに合わせるなら、この main は実行されなくても問題ありません。
    """
    input_path = os.getenv("INPUT_PATH", "").strip()
    mode = os.getenv("MODE", "ja").lower()
    if not input_path:
        print("INFO: INPUT_PATH 未指定のため main は何もしません（既存の呼び出しがこのモジュールの関数を使う想定）")
        return

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"INPUT not found: {input_path}")

    with open(input_path, "r", encoding="utf-8") as f:
        reviews = [line.rstrip("\n") for line in f if line.strip()]

    if mode == "ja":
        results = summarize_batch_ja(reviews)
    else:
        results = summarize_batch_zh(reviews)

    # 出力：単純に連結
    print("\n\n===== SUMMARY =====\n")
    print("\n\n---\n\n".join(results))

if __name__ == "__main__":
    main()
