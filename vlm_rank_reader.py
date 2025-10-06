name: Monthly rank extract & enrich via LLM (rank/1)

on:
  schedule:
    - cron: '15 0 3 * *'   # JST 09:15 毎月3日
  workflow_dispatch:

permissions:
  contents: write

jobs:
  run:
    runs-on: ubuntu-latest
    env:
      OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Cache pip
        uses: actions/cache@v4
        with:
          path: ~/.cache/pip
          key: ${{ runner.os }}-pip-${{ hashFiles('requirements.txt') }}
          restore-keys: ${{ runner.os }}-pip-

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install playwright openai pandas
          python -m playwright install chromium

      - name: Extract & enrich rank list (with retries)
        run: |
          set -e
          for i in 1 2 3; do
            echo "Attempt #$i"
            python tools/append_series_url_and_enrich_title_llm.py \
              --rank-url "https://www.autohome.com.cn/rank/1" \
              --output "data/autohome_raw_$(date +'%Y-%m')_with_brand.csv" \
              --model "gpt-4o-mini" && break || sleep 15
          done

      - name: Commit CSV
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/autohome_raw_*_with_brand.csv data/debug_rankpage_*.html || true
          git commit -m "monthly rank extract + LLM enrich ($(date +'%Y-%m'))" || echo "Nothing to commit"
          git push
