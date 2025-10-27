name: Translate columns (guarded)

on:
  workflow_dispatch:
    inputs:
      series_id:
        description: "Series ID (e.g. 197)"
        required: true
      commit_cache:
        description: "Commit cache back to repo"
        default: "true"
        required: false

jobs:
  translate_columns:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pandas openai

      - name: Translate columns (guarded)
        env:
          SERIES_ID: ${{ inputs.series_id }}
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
          OPENAI_MODEL: gpt-4.1-mini
          CACHE_REPO_DIR: cache
        run: |
          set -euo pipefail
          python tools/translate_columns.py

      - name: Commit repo cache and outputs
        if: ${{ inputs.commit_cache == 'true' }}
        run: |
          set -e
          git config user.name  "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add cache/${{ inputs.series_id }}/cn.csv || true
          git add cache/${{ inputs.series_id }}/ja.csv || true
          git add output/autohome/${{ inputs.series_id }}/config_${{ inputs.series_id }}.ja.csv || true
          git commit -m "update series ${{ inputs.series_id }} (CN/JA outputs & cache)" || echo "No changes"
          git push || echo "No changes"

      - name: Upload artifacts (cache + outputs)
        uses: actions/upload-artifact@v4
        with:
          name: translated-series-${{ inputs.series_id }}
          path: |
            cache/${{ inputs.series_id }}/
            output/autohome/${{ inputs.series_id }}/
