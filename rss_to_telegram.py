name: Backfill News Ticker

# Manual, one-off: populates latest_news.json straight from each feed's
# current live content (translated, full title + description), without
# sending anything to Telegram and without touching state.json.
#
# Uses its own concurrency group (separate from "rss-to-telegram") so it
# can run immediately even while the main polling-loop job is mid-run,
# instead of queuing behind it for hours.
#
# Run it from: Actions tab -> "Backfill News Ticker" -> "Run workflow".

on:
  workflow_dispatch: {}

permissions:
  contents: write

concurrency:
  group: backfill-news
  cancel-in-progress: true

jobs:
  backfill:
    runs-on: ubuntu-latest
    timeout-minutes: 10

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4
        with:
          persist-credentials: true

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Backfill latest_news.json from current feed content
        env:
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
          BACKFILL_NEWS_ONLY: "true"
        run: python rss_to_telegram.py
