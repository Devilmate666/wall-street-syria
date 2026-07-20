#!/usr/bin/env python3
"""
Pulls new items from a list of RSS feeds and posts them to a Telegram channel
via a bot. Keeps track of what's already been sent in state.json so the same
item is never posted twice.

Required environment variables (set as GitHub Actions secrets):
    TELEGRAM_BOT_TOKEN  -> your bot's token from @BotFather
    TELEGRAM_CHAT_ID    -> the channel id, e.g. -1002264223809
"""

import hashlib
import json
import os
import sys
import time
from html import unescape
from pathlib import Path

import feedparser
import requests
from deep_translator import GoogleTranslator

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

FEEDS = [
    {"name": "منتدى Myfxbook", "url": "https://www.myfxbook.com/rss/forex-community-recent-topics"},
    {"name": "أخبار Myfxbook", "url": "https://www.myfxbook.com/rss/latest-forex-news"},
    {"name": "التقويم الاقتصادي - Myfxbook", "url": "https://www.myfxbook.com/rss/forex-economic-calendar-events"},
    {"name": "العربية", "url": "https://www.alarabiya.net/feed/rss2/ar.xml"},
    {"name": "العربية - العرب والعالم", "url": "https://www.alarabiya.net/feed/rss2/ar/arab-and-world.xml"},
    {"name": "العربية - أسواق", "url": "https://www.alarabiya.net/feed/rss2/ar/aswaq.xml"},
]

STATE_FILE = Path(__file__).parent / "state.json"
MAX_SEEN_PER_FEED = 300        # how many ids to remember per feed (keeps state.json small)
MAX_ITEMS_PER_FEED_PER_RUN = 50  # safety cap so a feed reset doesn't spam the channel
SEND_DELAY_SECONDS = 1.2        # be polite to Telegram's rate limits

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# When true, the script marks every current entry in every feed as "seen"
# WITHOUT sending any Telegram messages. Use this once to reset/prime the
# state so that only articles published from now on get sent.
BASELINE_ONLY = os.environ.get("BASELINE_ONLY", "false").lower() == "true"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def entry_id(entry) -> str:
    """Build a stable unique id for an RSS entry."""
    raw = entry.get("id") or entry.get("link") or entry.get("title", "")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def clean_text(text: str) -> str:
    text = unescape(text or "")
    return " ".join(text.split())


def is_mostly_arabic(text: str) -> bool:
    if not text:
        return True
    arabic_chars = sum(1 for c in text if "\u0600" <= c <= "\u06FF")
    letters = sum(1 for c in text if c.isalpha())
    if letters == 0:
        return True
    return (arabic_chars / letters) > 0.5


def to_arabic(text: str) -> str:
    """Translate text to Arabic. If it's already Arabic, or translation
    fails for any reason, just return the original text untouched."""
    text = text.strip()
    if not text or is_mostly_arabic(text):
        return text
    try:
        translated = GoogleTranslator(source="auto", target="ar").translate(text)
        return translated or text
    except Exception as exc:  # noqa: BLE001
        print(f"  ! Translation failed, using original text: {exc}", file=sys.stderr)
        return text


def escape_markdown_v2(text: str) -> str:
    escape_chars = r"_*[]()~`>#+-=|{}.!\\"
    return "".join(f"\\{c}" if c in escape_chars else c for c in text)


def build_message(feed_name: str, entry) -> str:
    title = clean_text(entry.get("title", "(no title)"))
    summary = clean_text(entry.get("summary", ""))
    if len(summary) > 400:
        summary = summary[:397] + "..."

    title_ar = to_arabic(title)
    summary_ar = to_arabic(summary) if summary else ""

    parts = [
        f"*{escape_markdown_v2(feed_name)}*",
        escape_markdown_v2(title_ar),
    ]
    if summary_ar and summary_ar != title_ar:
        parts.append(escape_markdown_v2(summary_ar))
    return "\n\n".join(parts)


def send_to_telegram(text: str) -> bool:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    resp = requests.post(url, json=payload, timeout=30)
    if resp.status_code != 200:
        print(f"  ! Telegram error {resp.status_code}: {resp.text}", file=sys.stderr)
        return False
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    if not BOT_TOKEN or not CHAT_ID:
        print("ERROR: TELEGRAM_BOT_TOKEN and/or TELEGRAM_CHAT_ID env vars are missing.", file=sys.stderr)
        sys.exit(1)

    state = load_state()
    total_sent = 0

    if BASELINE_ONLY:
        print("BASELINE_ONLY mode: marking all current items as seen, sending nothing.\n")

    for feed in FEEDS:
        name, url = feed["name"], feed["url"]
        print(f"Fetching: {name} ({url})")

        try:
            parsed = feedparser.parse(url)
        except Exception as exc:  # noqa: BLE001
            print(f"  ! Failed to fetch/parse: {exc}", file=sys.stderr)
            continue

        if parsed.bozo and not parsed.entries:
            print(f"  ! Feed looks broken: {parsed.bozo_exception}", file=sys.stderr)
            continue

        seen_ids = set(state.get(url, []))
        new_ids_this_run = []
        sent_this_feed = 0

        # feed entries are usually newest-first; reverse so we post oldest -> newest
        entries = list(reversed(parsed.entries))

        for entry in entries:
            eid = entry_id(entry)
            if eid in seen_ids:
                continue

            if BASELINE_ONLY:
                # Just record it as seen, don't send anything.
                new_ids_this_run.append(eid)
                continue

            if sent_this_feed >= MAX_ITEMS_PER_FEED_PER_RUN:
                print(f"  ~ Hit per-run cap ({MAX_ITEMS_PER_FEED_PER_RUN}) for this feed, will catch the rest next run.")
                break

            message = build_message(name, entry)
            ok = send_to_telegram(message)
            new_ids_this_run.append(eid)

            if ok:
                sent_this_feed += 1
                total_sent += 1
                print(f"  -> sent: {clean_text(entry.get('title', ''))[:80]}")

            time.sleep(SEND_DELAY_SECONDS)

        if BASELINE_ONLY and new_ids_this_run:
            print(f"  ~ Marked {len(new_ids_this_run)} existing item(s) as seen (no messages sent).")

        if new_ids_this_run:
            updated = list(seen_ids.union(new_ids_this_run))
            # keep only the most recent MAX_SEEN_PER_FEED ids
            state[url] = updated[-MAX_SEEN_PER_FEED:]

    save_state(state)
    print(f"Done. Sent {total_sent} new item(s) total.")


if __name__ == "__main__":
    main()
