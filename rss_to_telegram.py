#!/usr/bin/env python3
"""
Pulls new items from a list of RSS feeds and posts them to a Telegram channel
via a bot. Keeps track of what's already been sent in state.json so the same
item is never posted twice.

Instead of relying on GitHub's cron (which can be delayed or skipped), this
script runs one continuous loop for most of a GitHub Actions job's allowed
runtime, checking all feeds every LOOP_INTERVAL_SECONDS. It commits and
pushes state.json back to the repo after every pass, so progress is never
lost if the job is stopped or fails partway through.

A separate, infrequent cron trigger (e.g. every 6 hours) just re-launches
this loop in case it ever ends -- most of the actual feed-checking work
happens inside the loop, not the cron.

Required environment variables (set as GitHub Actions secrets):
    TELEGRAM_BOT_TOKEN  -> your bot's token from @BotFather
    TELEGRAM_CHAT_ID    -> the channel id, e.g. -1002264223809
"""

import hashlib
import json
import os
import subprocess
import sys
import time
from html import unescape
from pathlib import Path

import feedparser
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

FEEDS = [
    {"name": "Myfxbook Community", "url": "https://www.myfxbook.com/rss/forex-community-recent-topics"},
    {"name": "Myfxbook News", "url": "https://www.myfxbook.com/rss/latest-forex-news"},
    {"name": "Myfxbook Economic Calendar", "url": "https://www.myfxbook.com/rss/forex-economic-calendar-events"},
    {"name": "Al Arabiya", "url": "https://www.alarabiya.net/feed/rss2/ar.xml"},
    {"name": "Al Arabiya - العرب والعالم", "url": "https://www.alarabiya.net/feed/rss2/ar/arab-and-world.xml"},
    {"name": "Al Arabiya - أسواق", "url": "https://www.alarabiya.net/feed/rss2/ar/aswaq.xml"},
]

STATE_FILE = Path(__file__).parent / "state.json"
MAX_SEEN_PER_FEED = 300         # how many ids to remember per feed (keeps state.json small)
MAX_ITEMS_PER_FEED_PER_RUN = 8  # safety cap so a feed reset doesn't spam the channel
SEND_DELAY_SECONDS = 1.2        # be polite to Telegram's rate limits

LOOP_INTERVAL_SECONDS = 5 * 60            # check feeds every 5 minutes
MAX_RUNTIME_SECONDS = (5 * 60 + 50) * 60  # stop after ~5h50m, well under GitHub's 6h job cap

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

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


def commit_and_push_state() -> None:
    """Commit + push state.json if it changed. Never raises -- just logs."""
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain", str(STATE_FILE)],
            capture_output=True, text=True, check=True,
        )
        if not status.stdout.strip():
            return  # nothing changed
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(
            ["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"],
            check=True,
        )
        subprocess.run(["git", "add", str(STATE_FILE)], check=True)
        subprocess.run(
            ["git", "commit", "-m", "chore: update seen RSS items [skip ci]"],
            check=True,
        )
        subprocess.run(["git", "push"], check=True)
        print("  ~ committed + pushed state.json")
    except subprocess.CalledProcessError as exc:
        print(f"  ! git commit/push failed (will retry next pass): {exc}", file=sys.stderr)


def entry_id(entry) -> str:
    """Build a stable unique id for an RSS entry."""
    raw = entry.get("id") or entry.get("link") or entry.get("title", "")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def clean_text(text: str) -> str:
    text = unescape(text or "")
    return " ".join(text.split())


def escape_markdown_v2(text: str) -> str:
    escape_chars = r"_*[]()~`>#+-=|{}.!\\"
    return "".join(f"\\{c}" if c in escape_chars else c for c in text)


def build_message(feed_name: str, entry) -> str:
    title = clean_text(entry.get("title", "(no title)"))
    link = entry.get("link", "")
    summary = clean_text(entry.get("summary", ""))
    if len(summary) > 300:
        summary = summary[:297] + "..."

    parts = [
        f"*{escape_markdown_v2(feed_name)}*",
        escape_markdown_v2(title),
    ]
    if summary and summary != title:
        parts.append(escape_markdown_v2(summary))
    if link:
        parts.append(escape_markdown_v2(link))
    return "\n\n".join(parts)


def send_to_telegram(text: str) -> bool:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": False,
    }
    resp = requests.post(url, json=payload, timeout=30)
    if resp.status_code != 200:
        print(f"  ! Telegram error {resp.status_code}: {resp.text}", file=sys.stderr)
        return False
    return True


# ---------------------------------------------------------------------------
# One pass over all feeds
# ---------------------------------------------------------------------------


def run_one_pass(state: dict) -> int:
    """Check every feed once, send new items, mutate `state` in place.
    Returns the number of items actually sent."""
    total_sent = 0

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
        newly_sent_ids = []
        sent_this_feed = 0

        # feed entries are usually newest-first; reverse so we post oldest -> newest
        entries = list(reversed(parsed.entries))

        for entry in entries:
            if sent_this_feed >= MAX_ITEMS_PER_FEED_PER_RUN:
                print(f"  ~ Hit per-pass cap ({MAX_ITEMS_PER_FEED_PER_RUN}) for this feed, will catch the rest next pass.")
                break

            eid = entry_id(entry)
            if eid in seen_ids:
                continue

            message = build_message(name, entry)
            ok = send_to_telegram(message)

            if ok:
                # Only mark as seen if it actually sent -- a failed send
                # (rate limit, network blip, etc.) will be retried next pass.
                newly_sent_ids.append(eid)
                sent_this_feed += 1
                total_sent += 1
                print(f"  -> sent: {clean_text(entry.get('title', ''))[:80]}")
            else:
                print("  ~ will retry this item next pass")

            time.sleep(SEND_DELAY_SECONDS)

        if newly_sent_ids:
            updated = list(seen_ids.union(newly_sent_ids))
            # keep only the most recent MAX_SEEN_PER_FEED ids
            state[url] = updated[-MAX_SEEN_PER_FEED:]

    return total_sent


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main() -> None:
    if not BOT_TOKEN or not CHAT_ID:
        print("ERROR: TELEGRAM_BOT_TOKEN and/or TELEGRAM_CHAT_ID env vars are missing.", file=sys.stderr)
        sys.exit(1)

    start = time.time()
    pass_num = 0

    while True:
        pass_num += 1
        elapsed = time.time() - start
        print(f"\n=== Pass {pass_num} (elapsed {elapsed/60:.1f} min) ===")

        state = load_state()  # reload each pass in case of external changes
        sent = run_one_pass(state)
        save_state(state)
        commit_and_push_state()

        print(f"Pass {pass_num} done. Sent {sent} new item(s).")

        elapsed = time.time() - start
        if elapsed + LOOP_INTERVAL_SECONDS > MAX_RUNTIME_SECONDS:
            print("Approaching max runtime for this job -- exiting so a fresh job can pick up.")
            break

        time.sleep(LOOP_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
