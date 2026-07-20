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

Optional:
    BASELINE_ONLY       -> "true" to mark all current items as seen without
                            sending anything (use once to prime state.json)
"""

import hashlib
import json
import os
import re
import subprocess
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
    {"name": "العربية", "url": "https://www.alarabiya.net/feed/rss2/ar.xml"},
    {"name": "العربية - العرب والعالم", "url": "https://www.alarabiya.net/feed/rss2/ar/arab-and-world.xml"},
    {"name": "العربية - أسواق", "url": "https://www.alarabiya.net/feed/rss2/ar/aswaq.xml"},
    {"name": "الشرق الأوسط", "url": "https://aawsat.com/feed"},
    {"name": "سكاي نيوز عربية", "url": "https://www.skynewsarabia.com/web/rss/home.xml"},
]

# Only these two feeds are broad, general-news firehoses (sports, entertainment,
# local crime, etc all mixed in) so they need topic filtering. The Al Arabiya
# "arab-and-world" and "aswaq" (markets) feeds are already reasonably on-topic,
# and the main Al Arabiya feed is kept as-is too, but you can add it here if it
# ever gets noisy.
FEEDS_NEEDING_TOPIC_FILTER = {
    "https://aawsat.com/feed",
    "https://www.skynewsarabia.com/web/rss/home.xml",
}

# Keywords used to decide if a story from a general-news feed is relevant to
# forex/gold trading or major Middle East / US politics. Case-insensitive
# substring match against the (already-Arabic) title + summary.
TOPIC_KEYWORDS = [
    # markets / forex / gold
    "الذهب", "فوركس", "دولار", "اليورو", "البورصة", "الأسهم", "الأسواق",
    "النفط", "الفائدة", "التضخم", "الاقتصاد", "الفيدرالي", "البنك المركزي",
    "أوبك", "الناتج المحلي", "سوق العملات", "أسعار الفائدة",
    # geopolitics likely to move markets / matter to a forex-news audience
    "ترامب", "نتنياهو", "إيران", "إسرائيل", "غزة", "لبنان", "سوريا",
    "العراق", "الأردن", "السعودية", "الإمارات", "قطر", "روسيا", "أوكرانيا",
    "الحرب", "صواريخ", "عقوبات", "البيت الأبيض", "الكونغرس", "مجلس الأمن",
    "الأمم المتحدة", "الناتو", "وزارة الخارجية", "احتجاجات",
]


def matches_topic(title: str, summary: str) -> bool:
    haystack = f"{title} {summary}"
    return any(keyword in haystack for keyword in TOPIC_KEYWORDS)


STATE_FILE = Path(__file__).parent / "state.json"
MAX_SEEN_PER_FEED = 300          # how many ids to remember per feed (keeps state.json small)
MAX_ITEMS_PER_FEED_PER_RUN = 50  # safety cap so a feed reset doesn't spam the channel
SEND_DELAY_SECONDS = 1.2         # be polite to Telegram's rate limits

LOOP_INTERVAL_SECONDS = 5 * 60            # check feeds every 5 minutes
MAX_RUNTIME_SECONDS = (5 * 60 + 50) * 60  # stop after ~5h50m, well under GitHub's 6h job cap

# Some news sites block requests that don't look like a real browser
# (feedparser's default request has no User-Agent at all). We fetch the
# feed ourselves with browser-like headers, then hand the raw bytes to
# feedparser to parse.
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}
REQUEST_TIMEOUT_SECONDS = 20

# Some sites (Al Arabiya included) block requests coming from cloud/datacenter
# IP ranges like GitHub Actions runners, even with browser-like headers. If a
# direct fetch gets blocked (403/429/etc), retry once through a public
# read-only proxy that fetches the URL server-side from a different IP.
PROXY_FETCH_URL_TEMPLATE = "https://api.allorigins.win/raw?url={encoded_url}"

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


HTML_TAG_RE = re.compile(r"<[^>]+>")
BBCODE_QUOTE_RE = re.compile(r"\[quote\].*?\[/quote\]", re.DOTALL | re.IGNORECASE)
BBCODE_TAG_RE = re.compile(r"\[/?[a-zA-Z0-9]+(?:=[^\]]*)?\]")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = BBCODE_QUOTE_RE.sub("", text)   # drop quoted/reply blocks
    text = BBCODE_TAG_RE.sub("", text)     # drop remaining [tag] markup
    text = HTML_TAG_RE.sub(" ", text)      # strip actual HTML tags
    text = unescape(text)                  # decode entities (&amp; etc.)
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


EMOJI_RULES = [
    # (emoji, keywords) — checked in order, first match wins
    ("💰", ["الذهب", "أونصة", "أوقية"]),
    ("🛢️", ["النفط", "أوبك", "برميل"]),
    ("💵", ["الدولار", "اليورو", "العملات", "فوركس", "سعر الصرف"]),
    ("🏦", ["الفائدة", "الفيدرالي", "البنك المركزي", "التضخم"]),
    ("📈", ["البورصة", "الأسهم", "الأسواق", "الاقتصاد"]),
    ("🚨", ["حرب", "صواريخ", "هجوم", "تصعيد", "عاجل", "اشتباك"]),
    ("🌍", ["ترامب", "نتنياهو", "إيران", "إسرائيل", "غزة", "البيت الأبيض", "عقوبات"]),
]
DEFAULT_EMOJI = "📰"


def pick_emoji(title: str, summary: str) -> str:
    haystack = f"{title} {summary}"
    for emoji, keywords in EMOJI_RULES:
        if any(keyword in haystack for keyword in keywords):
            return emoji
    return DEFAULT_EMOJI


def build_message(feed_name: str, entry) -> str:
    title = clean_text(entry.get("title", "(no title)"))
    summary = clean_text(entry.get("summary", ""))
    if len(summary) > 400:
        summary = summary[:397] + "..."

    title_ar = to_arabic(title)
    summary_ar = to_arabic(summary) if summary else ""

    emoji = pick_emoji(title_ar, summary_ar)

    parts = [f"{emoji} {escape_markdown_v2(title_ar)}"]
    if summary_ar and summary_ar != title_ar:
        parts.append(escape_markdown_v2(summary_ar))
    return "\n\n".join(parts)


def fetch_feed(name: str, url: str):
    """Fetch and parse a feed. Tries a direct request first; if that's
    blocked (403/429/other error), retries once through a public proxy
    that fetches server-side from a different IP. Returns a feedparser
    result, or None if both attempts failed."""
    try:
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        return feedparser.parse(resp.content)
    except Exception as exc:  # noqa: BLE001
        print(f"  ! Direct fetch failed ({exc}); retrying via proxy...")

    try:
        from urllib.parse import quote

        proxy_url = PROXY_FETCH_URL_TEMPLATE.format(encoded_url=quote(url, safe=""))
        resp = requests.get(proxy_url, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        return feedparser.parse(resp.content)
    except Exception as exc:  # noqa: BLE001
        print(f"  ! Proxy fetch also failed: {exc}", file=sys.stderr)
        return None


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
# One pass over all feeds
# ---------------------------------------------------------------------------


def run_one_pass(state: dict) -> int:
    """Check every feed once, send new items, mutate `state` in place.
    Returns the number of items actually sent."""
    total_sent = 0

    if BASELINE_ONLY:
        print("BASELINE_ONLY mode: marking all current items as seen, sending nothing.\n")

    for feed in FEEDS:
        name, url = feed["name"], feed["url"]
        print(f"Fetching: {name} ({url})")

        parsed = fetch_feed(name, url)
        if parsed is None:
            print(f"  ! Failed to fetch/parse (direct + proxy both failed)", file=sys.stderr)
            continue

        if parsed.bozo and not parsed.entries:
            print(f"  ! Feed looks broken: {parsed.bozo_exception}", file=sys.stderr)
            continue

        seen_ids = set(state.get(url, []))
        is_new_feed = url not in state
        newly_seen_ids = []   # items to mark seen: skipped-by-filter, baseline, AND successful sends
        sent_this_feed = 0

        if is_new_feed and not BASELINE_ONLY:
            print("  ~ First time seeing this feed: priming silently, no messages will be sent for its current backlog.")

        # feed entries are usually newest-first; reverse so we post oldest -> newest
        entries = list(reversed(parsed.entries))

        for entry in entries:
            eid = entry_id(entry)
            if eid in seen_ids:
                continue

            if BASELINE_ONLY or is_new_feed:
                # Just record it as seen, don't send anything.
                newly_seen_ids.append(eid)
                continue

            if url in FEEDS_NEEDING_TOPIC_FILTER:
                raw_title = clean_text(entry.get("title", ""))
                raw_summary = clean_text(entry.get("summary", ""))
                if not matches_topic(raw_title, raw_summary):
                    newly_seen_ids.append(eid)  # mark seen, skip silently
                    continue

            if sent_this_feed >= MAX_ITEMS_PER_FEED_PER_RUN:
                print(f"  ~ Hit per-pass cap ({MAX_ITEMS_PER_FEED_PER_RUN}) for this feed, will catch the rest next pass.")
                break

            message = build_message(name, entry)
            ok = send_to_telegram(message)

            if ok:
                # Only mark as seen if it actually sent -- a failed send
                # (rate limit, network blip, etc.) will be retried next pass.
                newly_seen_ids.append(eid)
                sent_this_feed += 1
                total_sent += 1
                print(f"  -> sent: {clean_text(entry.get('title', ''))[:80]}")
            else:
                print("  ~ will retry this item next pass")

            time.sleep(SEND_DELAY_SECONDS)

        if BASELINE_ONLY and newly_seen_ids:
            print(f"  ~ Marked {len(newly_seen_ids)} existing item(s) as seen (no messages sent).")

        if newly_seen_ids:
            updated = list(seen_ids.union(newly_seen_ids))
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

        if BASELINE_ONLY:
            print("BASELINE_ONLY was set -- exiting after one pass instead of looping.")
            print("Remove/unset BASELINE_ONLY and re-run to start sending new items normally.")
            break

        elapsed = time.time() - start
        if elapsed + LOOP_INTERVAL_SECONDS > MAX_RUNTIME_SECONDS:
            print("Approaching max runtime for this job -- exiting so a fresh job can pick up.")
            break

        time.sleep(LOOP_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
