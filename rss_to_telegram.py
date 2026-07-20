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
import re
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
MAX_SEEN_PER_FEED = 300        # how many ids to remember per feed (keeps state.json small)
MAX_ITEMS_PER_FEED_PER_RUN = 50  # safety cap so a feed reset doesn't spam the channel
SEND_DELAY_SECONDS = 1.2        # be polite to Telegram's rate limits

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
            resp = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            parsed = feedparser.parse(resp.content)
        except Exception as exc:  # noqa: BLE001
            print(f"  ! Failed to fetch/parse: {exc}", file=sys.stderr)
            continue

        if parsed.bozo and not parsed.entries:
            print(f"  ! Feed looks broken: {parsed.bozo_exception}", file=sys.stderr)
            continue

        seen_ids = set(state.get(url, []))
        is_new_feed = url not in state
        new_ids_this_run = []
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
                new_ids_this_run.append(eid)
                continue

            if url in FEEDS_NEEDING_TOPIC_FILTER:
                raw_title = clean_text(entry.get("title", ""))
                raw_summary = clean_text(entry.get("summary", ""))
                if not matches_topic(raw_title, raw_summary):
                    new_ids_this_run.append(eid)  # mark seen, skip silently
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
