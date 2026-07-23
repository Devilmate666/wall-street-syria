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
    {"name": "Forex Bundle", "url": "https://rss.app/feeds/_c5P6nJZM3e6ZSbyE.xml"},
]

# No topic filtering needed anymore -- the RSS.app bundle is already curated
# to just the sources you picked, so everything in it is on-topic.
FEEDS_NEEDING_TOPIC_FILTER = set()

# Keywords used to decide if a story from a general-news feed is relevant to
# forex/gold trading or major Middle East / US politics. Case-insensitive
# substring match against the (already-Arabic) title + summary. Unused while
# FEEDS_NEEDING_TOPIC_FILTER is empty, kept here in case you add a broad
# general-news feed back in later.
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

# Small public log of the last few items actually posted to Telegram, kept
# separate from state.json (which is just id hashes) so the website can
# fetch this one directly from raw.githubusercontent.com and show real,
# live headlines -- no third-party RSS-to-JSON converter, no CORS proxy,
# just the bot's own real output.
NEWS_FILE = Path(__file__).parent / "latest_news.json"
MAX_NEWS_ITEMS = 8

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
    """Commit + push state.json if it changed. Never raises -- just logs.
    If the push is rejected (e.g. a previous job's commit landed a moment
    ago and we're now behind), pulls with rebase and retries once instead
    of giving up -- this is what prevents the "sent twice around the
    restart" race: without this, a stale push failure would silently
    leave this job's state.json ahead only in its local working copy,
    and the next job could start from the older, already-pushed version
    and re-send whatever this job just sent."""
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

        push = subprocess.run(["git", "push"], capture_output=True, text=True)
        if push.returncode != 0:
            print("  ~ push rejected (likely a race with another job), pulling + retrying once...")
            subprocess.run(["git", "pull", "--rebase"], check=True)
            subprocess.run(["git", "push"], check=True)

        print("  ~ committed + pushed state.json")
    except subprocess.CalledProcessError as exc:
        print(f"  ! git commit/push failed (will retry next pass): {exc}", file=sys.stderr)


def normalize_title(title: str) -> str:
    """Collapse a title down to a stable comparison key: lowercased,
    punctuation/whitespace stripped. Used as a backstop dedup check in
    case two different feed entries (e.g. same article re-fetched with a
    different id/link, such as a tracking-param change) are really the
    same story."""
    text = title.lower().strip()
    text = re.sub(r"[^\w\u0600-\u06FF]+", "", text)
    return text


def entry_id(entry) -> str:
    """Build a stable unique id for an RSS entry."""
    raw = entry.get("id") or entry.get("link") or entry.get("title", "")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


HTML_TAG_RE = re.compile(r"<[^>]+>")
BBCODE_QUOTE_RE = re.compile(r"\[quote\].*?\[/quote\]", re.DOTALL | re.IGNORECASE)
BBCODE_TAG_RE = re.compile(r"\[/?[a-zA-Z0-9]+(?:=[^\]]*)?\]")
URL_RE = re.compile(r"https?://\S+")

# Sentence-splitting delimiters (Arabic + Latin punctuation)
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?؟。])\s+")

# Any sentence containing one of these is a "go watch the video" callout
# that's meaningless without a link, so we drop the whole sentence.
VIDEO_MENTION_MARKERS = [
    "فيديو", "الفيديو", "شاهد", "مقطع مصور", "video", "youtube", "يوتيوب",
]


def strip_urls(text: str) -> str:
    return URL_RE.sub("", text).strip()


def strip_video_mentions(text: str) -> str:
    """Remove sentences that tell the reader to go watch a video, since
    there's no link included for them to actually do that."""
    if not text:
        return text
    sentences = SENTENCE_SPLIT_RE.split(text)
    kept = [
        s for s in sentences
        if not any(marker in s.lower() for marker in VIDEO_MENTION_MARKERS)
    ]
    return " ".join(s.strip() for s in kept if s.strip())


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = BBCODE_QUOTE_RE.sub("", text)   # drop quoted/reply blocks
    text = BBCODE_TAG_RE.sub("", text)     # drop remaining [tag] markup
    text = HTML_TAG_RE.sub(" ", text)      # strip actual HTML tags
    text = unescape(text)                  # decode entities (&amp; etc.)
    text = strip_urls(text)                # remove any raw links
    text = strip_video_mentions(text)      # remove "watch the video" callouts
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


def extract_image_url(entry) -> str:
    """Pull the best available image URL from a feed entry, checking the
    common places different feeds put it. Returns '' if none found."""
    media_content = entry.get("media_content") or []
    for m in media_content:
        if m.get("url") and m.get("medium", "image") in ("image", None):
            return m["url"]

    media_thumb = entry.get("media_thumbnail") or []
    for m in media_thumb:
        if m.get("url"):
            return m["url"]

    for enc in entry.get("links", []) or []:
        if enc.get("rel") == "enclosure" and str(enc.get("type", "")).startswith("image"):
            return enc.get("href", "")

    # Fall back to sniffing the raw (uncleaned) summary/description for <img src="...">
    raw_html = entry.get("summary", "") or entry.get("description", "")
    match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', raw_html)
    if match:
        return match.group(1)

    return ""


def build_message(feed_name: str, entry) -> tuple[str, str]:
    """Returns (message_text, image_url). image_url is '' if none found."""
    title = clean_text(entry.get("title", "(no title)"))
    summary = clean_text(entry.get("summary", ""))

    image_url = extract_image_url(entry)
    # Telegram photo captions cap at 1024 chars, plain text messages at 4096.
    # Leave headroom for the emoji/title/markdown-escaping overhead, but
    # otherwise let the full source summary through instead of cutting it
    # short artificially.
    summary_limit = 900 if image_url else 3500
    if len(summary) > summary_limit:
        summary = summary[: summary_limit - 1].rsplit(" ", 1)[0] + "…"

    title_ar = to_arabic(title)
    summary_ar = to_arabic(summary) if summary else ""

    emoji = pick_emoji(title_ar, summary_ar)

    parts = [f"{emoji} {escape_markdown_v2(title_ar)}"]
    if summary_ar and summary_ar != title_ar:
        parts.append(escape_markdown_v2(summary_ar))
    return "\n\n".join(parts), image_url


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


# send_to_telegram / send_to_telegram_photo return one of three outcomes
# instead of a plain bool. This matters because a network *timeout* is not
# the same thing as Telegram *rejecting* the message: on a timeout we genuinely
# don't know whether Telegram received and posted it before our connection
# dropped. Treating "ambiguous" the same as "failed" is exactly what caused
# duplicate posts before this fix -- a timed-out sendPhoto would trigger a
# text fallback even when the photo had actually gone through, so the same
# story appeared twice (once as a photo, once as text).
#   "sent"      -> Telegram confirmed with HTTP 200, definitely posted once
#   "failed"    -> Telegram gave a clear error (bad photo, bad markdown,
#                   etc.) -- definitely NOT posted, safe to fall back / retry
#   "ambiguous" -> connection dropped or timed out mid-request -- unknown
#                   whether it posted. We treat this as "sent" to bias
#                   toward never double-posting, at the small cost of
#                   occasionally missing an item on a bad network blip.

def send_to_telegram(text: str) -> str:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=30)
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
        print(f"  ! sendMessage timed out / connection dropped: {exc}", file=sys.stderr)
        return "ambiguous"
    except Exception as exc:  # noqa: BLE001
        print(f"  ! sendMessage request failed: {exc}", file=sys.stderr)
        return "failed"
    if resp.status_code != 200:
        print(f"  ! Telegram error {resp.status_code}: {resp.text}", file=sys.stderr)
        return "failed"
    return "sent"


def send_to_telegram_photo(image_url: str, caption: str) -> str:
    """Send a photo with a caption. See outcome docstring above."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    payload = {
        "chat_id": CHAT_ID,
        "photo": image_url,
        "caption": caption,
        "parse_mode": "MarkdownV2",
    }
    try:
        # Telegram has to fetch the remote image itself before it can
        # respond, which can take a while -- give this more headroom than
        # a plain text send so we don't time out on slow source images.
        resp = requests.post(url, json=payload, timeout=45)
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
        print(f"  ! sendPhoto timed out / connection dropped: {exc}", file=sys.stderr)
        return "ambiguous"
    except Exception as exc:  # noqa: BLE001
        print(f"  ! sendPhoto request failed: {exc}", file=sys.stderr)
        return "failed"
    if resp.status_code != 200:
        print(f"  ! Telegram sendPhoto error {resp.status_code}: {resp.text}", file=sys.stderr)
        return "failed"
    return "sent"


def send_post(text: str, image_url: str) -> bool:
    """Send a post, with an image if one was found. Falls back to a
    text-only message only on a CLEAN photo failure -- never after an
    ambiguous timeout, since that's what used to cause double-posts."""
    if image_url:
        status = send_to_telegram_photo(image_url, text)
        if status == "sent":
            return True
        if status == "ambiguous":
            print("  ~ photo send was ambiguous (timeout) -- treating as sent to avoid posting it twice")
            return True
        print("  ~ photo send failed cleanly, falling back to text-only message")

    status = send_to_telegram(text)
    if status == "sent":
        return True
    if status == "ambiguous":
        print("  ~ text send was ambiguous (timeout) -- treating as sent to avoid posting it twice")
        return True
    return False


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
        seen_titles = set(state.get(url + "::titles", []))
        is_new_feed = url not in state
        newly_seen_ids = []     # items to mark seen: skipped-by-filter, baseline, AND successful sends
        newly_seen_titles = []  # normalized titles of anything actually sent, for the dedup backstop
        sent_this_feed = 0

        if is_new_feed and not BASELINE_ONLY:
            print("  ~ First time seeing this feed: priming silently, no messages will be sent for its current backlog.")

        # feed entries are usually newest-first; reverse so oldest is first,
        # newest is last -- lets us easily grab "the latest new one."
        entries = list(reversed(parsed.entries))

        if BASELINE_ONLY or is_new_feed:
            for entry in entries:
                eid = entry_id(entry)
                if eid not in seen_ids:
                    newly_seen_ids.append(eid)
                    newly_seen_titles.append(normalize_title(clean_text(entry.get("title", ""))))
        else:
            # Collect every entry that's genuinely new (and passes the topic
            # filter, if any) -- but only ever SEND the most recent one.
            # Everything older than that gets marked seen and discarded
            # silently, so a burst of new items never floods the channel.
            candidates = []  # (eid, entry)
            for entry in entries:
                eid = entry_id(entry)
                if eid in seen_ids:
                    continue

                if url in FEEDS_NEEDING_TOPIC_FILTER:
                    raw_title = clean_text(entry.get("title", ""))
                    raw_summary = clean_text(entry.get("summary", ""))
                    if not matches_topic(raw_title, raw_summary):
                        newly_seen_ids.append(eid)  # mark seen, skip silently
                        continue

                # Backstop dedup: same normalized title already sent
                # recently (e.g. the same article re-fetched with a
                # rotated id/link around a job restart) -- mark seen,
                # don't send it again.
                norm = normalize_title(clean_text(entry.get("title", "")))
                if norm and norm in seen_titles:
                    newly_seen_ids.append(eid)
                    continue

                candidates.append((eid, entry))

            if candidates:
                if len(candidates) > 1:
                    skipped_titles = [clean_text(e.get("title", ""))[:60] for _, e in candidates[:-1]]
                    print(f"  ~ {len(candidates) - 1} older new item(s) discarded (only sending the latest):")
                    for t in skipped_titles:
                        print(f"      - {t}")
                    # mark all but the last as seen so they're never sent later
                    newly_seen_ids.extend(eid for eid, _ in candidates[:-1])

                latest_eid, latest_entry = candidates[-1]
                message, image_url = build_message(name, latest_entry)
                ok = send_post(message, image_url)

                if ok:
                    newly_seen_ids.append(latest_eid)
                    newly_seen_titles.append(normalize_title(clean_text(latest_entry.get("title", ""))))
                    sent_this_feed += 1
                    total_sent += 1
                    print(f"  -> sent: {clean_text(latest_entry.get('title', ''))[:80]}")
                else:
                    print("  ~ send failed, will retry this one next pass")

                time.sleep(SEND_DELAY_SECONDS)

        if BASELINE_ONLY and newly_seen_ids:
            print(f"  ~ Marked {len(newly_seen_ids)} existing item(s) as seen (no messages sent).")

        if newly_seen_ids:
            updated = list(seen_ids.union(newly_seen_ids))
            # keep only the most recent MAX_SEEN_PER_FEED ids
            state[url] = updated[-MAX_SEEN_PER_FEED:]

        if newly_seen_titles:
            updated_titles = list(seen_titles.union(t for t in newly_seen_titles if t))
            state[url + "::titles"] = updated_titles[-MAX_SEEN_PER_FEED:]

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
        try:
            sent = run_one_pass(state)
        except Exception as exc:  # noqa: BLE001
            # `state` is mutated in place inside run_one_pass, so even if it
            # blew up partway through (e.g. an unexpected error on feed 2 of
            # N), whatever it already marked as sent for earlier feeds is
            # still in this dict. Saving it here -- instead of losing it --
            # is what prevents "job crashed mid-pass" from causing the next
            # job to re-send items that already went out. The pass itself
            # still counts as a failure; we just don't throw away its work.
            print(f"! Unexpected error during pass, saving partial progress: {exc}", file=sys.stderr)
            sent = 0
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
