import feedparser
import requests
import trafilatura
from deep_translator import GoogleTranslator

# ---------------------------------------------------------------------------
@@ -233,6 +234,37 @@
    return " ".join(text.split())


def truncate_to_sentence(text: str, limit: int) -> str:
    """Shorten `text` to fit within `limit` chars, but end on a complete
    sentence (full stop / ? / ! / Arabic ؟ / etc.) instead of chopping a
    word in half and slapping an ellipsis on it.

    Walks the sentences (already split on the same punctuation used
    elsewhere in this script) and keeps adding them while they still fit.
    If even the first sentence is longer than `limit` (rare, but possible
    for a feed with no punctuation at all), falls back to the old
    word-boundary + ellipsis behaviour so we still respect Telegram's
    caption/message size caps.
    """
    sentences = [s.strip() for s in SENTENCE_SPLIT_RE.split(text) if s.strip()]

    kept = []
    length = 0
    for sentence in sentences:
        # +1 for the joining space between sentences
        added = len(sentence) + (1 if kept else 0)
        if length + added > limit:
            break
        kept.append(sentence)
        length += added

    if kept:
        return " ".join(kept)

    # No single complete sentence fits -- fall back to a hard cut.
    return text[: limit - 1].rsplit(" ", 1)[0] + "…"


def is_mostly_arabic(text: str) -> bool:
    if not text:
        return True
@@ -309,19 +341,87 @@
    return ""


TRUNCATION_ENDINGS = ("...", "…")


def looks_truncated(text: str) -> bool:
    """True if a (cleaned) summary appears to have been cut off mid-thought
    by the RSS feed itself, e.g. ending in '...' or '…'."""
    if not text:
        return False
    return text.rstrip().endswith(TRUNCATION_ENDINGS)


def complete_truncated_summary(feed_summary: str, article_text: str) -> str:
    """If `feed_summary` was cut off by the RSS feed (ends in '...'/'…'),
    use the full source article to finish just that last, cut-off sentence
    -- not to replace the whole summary with the whole article.

    Works by finding where the tail end of the truncated snippet lines up
    verbatim in the article (RSS feeds that truncate usually do so by
    literally chopping the article's own first paragraph), then picking up
    right there and reading only as far as the next sentence-ending mark.
    If no reliable match is found (e.g. the feed wrote its own short teaser
    instead of truncating the real text), the original summary is returned
    unchanged rather than guessing.
    """
    incomplete = feed_summary.rstrip()
    for suffix in TRUNCATION_ENDINGS:
        if incomplete.endswith(suffix):
            incomplete = incomplete[: -len(suffix)].rstrip()
            break

    if not incomplete or not article_text:
        return feed_summary

    # Anchor on the tail end of the cut-off snippet so we can locate the
    # exact spot in the article where the feed's text stopped.
    anchor_len = min(60, len(incomplete))
    anchor = incomplete[-anchor_len:]
    pos = article_text.find(anchor)

    if pos == -1:
        return feed_summary  # no confident match -- don't guess

    resume_at = pos + len(anchor)
    rest = article_text[resume_at:].strip()
    if not rest:
        return feed_summary

    # Only take enough of the article to finish the sentence that was cut
    # off -- the rest of the article is not included.
    completion = SENTENCE_SPLIT_RE.split(rest, maxsplit=1)[0].strip()
    if not completion:
        return feed_summary

    return f"{incomplete} {completion}".strip()


def build_message(feed_name: str, entry) -> tuple[str, str]:
    """Returns (message_text, image_url). image_url is '' if none found."""
    title = clean_text(entry.get("title", "(no title)"))
    summary = clean_text(entry.get("summary", ""))

    if looks_truncated(summary):
        article_text = clean_text(fetch_article_text(entry.get("link", "")))
        if article_text:
            completed = complete_truncated_summary(summary, article_text)
            if completed != summary:
                summary = completed
                print("  ~ completed a cut-off summary using the source article")
            else:
                print("  ~ cut-off summary didn't match the article text, leaving as-is")
        else:
            print("  ~ summary looked cut off but article fetch failed, leaving as-is")

    image_url = extract_image_url(entry)
    # Telegram photo captions cap at 1024 chars, plain text messages at 4096.
    # Leave headroom for the emoji/title/markdown-escaping overhead, but
    # otherwise let the full source summary through instead of cutting it
    # short artificially.
    summary_limit = 900 if image_url else 3500
    if len(summary) > summary_limit:
        summary = summary[: summary_limit - 1].rsplit(" ", 1)[0] + "…"
        summary = truncate_to_sentence(summary, summary_limit)

    title_ar = to_arabic(title)
    summary_ar = to_arabic(summary) if summary else ""
@@ -334,278 +434,323 @@
    return "\n\n".join(parts), image_url


def fetch_article_text(link: str) -> str:
    """Fetch the actual article page (not the RSS feed) and extract its
    full body text with trafilatura. This is what lets us post the real,
    complete story instead of whatever (possibly truncated) snippet the
    RSS feed's <summary> happened to include.

    Same direct-then-proxy fetch pattern as fetch_feed(), since the sites
    that block datacenter IPs for the feed itself will just as often block
    the article page too. Returns '' on any failure -- callers fall back
    to the feed's own summary in that case.
    """
    if not link:
        return ""

    html = None
    try:
        resp = requests.get(link, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        html = resp.text
    except Exception as exc:  # noqa: BLE001
        print(f"  ~ direct article fetch failed ({exc}); retrying via proxy...")
        try:
            from urllib.parse import quote

            proxy_url = PROXY_FETCH_URL_TEMPLATE.format(encoded_url=quote(link, safe=""))
            resp = requests.get(proxy_url, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            html = resp.text
        except Exception as exc2:  # noqa: BLE001
            print(f"  ! proxy article fetch also failed: {exc2}", file=sys.stderr)
            return ""

    try:
        extracted = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False,
            favor_precision=True,
        )
        return extracted or ""
    except Exception as exc:  # noqa: BLE001
        print(f"  ! article text extraction failed: {exc}", file=sys.stderr)
        return ""


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
