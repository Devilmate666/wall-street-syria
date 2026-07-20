# RSS → Telegram Forex Channel Bot

Pulls new items from the RSS feeds below and posts them to your Telegram
channel using your bot, on a schedule via GitHub Actions.

## Feeds included
- Myfxbook — Community recent topics
- Myfxbook — Latest forex news
- Myfxbook — Economic calendar events
- Al Arabiya — main feed (ar)
- Al Arabiya — Arab & world (ar)
- Al Arabiya — Markets / أسواق (ar)

> Note: `https://web.telegram.org/a/#-1002264223809` is **not** an RSS feed —
> it's just the web.telegram.org link to your channel. The number
> `-1002264223809` is your channel's **chat ID**, which the bot needs in
> order to *post* there. That's used as `TELEGRAM_CHAT_ID` below, not as a
> source feed.

## 1. Put these files in a GitHub repo
```
rss_to_telegram.py
requirements.txt
state.json
.github/workflows/rss-telegram.yml
```

## 2. Name your secrets correctly
You said you already added a secret with the bot token but it doesn't have
a name yet — go to **Repo → Settings → Secrets and variables → Actions →
New repository secret** and create exactly these two:

| Secret name           | Value                                      |
|------------------------|---------------------------------------------|
| `TELEGRAM_BOT_TOKEN`   | the token from @BotFather                   |
| `TELEGRAM_CHAT_ID`     | `-1002264223809` (your channel's chat id)   |

The workflow reads these exact names (`secrets.TELEGRAM_BOT_TOKEN` /
`secrets.TELEGRAM_CHAT_ID`), so they must match.

**Important:** your bot must be an **admin** of the channel (or at least
have "post messages" permission), otherwise `sendMessage` will fail with a
403.

## 3. How it works
- Runs every 15 minutes (`cron: "*/15 * * * *"`), and you can also trigger
  it manually from the **Actions** tab (`workflow_dispatch`).
- Each run fetches all 6 feeds, checks `state.json` for IDs it has already
  posted, and sends only new items.
- After sending, it commits the updated `state.json` back to the repo so
  the next run doesn't re-send old items. This means the workflow needs
  `permissions: contents: write` (already set in the yml) — no extra setup
  needed for a normal repo.
- On the very first run, every feed will look "new," so it will post the
  most recent items (capped at 8 per feed per run, `MAX_ITEMS_PER_FEED_PER_RUN`
  in the script) rather than dumping the entire feed history into the
  channel at once. It'll catch up on the rest over the next couple of runs.

## 4. Adjusting behavior
Open `rss_to_telegram.py` and tweak:
- `FEEDS` — add/remove feeds
- `MAX_ITEMS_PER_FEED_PER_RUN` — how many items to send per feed per run
- `SEND_DELAY_SECONDS` — delay between messages (avoids Telegram flood limits)
- `MAX_SEEN_PER_FEED` — how much history to remember per feed

## 5. Test locally (optional)
```bash
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN="123456:ABC..."
export TELEGRAM_CHAT_ID="-1002264223809"
python rss_to_telegram.py
```
