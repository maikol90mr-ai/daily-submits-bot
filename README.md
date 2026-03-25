# Daily Submits Bot

Discord bot that monitors `#daily-submits` for life insurance agent sales posts, logs them to SQLite, and provides stat commands in `#bot-stats`.

## Setup

### 1. Clone and enter the project

```bash
cd daily-submits-bot
```

### 2. Create and activate a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate        # macOS/Linux
venv\Scripts\activate           # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Edit `.env`:

```
DISCORD_BOT_TOKEN=your_bot_token_here
ADMIN_ID_MAIKOL=your_discord_user_id_here
ADMIN_ID_COLTON=coltons_discord_user_id_here
```

**How to find your Discord user ID:** Enable Developer Mode in Discord settings → right-click your username → Copy User ID.

### 5. Create the Discord bot

1. Go to https://discord.com/developers/applications
2. New Application → Bot → Add Bot
3. Under **Privileged Gateway Intents**, enable **Message Content Intent**
4. Copy the token → paste into `.env`
5. Invite the bot to your server with `bot` + `applications.commands` scopes and **Send Messages**, **Read Message History**, **Add Reactions** permissions

### 6. Run locally

```bash
source venv/bin/activate
python bot.py
```

---

## Commands (in #bot-stats only)

| Command | Description |
|---|---|
| `!recap` | Today's team total AP + deal count |
| `!week` | This week's leaderboard |
| `!month` | Month-to-date leaderboard |
| `!me` | Your personal stats |
| `!stats @agent` | Any agent's breakdown |
| `!carriers` | Team AP split by carrier |
| `!top` | All-time leaderboard |
| `!help` | Show this command list |
| `!delete <link>` | *(admin)* Remove a submission |
| `!fix @agent $amount` | *(admin)* Correct a submission amount |
| `!map <emoji> <Name>` | *(admin)* Add a new carrier emoji mapping |

---

## Carrier Detection

The bot detects carriers from emojis and text:

| Emoji / Text | Carrier |
|---|---|
| 🦆 | Aflac |
| 🏠 🏡 | American Home Life |
| 🔺 🏳️‍⚧️ | Transamerica |
| 🦅 | Americo |
| ♻️ | Ethos |
| 🐮 🐄 | Mutual of Omaha |
| 🪖 | Corebridge |
| 👑 | Royal Neighbors |
| `amam` (text) | American Amicable |

Use `!map <emoji> <CarrierName>` to add new mappings at runtime.

---

## Deploy to Railway.app

1. Push your code to a GitHub repo (**do not commit `.env`**)
2. Go to https://railway.app → New Project → Deploy from GitHub repo
3. Select your repo
4. In Railway dashboard → your service → **Variables**, add:
   - `DISCORD_BOT_TOKEN`
   - `ADMIN_ID_MAIKOL`
   - `ADMIN_ID_COLTON`
5. Railway will auto-detect Python and run `python bot.py`
   - If it doesn't, add a `Procfile`:
     ```
     worker: python bot.py
     ```
6. Deploy — the bot will start automatically

> **Note:** Railway's free tier includes 500 hours/month. For a persistent bot, consider the $5/month Hobby plan.

### Persistent database on Railway

SQLite writes to the local filesystem, which resets on redeploy. For production persistence, either:
- Use Railway's **Volume** feature (mount at `/app/submissions.db`)
- Or migrate to PostgreSQL (Railway has a free Postgres plugin)
