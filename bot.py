import os
import re
import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import discord
import psycopg2
import psycopg2.extras
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
SUBMIT_CHANNEL = "daily-submits"
STATS_CHANNEL = "bot-stats"
EASTERN = ZoneInfo("America/New_York")

# Admin Discord user IDs — replace with real IDs in .env
def _parse_id(val: Optional[str]) -> int:
    try:
        return int(val or "0")
    except ValueError:
        return 0

ADMIN_IDS = {
    _parse_id(os.getenv("ADMIN_ID_MAIKOL")),
    _parse_id(os.getenv("ADMIN_ID_COLTON")),
} - {0}

# ---------------------------------------------------------------------------
# Carrier detection
# ---------------------------------------------------------------------------

EMOJI_CARRIER_MAP = {
    "🦆": "Aflac",
    "🏠": "American Home Life",
    "🏡": "American Home Life",
    "🔺": "Transamerica",
    "🏳️‍⚧️": "Transamerica",
    "🦅": "Americo",
    "♻️": "Ethos",
    "🐮": "Mutual of Omaha",
    "🐄": "Mutual of Omaha",
    "🪖": "Corebridge",
    "👑": "Royal Neighbors",
    "🚂": "American Amicable",
}

CUSTOM_EMOJI_CARRIER_MAP: dict[str, str] = {}  # populated via !map command


def parse_submissions(content: str) -> list:
    """
    Return a list of (amount, carrier_name) for every $amount + carrier emoji
    pair found in the message. Pairs must have the carrier emoji within ~5
    characters of the amount. Any $amount without a directly following carrier
    emoji is ignored.
    """
    all_carriers = {**EMOJI_CARRIER_MAP, **CUSTOM_EMOJI_CARRIER_MAP}
    emoji_pattern = "|".join(re.escape(e) for e in all_carriers)
    pattern = r"\$\s*([\d,]+(?:\.\d{1,2})?)\s{0,5}(" + emoji_pattern + ")"
    return [
        (float(m.group(1).replace(",", "")), all_carriers[m.group(2)])
        for m in re.finditer(pattern, content)
    ]


def extract_date(content: str) -> Optional[str]:
    match = re.search(r"\b(\d{1,2})[/\-](\d{1,2})\b", content)
    if not match:
        return None
    month, day = int(match.group(1)), int(match.group(2))
    year = datetime.now(EASTERN).year
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DATABASE_URL = os.getenv("DATABASE_URL")


class _DBConn:
    """Thin wrapper so `with get_conn() as conn: conn.execute(sql, params).fetch*()` works uniformly."""

    def __init__(self, conn):
        self._conn = conn
        self._cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def execute(self, sql, params=None):
        self._cur.execute(sql, params)
        return self._cur

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if exc_type:
            self._conn.rollback()
        else:
            self._conn.commit()
        self._cur.close()
        self._conn.close()


def get_conn() -> _DBConn:
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return _DBConn(conn)
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        raise


def init_db():
    try:
        with get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS submissions (
                    id          SERIAL PRIMARY KEY,
                    discord_id  TEXT NOT NULL,
                    username    TEXT NOT NULL,
                    ap_amount   REAL NOT NULL,
                    carriers    TEXT NOT NULL,
                    deal_date   TEXT,
                    posted_at   TEXT NOT NULL,
                    raw_message TEXT NOT NULL,
                    message_id  TEXT UNIQUE,
                    deleted     INTEGER NOT NULL DEFAULT 0
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS carrier_map (
                    emoji       TEXT PRIMARY KEY,
                    carrier     TEXT NOT NULL
                )
            """)
            # Load persisted custom emoji maps
            rows = conn.execute("SELECT emoji, carrier FROM carrier_map").fetchall()
            for row in rows:
                CUSTOM_EMOJI_CARRIER_MAP[row["emoji"]] = row["carrier"]
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
        raise


def insert_submission(
    discord_id: str,
    username: str,
    ap_amount: float,
    carriers: list[str],
    deal_date: Optional[str],
    posted_at: str,
    raw_message: str,
    message_id: str,
):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO submissions
                (discord_id, username, ap_amount, carriers, deal_date, posted_at, raw_message, message_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (message_id) DO NOTHING
            """,
            (
                discord_id,
                username,
                ap_amount,
                ", ".join(carriers) if carriers else "Unknown",
                deal_date,
                posted_at,
                raw_message,
                message_id,
            ),
        )


def delete_submission_by_message_id(message_id: str) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE submissions SET deleted=1 WHERE message_id LIKE %s AND deleted=0",
            (f"{message_id}_%",),
        )
        return cur.rowcount > 0


def fix_submission(discord_id: str, ap_amount: float) -> bool:
    """Update the most recent non-deleted submission for a user."""
    with get_conn() as conn:
        cur = conn.execute(
            """
            UPDATE submissions SET ap_amount=%s
            WHERE id=(
                SELECT id FROM submissions
                WHERE discord_id=%s AND deleted=0
                ORDER BY posted_at DESC LIMIT 1
            )
            """,
            (ap_amount, discord_id),
        )
        return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def fmt_money(amount: float) -> str:
    return f"${amount:,.2f}"


def week_bounds() -> tuple[str, str]:
    today = datetime.now(EASTERN).date()
    start = today - timedelta(days=today.weekday())  # Monday
    end = today
    return start.isoformat(), end.isoformat()


def month_bounds() -> tuple[str, str]:
    today = datetime.now(EASTERN).date()
    start = today.replace(day=1)
    return start.isoformat(), today.isoformat()


def build_leaderboard(rows, title: str) -> str:
    if not rows:
        return f"**{title}**\nNo data yet."
    lines = [f"**{title}**"]
    medals = ["🥇", "🥈", "🥉"]
    for i, row in enumerate(rows):
        medal = medals[i] if i < 3 else f"{i+1}."
        lines.append(f"{medal} **{row['username']}** — {fmt_money(row['total'])} ({row['deals']} deal{'s' if row['deals'] != 1 else ''})")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Bot setup
# ---------------------------------------------------------------------------

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)


def is_stats_channel(ctx: commands.Context) -> bool:
    return ctx.channel.name == STATS_CHANNEL


def is_admin(ctx: commands.Context) -> bool:
    return ctx.author.id in ADMIN_IDS


# ---------------------------------------------------------------------------
# Message parsing
# ---------------------------------------------------------------------------

@bot.event
async def on_ready():
    init_db()
    print(f"Logged in as {bot.user} ({bot.user.id})")


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    if message.channel.name == SUBMIT_CHANNEL:
        await handle_submission(message)

    await bot.process_commands(message)


async def handle_submission(message: discord.Message):
    content = message.content
    deals = parse_submissions(content)
    if not deals:
        return  # silently ignore — no $amount + carrier emoji pair found

    deal_date = extract_date(content)
    posted_at = datetime.now(EASTERN).isoformat()

    for i, (amount, carrier) in enumerate(deals):
        insert_submission(
            discord_id=str(message.author.id),
            username=message.author.display_name,
            ap_amount=amount,
            carriers=[carrier],
            deal_date=deal_date,
            posted_at=posted_at,
            raw_message=content,
            message_id=f"{message.id}_{i}",
        )

    await message.add_reaction("🤖")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def stats_only():
    async def predicate(ctx: commands.Context):
        if not is_stats_channel(ctx):
            await ctx.send(f"⚠️ Commands only work in **#{STATS_CHANNEL}**.")
            return False
        return True
    return commands.check(predicate)


@bot.command(name="recap")
@stats_only()
async def cmd_recap(ctx: commands.Context):
    today = datetime.now(EASTERN).date().isoformat()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT SUM(ap_amount) as total, COUNT(*) as deals FROM submissions WHERE substr(posted_at,1,10)=%s AND deleted=0",
            (today,),
        ).fetchone()
    total = row["total"] or 0
    deals = row["deals"] or 0
    await ctx.send(f"**Today's Recap ({today})**\nTotal AP: {fmt_money(total)} | Deals: {deals}")


@bot.command(name="week")
@stats_only()
async def cmd_week(ctx: commands.Context):
    start, end = week_bounds()
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT username, SUM(ap_amount) as total, COUNT(*) as deals
            FROM submissions
            WHERE substr(posted_at,1,10) BETWEEN %s AND %s AND deleted=0
            GROUP BY discord_id, username ORDER BY total DESC
            """,
            (start, end),
        ).fetchall()
    await ctx.send(build_leaderboard(rows, f"Weekly Leaderboard ({start} → {end})"))


@bot.command(name="month")
@stats_only()
async def cmd_month(ctx: commands.Context):
    start, end = month_bounds()
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT username, SUM(ap_amount) as total, COUNT(*) as deals
            FROM submissions
            WHERE substr(posted_at,1,10) BETWEEN %s AND %s AND deleted=0
            GROUP BY discord_id, username ORDER BY total DESC
            """,
            (start, end),
        ).fetchall()
    await ctx.send(build_leaderboard(rows, f"Month-to-Date Leaderboard ({start} → {end})"))


@bot.command(name="me")
@stats_only()
async def cmd_me(ctx: commands.Context):
    discord_id = str(ctx.author.id)
    with get_conn() as conn:
        row = conn.execute(
            "SELECT SUM(ap_amount) as total, COUNT(*) as deals FROM submissions WHERE discord_id=%s AND deleted=0",
            (discord_id,),
        ).fetchone()
    total = row["total"] or 0
    deals = row["deals"] or 0
    await ctx.send(f"**{ctx.author.display_name}'s Stats**\nTotal AP: {fmt_money(total)} | Deals: {deals}")


@bot.command(name="stats")
@stats_only()
async def cmd_stats(ctx: commands.Context, member: discord.Member = None):
    if member is None:
        await ctx.send("Usage: `!stats @agent`")
        return
    discord_id = str(member.id)
    with get_conn() as conn:
        row = conn.execute(
            "SELECT SUM(ap_amount) as total, COUNT(*) as deals FROM submissions WHERE discord_id=%s AND deleted=0",
            (discord_id,),
        ).fetchone()
    total = row["total"] or 0
    deals = row["deals"] or 0
    await ctx.send(f"**{member.display_name}'s Stats**\nTotal AP: {fmt_money(total)} | Deals: {deals}")


@bot.command(name="carriers")
@stats_only()
async def cmd_carriers(ctx: commands.Context):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT carriers, SUM(ap_amount) as total, COUNT(*) as deals FROM submissions WHERE deleted=0 GROUP BY carriers ORDER BY total DESC"
        ).fetchall()
    if not rows:
        await ctx.send("No data yet.")
        return
    lines = ["**Team AP by Carrier (All Time)**"]
    for row in rows:
        lines.append(f"• {row['carriers']} — {fmt_money(row['total'])} ({row['deals']} deal{'s' if row['deals'] != 1 else ''})")
    await ctx.send("\n".join(lines))


@bot.command(name="top")
@stats_only()
async def cmd_top(ctx: commands.Context):
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT username, SUM(ap_amount) as total, COUNT(*) as deals
            FROM submissions WHERE deleted=0
            GROUP BY discord_id, username ORDER BY total DESC
            """
        ).fetchall()
    await ctx.send(build_leaderboard(rows, "All-Time Leaderboard"))


@bot.command(name="wipedata")
@stats_only()
async def cmd_wipedata(ctx: commands.Context, confirm: Optional[str] = None):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
    if confirm != "CONFIRM":
        await ctx.send("⚠️ This will delete **all submissions**. Type `!wipedata CONFIRM` to proceed.")
        return
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM submissions")
        count = cur.rowcount
    await ctx.send(f"🗑️ Wiped {count} row{'s' if count != 1 else ''} from submissions.")


@bot.command(name="delete")
@stats_only()
async def cmd_delete(ctx: commands.Context, message_link: Optional[str] = None):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
    if not message_link:
        await ctx.send("Usage: `!delete <message_link>`")
        return
    # Extract message ID from link: .../channels/guild/channel/message_id
    parts = message_link.rstrip("/").split("/")
    if not parts:
        await ctx.send("Invalid message link.")
        return
    message_id = parts[-1]
    success = delete_submission_by_message_id(message_id)
    if success:
        await ctx.send(f"✅ Submission `{message_id}` marked as deleted.")
    else:
        await ctx.send(f"⚠️ No active submission found for message ID `{message_id}`.")


@bot.command(name="fix")
@stats_only()
async def cmd_fix(ctx: commands.Context, member: Optional[discord.Member] = None, amount_str: Optional[str] = None):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
    if member is None or amount_str is None:
        await ctx.send("Usage: `!fix @agent $amount`")
        return
    assert member is not None and amount_str is not None
    match = re.match(r"\$?([\d,]+(?:\.\d{1,2})?)", amount_str)
    if not match:
        await ctx.send("Invalid amount. Use format: `$780` or `$1,234.56`")
        return
    amount = float(match.group(1).replace(",", ""))
    success = fix_submission(str(member.id), amount)
    if success:
        await ctx.send(f"✅ Updated {member.display_name}'s most recent submission to {fmt_money(amount)}.")
    else:
        await ctx.send(f"⚠️ No submission found for {member.display_name}.")


@bot.command(name="map")
@stats_only()
async def cmd_map(ctx: commands.Context, emoji: Optional[str] = None, *, carrier_name: Optional[str] = None):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
    if emoji is None or carrier_name is None:
        await ctx.send("Usage: `!map <emoji> <CarrierName>`")
        return
    assert emoji is not None and carrier_name is not None
    CUSTOM_EMOJI_CARRIER_MAP[emoji] = carrier_name
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO carrier_map (emoji, carrier) VALUES (%s, %s) ON CONFLICT (emoji) DO UPDATE SET carrier = EXCLUDED.carrier",
            (emoji, carrier_name),
        )
    await ctx.send(f"✅ Mapped {emoji} → **{carrier_name}**")


@bot.command(name="help")
@stats_only()
async def cmd_help(ctx: commands.Context):
    msg = (
        "**Daily Submits Bot Commands** *(use in #bot-stats)*\n"
        "`!recap` — today's team total AP + deal count\n"
        "`!week` — this week's leaderboard\n"
        "`!month` — month-to-date leaderboard\n"
        "`!me` — your personal stats\n"
        "`!stats @agent` — any agent's breakdown\n"
        "`!carriers` — team AP split by carrier\n"
        "`!top` — all-time leaderboard\n\n"
        "**Admin only:**\n"
        "`!delete <message_link>` — remove a submission\n"
        "`!fix @agent $amount` — correct a submission amount\n"
        "`!map <emoji> <CarrierName>` — add a new carrier emoji\n"
    )
    await ctx.send(msg)


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

@bot.event
async def on_command_error(ctx: commands.Context, error):
    if isinstance(error, commands.CheckFailure):
        pass  # already handled in predicate
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(f"⚠️ Member not found: {error.argument}")
    elif isinstance(error, commands.CommandNotFound):
        pass
    else:
        await ctx.send(f"⚠️ Error: {error}")
        raise error


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
    bot.run(TOKEN)
