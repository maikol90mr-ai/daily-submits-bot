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

# Admin Discord user IDs — comma-separated in ADMIN_USER_IDS env var
ADMIN_IDS: set[str] = {
    uid.strip()
    for uid in os.getenv("ADMIN_USER_IDS", "").split(",")
    if uid.strip()
}

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


def _parse_deal_date(raw: Optional[str]) -> Optional[str]:
    """Convert a raw 'M/D' or 'MM/DD' string to an ISO date, or return None."""
    if not raw:
        return None
    try:
        month, day = (int(p) for p in raw.split("/"))
        return date(datetime.now(EASTERN).year, month, day).isoformat()
    except (ValueError, TypeError):
        return None


def parse_submissions(content: str) -> list:
    """
    Return a list of (amount, carrier_name, deal_date) for every
    $amount + carrier emoji pair found in the message.
    The date (M/D or MM/DD) is captured immediately after the carrier emoji.
    Any $amount without a directly following carrier emoji is ignored.
    """
    all_carriers = {**EMOJI_CARRIER_MAP, **CUSTOM_EMOJI_CARRIER_MAP}
    emoji_pattern = "|".join(re.escape(e) for e in all_carriers)
    pattern = (
        r"\$\s*([\d,]+(?:\.\d{1,2})?)"   # group 1: amount
        r"\s{0,5}(" + emoji_pattern + ")" # group 2: carrier emoji
        r"(?:\s+(\d{1,2}/\d{1,2}))?"      # group 3: optional date after emoji
    )
    return [
        (
            float(m.group(1).replace(",", "")),
            all_carriers[m.group(2)],
            _parse_deal_date(m.group(3)),
        )
        for m in re.finditer(pattern, content)
    ]


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
    return str(ctx.author.id) in ADMIN_IDS


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

    posted_at = datetime.now(EASTERN).isoformat()

    for i, (amount, carrier, deal_date) in enumerate(deals):
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


def _fmt_effective_date(iso: str) -> str:
    d = date.fromisoformat(iso)
    return f"{d.month}/{d.day}"


def _build_effective_list(rows, title: str) -> str:
    if not rows:
        return f"**{title}**\nNo deals found."
    lines = [f"**{title}**"]
    for row in rows:
        lines.append(
            f"- {row['username']} — {fmt_money(row['ap_amount'])} {row['carriers']} — effective {_fmt_effective_date(row['deal_date'])}"
        )
    return "\n".join(lines)


# --- !daily ---

@bot.command(name="daily")
@stats_only()
async def cmd_daily(ctx: commands.Context):
    today = datetime.now(EASTERN).date().isoformat()
    if is_admin(ctx):
        with get_conn() as conn:
            summary = conn.execute(
                "SELECT SUM(ap_amount) as total, COUNT(*) as deals FROM submissions WHERE substr(posted_at,1,10)=%s AND deleted=0",
                (today,),
            ).fetchone()
            rows = conn.execute(
                """
                SELECT username, SUM(ap_amount) as total, COUNT(*) as deals
                FROM submissions
                WHERE substr(posted_at,1,10)=%s AND deleted=0
                GROUP BY discord_id, username ORDER BY total DESC
                """,
                (today,),
            ).fetchall()
        total = summary["total"] or 0
        deals = summary["deals"] or 0
        msg = f"**Team Daily ({today})**\nTotal AP: {fmt_money(total)} | Deals: {deals}\n\n"
        msg += build_leaderboard(rows, "Today's Leaderboard")
    else:
        discord_id = str(ctx.author.id)
        with get_conn() as conn:
            row = conn.execute(
                "SELECT SUM(ap_amount) as total, COUNT(*) as deals FROM submissions WHERE discord_id=%s AND substr(posted_at,1,10)=%s AND deleted=0",
                (discord_id, today),
            ).fetchone()
        total = row["total"] or 0
        deals = row["deals"] or 0
        msg = f"**Your Daily ({today})**\nTotal AP: {fmt_money(total)} | Deals: {deals}"
    await ctx.send(msg)


# --- !week ---

@bot.command(name="week")
@stats_only()
async def cmd_week(ctx: commands.Context):
    start, end = week_bounds()
    if is_admin(ctx):
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
    else:
        discord_id = str(ctx.author.id)
        with get_conn() as conn:
            row = conn.execute(
                """
                SELECT SUM(ap_amount) as total, COUNT(*) as deals FROM submissions
                WHERE discord_id=%s AND substr(posted_at,1,10) BETWEEN %s AND %s AND deleted=0
                """,
                (discord_id, start, end),
            ).fetchone()
        total = row["total"] or 0
        deals = row["deals"] or 0
        await ctx.send(f"**Your Week ({start} → {end})**\nTotal AP: {fmt_money(total)} | Deals: {deals}")


# --- !month ---

@bot.command(name="month")
@stats_only()
async def cmd_month(ctx: commands.Context):
    start, end = month_bounds()
    if is_admin(ctx):
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
    else:
        discord_id = str(ctx.author.id)
        with get_conn() as conn:
            row = conn.execute(
                """
                SELECT SUM(ap_amount) as total, COUNT(*) as deals FROM submissions
                WHERE discord_id=%s AND substr(posted_at,1,10) BETWEEN %s AND %s AND deleted=0
                """,
                (discord_id, start, end),
            ).fetchone()
        total = row["total"] or 0
        deals = row["deals"] or 0
        await ctx.send(f"**Your Month ({start} → {end})**\nTotal AP: {fmt_money(total)} | Deals: {deals}")


# --- !me ---

@bot.command(name="me")
@stats_only()
async def cmd_me(ctx: commands.Context):
    discord_id = str(ctx.author.id)
    with get_conn() as conn:
        summary = conn.execute(
            "SELECT SUM(ap_amount) as total, COUNT(*) as deals FROM submissions WHERE discord_id=%s AND deleted=0",
            (discord_id,),
        ).fetchone()
        carrier_rows = conn.execute(
            """
            SELECT carriers, SUM(ap_amount) as total, COUNT(*) as deals
            FROM submissions WHERE discord_id=%s AND deleted=0
            GROUP BY carriers ORDER BY total DESC
            """,
            (discord_id,),
        ).fetchall()
    total = summary["total"] or 0
    deals = summary["deals"] or 0
    lines = [f"**{ctx.author.display_name}'s All-Time Stats**",
             f"Total AP: {fmt_money(total)} | Deals: {deals}"]
    if carrier_rows:
        lines.append("**By Carrier:**")
        for r in carrier_rows:
            lines.append(f"• {r['carriers']} — {fmt_money(r['total'])} ({r['deals']} deal{'s' if r['deals'] != 1 else ''})")
    await ctx.send("\n".join(lines))


# --- !stats @agent ---

@bot.command(name="stats")
@stats_only()
async def cmd_stats(ctx: commands.Context, member: discord.Member = None):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
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


# --- !top (admin only) ---

@bot.command(name="top")
@stats_only()
async def cmd_top(ctx: commands.Context):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT username, SUM(ap_amount) as total, COUNT(*) as deals
            FROM submissions WHERE deleted=0
            GROUP BY discord_id, username ORDER BY total DESC
            """
        ).fetchall()
    await ctx.send(build_leaderboard(rows, "All-Time Leaderboard"))


# --- !carriers (admin only) ---

@bot.command(name="carriers")
@stats_only()
async def cmd_carriers(ctx: commands.Context):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
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


# --- !upcoming (own deals for agents, own deals for admins too — use !allupcoming for team) ---

@bot.command(name="upcoming")
@stats_only()
async def cmd_upcoming(ctx: commands.Context):
    today = datetime.now(EASTERN).date()
    end = (today + timedelta(days=7)).isoformat()
    today_iso = today.isoformat()
    discord_id = str(ctx.author.id)
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT username, ap_amount, carriers, deal_date
            FROM submissions
            WHERE discord_id=%s AND deal_date BETWEEN %s AND %s AND deleted=0
            ORDER BY deal_date ASC
            """,
            (discord_id, today_iso, end),
        ).fetchall()
    await ctx.send(_build_effective_list(rows, "📅 Your Upcoming Effective Dates (Next 7 Days)"))


# --- !pending (own deals only) ---

@bot.command(name="pending")
@stats_only()
async def cmd_pending(ctx: commands.Context):
    today_iso = datetime.now(EASTERN).date().isoformat()
    discord_id = str(ctx.author.id)
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT username, ap_amount, carriers, deal_date
            FROM submissions
            WHERE discord_id=%s AND deal_date > %s AND deleted=0
            ORDER BY deal_date ASC
            """,
            (discord_id, today_iso),
        ).fetchall()
    await ctx.send(_build_effective_list(rows, "⏳ Your Pending Effective Dates"))


# --- !effective today / !effective @agent ---

@bot.command(name="effective")
@stats_only()
async def cmd_effective(ctx: commands.Context, *, arg: Optional[str] = None):
    if not arg:
        await ctx.send("Usage: `!effective today` or `!effective @agent`")
        return
    if arg.strip().lower() == "today":
        today_iso = datetime.now(EASTERN).date().isoformat()
        discord_id = str(ctx.author.id)
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT username, ap_amount, carriers, deal_date
                FROM submissions
                WHERE discord_id=%s AND deal_date = %s AND deleted=0
                ORDER BY deal_date ASC
                """,
                (discord_id, today_iso),
            ).fetchall()
        await ctx.send(_build_effective_list(rows, f"📅 Your Deals Effective Today ({today_iso})"))
    else:
        try:
            member = await commands.MemberConverter().convert(ctx, arg.strip())
        except commands.MemberNotFound:
            await ctx.send("Usage: `!effective today` or `!effective @agent`")
            return
        today_iso = datetime.now(EASTERN).date().isoformat()
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT username, ap_amount, carriers, deal_date
                FROM submissions
                WHERE discord_id=%s AND deal_date >= %s AND deleted=0
                ORDER BY deal_date ASC
                """,
                (str(member.id), today_iso),
            ).fetchall()
        await ctx.send(_build_effective_list(rows, f"📅 {member.display_name}'s Upcoming Effective Dates"))


# --- !allupcoming (admin only) ---

@bot.command(name="allupcoming")
@stats_only()
async def cmd_allupcoming(ctx: commands.Context):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
    today = datetime.now(EASTERN).date()
    end = (today + timedelta(days=7)).isoformat()
    today_iso = today.isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT username, ap_amount, carriers, deal_date
            FROM submissions
            WHERE deal_date BETWEEN %s AND %s AND deleted=0
            ORDER BY deal_date ASC
            """,
            (today_iso, end),
        ).fetchall()
    await ctx.send(_build_effective_list(rows, "📅 All Upcoming Effective Dates (Next 7 Days)"))


# --- !allpending (admin only) ---

@bot.command(name="allpending")
@stats_only()
async def cmd_allpending(ctx: commands.Context):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
    today_iso = datetime.now(EASTERN).date().isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT username, ap_amount, carriers, deal_date
            FROM submissions
            WHERE deal_date > %s AND deleted=0
            ORDER BY deal_date ASC
            """,
            (today_iso,),
        ).fetchall()
    await ctx.send(_build_effective_list(rows, "⏳ All Agents — Pending Effective Dates"))


# --- !wipedata (admin only) ---

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


# --- !delete (admin only) ---

@bot.command(name="delete")
@stats_only()
async def cmd_delete(ctx: commands.Context, message_link: Optional[str] = None):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
    if not message_link:
        await ctx.send("Usage: `!delete <message_link>`")
        return
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


# --- !fix (admin only) ---

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


# --- !map (admin only) ---

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


# --- !help ---

@bot.command(name="help")
@stats_only()
async def cmd_help(ctx: commands.Context):
    if is_admin(ctx):
        msg = (
            "**Daily Submits Bot — Admin Commands**\n"
            "`!daily` — team total AP + deal count + leaderboard for today\n"
            "`!week` — full team leaderboard this week\n"
            "`!month` — full team leaderboard this month\n"
            "`!top` — all-time leaderboard\n"
            "`!carriers` — team AP split by carrier\n"
            "`!allupcoming` — all agents' effective dates in next 7 days\n"
            "`!allpending` — all agents' pending effective dates\n"
            "`!me` — your own all-time stats\n"
            "`!stats @agent` — any agent's breakdown\n"
            "`!effective @agent` — an agent's upcoming effective dates\n\n"
            "`!delete <link>` — remove a submission\n"
            "`!fix @agent $amount` — correct a logged AP amount\n"
            "`!map <emoji> <CarrierName>` — add a carrier emoji\n"
            "`!wipedata CONFIRM` — wipe all submissions\n"
        )
    else:
        msg = (
            "**Daily Submits Bot Commands**\n"
            "`!daily` — your AP + deal count for today\n"
            "`!week` — your AP + deal count this week\n"
            "`!month` — your AP + deal count this month\n"
            "`!me` — your all-time stats + carrier breakdown\n"
            "`!upcoming` — your effective dates in the next 7 days\n"
            "`!pending` — your pending effective dates\n"
            "`!effective today` — your deals going effective today\n"
            "`!effective @agent` — an agent's upcoming effective dates\n"
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
