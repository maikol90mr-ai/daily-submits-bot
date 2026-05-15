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
    "🔻": "Transamerica",
    "🏳️‍⚧️": "Transamerica",
    "🦅": "Americo",
    "♻️": "Ethos",
    "🐮": "Mutual of Omaha",
    "🐄": "Mutual of Omaha",
    "🪖": "Corebridge",
    "👑": "Royal Neighbors",
    "🚂": "American Amicable",
    "🗽": "Liberty Bankers",
}

CUSTOM_EMOJI_CARRIER_MAP: dict[str, str] = {}  # populated via !map command

# Carrier names agents may type out in text (case-insensitive, whole-word match)
TEXT_CARRIER_MAP: dict[str, str] = {
    # Aflac
    "aflac": "Aflac",
    # Transamerica
    "transamerica": "Transamerica",
    "trans": "Transamerica",
    "ta": "Transamerica",
    # Americo
    "americo": "Americo",
    # Ethos
    "ethos": "Ethos",
    # Mutual of Omaha
    "mutual of omaha": "Mutual of Omaha",
    "mutual": "Mutual of Omaha",
    "moo": "Mutual of Omaha",
    # Corebridge
    "corebridge": "Corebridge",
    "core": "Corebridge",
    "cb": "Corebridge",
    # Royal Neighbors
    "royal neighbors": "Royal Neighbors",
    "royal": "Royal Neighbors",
    "rn": "Royal Neighbors",
    # American Amicable
    "american amicable": "American Amicable",
    "amicable": "American Amicable",
    "am am": "American Amicable",
    "amam": "American Amicable",
    "aa": "American Amicable",
    # American Home Life
    "american home life": "American Home Life",
    "home life": "American Home Life",
    "ahl": "American Home Life",
    # Liberty Bankers
    "liberty bankers": "Liberty Bankers",
    "liberty": "Liberty Bankers",
    "lbl": "Liberty Bankers",
}

# Date pattern: M/D, MM/DD, M/D/YYYY, MM/DD/YYYY, M/DD, etc.
_DATE_PAT = r"\d{1,2}/\d{1,2}(?:/\d{2,4})?"


def _parse_deal_date(raw: Optional[str]) -> Optional[str]:
    """Convert a raw date string (M/D, MM/DD, M/D/YYYY, etc.) to ISO date, or return None."""
    if not raw:
        return None
    try:
        parts = raw.split("/")
        month, day = int(parts[0]), int(parts[1])
        year = int(parts[2]) if len(parts) > 2 else datetime.now(EASTERN).year
        if year < 100:
            year += 2000
        return date(year, month, day).isoformat()
    except (ValueError, TypeError, IndexError):
        return None


def _extract_date(text: str) -> Optional[str]:
    """Find the first date-like token in a short text snippet."""
    m = re.search(_DATE_PAT, text)
    return _parse_deal_date(m.group(0)) if m else None


def parse_submissions(content: str) -> list:
    """
    Return a list of (amount, carrier_name, deal_date) for every deal found.

    Handles:
    - Optional $ prefix:  '$780' or '780'
    - Carrier as emoji or typed name (case-insensitive)
    - Date before or after carrier: '5/15 🔺' or '🔺 5/15' or '5/15/2026'
    - No spaces between amount and emoji: '$663👑'
    - Multiple deals per message (one per line or separated by newlines)

    A line/segment must contain both an amount AND a carrier signal to count.
    """
    all_emoji = {**EMOJI_CARRIER_MAP, **CUSTOM_EMOJI_CARRIER_MAP}

    emoji_pat = "|".join(re.escape(e) for e in all_emoji)
    # Text carrier names, longest first so "mutual of omaha" beats "moo"
    text_names = sorted(TEXT_CARRIER_MAP, key=len, reverse=True)
    text_pat = "|".join(re.escape(n) for n in text_names)

    carrier_pat = f"({emoji_pat}|{text_pat})"

    # Amount: optional $, digits with optional commas, optional decimals, optional trailing period
    amount_pat = r"\$?\s*([\d,]+(?:\.\d{1,2})?)\s*\.?"

    # Full pattern:
    #   amount  [optional date]  carrier  [optional date]
    # OR
    #   [optional date]  carrier  [optional date]  amount  (unusual but possible)
    # We handle both by doing two passes.

    # Allow up to ~30 chars of filler words between amount and carrier
    # (no newlines, no other digits, no other $ — keeps it on one line and
    # avoids gobbling a second amount as filler).
    filler = r"(?:[^\n\r$0-9]{0,30}?)"

    # Pass 1: amount → carrier  (e.g. "$500 🔺 5/20", "751.68 jet royal", "$624 sent to UW AMAM")
    amount_first = re.compile(
        amount_pat +
        r"[ \t]*(?:(" + _DATE_PAT + r")[ \t]*)?" +
        filler +
        carrier_pat +
        r"(?:[ \t]*(" + _DATE_PAT + r"))?",
        re.IGNORECASE,
    )

    # Pass 2: carrier → amount  (e.g. "👑 $564 6/11")
    carrier_first = re.compile(
        carrier_pat +
        r"[ \t]*(?:(" + _DATE_PAT + r")[ \t]*)?" +
        filler +
        amount_pat +
        r"(?:[ \t]*(" + _DATE_PAT + r"))?",
        re.IGNORECASE,
    )

    results = []
    seen_spans = []

    def _overlaps(start, end):
        return any(s < end and start < e for s, e in seen_spans)

    # Pass 1
    for m in amount_first.finditer(content):
        raw_amount = m.group(1)
        date_before = m.group(2)
        carrier_token = m.group(3)
        date_after = m.group(4)
        start, end = m.span()
        if _overlaps(start, end):
            continue
        try:
            amount = float(raw_amount.replace(",", ""))
        except ValueError:
            continue
        if carrier_token in all_emoji:
            carrier = all_emoji[carrier_token]
        else:
            carrier = TEXT_CARRIER_MAP.get(carrier_token.lower(), carrier_token.title())
        raw_date = date_before or date_after
        deal_date = _parse_deal_date(raw_date) if raw_date else None
        results.append((amount, carrier, deal_date))
        seen_spans.append((start, end))

    # Pass 2 — carrier first
    for m in carrier_first.finditer(content):
        carrier_token = m.group(1)
        date_before = m.group(2)
        raw_amount = m.group(3)
        date_after = m.group(4)
        start, end = m.span()
        if _overlaps(start, end):
            continue
        try:
            amount = float(raw_amount.replace(",", ""))
        except ValueError:
            continue
        if carrier_token in all_emoji:
            carrier = all_emoji[carrier_token]
        else:
            carrier = TEXT_CARRIER_MAP.get(carrier_token.lower(), carrier_token.title())
        raw_date = date_before or date_after
        deal_date = _parse_deal_date(raw_date) if raw_date else None
        results.append((amount, carrier, deal_date))
        seen_spans.append((start, end))

    return results


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

    await message.add_reaction("🥷")


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


async def send_long(ctx: commands.Context, text: str, limit: int = 1990) -> None:
    if len(text) <= limit:
        await ctx.send(text)
        return
    buf = ""
    for line in text.split("\n"):
        if len(buf) + len(line) + 1 > limit:
            if buf:
                await ctx.send(buf)
            buf = line
        else:
            buf = f"{buf}\n{line}" if buf else line
    if buf:
        await ctx.send(buf)


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
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
    today = datetime.now(EASTERN).date().isoformat()
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
    await send_long(ctx, msg)


# --- !week ---

@bot.command(name="week")
@stats_only()
async def cmd_week(ctx: commands.Context):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
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
    await send_long(ctx, build_leaderboard(rows, f"Weekly Leaderboard ({start} → {end})"))


# --- !month ---

@bot.command(name="month")
@stats_only()
async def cmd_month(ctx: commands.Context):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
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
    await send_long(ctx, build_leaderboard(rows, f"Month-to-Date Leaderboard ({start} → {end})"))


# --- !stats @agent ---

@bot.command(name="stats")
@stats_only()
async def cmd_stats(ctx: commands.Context, *, query: str = None):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
    if query is None:
        await ctx.send("Usage: `!stats @agent` or `!stats firstname`")
        return

    # Try resolving as a mention first, then fall back to name search in DB
    member = None
    try:
        member = await commands.MemberConverter().convert(ctx, query.strip())
        discord_id = str(member.id)
        display_name = member.display_name
    except commands.MemberNotFound:
        # Search by name in submissions table
        with get_conn() as conn:
            row = conn.execute(
                "SELECT discord_id, username FROM submissions WHERE LOWER(username) LIKE %s AND deleted=0 LIMIT 1",
                (f"%{query.lower()}%",),
            ).fetchone()
        if not row:
            await ctx.send(f"⚠️ No submissions found for `{query}`.")
            return
        discord_id = row["discord_id"]
        display_name = row["username"]

    with get_conn() as conn:
        row = conn.execute(
            "SELECT SUM(ap_amount) as total, COUNT(*) as deals FROM submissions WHERE discord_id=%s AND deleted=0",
            (discord_id,),
        ).fetchone()
    total = row["total"] or 0
    deals = row["deals"] or 0
    await ctx.send(f"**{display_name}'s Stats**\nTotal AP: {fmt_money(total)} | Deals: {deals}")


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
    await send_long(ctx, build_leaderboard(rows, "All-Time Leaderboard"))


# --- !carriers (admin only) ---

@bot.command(name="carriers")
@stats_only()
async def cmd_carriers(ctx: commands.Context, period: Optional[str] = None):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return

    period = (period or "all").lower()
    today = datetime.now(EASTERN).date()

    if period in ("day", "daily", "today"):
        start = end = today.isoformat()
        label = f"Today ({start})"
    elif period in ("week", "weekly"):
        start, end = week_bounds()
        label = f"This Week ({start} → {end})"
    elif period in ("month", "monthly"):
        start, end = month_bounds()
        label = f"This Month ({start} → {end})"
    elif period in ("all", "alltime", "all-time"):
        start = end = None
        label = "All Time"
    else:
        await ctx.send("Usage: `!carriers [daily|weekly|monthly|all]`")
        return

    with get_conn() as conn:
        if start and end:
            rows = conn.execute(
                """
                SELECT carriers, SUM(ap_amount) as total, COUNT(*) as deals
                FROM submissions
                WHERE deleted=0 AND substr(posted_at,1,10) BETWEEN %s AND %s
                GROUP BY carriers ORDER BY total DESC
                """,
                (start, end),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT carriers, SUM(ap_amount) as total, COUNT(*) as deals
                FROM submissions WHERE deleted=0
                GROUP BY carriers ORDER BY total DESC
                """
            ).fetchall()

    if not rows:
        await ctx.send(f"**Team AP by Carrier — {label}**\nNo data yet.")
        return
    lines = [f"**Team AP by Carrier — {label}**"]
    for row in rows:
        lines.append(f"• {row['carriers']} — {fmt_money(row['total'])} ({row['deals']} deal{'s' if row['deals'] != 1 else ''})")
    await send_long(ctx, "\n".join(lines))


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
    await send_long(ctx, _build_effective_list(rows, "⏳ All Agents — Pending Effective Dates"))


# --- !log (admin only) — manually add a submission ---

@bot.command(name="log")
@stats_only()
async def cmd_log(ctx: commands.Context, *, args: Optional[str] = None):
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
    if not args:
        await ctx.send(
            "Usage: `!log @agent $amount <carrier> [M/D]`\n"
            "Example: `!log @Matt $564 royal 6/11`"
        )
        return

    # Parse the member mention first
    parts = args.split(None, 1)
    if len(parts) < 2:
        await ctx.send("Usage: `!log @agent $amount <carrier> [M/D]`")
        return
    try:
        member = await commands.MemberConverter().convert(ctx, parts[0])
    except commands.MemberNotFound:
        await ctx.send(f"⚠️ Member not found: {parts[0]}")
        return

    rest = parts[1]
    deals = parse_submissions(rest)
    if not deals:
        await ctx.send(
            "⚠️ Couldn't parse a `$amount + carrier` from that. "
            "Try: `!log @agent $500 royal 6/11`"
        )
        return

    posted_at = datetime.now(EASTERN).isoformat()
    # Use a synthetic message_id so it doesn't collide with real Discord IDs
    synthetic_id = f"manual_{int(datetime.now(EASTERN).timestamp())}"

    logged = []
    for i, (amount, carrier, deal_date) in enumerate(deals):
        insert_submission(
            discord_id=str(member.id),
            username=member.display_name,
            ap_amount=amount,
            carriers=[carrier],
            deal_date=deal_date,
            posted_at=posted_at,
            raw_message=f"[manual entry by {ctx.author.display_name}] {rest}",
            message_id=f"{synthetic_id}_{i}",
        )
        date_str = f" — effective {_fmt_effective_date(deal_date)}" if deal_date else ""
        logged.append(f"{fmt_money(amount)} {carrier}{date_str}")

    await ctx.send(
        f"✅ Logged for **{member.display_name}**:\n" +
        "\n".join(f"• {line}" for line in logged)
    )


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
    if not is_admin(ctx):
        await ctx.send("⛔ Admin only.")
        return
    msg = (
        "**Daily Submits Bot — Admin Commands**\n"
        "`!daily` — team total AP + leaderboard for today\n"
        "`!week` — full team leaderboard this week\n"
        "`!month` — full team leaderboard this month\n"
        "`!top` — all-time leaderboard\n"
        "`!carriers [daily|weekly|monthly|all]` — team AP by carrier (default: all)\n"
        "`!allpending` — all agents' pending effective dates\n"
        "`!stats <name>` — any agent's breakdown\n\n"
        "`!log @agent $amount <carrier> [M/D]` — manually add a submission\n"
        "`!delete <link>` — remove a submission\n"
        "`!fix @agent $amount` — correct a logged AP amount\n"
        "`!map <emoji> <CarrierName>` — add a carrier emoji\n"
        "`!wipedata CONFIRM` — wipe all submissions\n"
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
