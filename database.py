import aiosqlite
import asyncio
import hashlib
from datetime import datetime, timedelta

_db = None
_db_lock = asyncio.Lock()


async def get_db():
    global _db
    if _db is None:
        _db = await aiosqlite.connect("messages.db")
        await _db.execute("PRAGMA journal_mode=WAL;")
    return _db


async def init_database():
    db = await get_db()
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            user_id INTEGER,
            sender TEXT,
            message TEXT,
            timestamp TEXT,
            channel TEXT,
            hash TEXT UNIQUE
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS pictures (
            sender TEXT,
            message TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS scraped_channels (
            channel_username TEXT PRIMARY KEY,
            last_scraped DATETIME
        )
        """
    )
    await db.commit()


async def add_message_to_database(
    user_id, sender, message, timestamp=None, channel=None
):
    db = await get_db()
    msg_hash = hashlib.sha256(
        f"{user_id}:{sender}:{message}:{channel}".encode()
    ).hexdigest()
    try:
        await db.execute(
            "INSERT INTO messages (user_id, sender, message, timestamp, channel, hash) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, sender, message, timestamp, channel, msg_hash),
        )
        await db.commit()
    except aiosqlite.IntegrityError:
        pass  # Duplicate message, ignore


async def add_picture_to_database(sender, message):
    db = await get_db()
    await db.execute(
        "INSERT INTO pictures (sender, message) VALUES (?, ?)", (sender, message)
    )
    await db.commit()


async def add_scraped_channel(channel_username):
    db = await get_db()
    now = datetime.utcnow().isoformat()
    await db.execute(
        "INSERT OR REPLACE INTO scraped_channels (channel_username, last_scraped) VALUES (?, ?)",
        (channel_username, now),
    )
    await db.commit()


async def can_scrape_channel(channel_username):
    db = await get_db()
    async with db.execute(
        "SELECT last_scraped FROM scraped_channels WHERE channel_username = ?",
        (channel_username,),
    ) as cursor:
        row = await cursor.fetchone()
        if not row or not row[0]:
            return True  # Never scraped
        last_scraped = datetime.fromisoformat(row[0])
        return datetime.utcnow() - last_scraped > timedelta(hours=24)


async def get_scraped_channels():
    db = await get_db()
    async with db.execute("SELECT channel_username FROM scraped_channels") as cursor:
        return [row[0] async for row in cursor]


async def get_messages_by_sender(nickname):
    db = await get_db()
    messages = []
    async with db.execute(
        "SELECT message, timestamp, user_id, channel FROM messages WHERE sender = ?",
        (nickname,),
    ) as cursor:
        async for row in cursor:
            messages.append(
                {
                    "message": row[0],
                    "timestamp": row[1],
                    "user_id": row[2],
                    "channel": row[3],
                }
            )
    return messages


async def get_nicknames_by_user_id(user_id):
    db = await get_db()
    nicknames = set()
    async with db.execute(
        "SELECT DISTINCT sender FROM messages WHERE user_id = ?", (user_id,)
    ) as cursor:
        async for row in cursor:
            nicknames.add(row[0])
    return list(nicknames)


async def close_db():
    global _db
    if _db is not None:
        await _db.close()
        _db = None
