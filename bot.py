import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.exceptions import TelegramNetworkError
from aiogram.types import (
    InputFile,
    FSInputFile,  # <-- add this import
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
)
from aiogram.filters import Command
from scapper import scare_messages
from database import init_database, get_messages_by_sender, get_nicknames_by_user_id
import aiosqlite
import os
import hashlib

API_TOKEN = ""  # Replace with your bot token

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

db_lock = asyncio.Lock()


@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer(
        "Welcome! Use /scrape <channel> to scrape a channel, or /search <nickname> to search messages."
    )


@dp.message(Command("scrape"))
async def scrape_cmd(message: types.Message):
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Usage: /scrape <channel_username>")
        return
    channel = args[1]
    await message.reply(f"Scraping channel: {channel}. This may take a while.")
    try:
        print(f"Scraping channel: {channel}, {message.from_user.username}")
        async with db_lock:
            username, total, time_taken, pictures = await scare_messages(channel)
        if pictures > 0:
            pictures_msg = f" and {pictures} pictures"
        else:
            pictures_msg = ""

        if total == 0:
            await message.reply(
                "No messages found in this channel. This could be because it was scraped less than 24 hours ago or its empty."
            )
            return
        await message.reply(
            f"Scraped {total} messages{pictures_msg} from {username} in {time_taken} seconds."
        )
    except Exception as e:
        await message.reply(f"Error: {e}")


@dp.message(Command("search"))
async def search_cmd(message: types.Message):
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Usage: /search <nickname>")
        return
    nickname = args[1]
    filename = f"{nickname}.txt"
    async with db_lock:
        messages = await get_messages_by_sender(nickname)
    if not messages:
        await message.reply("No messages found for this nickname.")
        return
    user_id = messages[0]["user_id"] if messages else None
    all_nicknames = []
    if user_id:
        all_nicknames = await get_nicknames_by_user_id(user_id)
    with open(filename, "w", encoding="utf-8") as f:
        if all_nicknames:
            f.write(
                f"Known nicknames for user_id {user_id}: {', '.join(all_nicknames)}\n\n"
            )
        for msg in messages:
            f.write(
                f"[{msg['timestamp']}] (from {msg['channel']}) {msg['message']}\n\n"
            )
    await message.reply_document(FSInputFile(filename))
    os.remove(filename)


@dp.inline_query()
async def inline_query_handler(inline_query: InlineQuery):
    query = inline_query.query.strip()
    results = []
    if not query:
        await inline_query.answer([], cache_time=1)
        return
    nickname = query.split()[0].replace("@", "")
    async with db_lock:
        messages = await get_messages_by_sender(nickname)
    if not messages:
        results.append(
            InlineQueryResultArticle(
                id=hashlib.md5(f"noresult{nickname}".encode()).hexdigest(),
                title="No messages found",
                input_message_content=InputTextMessageContent(
                    message_text=f"No messages found for {nickname}."
                ),
            )
        )
    else:
        count = len(messages)
        results.append(
            InlineQueryResultArticle(
                id=hashlib.md5(f"count{nickname}".encode()).hexdigest(),
                title=f"{nickname} has {count} messages",
                input_message_content=InputTextMessageContent(
                    message_text=f"User @{nickname} has {count} messages in the database."
                ),
            )
        )
    await inline_query.answer(results, cache_time=1)


async def start_bot_polling():
    try:
        await dp.start_polling(bot)
    except TelegramNetworkError:
        pass
