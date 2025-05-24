import time
import asyncio
from opentele.td import TDesktop
from opentele.api import API, CreateNewSession, UseCurrentSession
from telethon import events
from telethon.tl.functions.channels import JoinChannelRequest

from database import (
    init_database,
    add_message_to_database,
    add_picture_to_database,
    add_scraped_channel,
    get_scraped_channels,
    can_scrape_channel,
)

db_lock = asyncio.Lock()


async def scare_messages(channel_username, limit=None):

    pictures = 0
    started_time = time.time()

    tdataFolder = r"tdata"
    tdesk = TDesktop(tdataFolder)
    assert tdesk.isLoaded()

    api = API.TelegramIOS.Generate()
    client = await tdesk.ToTelethon("telethon.session", UseCurrentSession, api)

    await client.connect()

    channel_entity = await client.get_entity(channel_username)

    await client(JoinChannelRequest(channel_entity))

    # Check if channel can be scraped (24h cooldown)
    if not await can_scrape_channel(channel_username):
        print(f"Channel {channel_username} was scraped less than 24h ago.")
        return channel_username, 0, 0, 0

    message_count = (await client.get_messages(channel_username, limit=1)).total
    print(f"Channel {channel_username} has {message_count} messages.")

    messages = await client.get_messages(channel_username, limit=limit)

    await add_scraped_channel(channel_username)

    for message in messages:
        try:
            async with db_lock:
                sender_entity = await message.get_sender()
                sender_username = getattr(sender_entity, "username", None)
                sender_id = getattr(sender_entity, "id", None)
                if message.raw_text and sender_username and sender_id:
                    await add_message_to_database(
                        user_id=sender_id,
                        sender=sender_username,
                        message=message.raw_text,
                        timestamp=message.date,
                        channel=channel_username,
                    )

                """if message.photo:
                    pictures += 1
                    await message.download_media(file=f"media/{message.photo.id}.png")
                    await add_picture_to_database(
                        sender=(await message.get_sender()).username,
                        message=f"media/{message.photo.id}.png",
                    )"""

        except Exception as e:
            continue

    print(
        f"Processed {messages.total} messages from {channel_username}. Time taken: {time.time() - started_time:.2f} seconds."
    )

    return channel_username, messages.total, round(time.time() - started_time), pictures
