import os
import asyncio

from bot import start_bot_polling
from database import init_database


async def main():
    os.makedirs("media", exist_ok=True)

    await init_database()

    await start_bot_polling()


if __name__ == "__main__":
    asyncio.run(main())
