"""
find_chat_id.py – Vind het numerieke chat ID van je Telegram groepen.

Gebruik:
    python find_chat_id.py

Vereist: dezelfde .env als je bot (TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE)
         OF vul de waarden hieronder handmatig in.

Het script toont alle groepen/kanalen waar je lid van bent met
'wallstreet' in de naam, inclusief het numerieke ID dat je nodig hebt.
"""

import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv
from telethon import TelegramClient

# Laad .env uit dezelfde map als dit script, of de project root
env_path = Path(__file__).resolve().parent / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)

API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
PHONE = os.getenv("TELEGRAM_PHONE", "")
SESSION_DIR = os.getenv("SESSION_DIR", str(Path(__file__).resolve().parent / "sessions"))
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "telegram_session")

session_path = str(Path(SESSION_DIR) / SESSION_NAME)


async def main():
    client = TelegramClient(session_path, API_ID, API_HASH)
    await client.start(phone=PHONE)

    print("\n🔍 Zoeken naar groepen met 'wallstreet' in de naam...\n")
    print(f"{'Naam':<45} {'Chat ID':<25} {'Type'}")
    print("-" * 90)

    found = False
    async for dialog in client.iter_dialogs():
        if "wallstreet" in dialog.name.lower():
            found = True
            chat_type = "Channel" if dialog.is_channel else ("Group" if dialog.is_group else "User")
            print(f"{dialog.name:<45} {str(dialog.id):<25} {chat_type}")

    if not found:
        print("Geen groepen gevonden met 'wallstreet' in de naam.")
        print("\nAlle groepen/kanalen:")
        print("-" * 90)
        async for dialog in client.iter_dialogs():
            if dialog.is_group or dialog.is_channel:
                chat_type = "Channel" if dialog.is_channel else "Group"
                print(f"{dialog.name:<45} {str(dialog.id):<25} {chat_type}")

    print("\n✅ Kopieer het Chat ID hierboven naar je .env als TELEGRAM_GROUP_NAME")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
