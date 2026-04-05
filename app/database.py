import os

import aiosqlite


DATABASE_PATH = "data/alerts.db"
DATABASE_TIMEOUT_SECONDS = 30
DATABASE_BUSY_TIMEOUT_MS = 30000


async def init_db() -> aiosqlite.Connection:
    os.makedirs("data", exist_ok=True)

    connection = await aiosqlite.connect(DATABASE_PATH, timeout=DATABASE_TIMEOUT_SECONDS)
    connection.row_factory = aiosqlite.Row

    await connection.execute("PRAGMA journal_mode=WAL")
    await connection.execute(f"PRAGMA busy_timeout = {DATABASE_BUSY_TIMEOUT_MS}")
    await connection.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            event_id TEXT,
            start_date REAL,
            event_title TEXT,
            last_timestamp REAL,
            event_started INTEGER DEFAULT 0,
            event_ended INTEGER DEFAULT 0,
            first_message_id INTEGER,
            PRIMARY KEY (event_id, start_date)
        )
        """
    )
    await connection.commit()
    return connection
