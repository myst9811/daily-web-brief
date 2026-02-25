"""
storage.py — Async SQLite layer via aiosqlite.
Owns all DB state for the daily-web-brief agent.
"""
import json
import time

import aiosqlite


async def get_db(db_path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(db_path)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def init_db(db: aiosqlite.Connection) -> None:
    await db.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            url             TEXT PRIMARY KEY,
            title           TEXT,
            content_hash    TEXT,
            first_seen_ts   INTEGER
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS embeddings (
            cache_key   TEXT PRIMARY KEY,
            model       TEXT NOT NULL,
            vector      TEXT NOT NULL,
            created_ts  INTEGER NOT NULL
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS source_health (
            url                  TEXT PRIMARY KEY,
            consecutive_failures INTEGER NOT NULL DEFAULT 0,
            last_failure_ts      INTEGER,
            last_success_ts      INTEGER,
            disabled_until_ts    INTEGER
        )
    """)
    await db.commit()


async def is_url_seen(db: aiosqlite.Connection, url: str) -> bool:
    async with db.execute("SELECT 1 FROM seen WHERE url = ?", (url,)) as cur:
        return await cur.fetchone() is not None


async def is_hash_seen(db: aiosqlite.Connection, content_hash: str) -> bool:
    async with db.execute("SELECT 1 FROM seen WHERE content_hash = ?", (content_hash,)) as cur:
        return await cur.fetchone() is not None


async def mark_seen(
    db: aiosqlite.Connection,
    url: str,
    title: str,
    content_hash: str,
    ts: int,
) -> None:
    await db.execute(
        "INSERT OR IGNORE INTO seen(url, title, content_hash, first_seen_ts) VALUES (?, ?, ?, ?)",
        (url, title, content_hash, ts),
    )


async def get_cached_embedding(
    db: aiosqlite.Connection, cache_key: str
) -> "list[float] | None":
    async with db.execute(
        "SELECT vector FROM embeddings WHERE cache_key = ?", (cache_key,)
    ) as cur:
        row = await cur.fetchone()
        if row:
            return json.loads(row[0])
    return None


async def set_cached_embedding(
    db: aiosqlite.Connection,
    cache_key: str,
    model: str,
    vector: "list[float]",
) -> None:
    await db.execute(
        "INSERT OR REPLACE INTO embeddings(cache_key, model, vector, created_ts) VALUES (?, ?, ?, ?)",
        (cache_key, model, json.dumps(vector), int(time.time())),
    )


async def is_source_disabled(db: aiosqlite.Connection, source_url: str) -> bool:
    async with db.execute(
        "SELECT disabled_until_ts FROM source_health WHERE url = ?", (source_url,)
    ) as cur:
        row = await cur.fetchone()
        if row and row[0] is not None:
            return int(time.time()) < row[0]
    return False


async def record_source_success(db: aiosqlite.Connection, url: str) -> None:
    now = int(time.time())
    await db.execute(
        """
        INSERT INTO source_health(url, consecutive_failures, last_success_ts, disabled_until_ts)
        VALUES (?, 0, ?, NULL)
        ON CONFLICT(url) DO UPDATE SET
            consecutive_failures = 0,
            last_success_ts = excluded.last_success_ts,
            disabled_until_ts = NULL
        """,
        (url, now),
    )


async def record_source_failure(
    db: aiosqlite.Connection, url: str, disable_after_n: int = 5
) -> None:
    now = int(time.time())
    # Upsert: create row if not exists, then increment
    await db.execute(
        """
        INSERT INTO source_health(url, consecutive_failures, last_failure_ts)
        VALUES (?, 1, ?)
        ON CONFLICT(url) DO UPDATE SET
            consecutive_failures = consecutive_failures + 1,
            last_failure_ts = excluded.last_failure_ts
        """,
        (url, now),
    )
    # Check if we should disable
    async with db.execute(
        "SELECT consecutive_failures FROM source_health WHERE url = ?", (url,)
    ) as cur:
        row = await cur.fetchone()
        if row and row[0] >= disable_after_n:
            disabled_until = now + 24 * 3600
            await db.execute(
                "UPDATE source_health SET disabled_until_ts = ? WHERE url = ?",
                (disabled_until, url),
            )
