import sqlite3
import app.db.db_query as db_query
import aiosqlite
from contextlib import asynccontextmanager

DB_FOLDER_PATH="/Volumes/Ex_Drive/fashia-workspace/fashia_custom_backend"
DATABASE_PATH = DB_FOLDER_PATH+"/facilities.db"

def get_db():
    return sqlite3.connect(DATABASE_PATH)


def get_entity_table_count():
    conn = get_db()
    cursor = conn.cursor()

    # Execute count query
    cursor.execute(db_query.ENTITY_TABLE_COUNT_QUERY)
    count = cursor.fetchone()[0]

    print(f"Total records: {count}")

    conn.close()
    return count

@asynccontextmanager
async def get_db_connection():
    """Async context manager for SQLite connection."""
    conn = await aiosqlite.connect(DATABASE_PATH)
    await conn.execute('PRAGMA journal_mode=WAL')
    await conn.execute("PRAGMA query_only = ON")  
    conn.row_factory = aiosqlite.Row
    try:
        yield conn
    finally:
        await conn.close()