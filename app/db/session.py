import sqlite3
import app.db.db_query as db_query
from contextlib import asynccontextmanager

DB_FOLDER_PATH="/mnt/efs"
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
    conn = await sqlite3.connect(DATABASE_PATH)
    await conn.execute('PRAGMA journal_mode=WAL')  
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        await conn.close()
