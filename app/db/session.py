import sqlite3
import app.db.db_query as db_query
DB_FOLDER_PATH="/Users/flutterflowdevs/Desktop/continue_execution/fashia-data-importer-oct-11-mac-final"
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
