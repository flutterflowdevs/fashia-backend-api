#!/bin/sh
set -e

echo "=========================="
echo "🚀 Start.sh executed"
echo "EFS_DB_PATH: $EFS_DB_PATH"
echo "SQLITE_DB_PATH: $SQLITE_DB_PATH"
echo "=========================="

# If FORCE_DB_COPY is true, always copy DB
if [ "$FORCE_DB_COPY" = "true" ]; then
    echo "⚠️ FORCE_DB_COPY enabled — overriding existing DB."
    if [ -f "$EFS_DB_PATH" ]; then
        echo "📁 Found EFS DB at $EFS_DB_PATH"
        echo "📥 Copying DB from EFS to $SQLITE_DB_PATH ..."
        cp "$EFS_DB_PATH" "$SQLITE_DB_PATH"
        echo "✅ Copy complete."
    else
        echo "❌ ERROR: EFS DB NOT FOUND at $EFS_DB_PATH"
        ls -R /mnt/efs
        exit 1
    fi
else
    # Normal behavior — copy only if DB does not exist
    if [ ! -f "$SQLITE_DB_PATH" ]; then
        if [ -f "$EFS_DB_PATH" ]; then
            echo "📁 Found EFS DB at $EFS_DB_PATH"
            echo "📥 Copying DB from EFS to $SQLITE_DB_PATH ..."
            cp "$EFS_DB_PATH" "$SQLITE_DB_PATH"
            echo "✅ Copy complete."
        else
            echo "❌ ERROR: EFS DB NOT FOUND at $EFS_DB_PATH"
            ls -R /mnt/efs
            exit 1
        fi
    else
        echo "✅ DB already exists at $SQLITE_DB_PATH — skipping copy."
    fi
fi


echo "=========================="
echo "Starting API server..."
echo "Using DB at $SQLITE_DB_PATH"
echo "=========================="

# Start FastAPI / Uvicorn
uvicorn app.main:app --host 0.0.0.0 --port 80
