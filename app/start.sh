#!/bin/sh
set -e

echo "=========================="
echo "🚀 Start.sh executed"
echo "EFS_DB_PATH: $EFS_DB_PATH"
echo "SQLITE_DB_PATH: $SQLITE_DB_PATH"
echo "=========================="

# Check if EFS source DB exists
if [ -f "$EFS_DB_PATH" ]; then
    echo "📁 Found EFS DB at $EFS_DB_PATH"
    echo "📥 Copying DB from EFS to $SQLITE_DB_PATH ..."
    cp "$EFS_DB_PATH" "$SQLITE_DB_PATH"
    echo "✅ Copy complete."
else
    echo "❌ ERROR: EFS DB NOT FOUND at $EFS_DB_PATH"
    ls -R /mnt/efs
fi

echo "=========================="
echo "Starting API server..."
echo "Using DB at $SQLITE_DB_PATH"
echo "=========================="

# Start FastAPI / Uvicorn
uvicorn app.main:app --host 0.0.0.0 --port 80
