#!/bin/sh

echo ">>> Listing EFS mount contents..."
ls -R /mnt/efs

echo ">>> Expected DB Path: /mnt/efs/fashia-db/fashia.db"
echo ">>> Current files in /mnt/efs/fashia-db:"
ls -l /mnt/efs/fashia-db

echo ">>> Copying SQLite DB from EFS to /tmp..."
cp "$DB_PATH" /tmp/fashia.db

echo ">>> Confirming copied file:"
ls -l /tmp/fashia.db

echo ">>> Starting FastAPI with /tmp DB..."
exec uvicorn app.main:app --host 0.0.0.0 --port 80
