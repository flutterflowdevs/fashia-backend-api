#!/bin/sh

echo ">>> Copying SQLite DB from EFS to /tmp..."
cp /mnt/efs/fashia.db /tmp/fashia.db

echo ">>> Starting FastAPI with /tmp DB..."
exec uvicorn app.main:app --host 0.0.0.0 --port 80
