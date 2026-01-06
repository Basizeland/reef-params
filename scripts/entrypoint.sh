#!/bin/sh
set -e

if ! python -c "import psycopg" >/dev/null 2>&1; then
  pip install --no-cache-dir -r /app/requirements.txt
fi

exec uvicorn main:app --host 0.0.0.0 --port 8000
