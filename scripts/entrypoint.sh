#!/bin/sh
set -e

if ! python -c "import psycopg" >/dev/null 2>&1; then
  pip install --no-cache-dir -r /app/requirements.txt
fi

# Run database migrations
echo "Running database migrations..."
python -c "
from alembic.config import Config
from alembic import command
import os

cfg = Config('alembic.ini')
# Override sqlalchemy.url from environment if set
if os.environ.get('DATABASE_URL'):
    from database import SQLALCHEMY_DATABASE_URL
    cfg.set_main_option('sqlalchemy.url', SQLALCHEMY_DATABASE_URL)

command.upgrade(cfg, 'head')
print('Migrations completed successfully')
"

exec uvicorn main:app --host 0.0.0.0 --port 8000
