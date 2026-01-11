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
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text
import os

cfg = Config('alembic.ini')

# Override sqlalchemy.url from environment if set
if os.environ.get('DATABASE_URL'):
    from database import SQLALCHEMY_DATABASE_URL
    cfg.set_main_option('sqlalchemy.url', SQLALCHEMY_DATABASE_URL)

db_url = cfg.get_main_option('sqlalchemy.url')
engine = create_engine(db_url)

# Check if this is a database with existing tables but no alembic version
with engine.connect() as conn:
    inspector = inspect(conn)
    tables = inspector.get_table_names()

    # Check if alembic_version exists
    has_alembic = 'alembic_version' in tables

    if not has_alembic and len(tables) > 0:
        # Database has tables but no alembic tracking
        # Determine what version to stamp based on existing tables
        print('Existing database detected without migration tracking.')

        # Check which tables exist to determine the migration level
        if 'icp_uploads' in tables and 'additives' in inspector.get_table_names():
            # Check if additives has owner_user_id column
            additives_cols = [c['name'] for c in inspector.get_columns('additives')]
            if 'owner_user_id' in additives_cols:
                stamp_version = '0004_add_additives_owner_user_id'
                print(f'Stamping database at {stamp_version}')
            else:
                # Has icp_uploads but not additives.owner_user_id
                stamp_version = '0002_add_icp_uploads'
                print(f'Stamping database at {stamp_version}')
        elif len(tables) > 0:
            # Has some tables, assume initial schema
            stamp_version = '0001_initial'
            print(f'Stamping database at {stamp_version}')

        # Stamp the database
        command.stamp(cfg, stamp_version)
        print('Database stamped successfully')

# Now run migrations to bring database to latest version
print('Applying any pending migrations...')
command.upgrade(cfg, 'head')
print('Migrations completed successfully')
"

exec uvicorn main:app --host 0.0.0.0 --port 8000
