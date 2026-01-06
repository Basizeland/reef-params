import argparse
import os
import sqlite3
import sys
from typing import Iterable, List

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy import create_engine, text

from database import normalize_database_url


DEFAULT_TABLE_ORDER: List[str] = [
    "users",
    "tanks",
    "user_tanks",
    "tank_profiles",
    "parameter_defs",
    "test_kits",
    "additives",
    "samples",
    "parameters",
    "sample_values",
    "sample_value_kits",
    "targets",
    "dose_logs",
    "dosing_entries",
    "dose_plan_checks",
    "dosing_notifications",
    "api_tokens",
    "sessions",
    "push_subscriptions",
    "app_settings",
    "audit_logs",
    "icp_recommendations",
    "icp_dose_checks",
    "tank_journal",
    "tank_maintenance_tasks",
]


def iter_rows(conn: sqlite3.Connection, table: str, columns: Iterable[str]):
    cursor = conn.execute(f"SELECT {', '.join(columns)} FROM {table}")
    rows = cursor.fetchall()
    for row in rows:
        yield dict(zip(columns, row))


def get_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cursor = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cursor.fetchall()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate data from SQLite to Postgres.")
    parser.add_argument("--sqlite-path", required=True, help="Path to the SQLite database.")
    parser.add_argument("--postgres-url", required=True, help="Postgres SQLAlchemy URL.")
    args = parser.parse_args()

    sqlite_conn = sqlite3.connect(args.sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row

    postgres_url = normalize_database_url(args.postgres_url)

    engine = create_engine(postgres_url, connect_args={"sslmode": "require"})

    with engine.begin() as pg_conn:
        for table in DEFAULT_TABLE_ORDER:
            columns = get_columns(sqlite_conn, table)
            if not columns:
                continue
            insert_sql = text(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(':' + c for c in columns)})"
            )
            for row in iter_rows(sqlite_conn, table, columns):
                pg_conn.execute(insert_sql, row)

    sqlite_conn.close()


if __name__ == "__main__":
    main()
