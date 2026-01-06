import argparse
import os
import sqlite3
import sys
from typing import Iterable, List

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy import create_engine, inspect, text

from database import normalize_database_url, Base
import models  # noqa: F401


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


def get_pg_columns(inspector, table: str) -> List[str]:
    if not inspector.has_table(table):
        return []
    return [col["name"] for col in inspector.get_columns(table)]

def get_foreign_keys(conn: sqlite3.Connection, table: str) -> List[dict]:
    cursor = conn.execute(f"PRAGMA foreign_key_list({table})")
    return [{"table": row[2], "from": row[3], "to": row[4]} for row in cursor.fetchall()]


def get_column_values(
    conn: sqlite3.Connection, table: str, column: str, cache: dict[tuple[str, str], set]
) -> set:
    key = (table, column)
    if key in cache:
        return cache[key]
    cursor = conn.execute(f"SELECT {column} FROM {table}")
    values = {row[0] for row in cursor.fetchall()}
    cache[key] = values
    return values


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate data from SQLite to Postgres.")
    parser.add_argument("--sqlite-path", required=True, help="Path to the SQLite database.")
    parser.add_argument("--postgres-url", required=True, help="Postgres SQLAlchemy URL.")
    args = parser.parse_args()

    try:
        import psycopg  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "psycopg is not available. Install with `pip install -r requirements.txt` "
            "or `pip install psycopg[binary]`."
        ) from exc

    sqlite_path = os.path.expanduser(args.sqlite_path)
    if not os.path.exists(sqlite_path):
        raise FileNotFoundError(f"SQLite database not found: {sqlite_path}")

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row

    postgres_url = normalize_database_url(args.postgres_url)

    engine = create_engine(postgres_url, connect_args={"sslmode": "require"})

    Base.metadata.create_all(engine)

    inspector = inspect(engine)
    if inspector.has_table("users"):
        existing_columns = {col["name"] for col in inspector.get_columns("users")}
        missing = []
        if "last_login_at" not in existing_columns:
            missing.append("ALTER TABLE users ADD COLUMN last_login_at TEXT")
        if "is_admin" not in existing_columns:
            missing.append("ALTER TABLE users ADD COLUMN is_admin INTEGER DEFAULT 0")
        if missing:
            with engine.begin() as pg_conn:
                for ddl in missing:
                    pg_conn.execute(text(ddl))

    fk_cache: dict[tuple[str, str], set] = {}

    with engine.begin() as pg_conn:
        for table in DEFAULT_TABLE_ORDER:
            sqlite_columns = get_columns(sqlite_conn, table)
            if not sqlite_columns:
                continue
            pg_columns = set(get_pg_columns(inspector, table))
            columns = [col for col in sqlite_columns if col in pg_columns]
            if not columns:
                print(
                    f"Skipped {table} because no matching columns exist in Postgres.",
                    file=sys.stderr,
                )
                continue
            if len(columns) != len(sqlite_columns):
                missing_columns = ", ".join(
                    col for col in sqlite_columns if col not in pg_columns
                )
                print(
                    f"Skipping columns not present in Postgres for {table}: {missing_columns}",
                    file=sys.stderr,
                )
            insert_sql = text(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(':' + c for c in columns)})"
            )
            foreign_keys = [
                fk for fk in get_foreign_keys(sqlite_conn, table) if fk["from"] in columns
            ]
            skipped = 0
            for row in iter_rows(sqlite_conn, table, columns):
                if foreign_keys:
                    valid = True
                    for fk in foreign_keys:
                        value = row.get(fk["from"])
                        if value is None:
                            continue
                        ref_values = get_column_values(
                            sqlite_conn, fk["table"], fk["to"], fk_cache
                        )
                        if value not in ref_values:
                            valid = False
                            break
                    if not valid:
                        skipped += 1
                        continue
                pg_conn.execute(insert_sql, row)
            if skipped:
                print(
                    f"Skipped {skipped} orphaned row(s) in {table} due to missing foreign keys.",
                    file=sys.stderr,
                )

    sqlite_conn.close()


if __name__ == "__main__":
    main()
