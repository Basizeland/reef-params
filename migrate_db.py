import sqlite3
import os

DB_PATH = os.environ.get("DATABASE_PATH", "/app/reef.db")

def migrate():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        return

    print(f"Opening database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1. Check if 'parameters' table exists (Legacy data)
    try:
        cur.execute("SELECT count(*) FROM parameters")
        count = cur.fetchone()[0]
        print(f"Found {count} legacy readings in 'parameters' table.")
    except sqlite3.OperationalError:
        print("No legacy 'parameters' table found. Data might already be migrated or DB is empty.")
        return

    if count == 0:
        print("Legacy table is empty. Nothing to migrate.")
        return

    print("Migrating data to 'sample_values'...")

    # 2. Ensure parameter definitions exist
    # Get all unique parameter names from legacy data
    cur.execute("SELECT DISTINCT name, unit FROM parameters")
    legacy_params = cur.fetchall()

    param_map = {} # name -> id

    for lp in legacy_params:
        name = lp["name"]
        unit = lp["unit"]

        # Check if this param exists in new definition table
        cur.execute("SELECT id FROM parameter_defs WHERE name=?", (name,))
        existing = cur.fetchone()

        if existing:
            param_map[name] = existing["id"]
        else:
            # Create it
            print(f"Creating definition for: {name}")
            cur.execute("INSERT INTO parameter_defs (name, unit, active) VALUES (?, ?, 1)", (name, unit))
            param_map[name] = cur.lastrowid

    # 3. Move Data
    # Select all legacy rows
    cur.execute("SELECT sample_id, name, value FROM parameters")
    rows = cur.fetchall()
    migrated_count = 0

    for r in rows:
        pid = param_map.get(r["name"])
        if pid:
            # Insert into new table
            try:
                cur.execute(
                    "INSERT INTO sample_values (sample_id, parameter_id, value) VALUES (?, ?, ?)",
                    (r["sample_id"], pid, r["value"])
                )
                migrated_count += 1
            except sqlite3.IntegrityError:
                # Already exists
                pass

    # 4. Cleanup
    # We rename the old table instead of deleting it, just in case.
    cur.execute("ALTER TABLE parameters RENAME TO parameters_backup")

    conn.commit()
    conn.close()
    print(f"Success! Migrated {migrated_count} readings.")
    print("Please restart your container now.")

if __name__ == "__main__":
    migrate()
