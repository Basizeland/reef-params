import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DATABASE_PATH", os.path.join(BASE_DIR, "reef.db"))

def update():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    mapping = {
        "Alkalinity/KH": "Alkalinity - KH",
        "Calcium": "Calcium - CA",
        "Magnesium": "Magnesium - MG",
        "Phosphate": "Phosphate - PO4",
        "Nitrate": "Nitrate - NO3"
    }

    tables = [
        ("additives", "parameter"),
        ("targets", "parameter"),
        ("parameter_defs", "name"),
        ("parameters", "name"),
        ("test_kits", "parameter")
    ]

    # 1. Rename Parameters
    for old, new in mapping.items():
        print(f"Updating {old} -> {new}")
        for table, col in tables:
            cur.execute(f"UPDATE {table} SET {col}=? WHERE {col}=?", (new, old))

    # 2. Remove Max Daily Values
    print("Removing Max Daily values from additives...")
    cur.execute("UPDATE additives SET max_daily = NULL")
    
    conn.commit()
    conn.close()
    print("\nFinished!")

if __name__ == "__main__":
    update()
