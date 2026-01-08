import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DATABASE_PATH", os.path.join(BASE_DIR, "reef.db"))

def insert_additives():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    catalog = [
        ("Aquaforest", "KH Plus", "Alkalinity - KH", 0.025, "dKH"),
        ("Aquaforest", "Calcium Plus", "Calcium - CA", 1.5, "ppm"),
        ("Aquaforest", "Magnesium Plus", "Magnesium - MG", 0.75, "ppm"),
        ("Aquaforest", "Component 1+ 2+ 3+", "Alkalinity - KH", 0.09, "dKH"),
        ("Aquaforest", "Component 1+ 2+ 3+", "Calcium - CA", 1.8, "ppm"),
        ("Aquaforest", "Component 1+ 2+ 3+", "Magnesium - MG", 0.1, "ppm"),
        ("Red Sea", "Foundation A (Calcium)", "Calcium - CA", 2.0, "ppm"),
        ("Red Sea", "Foundation B (Alk)", "Alkalinity - KH", 0.1, "dKH"),
        ("Red Sea", "Foundation C (Mag)", "Magnesium - MG", 1.0, "ppm"),
        ("Fritz", "RPM Alkalinity", "Alkalinity - KH", 0.15, "dKH"),
        ("Fritz", "RPM Calcium", "Calcium - CA", 0.76, "ppm"),
        ("Fritz", "RPM Magnesium", "Magnesium - MG", 0.76, "ppm"),
        ("ATI", "Essentials Pro #1 (Alk)", "Alkalinity - KH", 0.28, "dKH"),
        ("ATI", "Essentials Pro #2 (Ca/Mg)", "Calcium - CA", 2.0, "ppm"),
        ("ATI", "Essentials Pro #2 (Ca/Mg)", "Magnesium - MG", 1.0, "ppm"),
        ("Brightwell", "NeoNitro", "Nitrate - NO3", 1.5, "ppm"),
        ("Brightwell", "NeoPhos", "Phosphate - PO4", 0.012, "ppm"),
        ("Triton", "Core7 Base Elements", "Alkalinity - KH", 0.2, "dKH")
    ]

    added_count = 0
    for brand, name, param, strength, unit in catalog:
        full_name = f"{brand} {name}"
        cur.execute("SELECT id FROM additives WHERE name=? AND parameter=?", (full_name, param))
        if not cur.fetchone():
            cur.execute("INSERT INTO additives (name, parameter, strength, unit, max_daily, active) VALUES (?, ?, ?, ?, NULL, 1)", (full_name, param, strength, unit))
            added_count += 1

    conn.commit()
    conn.close()
    print(f"Done! Seeded {added_count} additives with new names.")

if __name__ == "__main__":
    insert_additives()
