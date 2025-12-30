import sqlite3
import os

# Configuration: Path to your database
DB_PATH = "reef.db"

def get_db():
    return sqlite3.connect(DB_PATH)

def insert_additives():
    conn = get_db()
    cur = conn.cursor()

    # --- DEFINITIONS ---
    # Format: (Brand, Name, Parameter, Strength, Unit, Max_Daily_Safe_Limit)
    # Strength = How much 1mL raises the parameter in 100 Liters
    
    additives = [
        # --- AQUAFOREST ---
        ("Aquaforest", "KH Plus", "Alkalinity/KH", 0.025, "dKH", 1.0),
        ("Aquaforest", "Calcium Plus", "Calcium", 1.5, "ppm", 20.0),
        ("Aquaforest", "Magnesium Plus", "Magnesium", 0.75, "ppm", 50.0),
        ("Aquaforest", "Component 1+ 2+ 3+", "Alkalinity/KH", 0.09, "dKH", 1.0), # Comp 2
        ("Aquaforest", "Component 1+ 2+ 3+", "Calcium", 1.8, "ppm", 20.0),      # Comp 1
        ("Aquaforest", "Component 1+ 2+ 3+", "Magnesium", 0.1, "ppm", 50.0),    # Comp 1 (trace Mg)

        # --- RED SEA ---
        ("Red Sea", "Foundation A (Calcium)", "Calcium", 2.0, "ppm", 20.0),
        ("Red Sea", "Foundation B (Alk)", "Alkalinity/KH", 0.1, "dKH", 1.0),
        ("Red Sea", "Foundation C (Mag)", "Magnesium", 1.0, "ppm", 50.0),

        # --- FRITZ AQUATICS (RPM) ---
        ("Fritz", "RPM Alkalinity", "Alkalinity/KH", 0.15, "dKH", 1.0),
        ("Fritz", "RPM Calcium", "Calcium", 0.76, "ppm", 20.0),
        ("Fritz", "RPM Magnesium", "Magnesium", 0.76, "ppm", 50.0),

        # --- ATI (Essentials Pro) ---
        ("ATI", "Essentials Pro #1 (Alk)", "Alkalinity/KH", 0.28, "dKH", 1.0),
        ("ATI", "Essentials Pro #2 (Ca/Mg)", "Calcium", 2.0, "ppm", 20.0),
        # Note: ATI #2 contains Mg, but usually we dose based on Ca consumption. 
        # Including Mg entry for completeness if you dose specifically for Mg.
        ("ATI", "Essentials Pro #2 (Ca/Mg)", "Magnesium", 1.0, "ppm", 50.0), 

        # --- BRIGHTWELL (Nutrients) ---
        # NeoNitro: 1ml raises 20gal (75.7L) by 2ppm -> 1ml in 100L = ~1.5ppm
        ("Brightwell", "NeoNitro", "Nitrate", 1.5, "ppm", 2.0),
        # NeoPhos: 1.2ml raises 20gal (75.7L) by 0.02ppm -> 1ml in 100L = ~0.012ppm
        ("Brightwell", "NeoPhos", "Phosphate", 0.012, "ppm", 0.05),
        
        # --- TRITON (Core7) ---
        # Core7 Base Elements: 1ml raises 100L by 0.2 dKH (Part 1 or 2 depending on version)
        ("Triton", "Core7 Base Elements", "Alkalinity/KH", 0.2, "dKH", 1.0),

        # --- TROPIC MARIN ---
        # All For Reef: 1ml raises 100L by ~0.05 dKH (based on 10ml/100L -> 0.5 dKH)
        ("", "All For Reef", "Alkalinity/KH", 0.05, "dKH", 1.0),

        # --- KALKWASSER ---
        # Saturated kalkwasser: ~112 dKH solution -> 1ml/100L raises ~0.00112 dKH
        ("", "Kalkwasser (Saturated)", "Alkalinity/KH", 0.00112, "dKH", 1.0),
    ]

    print(f"Connecting to {DB_PATH}...")
    
    count = 0
    for brand, name, param, strength, unit, max_daily in additives:
        full_name = f"{brand} {name}".strip()
        
        # Check if exists to avoid duplicates
        cur.execute("SELECT id FROM additives WHERE name=? AND parameter=?", (full_name, param))
        exists = cur.fetchone()
        
        if not exists:
            print(f"Adding: {full_name} ({param})")
            cur.execute("""
                INSERT INTO additives (name, parameter, strength, unit, max_daily, active)
                VALUES (?, ?, ?, ?, ?, 1)
            """, (full_name, param, strength, unit, max_daily))
            count += 1
        else:
            print(f"Skipping: {full_name} (Already exists)")

    conn.commit()
    conn.close()
    print(f"\nSuccess! Added {count} new additives.")

if __name__ == "__main__":
    if not os.path.exists(DB_PATH):
        print(f"Error: Database file '{DB_PATH}' not found. Make sure this script is in the same folder as main.py")
    else:
        insert_additives()
