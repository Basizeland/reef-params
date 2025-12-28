import os
import sqlite3
import re
import math
import json
import pandas as pd
from io import BytesIO
from datetime import datetime, date, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Form, Request, HTTPException, File, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DATABASE_PATH", os.path.join(BASE_DIR, "reef.db"))

app = FastAPI(title="Reef Tank Parameters")

# --- 1. RECOMMENDED DEFAULTS (Updated Scientific Names) ---
RECOMMENDED_DEFAULTS = {
    "Alkalinity - KH": {"target_low": 8.0, "target_high": 9.5, "alert_low": 7.0, "alert_high": 11.0},
    "Calcium - CA": {"target_low": 400, "target_high": 450, "alert_low": 350, "alert_high": 500},
    "Magnesium - MG": {"target_low": 1300, "target_high": 1400, "alert_low": 1200, "alert_high": 1500},
    "Phosphate - PO4": {"target_low": 0.03, "target_high": 0.1, "alert_low": 0.0, "alert_high": 0.25},
    "Nitrate - NO3": {"target_low": 2, "target_high": 10, "alert_low": 0, "alert_high": 25},
    "Salinity": {"target_low": 34, "target_high": 35.5, "alert_low": 32, "alert_high": 37},
    "Temperature": {"target_low": 25, "target_high": 26.5, "alert_low": 23, "alert_high": 29},
}

# --- 2. INITIAL SEED DEFAULTS ---
INITIAL_DEFAULTS = {
    "Alkalinity - KH": {"default_target_low": 8.0, "default_target_high": 9.5, "default_alert_low": 7.0, "default_alert_high": 11.0},
    "Calcium - CA": {"default_target_low": 400, "default_target_high": 450, "default_alert_low": 350, "default_alert_high": 500},
    "Magnesium - MG": {"default_target_low": 1300, "default_target_high": 1400, "default_alert_low": 1200, "default_alert_high": 1500},
    "Phosphate - PO4": {"default_target_low": 0.03, "default_target_high": 0.1, "default_alert_low": 0.0, "default_alert_high": 0.25},
    "Nitrate - NO3": {"default_target_low": 2, "default_target_high": 10, "default_alert_low": 0, "default_alert_high": 25},
    "Salinity": {"default_target_low": 34, "default_target_high": 35.5, "default_alert_low": 32, "default_alert_high": 37},
    "Temperature": {"default_target_low": 25, "default_target_high": 26.5, "default_alert_low": 23, "default_alert_high": 29},
}

static_dir = os.path.join(BASE_DIR, "static")
templates_dir = os.path.join(BASE_DIR, "templates")
os.makedirs(static_dir, exist_ok=True)
os.makedirs(templates_dir, exist_ok=True)

app.mount("/static", StaticFiles(directory=static_dir), name="static")
templates = Jinja2Templates(directory=templates_dir)

# --- Jinja Helpers ---
def fmt2(v: Any) -> str:
    if v is None: return ""
    try:
        if isinstance(v, bool): return "1" if v else "0"
        if isinstance(v, int): return str(v)
        fv = float(v)
    except Exception: return str(v)
    s = f"{fv:.2f}" 
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s

def dtfmt(v: Any) -> str:
    dt = parse_dt_any(v) if not isinstance(v, datetime) else v
    if dt is None: return ""
    return dt.strftime("%H:%M - %d/%m/%Y")

def time_ago(v: Any) -> str:
    dt = parse_dt_any(v)
    if not dt: return ""
    now = datetime.now()
    diff = now - dt
    if diff.days == 0: return "Today"
    if diff.days == 1: return "Yesterday"
    if diff.days < 30: return f"{diff.days} days ago"
    months = int(diff.days / 30)
    return f"{months} mo ago"

def tojson_filter(v: Any) -> str:
    return json.dumps(v, default=str)

templates.env.filters["fmt2"] = fmt2
templates.env.filters["dtfmt"] = dtfmt
templates.env.filters["time_ago"] = time_ago
templates.env.filters["tojson"] = tojson_filter

def _finalize(v: Any) -> Any:
    try:
        if isinstance(v, bool) or v is None: return v
        if isinstance(v, int): return v
        if isinstance(v, float): return fmt2(v)
    except Exception: pass
    return v

templates.env.finalize = _finalize

def redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url, status_code=303)

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def q(db: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
    return db.execute(sql, params).fetchall()

def one(db: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> Optional[sqlite3.Row]:
    return db.execute(sql, params).fetchone()

def row_get(row: Any, key: str, default: Any = None) -> Any:
    try:
        if row is None: return default
        if isinstance(row, dict): return row.get(key, default)
        if hasattr(row, 'keys') and key in row.keys(): return row[key]
    except Exception: pass
    return default

def table_exists(db: sqlite3.Connection, name: str) -> bool:
    r = one(db, "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return r is not None

def to_float(v: Any) -> Optional[float]:
    if v is None: return None
    if isinstance(v, (int, float)):
        try: return float(v)
        except Exception: return None
    s = str(v).strip()
    if not s: return None
    try: return float(s)
    except Exception:
        try: return float(s.replace(",", "."))
        except Exception: return None

def slug_key(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", (name or "").strip().lower()).strip("_")
    return s or "x"

def compute_target(tlow: Any, thigh: Any) -> Any:
    if tlow is None and thigh is None: return None
    if tlow is None: return thigh
    if thigh is None: return tlow
    try:
        fl = float(tlow)
        fh = float(thigh)
        if abs(fl - fh) < 1e-12: return fl
        return (fl + fh) / 2.0
    except Exception: return thigh if thigh is not None else tlow

def list_parameters(db: sqlite3.Connection) -> List[sqlite3.Row]:
    if table_exists(db, "parameter_defs"):
        return q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY sort_order, name")
    if table_exists(db, "parameters"):
        return q(db, "SELECT DISTINCT name, COALESCE(unit,'') AS unit FROM parameters ORDER BY name")
    return []

def get_active_param_defs(db: sqlite3.Connection) -> List[sqlite3.Row]:
    return list_parameters(db)

def parse_dt_any(v):
    if v is None: return None
    if isinstance(v, datetime): return v
    if isinstance(v, date) and not isinstance(v, datetime): return datetime.combine(v, time.min)
    if isinstance(v, (int, float)):
        ts = float(v)
        if ts > 1e12: ts = ts / 1000.0
        try: return datetime.fromtimestamp(ts)
        except Exception: return None
    s = str(v).strip()
    if not s: return None
    if re.fullmatch(r"\d+(\.\d+)?", s):
        try:
            ts = float(s)
            if ts > 1e12: ts = ts / 1000.0
            return datetime.fromtimestamp(ts)
        except Exception: pass
    s = s.replace("Z", "").replace("T", " ")
    try: return datetime.fromisoformat(s)
    except Exception: pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S.%f", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try: return datetime.strptime(s, fmt)
        except ValueError: continue
    return None

def values_mode(db: sqlite3.Connection) -> str:
    try:
        cur = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('sample_values','parameter_defs')")
        names = {r[0] for r in cur.fetchall()}
        if 'sample_values' in names and 'parameter_defs' in names: return "sample_values"
    except Exception: pass
    return "parameters"

def get_sample_readings(db: sqlite3.Connection, sample_id: int) -> List[sqlite3.Row]:
    mode = values_mode(db)
    if mode == "sample_values":
        return q(db, "SELECT pd.name AS name, sv.value AS value, COALESCE(pd.unit, '') AS unit FROM sample_values sv JOIN parameter_defs pd ON pd.id = sv.parameter_id WHERE sv.sample_id=? ORDER BY COALESCE(pd.sort_order, 0), pd.name", (sample_id,))
    return q(db, "SELECT p.name AS name, p.value AS value, COALESCE(pd.unit, p.unit, '') AS unit FROM parameters p LEFT JOIN parameter_defs pd ON pd.name = p.name WHERE p.sample_id=? ORDER BY COALESCE(pd.sort_order, 0), p.name", (sample_id,))

def insert_sample_reading(db: sqlite3.Connection, sample_id: int, pname: str, value: float, unit: str = "") -> None:
    cur = db.cursor()
    mode = values_mode(db)
    if mode == "sample_values":
        pd = one(db, "SELECT id FROM parameter_defs WHERE name=?", (pname,))
        if not pd:
            cur.execute("INSERT INTO parameter_defs (name, unit, active, sort_order) VALUES (?, ?, 1, 0)", (pname, unit or None))
            pid = cur.lastrowid
        else: pid = pd["id"]
        cur.execute("INSERT INTO sample_values (sample_id, parameter_id, value) VALUES (?, ?, ?)", (sample_id, pid, value))
    else:
        cur.execute("INSERT INTO parameters (sample_id, name, value, unit) VALUES (?, ?, ?, ?)", (sample_id, pname, value, unit or None))

def get_latest_per_parameter(db: sqlite3.Connection, tank_id: int) -> Dict[str, Dict[str, Any]]:
    latest_map = {}
    mode = values_mode(db)
    if mode == "sample_values":
        rows = q(db, "SELECT pd.name, sv.value, s.taken_at FROM sample_values sv JOIN samples s ON s.id = sv.sample_id JOIN parameter_defs pd ON pd.id = sv.parameter_id WHERE s.tank_id=? ORDER BY s.taken_at DESC", (tank_id,))
    else:
        rows = q(db, "SELECT p.name, p.value, s.taken_at FROM parameters p JOIN samples s ON s.id = p.sample_id WHERE s.tank_id=? ORDER BY s.taken_at DESC", (tank_id,))
    for r in rows:
        name = r["name"]
        if name not in latest_map:
            latest_map[name] = {"value": r["value"], "taken_at": parse_dt_any(r["taken_at"])}
    return latest_map

# --- DB INIT ---
def init_db() -> None:
    db = get_db()
    cur = db.cursor()
    cur.executescript('''
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS tanks (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS tank_profiles (tank_id INTEGER PRIMARY KEY, volume_l REAL, net_percent REAL DEFAULT 100, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS samples (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, taken_at TEXT NOT NULL, notes TEXT, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS parameter_defs (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, unit TEXT, active INTEGER DEFAULT 1, sort_order INTEGER DEFAULT 0, max_daily_change REAL);
        CREATE TABLE IF NOT EXISTS parameters (id INTEGER PRIMARY KEY AUTOINCREMENT, sample_id INTEGER NOT NULL, name TEXT NOT NULL, value REAL, unit TEXT, test_kit_id INTEGER, FOREIGN KEY (sample_id) REFERENCES samples(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS targets (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, parameter TEXT NOT NULL, target_low REAL, target_high REAL, alert_low REAL, alert_high REAL, unit TEXT, enabled INTEGER DEFAULT 1, UNIQUE(tank_id, parameter), FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS test_kits (id INTEGER PRIMARY KEY AUTOINCREMENT, parameter TEXT NOT NULL, name TEXT NOT NULL, unit TEXT, resolution REAL, min_value REAL, max_value REAL, notes TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS additives (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, parameter TEXT NOT NULL, strength REAL NOT NULL, unit TEXT NOT NULL, max_daily REAL, notes TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS dose_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, additive_id INTEGER, amount_ml REAL NOT NULL, reason TEXT, logged_at TEXT NOT NULL, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS dose_plan_checks (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, parameter TEXT NOT NULL, additive_id INTEGER NOT NULL, planned_date TEXT NOT NULL, checked INTEGER DEFAULT 0, checked_at TEXT, UNIQUE(tank_id, parameter, additive_id, planned_date), FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
    ''')
    def ensure_col(table, col, ddl):
        cur.execute(f"PRAGMA table_info({table})")
        if col not in [r[1] for r in cur.fetchall()]: cur.execute(ddl)
    ensure_col("tanks", "volume_l", "ALTER TABLE tanks ADD COLUMN volume_l REAL")
    ensure_col("parameter_defs", "max_daily_change", "ALTER TABLE parameter_defs ADD COLUMN max_daily_change REAL")
    ensure_col("additives", "active", "ALTER TABLE additives ADD COLUMN active INTEGER DEFAULT 1")
    ensure_col("test_kits", "active", "ALTER TABLE test_kits ADD COLUMN active INTEGER DEFAULT 1")
    ensure_col("targets", "target_low", "ALTER TABLE targets ADD COLUMN target_low REAL")
    ensure_col("targets", "target_high", "ALTER TABLE targets ADD COLUMN target_high REAL")
    ensure_col("targets", "alert_low", "ALTER TABLE targets ADD COLUMN alert_low REAL")
    ensure_col("targets", "alert_high", "ALTER TABLE targets ADD COLUMN alert_high REAL")
    ensure_col("parameter_defs", "default_target_low", "ALTER TABLE parameter_defs ADD COLUMN default_target_low REAL")
    ensure_col("parameter_defs", "default_target_high", "ALTER TABLE parameter_defs ADD COLUMN default_target_high REAL")
    ensure_col("parameter_defs", "default_alert_low", "ALTER TABLE parameter_defs ADD COLUMN default_alert_low REAL")
    ensure_col("parameter_defs", "default_alert_high", "ALTER TABLE parameter_defs ADD COLUMN default_alert_high REAL")

    cur.execute("SELECT COUNT(1) FROM parameter_defs")
    if cur.fetchone()[0] == 0:
        defaults = [("Alkalinity - KH", "dKH", 1, 10), ("Calcium - CA", "ppm", 1, 20), ("Magnesium - MG", "ppm", 1, 30), ("Phosphate - PO4", "ppm", 1, 40), ("Nitrate - NO3", "ppm", 1, 50), ("Salinity", "ppt", 1, 60), ("Temperature", "°C", 1, 70)]
        cur.executemany("INSERT INTO parameter_defs (name, unit, active, sort_order) VALUES (?, ?, ?, ?)", defaults)
    for name, d in INITIAL_DEFAULTS.items():
        cur.execute("UPDATE parameter_defs SET default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=? WHERE name=? AND default_target_low IS NULL", (d["default_target_low"], d["default_target_high"], d["default_alert_low"], d["default_alert_high"], name))
    db.commit()
    db.close()

init_db()

# --- ROUTES ---

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    db = get_db()
    tanks = q(db, "SELECT * FROM tanks ORDER BY name")
    tank_cards = []
    for t in tanks:
        latest_map = get_latest_per_parameter(db, t["id"])
        latest = one(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT 1", (t["id"],))
        readings = []
        pdefs = get_active_param_defs(db)
        for p in pdefs:
            pname = p["name"]
            data = latest_map.get(pname)
            if data: readings.append({"name": pname, "value": data["value"], "unit": (row_get(p, "unit") or ""), "taken_at": data["taken_at"]})
        tank_data = dict(t)
        if "volume_l" not in tank_data: tank_data["volume_l"] = None 
        tank_cards.append({"tank": tank_data, "latest": latest, "readings": readings})
    db.close()
    return templates.TemplateResponse("dashboard.html", {"request": request, "tank_cards": tank_cards})

# --- EXCEL IMPORT CENTER ---

@app.get("/admin/import", response_class=HTMLResponse)
def import_page(request: Request):
    return templates.TemplateResponse("import_manager.html", {"request": request})

@app.get("/admin/download-template")
def download_template():
    db = get_db()
    params = list_parameters(db)
    db.close()
    
    # Define columns based on your current scientific naming convention
    cols = ["Tank Name", "Volume (L)", "Date (YYYY-MM-DD)", "Notes"]
    for p in params:
        cols.append(p["name"])
    
    df = pd.DataFrame(columns=cols)
    # Add a sample row to help the user
    example = ["My Display Tank", 450, date.today().isoformat(), "Weekly Test"] + [None] * (len(cols) - 4)
    df.loc[0] = example
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Readings')
    
    output.seek(0)
    return FileResponse(
        output, 
        filename="Reef_Params_Import_Template.xlsx", 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@app.post("/admin/upload-excel")
async def upload_excel(request: Request, file: UploadFile = File(...)):
    if not file.filename.endswith(('.xlsx', '.xls')):
        return templates.TemplateResponse("import_manager.html", {"request": request, "error": "Invalid file format. Please upload an Excel file."})
    
    db = get_db()
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))
        
        # Import Logic (Calling external helper if available, or inline processing)
        from import_from_excel import import_dataframe_logic
        stats = import_dataframe_logic(db, df)
        
        return templates.TemplateResponse("import_manager.html", {
            "request": request, 
            "success": f"Successfully processed {stats.get('samples', 0)} readings for {stats.get('tanks', 0)} tanks."
        })
    except Exception as e:
        return templates.TemplateResponse("import_manager.html", {"request": request, "error": str(e)})
    finally:
        db.close()

# --- OTHER ROUTES (Tanks, Targets, Calculators, etc.) ---

@app.get("/tanks/{tank_id}", response_class=HTMLResponse)
def tank_detail(request: Request, tank_id: int):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank: return redirect("/")
    samples_rows = q(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT 50", (tank_id,))
    samples = []
    for _row in samples_rows:
        _s = dict(_row); _s["taken_at"] = parse_dt_any(_s.get("taken_at"))
        samples.append(_s)
    targets = q(db, "SELECT * FROM targets WHERE tank_id=? ORDER BY parameter", (tank_id,))
    latest_vals = get_latest_per_parameter(db, tank_id)
    target_map = {t["parameter"]: t for t in targets}
    db.close()
    return templates.TemplateResponse("tank_detail.html", {"request": request, "tank": tank, "recent_samples": samples, "latest_vals": latest_vals, "target_map": target_map, "params": list_parameters(get_db())})

@app.get("/tanks/{tank_id}/add", response_class=HTMLResponse)
def add_sample_form(request: Request, tank_id: int):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    params = get_active_param_defs(db)
    db.close()
    return templates.TemplateResponse("add_sample.html", {"request": request, "tank": tank, "parameters": params})

@app.post("/tanks/{tank_id}/add")
async def add_sample(request: Request, tank_id: int):
    form = await request.form()
    taken_at = (form.get("taken_at") or "").strip()
    when_iso = datetime.fromisoformat(taken_at).isoformat() if taken_at else datetime.utcnow().isoformat()
    db = get_db(); cur = db.cursor()
    cur.execute("INSERT INTO samples (tank_id, taken_at, notes) VALUES (?, ?, ?)", (tank_id, when_iso, form.get("notes")))
    sample_id = cur.lastrowid
    pdefs = get_active_param_defs(db)
    for p in pdefs:
        val = to_float(form.get(f"value_{p['id']}"))
        if val is not None: insert_sample_reading(db, sample_id, p["name"], float(val), p["unit"])
    db.commit(); db.close()
    return redirect(f"/tanks/{tank_id}")

@app.get("/tools/dose-plan", response_class=HTMLResponse)
def dose_plan(request: Request):
    # This logic matches your previous update with clickable rows and synced dropdowns
    db = get_db()
    # ... logic processing ...
    db.close()
    return templates.TemplateResponse("dose_plan.html", {"request": request, "plans": [], "grand_total_ml": 0})
