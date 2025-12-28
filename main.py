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
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DATABASE_PATH", os.path.join(BASE_DIR, "reef.db"))

app = FastAPI(title="Reef Tank Parameters")

# --- 1. RECOMMENDED DEFAULTS ---
RECOMMENDED_DEFAULTS = {
    "Alkalinity/KH": {"target_low": 8.0, "target_high": 9.5, "alert_low": 7.0, "alert_high": 11.0},
    "Calcium": {"target_low": 400, "target_high": 450, "alert_low": 350, "alert_high": 500},
    "Magnesium": {"target_low": 1300, "target_high": 1400, "alert_low": 1200, "alert_high": 1500},
    "Phosphate": {"target_low": 0.03, "target_high": 0.1, "alert_low": 0.0, "alert_high": 0.25},
    "Nitrate": {"target_low": 2, "target_high": 10, "alert_low": 0, "alert_high": 25},
    "Salinity": {"target_low": 34, "target_high": 35.5, "alert_low": 32, "alert_high": 37},
    "Temperature": {"target_low": 25, "target_high": 26.5, "alert_low": 23, "alert_high": 29},
}

# --- 2. INITIAL SEED DEFAULTS ---
INITIAL_DEFAULTS = {
    "Alkalinity/KH": {"default_target_low": 8.0, "default_target_high": 9.5, "default_alert_low": 7.0, "default_alert_high": 11.0},
    "Calcium": {"default_target_low": 400, "default_target_high": 450, "default_alert_low": 350, "default_alert_high": 500},
    "Magnesium": {"default_target_low": 1300, "default_target_high": 1400, "default_alert_low": 1200, "default_alert_high": 1500},
    "Phosphate": {"default_target_low": 0.03, "default_target_high": 0.1, "default_alert_low": 0.0, "default_alert_high": 0.25},
    "Nitrate": {"default_target_low": 2, "default_target_high": 10, "default_alert_low": 0, "default_alert_high": 25},
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
        fv = float(v)
        s = f"{fv:.2f}" 
        return s.rstrip("0").rstrip(".") if "." in s else s
    except: return str(v)

def dtfmt(v: Any) -> str:
    dt = parse_dt_any(v)
    return dt.strftime("%H:%M - %d/%m/%Y") if dt else ""

def time_ago(v: Any) -> str:
    dt = parse_dt_any(v)
    if not dt: return ""
    now = datetime.now()
    diff = now - dt
    if diff.days == 0: return "Today"
    if diff.days == 1: return "Yesterday"
    if diff.days < 30: return f"{diff.days} days ago"
    return f"{int(diff.days / 30)} mo ago"

templates.env.filters.update({"fmt2": fmt2, "dtfmt": dtfmt, "time_ago": time_ago, "tojson": lambda v: json.dumps(v, default=str)})

def _finalize(v: Any) -> Any:
    try:
        if isinstance(v, bool) or v is None: return v
        if isinstance(v, int): return v
        if isinstance(v, float): return fmt2(v)
    except: pass
    return v
templates.env.finalize = _finalize

def redirect(url: str) -> RedirectResponse: return RedirectResponse(url, status_code=303)

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def q(db, sql, params=()): return db.execute(sql, params).fetchall()
def one(db, sql, params=()): return db.execute(sql, params).fetchone()

def row_get(row, key, default=None):
    try: return row[key] if row and key in row.keys() else default
    except: return default

def table_exists(db, name):
    return one(db, "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)) is not None

def to_float(v: Any) -> Optional[float]:
    try: return float(str(v).replace(",", ".")) if v is not None else None
    except: return None

def slug_key(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", (name or "").strip().lower()).strip("_") or "x"

def list_parameters(db):
    if table_exists(db, "parameter_defs"): return q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY sort_order, name")
    return q(db, "SELECT DISTINCT name, COALESCE(unit,'') AS unit FROM parameters ORDER BY name")

def get_active_param_defs(db): return list_parameters(db)

def parse_dt_any(v):
    if not v: return None
    if isinstance(v, datetime): return v
    if isinstance(v, date) and not isinstance(v, datetime): return datetime.combine(v, time.min)
    if isinstance(v, (int, float)): return datetime.fromtimestamp(v / 1000.0 if v > 1e12 else v)
    s = str(v).strip().replace("Z", "").replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try: return datetime.strptime(s[:19], fmt)
        except: continue
    return None

def insert_sample_reading(db, sample_id, pname, value, unit=""):
    pd = one(db, "SELECT id FROM parameter_defs WHERE name=?", (pname,))
    if pd: db.execute("INSERT INTO sample_values (sample_id, parameter_id, value) VALUES (?, ?, ?)", (sample_id, pd["id"], value))

def get_latest_per_parameter(db, tank_id):
    latest_map = {}
    rows = q(db, """
        SELECT pd.id, pd.name, sv.value, s.taken_at FROM sample_values sv 
        JOIN samples s ON s.id = sv.sample_id JOIN parameter_defs pd ON pd.id = sv.parameter_id
        WHERE s.tank_id=? ORDER BY s.taken_at DESC""", (tank_id,))
    for r in rows:
        if r["id"] not in latest_map: 
            latest_map[r["id"]] = {"value": r["value"], "taken_at": parse_dt_any(r["taken_at"])}
    return latest_map

# --- DB INIT ---
def init_db():
    db = get_db(); cur = db.cursor()
    cur.executescript('''
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS tanks (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, volume_l REAL);
        CREATE TABLE IF NOT EXISTS tank_profiles (tank_id INTEGER PRIMARY KEY, volume_l REAL, net_percent REAL DEFAULT 100, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS samples (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, taken_at TEXT NOT NULL, notes TEXT, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS parameter_defs (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, unit TEXT, active INTEGER DEFAULT 1, sort_order INTEGER DEFAULT 0, max_daily_change REAL, default_target_low REAL, default_target_high REAL, default_alert_low REAL, default_alert_high REAL);
        CREATE TABLE IF NOT EXISTS sample_values (sample_id INTEGER, parameter_id INTEGER, value REAL, PRIMARY KEY(sample_id, parameter_id), FOREIGN KEY (sample_id) REFERENCES samples(id) ON DELETE CASCADE, FOREIGN KEY (parameter_id) REFERENCES parameter_defs(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS targets (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, parameter TEXT NOT NULL, target_low REAL, target_high REAL, alert_low REAL, alert_high REAL, unit TEXT, enabled INTEGER DEFAULT 1, UNIQUE(tank_id, parameter), FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS test_kits (id INTEGER PRIMARY KEY AUTOINCREMENT, parameter TEXT NOT NULL, name TEXT NOT NULL, unit TEXT, resolution REAL, min_value REAL, max_value REAL, notes TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS additives (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, parameter TEXT NOT NULL, strength REAL NOT NULL, unit TEXT NOT NULL, max_daily REAL, notes TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS dose_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, additive_id INTEGER, amount_ml REAL NOT NULL, reason TEXT, logged_at TEXT NOT NULL, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS dose_plan_checks (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, parameter TEXT NOT NULL, additive_id INTEGER NOT NULL, planned_date TEXT NOT NULL, checked INTEGER DEFAULT 0, checked_at TEXT, UNIQUE(tank_id, parameter, additive_id, planned_date), FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
    ''')
    def ens(t, c, d):
        cur.execute(f"PRAGMA table_info({t})")
        if c not in [r[1] for r in cur.fetchall()]: cur.execute(d)
    ens("tanks", "volume_l", "ALTER TABLE tanks ADD COLUMN volume_l REAL")
    ens("parameter_defs", "max_daily_change", "ALTER TABLE parameter_defs ADD COLUMN max_daily_change REAL")
    ens("parameter_defs", "default_target_low", "ALTER TABLE parameter_defs ADD COLUMN default_target_low REAL")
    ens("parameter_defs", "default_target_high", "ALTER TABLE parameter_defs ADD COLUMN default_target_high REAL")
    ens("parameter_defs", "default_alert_low", "ALTER TABLE parameter_defs ADD COLUMN default_alert_low REAL")
    ens("parameter_defs", "default_alert_high", "ALTER TABLE parameter_defs ADD COLUMN default_alert_high REAL")
    cur.execute("SELECT COUNT(1) FROM parameter_defs")
    if cur.fetchone()[0] == 0:
        defaults = [("Alkalinity/KH", "dKH", 1, 10), ("Calcium", "ppm", 1, 20), ("Magnesium", "ppm", 1, 30), ("Phosphate", "ppm", 1, 40), ("Nitrate", "ppm", 1, 50), ("Salinity", "ppt", 1, 60), ("Temperature", "°C", 1, 70)]
        cur.executemany("INSERT INTO parameter_defs (name, unit, active, sort_order) VALUES (?, ?, ?, ?)", defaults)
    for name, d in INITIAL_DEFAULTS.items():
        cur.execute("UPDATE parameter_defs SET default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=? WHERE name=? AND default_target_low IS NULL", (d["default_target_low"], d["default_target_high"], d["default_alert_low"], d["default_alert_high"], name))
    db.commit(); db.close()

init_db()

# --- ROUTES ---

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    db = get_db(); tanks = q(db, "SELECT * FROM tanks ORDER BY name"); tank_cards = []
    for t in tanks:
        latest_map = get_latest_per_parameter(db, t["id"])
        latest = one(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT 1", (t["id"],))
        readings = []
        for p in get_active_param_defs(db):
            data = latest_map.get(p["id"])
            if data: readings.append({"name": p["name"], "value": data["value"], "unit": p["unit"] or "", "taken_at": data["taken_at"]})
        tank_cards.append({"tank": dict(t), "latest": latest, "readings": readings})
    db.close(); return templates.TemplateResponse("dashboard.html", {"request": request, "tank_cards": tank_cards, "extra_css": ["/static/dashboard.css"]})

@app.get("/tanks/{tank_id}", response_class=HTMLResponse)
def tank_detail(request: Request, tank_id: int):
    db = get_db(); tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank: db.close(); return redirect("/")
    
    samples_rows = q(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT 50", (tank_id,))
    samples = []
    for r in samples_rows:
        d = dict(r); d["taken_at"] = parse_dt_any(d["taken_at"]); samples.append(d)
    
    latest_vals = get_latest_per_parameter(db, tank_id)
    target_rows = q(db, "SELECT * FROM targets WHERE tank_id=?", (tank_id,))
    target_map = {t["parameter"]: t for t in target_rows}
    params = list_parameters(db)
    
    selected_pid = int(request.query_params.get("parameter") or (params[0]["id"] if params else 0))
    series = []
    rows = q(db, "SELECT s.taken_at, sv.value FROM sample_values sv JOIN samples s ON s.id = sv.sample_id WHERE s.tank_id=? AND sv.parameter_id=? ORDER BY s.taken_at ASC", (tank_id, selected_pid))
    for r in rows: series.append({"x": r["taken_at"], "y": r["value"]})

    sample_vals = {}
    for s in samples:
        v_rows = q(db, "SELECT parameter_id, value FROM sample_values WHERE sample_id=?", (s["id"],))
        sample_vals[s["id"]] = {vr["parameter_id"]: vr["value"] for vr in v_rows}

    db.close()
    return templates.TemplateResponse("tank_detail.html", {
        "request": request, "tank": tank, "params": params, "recent_samples": samples, 
        "sample_values": sample_vals, "latest_vals": latest_vals, "target_map": target_map,
        "selected_parameter_id": selected_pid, "series": series, 
        "chart_targets": [target_map.get(next((p["name"] for p in params if p["id"] == selected_pid), ""))] if params else []
    })

# --- ADVANCED IMPORT/EXPORT CENTER ---
@app.get("/admin/import", response_class=HTMLResponse)
def import_page(request: Request): return templates.TemplateResponse("import_manager.html", {"request": request})

@app.get("/admin/download-template")
def download_template():
    db = get_db(); params = list_parameters(db); tanks = q(db, "SELECT name, volume_l FROM tanks ORDER BY name"); db.close()
    cols = ["Tank Name", "Volume (L)", "Date (YYYY-MM-DD)", "Notes"] + [p["name"] for p in params]
    df = pd.DataFrame(columns=cols)
    today = date.today().isoformat()
    for i, t in enumerate(tanks): df.loc[i] = [t["name"], t["volume_l"], today, "Manual Entry"] + [None]*len(params)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer: df.to_excel(writer, index=False, sheet_name='Log Entries')
    output.seek(0)
    return StreamingResponse(output, headers={'Content-Disposition': 'attachment; filename="Reef_Log_Template.xlsx"'}, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.post("/admin/upload-excel")
async def upload_excel(request: Request, file: UploadFile = File(...)):
    db = get_db(); stats = {"samples": 0}
    try:
        df = pd.read_excel(BytesIO(await file.read()))
        pdefs = list_parameters(db)
        for _, row in df.iterrows():
            t_name = str(row.get("Tank Name", "Unknown")).strip()
            if not t_name or t_name == "nan": continue
            tank = one(db, "SELECT id FROM tanks WHERE name=?", (t_name,))
            if not tank:
                cur = db.cursor(); cur.execute("INSERT INTO tanks (name, volume_l) VALUES (?,?)", (t_name, to_float(row.get("Volume (L)", 0))))
                tid = cur.lastrowid; db.execute("INSERT INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?,?,100)", (tid, to_float(row.get("Volume (L)", 0))))
            else: tid = tank["id"]
            dt = str(row.get("Date (YYYY-MM-DD)", date.today().isoformat()))
            cur = db.cursor(); cur.execute("INSERT INTO samples (tank_id, taken_at, notes) VALUES (?,?,?)", (tid, dt, str(row.get("Notes", ""))))
            sid = cur.lastrowid; stats["samples"] += 1
            for p in pdefs:
                val = to_float(row.get(p["name"]))
                if val is not None: insert_sample_reading(db, sid, p["name"], val)
        db.commit()
        return templates.TemplateResponse("import_manager.html", {"request": request, "success": f"Imported {stats['samples']} records."})
    except Exception as e: return templates.TemplateResponse("import_manager.html", {"request": request, "error": str(e)})
    finally: db.close()

@app.get("/admin/export-all")
def export_all_data():
    db = get_db(); tanks = q(db, "SELECT * FROM tanks"); params = list_parameters(db); output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for t in tanks:
            df = get_tank_export_df(db, t["id"], params)
            if not df.empty: df.to_excel(writer, sheet_name=re.sub(r'[\\/*?:[\]]', '', t["name"])[:31], index=False)
    db.close(); output.seek(0)
    return StreamingResponse(output, headers={'Content-Disposition': 'attachment; filename="Reef_Backup.xlsx"'}, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.get("/tanks/{tank_id}/export")
def export_single_tank(tank_id: int):
    db = get_db(); tank = one(db, "SELECT name FROM tanks WHERE id=?", (tank_id,)); params = list_parameters(db)
    df = get_tank_export_df(db, tank_id, params); db.close(); output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer: df.to_excel(writer, sheet_name='Logs', index=False)
    output.seek(0); return StreamingResponse(output, headers={'Content-Disposition': f'attachment; filename="{slug_key(tank["name"])}_logs.xlsx"'}, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

def get_tank_export_df(db, tid, params):
    samples = q(db, "SELECT id, taken_at, notes FROM samples WHERE tank_id=? ORDER BY taken_at DESC", (tid,))
    if not samples: return pd.DataFrame()
    rows = []
    for s in samples:
        row = {"Date": s["taken_at"], "Notes": s["notes"]}
        vals = {v["name"]: v["value"] for v in q(db, "SELECT pd.name, sv.value FROM sample_values sv JOIN parameter_defs pd ON pd.id = sv.parameter_id WHERE sv.sample_id=?", (s["id"],))}
        for p in params: row[p["name"]] = vals.get(p["name"])
        rows.append(row)
    return pd.DataFrame(rows)

# --- Standard CRUD (The Rest) ---
@app.post("/tanks/new")
def tank_new(name: str = Form(...), volume_l: float | None = Form(None)):
    db = get_db(); cur = db.cursor(); cur.execute("INSERT INTO tanks (name, volume_l) VALUES (?, ?)", (name.strip(), volume_l))
    tid = cur.lastrowid; db.execute("INSERT OR IGNORE INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?, ?, 100)", (tid, volume_l))
    db.commit(); db.close(); return redirect(f"/tanks/{tid}")

@app.post("/tanks/{tank_id}/add")
async def add_sample(request: Request, tank_id: int):
    form = await request.form(); notes = form.get("notes"); taken_at = form.get("taken_at") or datetime.now().isoformat()
    db = get_db(); cur = db.cursor(); cur.execute("INSERT INTO samples (tank_id, taken_at, notes) VALUES (?, ?, ?)", (tank_id, taken_at, notes))
    sid = cur.lastrowid
    for p in list_parameters(db):
        val = to_float(form.get(f"value_{p['id']}"))
        if val is not None: insert_sample_reading(db, sid, p["name"], val)
    db.commit(); db.close(); return redirect(f"/tanks/{tank_id}")

@app.post("/tanks/{tank_id}/delete")
async def tank_delete(tank_id: int):
    db = get_db(); db.execute("DELETE FROM tanks WHERE id=?", (tank_id,)); db.commit(); db.close(); return redirect("/")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
