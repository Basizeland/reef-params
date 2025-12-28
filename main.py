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

# --- 1. RECOMMENDED DEFAULTS (Ghosting for UI) ---
RECOMMENDED_DEFAULTS = {
    "Alkalinity - KH": {"target_low": 8.0, "target_high": 9.5, "alert_low": 7.0, "alert_high": 11.0},
    "Calcium - CA": {"target_low": 400, "target_high": 450, "alert_low": 350, "alert_high": 500},
    "Magnesium - MG": {"target_low": 1300, "target_high": 1400, "alert_low": 1200, "alert_high": 1500},
    "Phosphate - PO4": {"target_low": 0.03, "target_high": 0.1, "alert_low": 0.0, "alert_high": 0.25},
    "Nitrate - NO3": {"target_low": 2, "target_high": 10, "alert_low": 0, "alert_high": 25},
    "Salinity": {"target_low": 34, "target_high": 35.5, "alert_low": 32, "alert_high": 37},
    "Temperature": {"target_low": 25, "target_high": 26.5, "alert_low": 23, "alert_high": 29},
}

# --- 2. INITIAL SEED DEFAULTS (DB Migration) ---
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

# --- Database Core Helpers ---
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def q(db, sql, params=()): return db.execute(sql, params).fetchall()
def one(db, sql, params=()): return db.execute(sql, params).fetchone()

def row_get(row, key, default=None):
    try:
        if row is None: return default
        if isinstance(row, dict): return row.get(key, default)
        return row[key] if key in row.keys() else default
    except: return default

def to_float(v: Any) -> Optional[float]:
    try: return float(str(v).replace(",", ".")) if v is not None else None
    except: return None

def slug_key(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", (name or "").strip().lower()).strip("_") or "x"

def parse_dt_any(v):
    if not v: return None
    if isinstance(v, datetime): return v
    if isinstance(v, (int, float)): return datetime.fromtimestamp(v / 1000.0 if v > 1e12 else v)
    s = str(v).replace("Z", "").replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try: return datetime.strptime(s[:19], fmt)
        except: continue
    return None

def get_active_param_defs(db):
    return q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY sort_order, name")

def list_parameters(db): return get_active_param_defs(db)

def insert_sample_reading(db, sample_id, pname, value, unit=""):
    pd = one(db, "SELECT id FROM parameter_defs WHERE name=?", (pname,))
    if pd:
        db.execute("INSERT INTO sample_values (sample_id, parameter_id, value) VALUES (?, ?, ?)", (sample_id, pd["id"], value))

def get_latest_per_parameter(db, tank_id):
    latest_map = {}
    rows = q(db, """
        SELECT pd.name, sv.value, s.taken_at FROM sample_values sv 
        JOIN samples s ON s.id = sv.sample_id JOIN parameter_defs pd ON pd.id = sv.parameter_id
        WHERE s.tank_id=? ORDER BY s.taken_at DESC""", (tank_id,))
    for r in rows:
        if r["name"] not in latest_map:
            latest_map[r["name"]] = {"value": r["value"], "taken_at": parse_dt_any(r["taken_at"])}
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
    cur.execute("SELECT COUNT(1) FROM parameter_defs")
    if cur.fetchone()[0] == 0:
        defaults = [("Alkalinity - KH", "dKH", 1, 10), ("Calcium - CA", "ppm", 1, 20), ("Magnesium - MG", "ppm", 1, 30), ("Phosphate - PO4", "ppm", 1, 40), ("Nitrate - NO3", "ppm", 1, 50), ("Salinity", "ppt", 1, 60), ("Temperature", "°C", 1, 70)]
        cur.executemany("INSERT INTO parameter_defs (name, unit, active, sort_order) VALUES (?, ?, ?, ?)", defaults)
    for name, d in INITIAL_DEFAULTS.items():
        cur.execute("UPDATE parameter_defs SET default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=? WHERE name=? AND default_target_low IS NULL", (d["default_target_low"], d["default_target_high"], d["default_alert_low"], d["default_alert_high"], name))
    db.commit(); db.close()

init_db()

# --- Dashboard ---
@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    db = get_db(); tanks = q(db, "SELECT * FROM tanks ORDER BY name"); tank_cards = []
    for t in tanks:
        latest_map = get_latest_per_parameter(db, t["id"])
        latest = one(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT 1", (t["id"],))
        readings = []
        for p in get_active_param_defs(db):
            data = latest_map.get(p["name"])
            if data: readings.append({"name": p["name"], "value": data["value"], "unit": p["unit"] or "", "taken_at": data["taken_at"]})
        tank_cards.append({"tank": dict(t), "latest": latest, "readings": readings})
    db.close(); return templates.TemplateResponse("dashboard.html", {"request": request, "tank_cards": tank_cards})

# --- Tank Management ---
@app.get("/tanks/new", response_class=HTMLResponse)
def tank_new_form(request: Request):
    return templates.TemplateResponse("tank_profile.html", {"request": request, "tank": None})

@app.post("/tanks/new")
def tank_new(name: str = Form(...), volume_l: float = Form(None)):
    db = get_db(); cur = db.cursor()
    cur.execute("INSERT INTO tanks (name, volume_l) VALUES (?,?)", (name, volume_l))
    tid = cur.lastrowid
    cur.execute("INSERT INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?,?,100)", (tid, volume_l))
    db.commit(); db.close(); return RedirectResponse(f"/tanks/{tid}", status_code=303)

@app.post("/tanks/{tank_id}/delete")
def tank_delete(tank_id: int):
    db = get_db(); db.execute("DELETE FROM tanks WHERE id=?", (tank_id,)); db.commit(); db.close(); return RedirectResponse("/", status_code=303)

@app.get("/tanks/{tank_id}", response_class=HTMLResponse)
def tank_detail(request: Request, tank_id: int):
    db = get_db(); tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank: return RedirectResponse("/", status_code=303)
    
    samples_rows = q(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT 50", (tank_id,))
    samples = []
    for r in samples_rows:
        d = dict(r); d["taken_at"] = parse_dt_any(d["taken_at"]); samples.append(d)
    
    latest_vals = get_latest_per_parameter(db, tank_id)
    target_rows = q(db, "SELECT * FROM targets WHERE tank_id=?", (tank_id,))
    target_map = {t["parameter"]: t for t in target_rows}
    params = list_parameters(db)
    
    selected_pid = request.query_params.get("parameter") or (params[0]["name"] if params else "")
    series = []
    rows = q(db, """
        SELECT s.taken_at, sv.value FROM sample_values sv 
        JOIN samples s ON s.id = sv.sample_id JOIN parameter_defs pd ON pd.id = sv.parameter_id
        WHERE s.tank_id=? AND pd.name=? ORDER BY s.taken_at ASC""", (tank_id, selected_pid))
    for r in rows:
        dt = parse_dt_any(r["taken_at"])
        if dt: series.append({"x": dt.isoformat(), "y": r["value"]})

    sample_vals = {}
    for s in samples:
        v_rows = q(db, "SELECT parameter_id, value FROM sample_values WHERE sample_id=?", (s["id"],))
        sample_vals[s["id"]] = {vr["parameter_id"]: vr["value"] for vr in v_rows}

    db.close()
    return templates.TemplateResponse("tank_detail.html", {
        "request": request, "tank": tank, "params": params, "recent_samples": samples, 
        "sample_values": sample_vals, "latest_vals": latest_vals, "target_map": target_map,
        "selected_parameter_id": selected_pid, "series": series, 
        "chart_targets": [target_map.get(selected_pid)] if selected_pid in target_map else []
    })

@app.get("/tanks/{tank_id}/profile", response_class=HTMLResponse)
def tank_profile_edit(request: Request, tank_id: int):
    db = get_db(); tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    prof = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    db.close(); return templates.TemplateResponse("tank_profile.html", {"request": request, "tank": dict(tank), "profile": prof})

@app.post("/tanks/{tank_id}/profile")
async def tank_profile_save(tank_id: int, name: str = Form(...), volume_l: float = Form(...), net_percent: float = Form(100)):
    db = get_db(); db.execute("UPDATE tanks SET name=?, volume_l=? WHERE id=?", (name, volume_l, tank_id))
    db.execute("INSERT INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?,?,?) ON CONFLICT(tank_id) DO UPDATE SET volume_l=excluded.volume_l, net_percent=excluded.net_percent", (tank_id, volume_l, net_percent))
    db.commit(); db.close(); return RedirectResponse(f"/tanks/{tank_id}", status_code=303)

# --- Sample Management ---
@app.get("/tanks/{tank_id}/add", response_class=HTMLResponse)
def add_sample_form(request: Request, tank_id: int):
    db = get_db(); tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    params = get_active_param_defs(db); db.close()
    return templates.TemplateResponse("add_sample.html", {"request": request, "tank": tank, "parameters": params})

@app.post("/tanks/{tank_id}/add")
async def add_sample_save(request: Request, tank_id: int):
    form = await request.form(); taken_at = form.get("taken_at") or datetime.now().isoformat()
    db = get_db(); cur = db.cursor()
    cur.execute("INSERT INTO samples (tank_id, taken_at, notes) VALUES (?,?,?)", (tank_id, taken_at, form.get("notes")))
    sid = cur.lastrowid
    for p in list_parameters(db):
        val = to_float(form.get(f"value_{p['id']}"))
        if val is not None: insert_sample_reading(db, sid, p["name"], val)
    db.commit(); db.close(); return RedirectResponse(f"/tanks/{tank_id}", status_code=303)

@app.get("/tanks/{tank_id}/samples/{sample_id}", response_class=HTMLResponse)
def sample_detail(request: Request, tank_id: int, sample_id: int):
    db = get_db(); tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    sample = one(db, "SELECT * FROM samples WHERE id=? AND tank_id=?", (sample_id, tank_id))
    if not tank or not sample: raise HTTPException(status_code=404)
    s = dict(sample); s["taken_at"] = parse_dt_any(s["taken_at"])
    # Get values
    mode = values_mode(db)
    if mode == "sample_values":
        readings = q(db, "SELECT pd.name, sv.value, pd.unit FROM sample_values sv JOIN parameter_defs pd ON pd.id = sv.parameter_id WHERE sv.sample_id=?", (sample_id,))
    else:
        readings = q(db, "SELECT name, value, unit FROM parameters WHERE sample_id=?", (sample_id,))
    db.close(); return templates.TemplateResponse("sample_detail.html", {"request": request, "tank": tank, "sample": s, "readings": readings})

@app.post("/samples/{sample_id}/delete")
def sample_delete(sample_id: int, tank_id: int = Form(...)):
    db = get_db(); db.execute("DELETE FROM samples WHERE id=?", (sample_id,)); db.commit(); db.close()
    return RedirectResponse(f"/tanks/{tank_id}", status_code=303)

# --- Excel Import Center ---
@app.get("/admin/import", response_class=HTMLResponse)
def import_page(request: Request): return templates.TemplateResponse("import_manager.html", {"request": request})

@app.get("/admin/download-template")
def download_template():
    db = get_db(); params = list_parameters(db); db.close()
    cols = ["Tank Name", "Volume (L)", "Date (YYYY-MM-DD)", "Notes"] + [p["name"] for p in params]
    df = pd.DataFrame(columns=cols)
    df.loc[0] = ["Example Tank", 450, date.today().isoformat(), "Sample check"] + [None]*len(params)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Readings')
    output.seek(0)
    return FileResponse(output, filename="Reef_Import_Template.xlsx", media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.post("/admin/upload-excel")
async def upload_excel(request: Request, file: UploadFile = File(...)):
    db = get_db(); stats = {"tanks": 0, "samples": 0}
    try:
        df = pd.read_excel(BytesIO(await file.read()))
        cur = db.cursor()
        pdefs = list_parameters(db)
        for _, row in df.iterrows():
            t_name = str(row.get("Tank Name", "Unknown"))
            tank = one(db, "SELECT id FROM tanks WHERE name=?", (t_name,))
            if not tank:
                cur.execute("INSERT INTO tanks (name, volume_l) VALUES (?,?)", (t_name, to_float(row.get("Volume (L)", 0))))
                tid = cur.lastrowid; stats["tanks"] += 1
                cur.execute("INSERT INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?,?,100)", (tid, to_float(row.get("Volume (L)", 0))))
            else: tid = tank["id"]
            
            dt_str = str(row.get("Date (YYYY-MM-DD)", datetime.now().isoformat()))
            cur.execute("INSERT INTO samples (tank_id, taken_at, notes) VALUES (?,?,?)", (tid, dt_str, str(row.get("Notes", ""))))
            sid = cur.lastrowid; stats["samples"] += 1
            
            for p in pdefs:
                val = to_float(row.get(p["name"]))
                if val is not None: insert_sample_reading(db, sid, p["name"], val)
        db.commit()
        return templates.TemplateResponse("import_manager.html", {"request": request, "success": f"Imported {stats['samples']} samples."})
    except Exception as e: return templates.TemplateResponse("import_manager.html", {"request": request, "error": str(e)})
    finally: db.close()

# --- Tools: Calculators ---
@app.get("/tools/calculators", response_class=HTMLResponse)
def calculators_form(request: Request):
    db = get_db(); tanks = q(db, "SELECT * FROM tanks ORDER BY name"); adds = q(db, "SELECT * FROM additives WHERE active=1 ORDER BY parameter, name")
    profs = {p["tank_id"]: p for p in q(db, "SELECT * FROM tank_profiles")}
    db.close(); return templates.TemplateResponse("calculators.html", {"request": request, "tanks": tanks, "additives": adds, "profiles": profs})

@app.post("/tools/calculators")
def calculators_post(request: Request, tank_id: int = Form(...), additive_id: int = Form(...), desired_change: float = Form(...)):
    db = get_db(); tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,)); additive = one(db, "SELECT * FROM additives WHERE id=?", (additive_id,))
    prof = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    vol = (row_get(prof, "volume_l") or row_get(tank, "volume_l") or 0) * (row_get(prof, "net_percent", 100) / 100.0)
    
    dose_ml = (desired_change / additive["strength"]) * (vol / 100.0) if additive["strength"] else 0
    pdef = one(db, "SELECT max_daily_change, unit FROM parameter_defs WHERE name=?", (additive["parameter"],))
    mdc = row_get(pdef, "max_daily_change")
    days, daily_ml = 1, dose_ml
    if mdc and desired_change > mdc:
        days = int(math.ceil(desired_change / mdc))
        daily_ml = dose_ml / days

    res = {"dose_ml": round(dose_ml, 2), "days": days, "daily_ml": round(daily_ml, 2), "unit": row_get(pdef, "unit") or additive["unit"], "tank": tank, "additive": additive, "desired_change": desired_change, "daily_change": desired_change/days if days > 1 else desired_change}
    tanks = q(db, "SELECT * FROM tanks ORDER BY name"); adds = q(db, "SELECT * FROM additives WHERE active=1 ORDER BY parameter, name")
    profs = {p["tank_id"]: p for p in q(db, "SELECT * FROM tank_profiles")}
    db.close(); return templates.TemplateResponse("calculators.html", {"request": request, "result": res, "tanks": tanks, "additives": adds, "profiles": profs, "selected": {"tank_id": tank_id, "additive_id": additive_id}})

# --- Tools: Dose Plan ---
@app.get("/tools/dose-plan", response_class=HTMLResponse)
def dose_plan_route(request: Request):
    db = get_db(); today = date.today(); plans = []; grand_total_ml = 0.0
    chk_rows = q(db, "SELECT tank_id, parameter, additive_id, planned_date, checked FROM dose_plan_checks WHERE planned_date >= ?", (today.isoformat(),))
    check_map = {(r["tank_id"], r["parameter"], r["additive_id"], r["planned_date"]): r["checked"] for r in chk_rows}

    for t in q(db, "SELECT * FROM tanks ORDER BY name"):
        prof = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (t["id"],))
        vol = (row_get(prof, "volume_l") or row_get(t, "volume_l") or 0) * (row_get(prof, "net_percent", 100) / 100.0)
        latest_map = get_latest_per_parameter(db, t["id"])
        tank_rows = []
        
        for tr in q(db, "SELECT * FROM targets WHERE tank_id=? AND enabled=1", (t["id"],)):
            pname = tr["parameter"]; cur_val = latest_map.get(pname, {}).get("value")
            target_val = ((row_get(tr, "target_low") or 0) + (row_get(tr, "target_high") or 0)) / 2.0
            
            if cur_val is not None and cur_val < target_val:
                delta = target_val - cur_val
                pdef = one(db, "SELECT max_daily_change FROM parameter_defs WHERE name=?", (pname,))
                mdc = row_get(pdef, "max_daily_change")
                days = int(math.ceil(delta / mdc)) if mdc and delta > mdc else 1
                
                add_rows = []
                pref = one(db, "SELECT additive_id FROM dose_logs WHERE tank_id=? AND reason LIKE ? ORDER BY logged_at DESC LIMIT 1", (t["id"], f"%{pname}%"))
                pref_id = pref["additive_id"] if pref else None

                for a in q(db, "SELECT * FROM additives WHERE parameter=? AND active=1", (pname,)):
                    total_ml = (delta / a["strength"]) * (vol / 100.0) if a["strength"] else 0
                    sched = []
                    for i in range(days):
                        d = (today + timedelta(days=i)).isoformat()
                        sched.append({"day": i+1, "date": d, "ml": total_ml/days, "checked": check_map.get((t["id"], pname, a["id"], d), 0)})
                    add_rows.append({"additive_id": a["id"], "additive_name": a["name"], "total_ml": total_ml, "schedule": sched, "selected": (a["id"] == pref_id or not add_rows)})
                
                tank_rows.append({"parameter": pname, "latest": cur_val, "target": target_val, "unit": tr["unit"], "additives": add_rows})
        
        tank_total = sum(next((a["total_ml"] for a in r["additives"] if a["selected"]), 0) for r in tank_rows)
        grand_total_ml += tank_total
        plans.append({"tank": dict(t), "eff_vol_l": vol, "rows": tank_rows, "total_ml": tank_total})
    
    db.close(); return templates.TemplateResponse("dose_plan.html", {"request": request, "plans": plans, "grand_total_ml": grand_total_ml})

@app.post("/tools/dose-plan/check")
async def dose_plan_check(request: Request):
    form = await request.form(); db = get_db()
    tid, aid, param, p_date, ml, checked = form.get("tank_id"), form.get("additive_id"), form.get("parameter"), form.get("planned_date"), form.get("amount_ml"), form.get("checked")
    db.execute("INSERT INTO dose_plan_checks (tank_id, parameter, additive_id, planned_date, checked) VALUES (?,?,?,?,?) ON CONFLICT(tank_id, parameter, additive_id, planned_date) DO UPDATE SET checked=excluded.checked", (tid, param, aid, p_date, checked))
    if int(checked):
        db.execute("INSERT INTO dose_logs (tank_id, additive_id, amount_ml, reason, logged_at) VALUES (?,?,?,?,?)", (tid, aid, ml, f"Dose plan: {param} ({p_date})", f"{p_date}T00:00:00"))
    else:
        db.execute("DELETE FROM dose_logs WHERE tank_id=? AND additive_id=? AND reason=?", (tid, aid, f"Dose plan: {param} ({p_date})"))
    db.commit(); db.close(); return {"ok": True}

# --- Management & CRUD ---
@app.get("/additives", response_class=HTMLResponse)
def additives_list(request: Request):
    db = get_db(); rows = q(db, "SELECT * FROM additives ORDER BY parameter, name"); db.close()
    return templates.TemplateResponse("additives.html", {"request": request, "additives": rows})

@app.get("/additives/new", response_class=HTMLResponse)
def additive_new(request: Request):
    db = get_db(); params = list_parameters(db); db.close()
    return templates.TemplateResponse("additive_edit.html", {"request": request, "additive": None, "parameters": params})

@app.get("/additives/{additive_id}/edit", response_class=HTMLResponse)
def additive_edit(request: Request, additive_id: int):
    db = get_db(); additive = one(db, "SELECT * FROM additives WHERE id=?", (additive_id,))
    params = list_parameters(db); db.close()
    return templates.TemplateResponse("additive_edit.html", {"request": request, "additive": additive, "parameters": params})

@app.post("/additives/save")
def additive_save(additive_id: str = Form(None), name: str = Form(...), parameter: str = Form(...), strength: float = Form(...), unit: str = Form(...), active: str = Form(None)):
    db = get_db(); is_active = 1 if active else 0
    if additive_id and additive_id != "None": db.execute("UPDATE additives SET name=?, parameter=?, strength=?, unit=?, active=? WHERE id=?", (name, parameter, strength, unit, is_active, additive_id))
    else: db.execute("INSERT INTO additives (name, parameter, strength, unit, active) VALUES (?,?,?,?,?)", (name, parameter, strength, unit, is_active))
    db.commit(); db.close(); return RedirectResponse("/additives", status_code=303)

@app.post("/additives/{additive_id}/delete")
def additive_delete(additive_id: int):
    db = get_db(); db.execute("DELETE FROM additives WHERE id=?", (additive_id,)); db.commit(); db.close(); return RedirectResponse("/additives", status_code=303)

@app.get("/settings/parameters", response_class=HTMLResponse)
def parameters_settings(request: Request):
    db = get_db(); rows = q(db, "SELECT * FROM parameter_defs ORDER BY sort_order, name"); db.close()
    return templates.TemplateResponse("parameters.html", {"request": request, "parameters": rows})

@app.get("/settings/parameters/{param_id}/edit", response_class=HTMLResponse)
def parameter_edit(request: Request, param_id: int):
    db = get_db(); row = one(db, "SELECT * FROM parameter_defs WHERE id=?", (param_id,)); db.close()
    return templates.TemplateResponse("parameter_edit.html", {"request": request, "param": row})

@app.post("/settings/parameters/save")
def parameter_save(param_id: str = Form(None), name: str = Form(...), unit: str = Form(None), sort_order: int = Form(0), max_daily_change: float = Form(None), active: str = Form(None)):
    db = get_db(); is_active = 1 if active else 0
    if param_id and param_id != "None": db.execute("UPDATE parameter_defs SET name=?, unit=?, sort_order=?, max_daily_change=?, active=? WHERE id=?", (name, unit, sort_order, max_daily_change, is_active, param_id))
    else: db.execute("INSERT INTO parameter_defs (name, unit, sort_order, max_daily_change, active) VALUES (?,?,?,?,?)", (name, unit, sort_order, max_daily_change, is_active))
    db.commit(); db.close(); return RedirectResponse("/settings/parameters", status_code=303)

@app.get("/tanks/{tank_id}/targets", response_class=HTMLResponse)
def edit_targets_route(request: Request, tank_id: int):
    db = get_db(); tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    params = list_parameters(db); existing = {t["parameter"]: t for t in q(db, "SELECT * FROM targets WHERE tank_id=?", (tank_id,))}
    rows = []
    for p in params:
        t = existing.get(p["name"])
        rows.append({
            "parameter": p, "key": slug_key(p["name"]), 
            "target_low": row_get(t, "target_low") or row_get(p, "default_target_low"), 
            "target_high": row_get(t, "target_high") or row_get(p, "default_target_high"), 
            "enabled": row_get(t, "enabled", 1)
        })
    db.close(); return templates.TemplateResponse("edit_targets.html", {"request": request, "tank": tank, "rows": rows})

@app.post("/tanks/{tank_id}/targets")
async def save_targets_route(request: Request, tank_id: int):
    form = await request.form(); db = get_db()
    for p in list_parameters(db):
        key = slug_key(p["name"])
        low, high = to_float(form.get(f"target_low_{key}")), to_float(form.get(f"target_high_{key}"))
        enabled = 1 if form.get(f"enabled_{key}") else 0
        db.execute("INSERT INTO targets (tank_id, parameter, target_low, target_high, enabled, unit) VALUES (?,?,?,?,?,?) ON CONFLICT(tank_id, parameter) DO UPDATE SET target_low=excluded.target_low, target_high=excluded.target_high, enabled=excluded.enabled", (tank_id, p["name"], low, high, enabled, p["unit"]))
    db.commit(); db.close(); return RedirectResponse(f"/tanks/{tank_id}", status_code=303)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
