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

# --- 1. RECOMMENDED DEFAULTS (Used for the UI "Ghosting" logic) ---
RECOMMENDED_DEFAULTS = {
    "Alkalinity/KH": {"target_low": 8.0, "target_high": 9.5, "alert_low": 7.0, "alert_high": 11.0},
    "Calcium": {"target_low": 400, "target_high": 450, "alert_low": 350, "alert_high": 500},
    "Magnesium": {"target_low": 1300, "target_high": 1400, "alert_low": 1200, "alert_high": 1500},
    "Phosphate": {"target_low": 0.03, "target_high": 0.1, "alert_low": 0.0, "alert_high": 0.25},
    "Nitrate": {"target_low": 2, "target_high": 10, "alert_low": 0, "alert_high": 25},
    "Salinity": {"target_low": 34, "target_high": 35.5, "alert_low": 32, "alert_high": 37},
    "Temperature": {"target_low": 25, "target_high": 26.5, "alert_low": 23, "alert_high": 29},
}

# --- 2. INITIAL SEED DEFAULTS (Used for DB Migration only) ---
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

def format_value(p: Any, v: Any) -> str:
    if v is None: return ""
    try: fv = float(v)
    except Exception: return str(v)
    s = f"{fv:.2f}"
    if "." in s: s = s.rstrip("0").rstrip(".")
    return s

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
        rows = q(db, """
            SELECT pd.name, sv.value, s.taken_at 
            FROM sample_values sv 
            JOIN samples s ON s.id = sv.sample_id 
            JOIN parameter_defs pd ON pd.id = sv.parameter_id
            WHERE s.tank_id=? 
            ORDER BY s.taken_at DESC
        """, (tank_id,))
    else:
        rows = q(db, """
            SELECT p.name, p.value, s.taken_at 
            FROM parameters p
            JOIN samples s ON s.id = p.sample_id 
            WHERE s.tank_id=? 
            ORDER BY s.taken_at DESC
        """, (tank_id,))
    for r in rows:
        name = r["name"]
        if name not in latest_map:
            latest_map[name] = {"value": r["value"], "taken_at": parse_dt_any(r["taken_at"])}
    return latest_map

# --- DATABASE MODELS ---
from sqlalchemy import Column, Integer, String, Float, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from database import Base

class DictMixin:
    def __getitem__(self, key): return getattr(self, key)
    def get(self, key, default=None): return getattr(self, key, default)

class Tank(Base, DictMixin):
    __tablename__ = "tanks"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    volume_l = Column(Float, nullable=True)
    profile = relationship("TankProfile", uselist=False, back_populates="tank", cascade="all, delete-orphan")
    samples = relationship("Sample", back_populates="tank", order_by="desc(Sample.taken_at)", cascade="all, delete-orphan")
    targets = relationship("Target", back_populates="tank", cascade="all, delete-orphan")
    dose_logs = relationship("DoseLog", back_populates="tank", cascade="all, delete-orphan")

class TankProfile(Base, DictMixin):
    __tablename__ = "tank_profiles"
    tank_id = Column(Integer, ForeignKey("tanks.id"), primary_key=True)
    volume_l = Column(Float)
    net_percent = Column(Float, default=100)
    tank = relationship("Tank", back_populates="profile")

class ParameterDef(Base, DictMixin):
    __tablename__ = "parameter_defs"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)
    unit = Column(String, nullable=True)
    active = Column(Integer, default=1)
    sort_order = Column(Integer, default=0)
    max_daily_change = Column(Float, nullable=True)
    default_target_low = Column(Float, nullable=True)
    default_target_high = Column(Float, nullable=True)
    default_alert_low = Column(Float, nullable=True)
    default_alert_high = Column(Float, nullable=True)

class Sample(Base, DictMixin):
    __tablename__ = "samples"
    id = Column(Integer, primary_key=True, index=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"), nullable=False)
    taken_at = Column(String, nullable=False)
    notes = Column(String, nullable=True)
    tank = relationship("Tank", back_populates="samples")
    values = relationship("SampleValue", back_populates="sample", cascade="all, delete-orphan")
    @property
    def values_dict(self): return {v.parameter_id: v.value for v in self.values}

class SampleValue(Base, DictMixin):
    __tablename__ = "sample_values"
    sample_id = Column(Integer, ForeignKey("samples.id"), primary_key=True)
    parameter_id = Column(Integer, ForeignKey("parameter_defs.id"), primary_key=True)
    value = Column(Float)
    sample = relationship("Sample", back_populates="values")
    parameter_def = relationship("ParameterDef")

class Target(Base, DictMixin):
    __tablename__ = "targets"
    id = Column(Integer, primary_key=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"), nullable=False)
    parameter = Column(String, nullable=False)
    target_low = Column(Float); target_high = Column(Float)
    alert_low = Column(Float); alert_high = Column(Float)
    unit = Column(String); enabled = Column(Integer, default=1)
    tank = relationship("Tank", back_populates="targets")

class TestKit(Base, DictMixin):
    __tablename__ = "test_kits"
    id = Column(Integer, primary_key=True); parameter = Column(String) 
    name = Column(String); unit = Column(String); resolution = Column(Float)
    min_value = Column(Float); max_value = Column(Float); notes = Column(String)
    active = Column(Integer, default=1)

class Additive(Base, DictMixin):
    __tablename__ = "additives"
    id = Column(Integer, primary_key=True); name = Column(String)
    parameter = Column(String); strength = Column(Float); unit = Column(String)
    max_daily = Column(Float); notes = Column(String); active = Column(Integer, default=1)

class DoseLog(Base, DictMixin):
    __tablename__ = "dose_logs"
    id = Column(Integer, primary_key=True); tank_id = Column(Integer, ForeignKey("tanks.id"))
    additive_id = Column(Integer, ForeignKey("additives.id"), nullable=True)
    amount_ml = Column(Float); reason = Column(String); logged_at = Column(String)
    tank = relationship("Tank", back_populates="dose_logs"); additive = relationship("Additive")

class DosePlanCheck(Base, DictMixin):
    __tablename__ = "dose_plan_checks"
    id = Column(Integer, primary_key=True); tank_id = Column(Integer, ForeignKey("tanks.id"))
    parameter = Column(String); additive_id = Column(Integer); planned_date = Column(String)
    checked = Column(Integer, default=0); checked_at = Column(String)
    __table_args__ = (UniqueConstraint('tank_id', 'parameter', 'additive_id', 'planned_date', name='_tank_param_add_date_uc'),)

def init_db() -> None:
    db = get_db(); cur = db.cursor()
    cur.executescript('''
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS tanks (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS tank_profiles (tank_id INTEGER PRIMARY KEY, volume_l REAL, net_percent REAL DEFAULT 100, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS samples (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, taken_at TEXT NOT NULL, notes TEXT, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS parameter_defs (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, unit TEXT, active INTEGER DEFAULT 1, sort_order INTEGER DEFAULT 0, max_daily_change REAL);
        CREATE TABLE IF NOT EXISTS parameters (id INTEGER PRIMARY KEY AUTOINCREMENT, sample_id INTEGER NOT NULL, name TEXT NOT NULL, value REAL, unit TEXT, test_kit_id INTEGER, FOREIGN KEY (sample_id) REFERENCES samples(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS targets (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, parameter TEXT NOT NULL, low REAL, high REAL, unit TEXT, enabled INTEGER DEFAULT 1, UNIQUE(tank_id, parameter), FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS test_kits (id INTEGER PRIMARY KEY AUTOINCREMENT, parameter TEXT NOT NULL, name TEXT NOT NULL, unit TEXT, resolution REAL, min_value REAL, max_value REAL, notes TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS additives (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, parameter TEXT NOT NULL, strength REAL NOT NULL, unit TEXT NOT NULL, max_daily REAL, notes TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS dose_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, additive_id INTEGER, amount_ml REAL NOT NULL, reason TEXT, logged_at TEXT NOT NULL, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS dose_plan_checks (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, parameter TEXT NOT NULL, additive_id INTEGER NOT NULL, planned_date TEXT NOT NULL, checked INTEGER DEFAULT 0, checked_at TEXT, UNIQUE(tank_id, parameter, additive_id, planned_date), FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
    ''')
    def ensure_col(table: str, col: str, ddl: str) -> None:
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
        defaults = [("Alkalinity/KH", "dKH", 1, 10), ("Calcium", "ppm", 1, 20), ("Magnesium", "ppm", 1, 30), ("Phosphate", "ppm", 1, 40), ("Nitrate", "ppm", 1, 50), ("Salinity", "ppt", 1, 60), ("Temperature", "°C", 1, 70)]
        cur.executemany("INSERT INTO parameter_defs (name, unit, active, sort_order) VALUES (?, ?, ?, ?)", defaults)
    for name, d in INITIAL_DEFAULTS.items():
        cur.execute("UPDATE parameter_defs SET default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=? WHERE name=? AND default_target_low IS NULL", (d["default_target_low"], d["default_target_high"], d["default_alert_low"], d["default_alert_high"], name))
    db.commit(); db.close()

init_db()

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
    db.close(); return templates.TemplateResponse("dashboard.html", {"request": request, "tank_cards": tank_cards, "extra_css": ["/static/dashboard.css"]})

@app.get("/add", response_class=HTMLResponse)
def add_reading_selector(request: Request):
    db = get_db(); tanks = q(db, "SELECT * FROM tanks ORDER BY name"); db.close()
    html = """<html><head><title>Add Reading</title></head><body style="font-family: sans-serif; max-width: 720px; margin: 40px auto;"><h2>Add Reading</h2><form method="get" action="/tanks/0/add" onsubmit="event.preventDefault(); window.location = '/tanks/' + document.getElementById('tank').value + '/add';"><label>Select tank</label><br/><select id="tank" name="tank" style="width:100%; padding:10px; margin:10px 0;" required>{options}</select><button type="submit" style="padding:10px 16px;">Continue</button></form><p><a href="/">Back to dashboard</a></p></body></html>"""
    options = "\n".join([f'<option value="{t["id"]}">{t["name"]}</option>' for t in tanks])
    return HTMLResponse(html.format(options=options))

@app.get("/tanks/new", response_class=HTMLResponse)
def tank_new_form(request: Request):
    html = """<html><head><title>Add Tank</title></head><body style="font-family: sans-serif; max-width: 720px; margin: 40px auto;"><h2>Add Tank</h2><form method="post" action="/tanks/new"><label>Tank name</label><br/><input name="name" style="width:100%; padding:10px; margin:10px 0;" required /><label>Volume (L)</label><br/><input name="volume_l" type="number" step="any" style="width:100%; padding:10px; margin:10px 0;" placeholder="e.g. 1500" /><button type="submit" style="padding:10px 16px;">Create</button></form><p><a href="/">Back to dashboard</a></p></body></html>"""
    return HTMLResponse(html)

@app.post("/tanks/new")
def tank_new(name: str = Form(...), volume_l: float | None = Form(None)):
    db = get_db(); cur = db.cursor()
    cur.execute("INSERT INTO tanks (name, volume_l) VALUES (?, ?)", (name.strip(), volume_l))
    tid = cur.lastrowid; cur.execute("INSERT OR IGNORE INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?, ?, 100)", (tid, volume_l))
    db.commit(); db.close(); return redirect(f"/tanks/{tid}")

@app.post("/tanks/{tank_id}/delete")
async def tank_delete(tank_id: int):
    db = get_db(); db.execute("DELETE FROM tanks WHERE id=?", (tank_id,)); db.commit(); db.close(); return redirect("/")

@app.post("/samples/{sample_id}/delete")
async def sample_delete(sample_id: int, tank_id: int = Form(...)):
    db = get_db(); db.execute("DELETE FROM samples WHERE id=?", (sample_id,)); db.commit(); db.close(); return redirect(f"/tanks/{tank_id}")

@app.get("/tanks/{tank_id}", response_class=HTMLResponse)
def tank_detail(request: Request, tank_id: int):
    db = get_db(); tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank: db.close(); return templates.TemplateResponse("tank_detail.html", {"request": request, "tank": None})
    samples_rows = q(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT 50", (tank_id,))
    samples = []
    for _row in samples_rows:
        _s = dict(_row); _s["taken_at"] = parse_dt_any(_s.get("taken_at")); samples.append(_s)
    targets = q(db, "SELECT * FROM targets WHERE tank_id=? ORDER BY parameter", (tank_id,))
    series_map, sample_values, unit_by_name = {}, {}, {}
    if samples:
        s_ids = [s["id"] for s in samples]
        rows = q(db, f"SELECT pd.name, sv.value, pd.unit, s.taken_at, s.id as sid FROM sample_values sv JOIN parameter_defs pd ON pd.id = sv.parameter_id JOIN samples s ON s.id = sv.sample_id WHERE sv.sample_id IN ({','.join(['?']*len(s_ids))}) ORDER BY s.taken_at ASC", tuple(s_ids))
        for r in rows:
            series_map.setdefault(r["name"], []).append({"x": parse_dt_any(r["taken_at"]).isoformat(), "y": r["value"]})
            sample_values.setdefault(r["sid"], {})[r["name"]] = r["value"]
            if r["name"] not in unit_by_name: unit_by_name[r["name"]] = r["unit"] or ""
    available_params = sorted(series_map.keys())
    sel_p = request.query_params.get("parameter") or (available_params[0] if available_params else "")
    series = series_map.get(sel_p, [])
    latest_vals = get_latest_per_parameter(db, tank_id)
    target_map = {t["parameter"]: t for t in targets}
    db.close()
    return templates.TemplateResponse("tank_detail.html", {"request": request, "tank": tank, "params": list_parameters(get_db()), "recent_samples": samples, "sample_values": sample_values, "latest_vals": latest_vals, "target_map": target_map, "series": series, "selected_parameter_id": sel_p})

@app.get("/tanks/{tank_id}/profile", response_class=HTMLResponse)
@app.get("/tanks/{tank_id}/edit", response_class=HTMLResponse, include_in_schema=False)
def tank_profile(request: Request, tank_id: int):
    db = get_db(); tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    prof = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    db.close(); return templates.TemplateResponse("tank_profile.html", {"request": request, "tank": dict(tank), "profile": prof})

@app.post("/tanks/{tank_id}/profile")
async def tank_profile_save(tank_id: int, name: str = Form(...), volume_l: float = Form(...), net_percent: float = Form(100)):
    db = get_db(); db.execute("UPDATE tanks SET name=?, volume_l=? WHERE id=?", (name, volume_l, tank_id))
    db.execute("INSERT INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?, ?, ?) ON CONFLICT(tank_id) DO UPDATE SET volume_l=excluded.volume_l, net_percent=excluded.net_percent", (tank_id, volume_l, net_percent))
    db.commit(); db.close(); return redirect(f"/tanks/{tank_id}")

@app.get("/tanks/{tank_id}/add", response_class=HTMLResponse)
def add_sample_form(request: Request, tank_id: int):
    db = get_db(); tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    params = get_active_param_defs(db); db.close()
    return templates.TemplateResponse("add_sample.html", {"request": request, "tank": tank, "parameters": params})

@app.post("/tanks/{tank_id}/add")
async def add_sample(request: Request, tank_id: int):
    form = await request.form(); notes = form.get("notes"); taken_at = form.get("taken_at") or datetime.now().isoformat()
    db = get_db(); cur = db.cursor(); cur.execute("INSERT INTO samples (tank_id, taken_at, notes) VALUES (?, ?, ?)", (tank_id, taken_at, notes))
    sid = cur.lastrowid
    for p in list_parameters(db):
        val = to_float(form.get(f"value_{p['id']}"))
        if val is not None: insert_sample_reading(db, sid, p["name"], val)
    db.commit(); db.close(); return redirect(f"/tanks/{tank_id}")

# --- TOOLS: CALCULATORS & DOSE PLAN ---
@app.get("/tools/calculators", response_class=HTMLResponse)
def calculators(request: Request):
    db = get_db(); tanks = q(db, "SELECT * FROM tanks"); adds = q(db, "SELECT * FROM additives WHERE active=1"); profs = {p["tank_id"]: p for p in q(db, "SELECT * FROM tank_profiles")}
    db.close(); return templates.TemplateResponse("calculators.html", {"request": request, "tanks": tanks, "additives": adds, "profiles": profs})

@app.post("/tools/calculators")
def calculators_post(request: Request, tank_id: int = Form(...), additive_id: int = Form(...), desired_change: float = Form(...)):
    db = get_db(); tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,)); additive = one(db, "SELECT * FROM additives WHERE id=?", (additive_id,))
    prof = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    vol = (row_get(prof, "volume_l") or row_get(tank, "volume_l") or 0) * (row_get(prof, "net_percent", 100) / 100.0)
    dose = (desired_change / additive["strength"]) * (vol / 100.0) if additive["strength"] else 0
    pdef = one(db, "SELECT max_daily_change FROM parameter_defs WHERE name=?", (additive["parameter"],))
    mdc = row_get(pdef, "max_daily_change"); days = int(math.ceil(desired_change/mdc)) if mdc and desired_change > mdc else 1
    res = {"dose_ml": round(dose, 2), "days": days, "daily_ml": round(dose/days, 2), "unit": additive["unit"], "tank": tank, "additive": additive, "desired_change": desired_change}
    db.close(); return templates.TemplateResponse("calculators.html", {"request": request, "result": res, "tanks": q(get_db(), "SELECT * FROM tanks"), "additives": q(get_db(), "SELECT * FROM additives WHERE active=1"), "profiles": {p["tank_id"]: p for p in q(get_db(), "SELECT * FROM tank_profiles")}, "selected": {"tank_id": tank_id, "additive_id": additive_id}})

@app.get("/tools/dose-plan", response_class=HTMLResponse)
def dose_plan(request: Request):
    db = get_db(); today = date.today(); plans = []; grand_total = 0.0
    chk_rows = q(db, "SELECT * FROM dose_plan_checks WHERE planned_date >= ?", (today.isoformat(),))
    check_map = {(r["tank_id"], r["parameter"], r["additive_id"], r["planned_date"]): r["checked"] for r in chk_rows}
    for t in q(db, "SELECT * FROM tanks ORDER BY name"):
        prof = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (t["id"],))
        vol = (row_get(prof, "volume_l") or row_get(t, "volume_l") or 0) * (row_get(prof, "net_percent", 100) / 100.0)
        latest_map = get_latest_per_parameter(db, t["id"])
        tank_rows = []
        for tr in q(db, "SELECT * FROM targets WHERE tank_id=? AND enabled=1", (t["id"],)):
            cur_v = latest_map.get(tr["parameter"], {}).get("value")
            target_v = ((row_get(tr, "target_low") or 0) + (row_get(tr, "target_high") or 0)) / 2.0
            if cur_v is not None and cur_v < target_v:
                delta = target_v - cur_v
                pdef = one(db, "SELECT max_daily_change FROM parameter_defs WHERE name=?", (tr["parameter"],))
                mdc = row_get(pdef, "max_daily_change"); days = int(math.ceil(delta/mdc)) if mdc and delta > mdc else 1
                add_rows = []
                pref = one(db, "SELECT additive_id FROM dose_logs WHERE tank_id=? AND reason LIKE ? ORDER BY logged_at DESC LIMIT 1", (t["id"], f"%{tr['parameter']}%"))
                for a in q(db, "SELECT * FROM additives WHERE parameter=? AND active=1", (tr["parameter"],)):
                    ml = (delta/a["strength"])*(vol/100.0) if a["strength"] else 0
                    sched = [{"day": i+1, "date": (today+timedelta(days=i)).isoformat(), "ml": ml/days, "checked": check_map.get((t["id"], tr["parameter"], a["id"], (today+timedelta(days=i)).isoformat()), 0)} for i in range(days)]
                    add_rows.append({"additive_id": a["id"], "additive_name": a["name"], "total_ml": ml, "schedule": sched, "selected": (a["id"] == (pref["additive_id"] if pref else None) or not add_rows)})
                tank_rows.append({"parameter": tr["parameter"], "latest": cur_v, "target": target_v, "unit": tr["unit"], "additives": add_rows})
        total = sum(next((a["total_ml"] for a in r["additives"] if a["selected"]), 0) for r in tank_rows); grand_total += total
        plans.append({"tank": dict(t), "eff_vol_l": vol, "rows": tank_rows, "total_ml": total})
    db.close(); return templates.TemplateResponse("dose_plan.html", {"request": request, "plans": plans, "grand_total_ml": grand_total})

@app.post("/tools/dose-plan/check")
async def dose_plan_check(request: Request):
    form = await request.form(); db = get_db()
    tid, aid, param, p_date, checked = form.get("tank_id"), form.get("additive_id"), form.get("parameter"), form.get("planned_date"), form.get("checked")
    db.execute("INSERT INTO dose_plan_checks (tank_id, parameter, additive_id, planned_date, checked) VALUES (?,?,?,?,?) ON CONFLICT(tank_id, parameter, additive_id, planned_date) DO UPDATE SET checked=excluded.checked", (tid, param, aid, p_date, checked))
    if int(checked):
        ml = form.get("amount_ml")
        db.execute("INSERT INTO dose_logs (tank_id, additive_id, amount_ml, reason, logged_at) VALUES (?,?,?,?,?)", (tid, aid, ml, f"Dose plan: {param} ({p_date})", f"{p_date}T00:00:00"))
    db.commit(); db.close(); return {"ok": True}

# --- ADVANCED IMPORT/EXPORT CENTER ---
@app.get("/admin/import", response_class=HTMLResponse)
def import_page(request: Request): return templates.TemplateResponse("import_manager.html", {"request": request})

@app.get("/admin/download-template")
def download_template():
    db = get_db(); params = list_parameters(db); tanks = q(db, "SELECT name, volume_l FROM tanks ORDER BY name"); db.close()
    cols = ["Tank Name", "Volume (L)", "Date (YYYY-MM-DD)", "Notes"] + [p["name"] for p in params]
    df = pd.DataFrame(columns=cols)
    today = date.today().isoformat()
    for i, t in enumerate(tanks): df.loc[i] = [t["name"], t["volume_l"], today, "Bulk Import"] + [None]*len(params)
    if not tanks: df.loc[0] = ["Example Tank", 450, today, "Manual Entry"] + [None]*len(params)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer: df.to_excel(writer, index=False, sheet_name='Log Entries')
    output.seek(0)
    return StreamingResponse(output, headers={'Content-Disposition': 'attachment; filename="Reef_Log_Template.xlsx"'}, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.post("/admin/upload-excel")
async def upload_excel(request: Request, file: UploadFile = File(...)):
    db = get_db(); stats = {"tanks": 0, "samples": 0}
    try:
        df = pd.read_excel(BytesIO(await file.read()))
        cur = db.cursor(); pdefs = list_parameters(db)
        for _, row in df.iterrows():
            t_name = str(row.get("Tank Name", "Unknown")).strip()
            if not t_name or t_name == "nan": continue
            tank = one(db, "SELECT id FROM tanks WHERE name=?", (t_name,))
            if not tank:
                v = to_float(row.get("Volume (L)", 0)); cur.execute("INSERT INTO tanks (name, volume_l) VALUES (?,?)", (t_name, v))
                tid = cur.lastrowid; cur.execute("INSERT INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?,?,100)", (tid, v)); stats["tanks"] += 1
            else: tid = tank["id"]
            dt = str(row.get("Date (YYYY-MM-DD)", date.today().isoformat()))
            notes = str(row.get("Notes", "")) if row.get("Notes") and str(row.get("Notes")) != "nan" else ""
            cur.execute("INSERT INTO samples (tank_id, taken_at, notes) VALUES (?,?,?)", (tid, dt, notes))
            sid = cur.lastrowid; stats["samples"] += 1
            for p in pdefs:
                val = to_float(row.get(p["name"]))
                if val is not None: insert_sample_reading(db, sid, p["name"], val)
        db.commit(); return templates.TemplateResponse("import_manager.html", {"request": request, "success": f"Imported {stats['samples']} samples."})
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
    return StreamingResponse(output, headers={'Content-Disposition': 'attachment; filename="Reef_Data_Full_Export.xlsx"'}, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.get("/tanks/{tank_id}/export")
def export_single_tank(tank_id: int):
    db = get_db(); tank = one(db, "SELECT name FROM tanks WHERE id=?", (tank_id,)); params = list_parameters(db)
    df = get_tank_export_df(db, tank_id, params); db.close(); output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer: df.to_excel(writer, sheet_name='Logs', index=False)
    output.seek(0)
    return StreamingResponse(output, headers={'Content-Disposition': f'attachment; filename="{slug_key(tank["name"])}_logs.xlsx"'}, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

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

# --- STANDARD CRUD ---
@app.get("/settings/test-kits", response_class=HTMLResponse)
def test_kits_list(request: Request):
    db = get_db(); rows = q(db, "SELECT * FROM test_kits ORDER BY parameter, name"); db.close()
    return templates.TemplateResponse("test_kits.html", {"request": request, "kits": rows})

@app.get("/additives", response_class=HTMLResponse)
def additives_list(request: Request):
    db = get_db(); rows = q(db, "SELECT * FROM additives ORDER BY parameter, name"); db.close()
    return templates.TemplateResponse("additives.html", {"request": request, "additives": rows})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
