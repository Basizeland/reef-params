import os
import sqlite3
import re
import math
import json
from datetime import datetime, date, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Form, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DATABASE_PATH", os.path.join(BASE_DIR, "reef.db"))

app = FastAPI(title="Reef Tank Parameters")

# --- INITIAL SEED DEFAULTS (Used only for migration/fresh DB) ---
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

def tojson_filter(v: Any) -> str:
    return json.dumps(v, default=str)

templates.env.filters["fmt2"] = fmt2
templates.env.filters["dtfmt"] = dtfmt
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

# --- DATABASE MODELS (SQLAlchemy) ---
from sqlalchemy import Column, Integer, String, Float, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from database import Base

class DictMixin:
    def __getitem__(self, key):
        return getattr(self, key)
    def get(self, key, default=None):
        return getattr(self, key, default)

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
    target_low = Column(Float)
    target_high = Column(Float)
    alert_low = Column(Float)
    alert_high = Column(Float)
    unit = Column(String)
    enabled = Column(Integer, default=1)
    tank = relationship("Tank", back_populates="targets")

class TestKit(Base, DictMixin):
    __tablename__ = "test_kits"
    id = Column(Integer, primary_key=True)
    parameter = Column(String) 
    name = Column(String)
    unit = Column(String)
    resolution = Column(Float)
    min_value = Column(Float)
    max_value = Column(Float)
    notes = Column(String)
    active = Column(Integer, default=1)

class Additive(Base, DictMixin):
    __tablename__ = "additives"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    parameter = Column(String)
    strength = Column(Float)
    unit = Column(String)
    max_daily = Column(Float)
    notes = Column(String)
    active = Column(Integer, default=1)

class Preset(Base, DictMixin):
    __tablename__ = "presets"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)
    items = relationship("PresetItem", back_populates="preset", cascade="all, delete-orphan")

class PresetItem(Base, DictMixin):
    __tablename__ = "preset_items"
    id = Column(Integer, primary_key=True)
    preset_id = Column(Integer, ForeignKey("presets.id"))
    additive_name = Column(String)
    parameter = Column(String)
    strength = Column(Float)
    unit = Column(String)
    max_daily = Column(Float)
    notes = Column(String)
    preset = relationship("Preset", back_populates="items")

class DoseLog(Base, DictMixin):
    __tablename__ = "dose_logs"
    id = Column(Integer, primary_key=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"))
    additive_id = Column(Integer, ForeignKey("additives.id"), nullable=True)
    amount_ml = Column(Float)
    reason = Column(String)
    logged_at = Column(String)
    tank = relationship("Tank", back_populates="dose_logs")
    additive = relationship("Additive")

class DosePlanCheck(Base, DictMixin):
    __tablename__ = "dose_plan_checks"
    id = Column(Integer, primary_key=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"))
    parameter = Column(String)
    additive_id = Column(Integer)
    planned_date = Column(String)
    checked = Column(Integer, default=0)
    checked_at = Column(String)
    __table_args__ = (UniqueConstraint('tank_id', 'parameter', 'additive_id', 'planned_date', name='_tank_param_add_date_uc'),)

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
        CREATE TABLE IF NOT EXISTS targets (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, parameter TEXT NOT NULL, low REAL, high REAL, unit TEXT, enabled INTEGER DEFAULT 1, UNIQUE(tank_id, parameter), FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS test_kits (id INTEGER PRIMARY KEY AUTOINCREMENT, parameter TEXT NOT NULL, name TEXT NOT NULL, unit TEXT, resolution REAL, min_value REAL, max_value REAL, notes TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS additives (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, parameter TEXT NOT NULL, strength REAL NOT NULL, unit TEXT NOT NULL, max_daily REAL, notes TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS presets (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, description TEXT);
        CREATE TABLE IF NOT EXISTS preset_items (id INTEGER PRIMARY KEY AUTOINCREMENT, preset_id INTEGER NOT NULL, additive_name TEXT NOT NULL, parameter TEXT NOT NULL, strength REAL, unit TEXT, max_daily REAL, notes TEXT, FOREIGN KEY (preset_id) REFERENCES presets(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS dose_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, additive_id INTEGER, amount_ml REAL NOT NULL, reason TEXT, logged_at TEXT NOT NULL, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS dose_plan_checks (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, parameter TEXT NOT NULL, additive_id INTEGER NOT NULL, planned_date TEXT NOT NULL, checked INTEGER DEFAULT 0, checked_at TEXT, UNIQUE(tank_id, parameter, additive_id, planned_date), FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
    ''')
    def ensure_col(table: str, col: str, ddl: str) -> None:
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        if col not in cols: cur.execute(ddl)
    ensure_col("tanks", "volume_l", "ALTER TABLE tanks ADD COLUMN volume_l REAL")
    ensure_col("parameter_defs", "max_daily_change", "ALTER TABLE parameter_defs ADD COLUMN max_daily_change REAL")
    ensure_col("additives", "active", "ALTER TABLE additives ADD COLUMN active INTEGER DEFAULT 1")
    ensure_col("test_kits", "active", "ALTER TABLE test_kits ADD COLUMN active INTEGER DEFAULT 1")
    ensure_col("targets", "target_low", "ALTER TABLE targets ADD COLUMN target_low REAL")
    ensure_col("targets", "target_high", "ALTER TABLE targets ADD COLUMN target_high REAL")
    ensure_col("targets", "alert_low", "ALTER TABLE targets ADD COLUMN alert_low REAL")
    ensure_col("targets", "alert_high", "ALTER TABLE targets ADD COLUMN alert_high REAL")
    
    # NEW: Add default target columns
    ensure_col("parameter_defs", "default_target_low", "ALTER TABLE parameter_defs ADD COLUMN default_target_low REAL")
    ensure_col("parameter_defs", "default_target_high", "ALTER TABLE parameter_defs ADD COLUMN default_target_high REAL")
    ensure_col("parameter_defs", "default_alert_low", "ALTER TABLE parameter_defs ADD COLUMN default_alert_low REAL")
    ensure_col("parameter_defs", "default_alert_high", "ALTER TABLE parameter_defs ADD COLUMN default_alert_high REAL")

    cur.execute("SELECT COUNT(1) FROM parameter_defs")
    cnt = cur.fetchone()[0]
    if cnt == 0:
        defaults = [("Alkalinity/KH", "dKH", 1, 10), ("Calcium", "ppm", 1, 20), ("Magnesium", "ppm", 1, 30), ("Phosphate", "ppm", 1, 40), ("Nitrate", "ppm", 1, 50), ("Salinity", "ppt", 1, 60), ("Temperature", "°C", 1, 70)]
        cur.executemany("INSERT INTO parameter_defs (name, unit, active, sort_order) VALUES (?, ?, ?, ?)", defaults)
        
    # MIGRATION: Populate defaults if they are null
    for name, d in INITIAL_DEFAULTS.items():
        cur.execute("""
            UPDATE parameter_defs 
            SET default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=? 
            WHERE name=? AND default_target_low IS NULL
        """, (d["default_target_low"], d["default_target_high"], d["default_alert_low"], d["default_alert_high"], name))
        
    db.commit()
    db.close()

init_db()

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    db = get_db()
    tanks = q(db, "SELECT * FROM tanks ORDER BY name")
    tank_cards = []
    for t in tanks:
        latest = one(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT 1", (t["id"],))
        readings = []
        if latest:
            for r in get_sample_readings(db, latest["id"]):
                readings.append({"name": r["name"], "value": r["value"], "unit": (row_get(r, "unit") or "")})
        
        # Ensure tank dict has volume_l for display
        tank_data = dict(t)
        if "volume_l" not in tank_data:
            tank_data["volume_l"] = None 
            
        tank_cards.append({"tank": tank_data, "latest": latest, "readings": readings})
    db.close()
    return templates.TemplateResponse("dashboard.html", {"request": request, "tank_cards": tank_cards, "extra_css": ["/static/dashboard.css"]})

@app.get("/add", response_class=HTMLResponse)
def add_reading_selector(request: Request):
    db = get_db()
    tanks = q(db, "SELECT * FROM tanks ORDER BY name")
    db.close()
    html = """<html><head><title>Add Reading</title></head><body style="font-family: sans-serif; max-width: 720px; margin: 40px auto;"><h2>Add Reading</h2><form method="get" action="/tanks/0/add" onsubmit="event.preventDefault(); window.location = '/tanks/' + document.getElementById('tank').value + '/add';"><label>Select tank</label><br/><select id="tank" name="tank" style="width:100%; padding:10px; margin:10px 0;" required>{options}</select><button type="submit" style="padding:10px 16px;">Continue</button></form><p><a href="/">Back to dashboard</a></p></body></html>"""
    options = "\n".join([f'<option value="{t["id"]}">{t["name"]}</option>' for t in tanks])
    return HTMLResponse(html.format(options=options))

@app.get("/tanks/new", response_class=HTMLResponse)
def tank_new_form(request: Request):
    html = """<html><head><title>Add Tank</title></head><body style="font-family: sans-serif; max-width: 720px; margin: 40px auto;"><h2>Add Tank</h2><form method="post" action="/tanks/new"><label>Tank name</label><br/><input name="name" style="width:100%; padding:10px; margin:10px 0;" required /><label>Volume (L)</label><br/><input name="volume_l" type="number" step="any" style="width:100%; padding:10px; margin:10px 0;" placeholder="e.g. 1500" /><button type="submit" style="padding:10px 16px;">Create</button></form><p><a href="/">Back to dashboard</a></p></body></html>"""
    return HTMLResponse(html)

@app.post("/tanks/new")
def tank_new(name: str = Form(...), volume_l: float | None = Form(None)):
    db = get_db()
    cur = db.cursor()
    cur.execute("INSERT INTO tanks (name, volume_l) VALUES (?, ?)", (name.strip(), volume_l))
    tank_id = cur.lastrowid
    cur.execute("INSERT OR IGNORE INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?, ?, 100)", (tank_id, volume_l))
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}")

@app.post("/tanks/{tank_id}/delete")
async def tank_delete(tank_id: int):
    db = get_db()
    db.execute("DELETE FROM tanks WHERE id=?", (tank_id,))
    db.commit()
    db.close()
    return redirect("/")

@app.post("/samples/{sample_id}/delete")
async def sample_delete(sample_id: int, tank_id: int = Form(...)):
    db = get_db()
    db.execute("DELETE FROM samples WHERE id=?", (sample_id,))
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}")

@app.get("/tanks/{tank_id}", response_class=HTMLResponse)
def tank_detail(request: Request, tank_id: int):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank:
        db.close()
        return templates.TemplateResponse("tank_detail.html", {"request": request, "tank": None, "params": [], "recent_samples": [], "sample_values": {}, "latest_vals": {}, "status_by_param_id": {}, "targets": [], "series": [], "chart_targets": [], "selected_parameter_id": "", "format_value": format_value, "target_map": {}})
    
    samples_rows = q(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT 50", (tank_id,))
    samples = []
    for _row in samples_rows:
        _s = dict(_row)
        dt = parse_dt_any(_s.get("taken_at"))
        if dt is not None: _s["taken_at"] = dt
        samples.append(_s)
        
    targets = q(db, "SELECT * FROM targets WHERE tank_id=? ORDER BY parameter", (tank_id,))
    series_map, sample_values, unit_by_name = {}, {}, {}
    
    if samples:
        sample_ids = [s["id"] for s in samples]
        placeholders = ",".join(["?"] * len(sample_ids))
        mode = values_mode(db)
        if mode == "sample_values":
            rows = q(db, f"SELECT pd.name AS name, sv.value AS value, COALESCE(pd.unit, '') AS unit, s.taken_at AS taken_at, s.id AS sample_id FROM sample_values sv JOIN parameter_defs pd ON pd.id = sv.parameter_id JOIN samples s ON s.id = sv.sample_id WHERE sv.sample_id IN ({placeholders}) ORDER BY s.taken_at ASC", tuple(sample_ids))
        else:
            rows = q(db, f"SELECT p.name AS name, p.value AS value, COALESCE(pd.unit, p.unit, '') AS unit, s.taken_at AS taken_at, s.id AS sample_id FROM parameters p JOIN samples s ON s.id = p.sample_id LEFT JOIN parameter_defs pd ON pd.name = p.name WHERE p.sample_id IN ({placeholders}) ORDER BY s.taken_at ASC", tuple(sample_ids))
            
        for r in rows:
            name = r["name"]
            if name is None: continue
            try: value = float(r["value"]) if r["value"] is not None else None
            except Exception: value = r["value"]
            
            # --- FIX FOR CHART: Ensure date is ISO format string ---
            raw_date = r["taken_at"]
            dt_obj = parse_dt_any(raw_date)
            iso_date = dt_obj.isoformat() if dt_obj else str(raw_date)
            # -------------------------------------------------------

            sid = int(r["sample_id"])
            if value is not None:
                try: y = float(value)
                except Exception: y = None
                if y is not None: series_map.setdefault(name, []).append({"x": iso_date, "y": y})
            
            # Ensure sample_values keys are integers for template lookup
            sample_values.setdefault(sid, {})[name] = value
            
            try: u = r["unit"] or ""
            except Exception: u = ""
            if name not in unit_by_name and u: unit_by_name[name] = u

    all_param_names = set(series_map.keys())
    if samples: all_param_names.update(sample_values.get(int(samples[0]["id"]), {}).keys())
    available_params = sorted(all_param_names, key=lambda s: s.lower())
    
    selected_parameter_id = request.query_params.get("parameter") or ""
    if not selected_parameter_id or selected_parameter_id not in series_map: 
        selected_parameter_id = available_params[0] if available_params else ""
        
    series = series_map.get(selected_parameter_id, [])
    
    chart_targets = []
    for t in targets:
        if (t["parameter"] or "") == selected_parameter_id:
            t_low = row_get(t, "target_low") if row_get(t, "target_low") is not None else row_get(t, "low")
            t_high = row_get(t, "target_high") if row_get(t, "target_high") is not None else row_get(t, "high")
            chart_targets.append({"parameter": t["parameter"], "low": t_low, "high": t_high, "unit": row_get(t, "unit") or unit_by_name.get(selected_parameter_id, "")})
            
    params = [{"id": name, "name": name, "unit": unit_by_name.get(name, "")} for name in available_params]
    
    latest_by_param_id = dict(sample_values.get(int(samples[0]["id"]), {})) if samples else {}
    targets_by_param = {t["parameter"]: t for t in targets if row_get(t, "parameter") is not None}
    
    status_by_param_id = {}
    for pname in available_params:
        v = latest_by_param_id.get(pname)
        if v is None: status_by_param_id[pname] = "missing"; continue
        t = targets_by_param.get(pname)
        if not t: continue
        al, ah, tl, th = row_get(t, "alert_low"), row_get(t, "alert_high"), row_get(t, "target_low"), row_get(t, "target_high")
        if tl is None: tl = row_get(t, "low")
        if th is None: th = row_get(t, "high")
        try:
            fv = float(v)
            if al is not None and fv < float(al): status_by_param_id[pname] = "danger"; continue
            if ah is not None and fv > float(ah): status_by_param_id[pname] = "danger"; continue
            if tl is not None and fv < float(tl): status_by_param_id[pname] = "warn"; continue
            if th is not None and fv > float(th): status_by_param_id[pname] = "warn"; continue
            status_by_param_id[pname] = "ok"
        except Exception: continue
        
    recent_samples = samples[:10] if samples else []
    db.close()
    return templates.TemplateResponse("tank_detail.html", {"request": request, "tank": tank, "params": params, "recent_samples": recent_samples, "sample_values": sample_values, "latest_vals": latest_by_param_id, "status_by_param_id": status_by_param_id, "targets": targets, "target_map": targets_by_param, "series": series, "chart_targets": chart_targets, "selected_parameter_id": selected_parameter_id, "format_value": format_value})

@app.get("/tanks/{tank_id}/profile", response_class=HTMLResponse)
@app.get("/tanks/{tank_id}/edit", response_class=HTMLResponse, include_in_schema=False)
def tank_profile(request: Request, tank_id: int):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    if not profile:
        try: vol = tank["volume_l"] if "volume_l" in tank.keys() else None
        except Exception: vol = None
        db.execute("INSERT OR IGNORE INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?, ?, 100)", (tank_id, vol))
        db.commit()
        profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    tank_view = dict(tank)
    if profile:
        try:
            tank_view["volume_l"] = profile["volume_l"]
            tank_view["net_percent"] = profile["net_percent"]
        except Exception: pass
    db.close()
    return templates.TemplateResponse("tank_profile.html", {"request": request, "tank": tank_view})

@app.post("/tanks/{tank_id}/profile")
@app.post("/tanks/{tank_id}/edit", include_in_schema=False)
async def tank_profile_save(request: Request, tank_id: int):
    form = await request.form()
    volume_l = to_float(form.get("volume_l"))
    net_percent = to_float(form.get("net_percent"))
    if net_percent is None: net_percent = 100
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    db.execute("UPDATE tanks SET volume_l=? WHERE id=?", (volume_l, tank_id))
    db.execute("INSERT INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?, ?, ?) ON CONFLICT(tank_id) DO UPDATE SET volume_l=excluded.volume_l, net_percent=excluded.net_percent", (tank_id, volume_l, float(net_percent)))
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}")

@app.get("/tanks/{tank_id}/add", response_class=HTMLResponse)
def add_sample_form(request: Request, tank_id: int):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    params_rows = get_active_param_defs(db)
    kits = q(db, "SELECT * FROM test_kits WHERE active=1 ORDER BY parameter, name")
    kits_by_param = {}
    for k in kits: kits_by_param.setdefault(k["parameter"], []).append(k)
    db.close()
    return templates.TemplateResponse("add_sample.html", {"request": request, "tank_id": tank_id, "tank": tank, "parameters": params_rows, "kits_by_param": kits_by_param, "kits": kits})

@app.post("/tanks/{tank_id}/add")
async def add_sample(request: Request, tank_id: int):
    form = await request.form()
    notes = (form.get("notes") or "").strip() or None
    taken_at = (form.get("taken_at") or "").strip()
    db = get_db()
    cur = db.cursor()
    if taken_at:
        try: when_iso = datetime.fromisoformat(taken_at).isoformat()
        except ValueError: when_iso = datetime.utcnow().isoformat()
    else: when_iso = datetime.utcnow().isoformat()
    cur.execute("INSERT INTO samples (tank_id, taken_at, notes) VALUES (?, ?, ?)", (tank_id, when_iso, notes))
    sample_id = cur.lastrowid
    pdefs = get_active_param_defs(db)
    for p in pdefs:
        pid = p["id"]
        pname = p["name"]
        punit = (row_get(p, "unit") or "")
        val = to_float(form.get(f"value_{pid}"))
        if val is None: continue
        insert_sample_reading(db, sample_id, pname, float(val), punit)
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}")

@app.get("/tanks/{tank_id}/samples/{sample_id}", response_class=HTMLResponse)
def sample_detail(request: Request, tank_id: int, sample_id: int):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    sample = one(db, "SELECT * FROM samples WHERE id=? AND tank_id=?", (sample_id, tank_id))
    if not tank or not sample:
        db.close()
        raise HTTPException(status_code=404, detail="Sample not found")
    s = dict(sample)
    dt = parse_dt_any(s.get("taken_at"))
    if dt is not None: s["taken_at"] = dt
    readings = []
    for r in get_sample_readings(db, sample_id):
        readings.append({"name": r["name"], "value": r["value"], "unit": (r["unit"] or "")})
    db.close()
    return templates.TemplateResponse("sample_detail.html", {"request": request, "tank": tank, "sample": s, "readings": readings})

@app.get("/tanks/{tank_id}/samples/{sample_id}/edit", response_class=HTMLResponse)
def sample_edit(request: Request, tank_id: int, sample_id: int):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    sample = one(db, "SELECT * FROM samples WHERE id=? AND tank_id=?", (sample_id, tank_id))
    if not tank or not sample:
        db.close()
        raise HTTPException(status_code=404, detail="Sample not found")
    pdefs = get_active_param_defs(db)
    readings = {r["name"]: r["value"] for r in get_sample_readings(db, sample_id)}
    taken_dt = parse_dt_any(sample["taken_at"])
    taken_local = taken_dt.strftime("%Y-%m-%dT%H:%M") if taken_dt else ""
    db.close()
    return templates.TemplateResponse("sample_edit.html", {"request": request, "tank": tank, "tank_id": tank_id, "sample_id": sample_id, "sample": sample, "taken_local": taken_local, "pdefs": pdefs, "readings": readings})

@app.post("/tanks/{tank_id}/samples/{sample_id}/edit")
async def sample_edit_save(request: Request, tank_id: int, sample_id: int):
    form = await request.form()
    notes = (form.get("notes") or "").strip() or None
    taken_at = (form.get("taken_at") or "").strip()
    db = get_db()
    cur = db.cursor()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    sample = one(db, "SELECT * FROM samples WHERE id=? AND tank_id=?", (sample_id, tank_id))
    if not tank or not sample:
        db.close()
        raise HTTPException(status_code=404, detail="Sample not found")
    when_iso = None
    if taken_at:
        try: when_iso = datetime.fromisoformat(taken_at).isoformat()
        except Exception: when_iso = None
    if not when_iso: when_iso = (parse_dt_any(sample["taken_at"]) or datetime.utcnow()).isoformat()
    cur.execute("UPDATE samples SET taken_at=?, notes=? WHERE id=? AND tank_id=?", (when_iso, notes, sample_id, tank_id))
    mode = values_mode(db)
    if mode == "sample_values" and table_exists(db, "sample_values"): cur.execute("DELETE FROM sample_values WHERE sample_id=?", (sample_id,))
    else:
        if table_exists(db, "parameters"): cur.execute("DELETE FROM parameters WHERE sample_id=?", (sample_id,))
    pdefs = get_active_param_defs(db)
    for p in pdefs:
        pid = p["id"]
        pname = p["name"]
        punit = (p.get("unit") or "")
        val = to_float(form.get(f"value_{pid}"))
        if val is None: continue
        insert_sample_reading(db, sample_id, pname, float(val), punit)
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/samples/{sample_id}")

@app.get("/tanks/{tank_id}/targets", response_class=HTMLResponse)
def edit_targets(request: Request, tank_id: int):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
        
    params = list_parameters(db)
    existing = {}
    
    if table_exists(db, "targets"):
        for t in q(db, "SELECT * FROM targets WHERE tank_id=?", (tank_id,)): 
            existing[t["parameter"]] = t
            
    rows = []
    for p in params:
        name = p["name"]
        t = existing.get(name)
        
        # Helper: Use DB value if it exists, otherwise use Default (from DB column)
        def get_val(key_db, key_def):
            val = row_get(t, key_db)
            if val is not None:
                return val
            # New: Get default from parameter definition
            return row_get(p, key_def)

        # 1. Target Low/High
        t_low = get_val("target_low", "default_target_low")
        if t_low is None: t_low = row_get(t, "low") # Legacy DB support

        t_high = get_val("target_high", "default_target_high")
        if t_high is None: t_high = row_get(t, "high") # Legacy DB support

        # 2. Alert Low/High
        a_low = get_val("alert_low", "default_alert_low")
        a_high = get_val("alert_high", "default_alert_high")
        
        # 3. Resolve Unit and Enabled status safely
        p_unit = row_get(p, "unit")
        t_unit = row_get(t, "unit")
        unit = p_unit or t_unit or ""
        
        t_enabled = row_get(t, "enabled")
        enabled = t_enabled if t_enabled is not None else 1
        
        rows.append({
            "parameter": {"name": name, "unit": unit},
            "key": slug_key(name),
            "target": compute_target(t_low, t_high),
            "target_low": t_low,
            "target_high": t_high,
            "alert_low": a_low,
            "alert_high": a_high,
            "enabled": enabled
        })
        
    db.close()
    return templates.TemplateResponse("edit_targets.html", {"request": request, "tank": tank, "tank_id": tank_id, "rows": rows})

@app.post("/tanks/{tank_id}/targets")
async def save_targets(request: Request, tank_id: int):
    db = get_db()
    cur = db.cursor()
    form = await request.form()
    cur.execute('CREATE TABLE IF NOT EXISTS targets (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, parameter TEXT NOT NULL, target_low REAL, target_high REAL, alert_low REAL, alert_high REAL, unit TEXT, enabled INTEGER DEFAULT 1, UNIQUE(tank_id, parameter))')
    db.commit()
    cols = {r["name"] for r in q(db, "PRAGMA table_info(targets)")}
    def add_col(name: str, ddl: str) -> None:
        nonlocal cols
        if name not in cols:
            cur.execute(ddl)
            db.commit()
            cols = {r["name"] for r in q(db, "PRAGMA table_info(targets)")}
    add_col("target_low", "ALTER TABLE targets ADD COLUMN target_low REAL")
    add_col("target_high", "ALTER TABLE targets ADD COLUMN target_high REAL")
    add_col("alert_low", "ALTER TABLE targets ADD COLUMN alert_low REAL")
    add_col("alert_high", "ALTER TABLE targets ADD COLUMN alert_high REAL")
    add_col("unit", "ALTER TABLE targets ADD COLUMN unit TEXT")
    add_col("enabled", "ALTER TABLE targets ADD COLUMN enabled INTEGER DEFAULT 1")
    params = list_parameters(db)
    for p in params:
        name = p["name"]
        key = slug_key(name)
        enabled = 1 if (form.get(f"enabled_{key}") in ("1", "on", "true", "True")) else 0
        target = to_float(form.get(f"target_{key}"))
        t_low = to_float(form.get(f"target_low_{key}"))
        t_high = to_float(form.get(f"target_high_{key}"))
        if target is not None: t_low = target; t_high = target
        a_low = to_float(form.get(f"alert_low_{key}"))
        a_high = to_float(form.get(f"alert_high_{key}"))
        unit = (row_get(p, "unit") or "").strip()
        if (t_low is None and t_high is None and a_low is None and a_high is None) or enabled == 0:
            cur.execute("UPDATE targets SET enabled=0 WHERE tank_id=? AND parameter=?", (tank_id, name))
            continue
        cur.execute('''INSERT INTO targets (tank_id, parameter, target_low, target_high, alert_low, alert_high, unit, enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(tank_id, parameter) DO UPDATE SET target_low=excluded.target_low, target_high=excluded.target_high, alert_low=excluded.alert_low, alert_high=excluded.alert_high, unit=excluded.unit, enabled=excluded.enabled''', (tank_id, name, t_low, t_high, a_low, a_high, unit, enabled))
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/targets")

@app.get("/tank/{tank_id}/targets", response_class=HTMLResponse, include_in_schema=False)
def edit_targets_alias(request: Request, tank_id: int): return edit_targets(request, tank_id)

@app.post("/tank/{tank_id}/targets", include_in_schema=False)
async def save_targets_alias(request: Request, tank_id: int): return await save_targets(request, tank_id)

@app.get("/settings/parameters", response_class=HTMLResponse)
@app.get("/settings/parameters/", response_class=HTMLResponse, include_in_schema=False)
def parameters_settings(request: Request):
    db = get_db()
    rows = q(db, "SELECT * FROM parameter_defs ORDER BY sort_order, name")
    db.close()
    return templates.TemplateResponse("parameters.html", {"request": request, "parameters": rows})

@app.get("/settings/parameters/new", response_class=HTMLResponse)
@app.get("/settings/parameters/new/", response_class=HTMLResponse, include_in_schema=False)
def parameter_new(request: Request):
    return templates.TemplateResponse("parameter_edit.html", {"request": request, "param": None})

@app.get("/settings/parameters/{param_id}/edit", response_class=HTMLResponse)
def parameter_edit(request: Request, param_id: int):
    db = get_db()
    row = one(db, "SELECT * FROM parameter_defs WHERE id=?", (param_id,))
    db.close()
    return templates.TemplateResponse("parameter_edit.html", {"request": request, "param": row})

@app.post("/settings/parameters/save")
def parameter_save(
    param_id: Optional[str] = Form(None), 
    name: str = Form(...), 
    unit: Optional[str] = Form(None), 
    sort_order: Optional[str] = Form(None), 
    max_daily_change: Optional[str] = Form(None), 
    active: Optional[str] = Form(None),
    default_target_low: Optional[str] = Form(None),
    default_target_high: Optional[str] = Form(None),
    default_alert_low: Optional[str] = Form(None),
    default_alert_high: Optional[str] = Form(None)
):
    db = get_db()
    cur = db.cursor()
    
    # 1. Prepare Data
    is_active = 1 if (active in ("1", "on", "true", "True")) else 0
    order = int(to_float(sort_order) or 0)
    mdc = to_float(max_daily_change)
    dt_low = to_float(default_target_low)
    dt_high = to_float(default_target_high)
    da_low = to_float(default_alert_low)
    da_high = to_float(default_alert_high)
    
    clean_name = name.strip()
    
    if not clean_name:
        db.close()
        return redirect("/settings/parameters")

    data = (clean_name, (unit or "").strip() or None, mdc, order, is_active, dt_low, dt_high, da_low, da_high)

    try:
        # 2. Logic for Update/Merge
        if param_id and str(param_id).strip().isdigit():
            pid = int(param_id)
            old = one(db, "SELECT * FROM parameter_defs WHERE id=?", (pid,))
            old_name = (old["name"] if old else "").strip()
            
            # Check if we are renaming
            if old_name and clean_name and old_name != clean_name:
                # Does the target name already exist?
                existing = one(db, "SELECT id FROM parameter_defs WHERE name=?", (clean_name,))
                
                if existing:
                    # MERGE SCENARIO: 
                    existing_id = int(existing["id"])
                    
                    if table_exists(db, "sample_values"):
                        cur.execute("UPDATE sample_values SET parameter_id=? WHERE parameter_id=?", (existing_id, pid))
                    
                    for tbl, col in (("parameters", "name"), ("targets", "parameter"), ("additives", "parameter"), ("test_kits", "parameter")):
                        if table_exists(db, tbl): 
                            cur.execute(f"UPDATE {tbl} SET {col}=? WHERE {col}=?", (clean_name, old_name))

                    cur.execute("DELETE FROM parameter_defs WHERE id=?", (pid,))
                    cur.execute("""
                        UPDATE parameter_defs 
                        SET name=?, unit=?, max_daily_change=?, sort_order=?, active=?, 
                            default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=?
                        WHERE id=?""", (*data, existing_id))
                    
                else:
                    # RENAME SCENARIO (No conflict):
                    for tbl, col in (("parameters", "name"), ("targets", "parameter"), ("additives", "parameter"), ("test_kits", "parameter")):
                        if table_exists(db, tbl): 
                            cur.execute(f"UPDATE {tbl} SET {col}=? WHERE {col}=?", (clean_name, old_name))
                            
                    cur.execute("""
                        UPDATE parameter_defs 
                        SET name=?, unit=?, max_daily_change=?, sort_order=?, active=?, 
                            default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=?
                        WHERE id=?""", (*data, pid))
            else:
                # SIMPLE UPDATE
                cur.execute("""
                    UPDATE parameter_defs 
                    SET name=?, unit=?, max_daily_change=?, sort_order=?, active=?, 
                        default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=?
                    WHERE id=?""", (*data, pid))
        
        # 3. Logic for Insert
        else:
            existing = one(db, "SELECT id FROM parameter_defs WHERE name=?", (clean_name,))
            if existing:
                cur.execute("""
                    UPDATE parameter_defs 
                    SET unit=?, max_daily_change=?, sort_order=?, active=?, 
                        default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=?
                    WHERE id=?""", (data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], existing["id"]))
            else:
                cur.execute("""
                    INSERT INTO parameter_defs 
                    (name, unit, max_daily_change, sort_order, active, default_target_low, default_target_high, default_alert_low, default_alert_high) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", data)

        db.commit()
        
    except sqlite3.IntegrityError:
        print("Database Integrity Error ignored.")
        db.rollback()
        
    except Exception as e:
        print(f"Error saving parameter: {e}")
        db.rollback()
        
    finally:
        db.close()
        
    return redirect("/settings/parameters")

@app.post("/settings/parameters/{param_id}/delete")
def parameter_delete(param_id: int):
    db = get_db()
    db.execute("DELETE FROM parameter_defs WHERE id=?", (param_id,))
    db.commit()
    db.close()
    return redirect("/settings/parameters")

@app.get("/settings/test-kits", response_class=HTMLResponse)
@app.get("/settings/test-kits/", response_class=HTMLResponse, include_in_schema=False)
def test_kits(request: Request):
    db = get_db()
    rows = q(db, "SELECT * FROM test_kits ORDER BY parameter, name")
    db.close()
    return templates.TemplateResponse("test_kits.html", {"request": request, "kits": rows})

@app.get("/settings/test-kits/new", response_class=HTMLResponse)
@app.get("/settings/test-kits/new/", response_class=HTMLResponse, include_in_schema=False)
def test_kit_new(request: Request):
    db = get_db()
    parameters = q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY sort_order, name")
    db.close()
    return templates.TemplateResponse("test_kit_edit.html", {"request": request, "kit": None, "parameters": parameters})

@app.get("/settings/test-kits/{kit_id}/edit", response_class=HTMLResponse)
def test_kit_edit(request: Request, kit_id: int):
    db = get_db()
    kit = one(db, "SELECT * FROM test_kits WHERE id=?", (kit_id,))
    parameters = q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY sort_order, name")
    db.close()
    return templates.TemplateResponse("test_kit_edit.html", {"request": request, "kit": kit, "parameters": parameters})

@app.post("/settings/test-kits/save")
def test_kit_save(kit_id: Optional[str] = Form(None), parameter: Optional[str] = Form(None), parameter_id: Optional[str] = Form(None), name: str = Form(...), unit: Optional[str] = Form(None), resolution: Optional[str] = Form(None), min_value: Optional[str] = Form(None), max_value: Optional[str] = Form(None), notes: Optional[str] = Form(None), active: Optional[str] = Form(None)):
    db = get_db()
    cur = db.cursor()
    is_active = 1 if (active in ("1", "on", "true", "True")) else 0
    data = (parameter.strip(), name.strip(), (unit or "").strip() or None, to_float(resolution), to_float(min_value), to_float(max_value), (notes or "").strip() or None, is_active)
    if kit_id and str(kit_id).strip().isdigit(): cur.execute("UPDATE test_kits SET parameter=?, name=?, unit=?, resolution=?, min_value=?, max_value=?, notes=?, active=? WHERE id=?", (*data, int(kit_id)))
    else: cur.execute("INSERT INTO test_kits (parameter, name, unit, resolution, min_value, max_value, notes, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", data)
    db.commit()
    db.close()
    return redirect("/settings/test-kits")

@app.post("/settings/test-kits/{kit_id}/delete")
def test_kit_delete(kit_id: int):
    db = get_db()
    db.execute("DELETE FROM test_kits WHERE id=?", (kit_id,))
    db.commit()
    db.close()
    return redirect("/settings/test-kits")

@app.get("/additives", response_class=HTMLResponse)
@app.get("/additives/", response_class=HTMLResponse, include_in_schema=False)
def additives(request: Request):
    db = get_db()
    rows = q(db, "SELECT * FROM additives ORDER BY parameter, name")
    db.close()
    return templates.TemplateResponse("additives.html", {"request": request, "additives": rows, "rows": rows})

@app.get("/settings/additives", response_class=HTMLResponse, include_in_schema=False)
@app.get("/settings/additives/", response_class=HTMLResponse, include_in_schema=False)
def additives_settings_redirect(): return redirect("/additives")

@app.get("/additives/new", response_class=HTMLResponse)
@app.get("/additives/new/", response_class=HTMLResponse, include_in_schema=False)
def additive_new(request: Request):
    db = get_db()
    parameters = q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY sort_order, name")
    db.close()
    return templates.TemplateResponse("additive_edit.html", {"request": request, "additive": None, "parameters": parameters})

@app.get("/additives/{additive_id}/edit", response_class=HTMLResponse)
def additive_edit(request: Request, additive_id: int):
    db = get_db()
    additive = one(db, "SELECT * FROM additives WHERE id=?", (additive_id,))
    parameters = q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY sort_order, name")
    db.close()
    return templates.TemplateResponse("additive_edit.html", {"request": request, "additive": additive, "parameters": parameters})

@app.post("/additives/save")
def additive_save(additive_id: Optional[str] = Form(None), name: str = Form(...), parameter: Optional[str] = Form(None), parameter_id: Optional[str] = Form(None), strength: str = Form(...), unit: str = Form(...), max_daily: Optional[str] = Form(None), notes: Optional[str] = Form(None), active: Optional[str] = Form(None)):
    db = get_db()
    cur = db.cursor()
    is_active = 1 if (active in ("1", "on", "true", "True")) else 0
    data = (name.strip(), parameter.strip(), float(to_float(strength) or 0), unit.strip(), to_float(max_daily), (notes or "").strip() or None, is_active)
    if additive_id and str(additive_id).strip().isdigit(): cur.execute("UPDATE additives SET name=?, parameter=?, strength=?, unit=?, max_daily=?, notes=?, active=? WHERE id=?", (*data, int(additive_id)))
    else: cur.execute("INSERT INTO additives (name, parameter, strength, unit, max_daily, notes, active) VALUES (?, ?, ?, ?, ?, ?, ?)", data)
    db.commit()
    db.close()
    return redirect("/additives")

@app.post("/additives/{additive_id}/delete")
def additive_delete(additive_id: int):
    db = get_db()
    db.execute("DELETE FROM additives WHERE id=?", (additive_id,))
    db.commit()
    db.close()
    return redirect("/additives")

@app.get("/settings/presets", response_class=HTMLResponse)
@app.get("/settings/presets/", response_class=HTMLResponse, include_in_schema=False)
def presets(request: Request):
    db = get_db()
    rows = q(db, "SELECT * FROM presets ORDER BY name")
    db.close()
    return templates.TemplateResponse("presets.html", {"request": request, "presets": rows, "created": False})

@app.post("/settings/presets/create")
def presets_create(request: Request, name: str = Form(...), description: Optional[str] = Form(None)):
    db = get_db()
    db.execute("INSERT INTO presets (name, description) VALUES (?, ?)", (name.strip(), (description or "").strip() or None))
    db.commit()
    rows = q(db, "SELECT * FROM presets ORDER BY name")
    db.close()
    return templates.TemplateResponse("presets.html", {"request": request, "presets": rows, "created": True})

@app.get("/settings/presets/{preset_id}", response_class=HTMLResponse)
def preset_detail(request: Request, preset_id: int):
    db = get_db()
    preset = one(db, "SELECT * FROM presets WHERE id=?", (preset_id,))
    items = q(db, "SELECT * FROM preset_items WHERE preset_id=? ORDER BY id", (preset_id,))
    parameters = q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY sort_order, name")
    db.close()
    return templates.TemplateResponse("preset_detail.html", {"request": request, "preset": preset, "items": items, "parameters": parameters})

@app.post("/settings/presets/{preset_id}/items/create")
def preset_item_create(preset_id: int, additive_name: str = Form(...), parameter: str = Form(...), strength: Optional[str] = Form(None), unit: Optional[str] = Form(None), max_daily: Optional[str] = Form(None), notes: Optional[str] = Form(None)):
    db = get_db()
    db.execute("INSERT INTO preset_items (preset_id, additive_name, parameter, strength, unit, max_daily, notes) VALUES (?, ?, ?, ?, ?, ?, ?)", (preset_id, additive_name.strip(), parameter.strip(), to_float(strength), (unit or "").strip() or None, to_float(max_daily), (notes or "").strip() or None))
    db.commit()
    db.close()
    return redirect(f"/settings/presets/{preset_id}")

@app.post("/settings/presets/{preset_id}/items/{item_id}/save")
def preset_item_save(preset_id: int, item_id: int, additive_name: str = Form(...), parameter: str = Form(...), strength: Optional[str] = Form(None), unit: Optional[str] = Form(None), max_daily: Optional[str] = Form(None), notes: Optional[str] = Form(None)):
    db = get_db()
    db.execute("UPDATE preset_items SET additive_name=?, parameter=?, strength=?, unit=?, max_daily=?, notes=? WHERE id=? AND preset_id=?", (additive_name.strip(), parameter.strip(), to_float(strength), (unit or "").strip() or None, to_float(max_daily), (notes or "").strip() or None, item_id, preset_id))
    db.commit()
    db.close()
    return redirect(f"/settings/presets/{preset_id}")

@app.post("/settings/presets/{preset_id}/items/{item_id}/delete")
def preset_item_delete(preset_id: int, item_id: int):
    db = get_db()
    db.execute("DELETE FROM preset_items WHERE id=? AND preset_id=?", (item_id, preset_id))
    db.commit()
    db.close()
    return redirect(f"/settings/presets/{preset_id}")

@app.post("/settings/presets/{preset_id}/apply")
def preset_apply(preset_id: int):
    db = get_db()
    cur = db.cursor()
    items = q(db, "SELECT * FROM preset_items WHERE preset_id=?", (preset_id,))
    for it in items: cur.execute("INSERT INTO additives (name, parameter, strength, unit, max_daily, notes, active) VALUES (?, ?, COALESCE(?, 0), COALESCE(?, ''), ?, ?, 1)", (it["additive_name"], it["parameter"], it["strength"], it["unit"], it["max_daily"], it["notes"]))
    db.commit()
    db.close()
    return redirect("/additives")

@app.get("/tanks/{tank_id}/dosing-log", response_class=HTMLResponse)
@app.get("/tank/{tank_id}/dosing-log", response_class=HTMLResponse, include_in_schema=False)
def dosing_log(request: Request, tank_id: int):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    additives_rows = q(db, "SELECT * FROM additives WHERE active=1 ORDER BY parameter, name")
    logs = q(db, "SELECT dl.*, a.name AS additive_name FROM dose_logs dl LEFT JOIN additives a ON a.id = dl.additive_id WHERE dl.tank_id=? ORDER BY dl.logged_at DESC, dl.id DESC", (tank_id,))
    norm_logs = []
    for r in logs:
        d = dict(r)
        d["logged_at_dt"] = parse_dt_any(d.get("logged_at"))
        norm_logs.append(d)
    db.close()
    return templates.TemplateResponse("dosing_log.html", {"request": request, "tank": tank, "tank_id": tank_id, "additives": additives_rows, "logs": norm_logs})

@app.post("/tanks/{tank_id}/dosing-log")
@app.post("/tank/{tank_id}/dosing-log", include_in_schema=False)
async def dosing_log_add(request: Request, tank_id: int):
    form = await request.form()
    additive_id = form.get("additive_id")
    amount_ml = to_float(form.get("amount_ml"))
    reason = (form.get("reason") or "").strip() or None
    if amount_ml is None: return redirect(f"/tanks/{tank_id}/dosing-log")
    db = get_db()
    cur = db.cursor()
    when_iso = datetime.utcnow().isoformat()
    aid = int(additive_id) if additive_id and str(additive_id).isdigit() else None
    cur.execute("INSERT INTO dose_logs (tank_id, additive_id, amount_ml, reason, logged_at) VALUES (?, ?, ?, ?, ?)", (tank_id, aid, float(amount_ml), reason, when_iso))
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/dosing-log")

@app.post("/dose-logs/{log_id}/delete")
async def dosing_log_delete(log_id: int, tank_id: int = Form(...)):
    db = get_db()
    db.execute("DELETE FROM dose_logs WHERE id=?", (log_id,))
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/dosing-log")

@app.get("/admin/merge-parameters", response_class=HTMLResponse)
def merge_parameters_page(request: Request):
    db = get_db()
    rows = q(db, "SELECT id, name, unit, max_daily_change, sort_order, active FROM parameter_defs ORDER BY name")
    groups = {}
    for r in rows:
        key = (r["name"] or "").strip().lower()
        groups.setdefault(key, []).append(r)
    dups = []
    for key, items in groups.items():
        if key and len(items) > 1:
            items_sorted = sorted(items, key=lambda x: int(x["id"]))
            dups.append({"key": key, "keep": items_sorted[-1], "drop": items_sorted[:-1]})
    db.close()
    return templates.TemplateResponse("merge_parameters.html", {"request": request, "dups": dups})

@app.post("/admin/merge-parameters")
def merge_parameters_run():
    db = get_db()
    cur = db.cursor()
    rows = q(db, "SELECT id, name FROM parameter_defs")
    groups = {}
    for r in rows:
        key = (r["name"] or "").strip().lower()
        groups.setdefault(key, []).append(r)
    merged = 0
    for key, items in groups.items():
        if not key or len(items) <= 1: continue
        items_sorted = sorted(items, key=lambda x: int(x["id"]))
        keep_id = int(items_sorted[-1]["id"])
        keep_name = str(items_sorted[-1]["name"]).strip()
        for drop in items_sorted[:-1]:
            drop_id = int(drop["id"])
            drop_name = str(drop["name"]).strip()
            for tbl, col in (("parameters", "name"), ("targets", "parameter"), ("additives", "parameter"), ("test_kits", "parameter")):
                if table_exists(db, tbl): cur.execute(f"UPDATE {tbl} SET {col}=? WHERE {col}=?", (keep_name, drop_name))
            if table_exists(db, "sample_values"): cur.execute("UPDATE sample_values SET parameter_id=? WHERE parameter_id=?", (keep_id, drop_id))
            cur.execute("DELETE FROM parameter_defs WHERE id=?", (drop_id,))
            merged += 1
    db.commit()
    db.close()
    return redirect("/admin/merge-parameters")

@app.get("/tools/calculators", response_class=HTMLResponse)
@app.get("/tools/calculators/", response_class=HTMLResponse, include_in_schema=False)
def calculators(request: Request):
    db = get_db()
    tanks = q(db, "SELECT * FROM tanks ORDER BY name")
    additives_rows = q(db, "SELECT * FROM additives WHERE active=1 ORDER BY parameter, name")
    profiles = q(db, "SELECT * FROM tank_profiles")
    profile_by_tank = {p["tank_id"]: p for p in profiles}
    db.close()
    return templates.TemplateResponse("calculators.html", {"request": request, "tanks": tanks, "additives": additives_rows, "profiles": profile_by_tank})

@app.post("/tools/calculators", response_class=HTMLResponse)
def calculators_post(request: Request, tank_id: int = Form(...), additive_id: int = Form(...), desired_change: float = Form(...)):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    tank_profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    additive = one(db, "SELECT * FROM additives WHERE id=?", (additive_id,))
    if not tank or not additive:
        db.close()
        return redirect("/tools/calculators")
    volume_l = tank_profile["volume_l"] if tank_profile else None
    net_percent = ((tank_profile["net_percent"] if tank_profile and tank_profile["net_percent"] is not None else 100) if tank_profile else 100)
    strength = additive["strength"]
    error, dose_ml, days, daily_ml, daily_change, unit = None, None, 1, None, None, ""
    if volume_l is None or strength in (None, 0): error = "Tank volume or additive strength is missing/invalid."
    else:
        volume = float(volume_l) * (float(net_percent) / 100.0)
        dose_ml = (float(desired_change) / float(strength)) * (volume / 100.0)
        pname = (additive['parameter'] or '').strip()
        pdef = one(db, "SELECT * FROM parameter_defs WHERE name=?", (pname,))
        if not pdef and pname:
            defs = q(db, "SELECT * FROM parameter_defs WHERE active=1")
            lname = pname.lower()
            for d in defs:
                dn = (d['name'] or '').lower()
                if not dn: continue
                if dn in lname or lname in dn: pdef = d; break
        limit_change = (pdef['max_daily_change'] if pdef and ('max_daily_change' in pdef.keys()) else None)
        unit = ((pdef["unit"] if pdef and ("unit" in pdef.keys()) else None) or (additive["unit"] if ("unit" in additive.keys()) else None) or "")
        try:
            if limit_change is not None and float(limit_change) > 0 and float(desired_change) > float(limit_change):
                days = int(math.ceil(float(desired_change) / float(limit_change)))
                daily_change = float(desired_change) / float(days)
                daily_ml = (daily_change / float(strength)) * (volume / 100.0)
        except Exception: pass
    tanks = q(db, "SELECT * FROM tanks ORDER BY name")
    additives_rows = q(db, "SELECT * FROM additives WHERE active=1 ORDER BY parameter, name")
    profiles = q(db, "SELECT * FROM tank_profiles")
    profile_by_tank = {p["tank_id"]: p for p in profiles}
    db.close()
    return templates.TemplateResponse("calculators.html", {"request": request, "result": {"dose_ml": None if dose_ml is None else round(dose_ml, 2), "days": days, "daily_ml": None if daily_ml is None else round(daily_ml, 2), "daily_change": None if daily_change is None else round(daily_change, 4), "unit": unit, "error": error, "tank": tank, "additive": additive, "desired_change": desired_change}, "tanks": tanks, "additives": additives_rows, "profiles": profile_by_tank, "selected": {"tank_id": tank_id, "additive_id": additive_id}})

@app.get("/tools/dose-plan", response_class=HTMLResponse)
@app.get("/tools/dose-plan/", response_class=HTMLResponse, include_in_schema=False)
def dose_plan(request: Request):
    db = get_db()
    today = date.today()
    try:
        chk_rows = q(db, "SELECT tank_id, parameter, additive_id, planned_date, checked FROM dose_plan_checks WHERE planned_date>=? AND planned_date<=?", (today.isoformat(), (today + timedelta(days=60)).isoformat()))
        check_map = {(r["tank_id"], r["parameter"], r["additive_id"], r["planned_date"]): int(r["checked"] or 0) for r in chk_rows}
    except Exception: check_map = {}
    tanks = q(db, "SELECT * FROM tanks ORDER BY name")
    pdefs = q(db, "SELECT name, unit, max_daily_change FROM parameter_defs")
    pdef_map = {r["name"]: r for r in pdefs}
    plans = []
    grand_total_ml = 0.0
    for t in tanks:
        tank_id = int(t["id"])
        prof = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
        vol_l = None
        net_pct = 100.0
        if prof:
            vol_l = prof["volume_l"] if prof["volume_l"] is not None else None
            try: net_pct = float(prof["net_percent"] if prof["net_percent"] is not None else 100.0)
            except Exception: net_pct = 100.0
        if vol_l is None: vol_l = t["volume_l"] if ("volume_l" in t.keys() and t["volume_l"] is not None) else None
        eff_vol_l = None
        try:
            if vol_l is not None: eff_vol_l = float(vol_l) * (net_pct / 100.0)
        except Exception: eff_vol_l = None
        latest = one(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT 1", (tank_id,))
        latest_readings, latest_taken = {}, None
        if latest:
            latest_taken = parse_dt_any(latest["taken_at"]) or latest["taken_at"]
            try:
                for r in get_sample_readings(db, int(latest["id"])): latest_readings[r["name"]] = r["value"]
            except Exception: latest_readings = {}
        targets = q(db, "SELECT * FROM targets WHERE tank_id=? AND enabled=1 ORDER BY parameter", (tank_id,))
        tank_rows = []
        for tr in targets:
            pname = tr["parameter"]
            if not pname: continue
            tl = tr["target_low"] if "target_low" in tr.keys() else (tr["low"] if "low" in tr.keys() else None)
            th = tr["target_high"] if "target_high" in tr.keys() else (tr["high"] if "high" in tr.keys() else None)
            target_val = None
            try:
                if tl is not None and th is not None: target_val = (float(tl) + float(th)) / 2.0
                elif th is not None: target_val = float(th)
                elif tl is not None: target_val = float(tl)
            except Exception: target_val = None
            if target_val is None: continue
            cur_val = latest_readings.get(pname)
            try: cur_val_f = float(cur_val) if cur_val is not None else None
            except Exception: cur_val_f = None
            if cur_val_f is None: continue
            delta = target_val - cur_val_f
            if delta <= 0: continue
            unit = ""
            if pname in pdef_map and pdef_map[pname]["unit"]: unit = pdef_map[pname]["unit"]
            elif "unit" in tr.keys() and tr["unit"]: unit = tr["unit"]
            max_change = None
            if pname in pdef_map:
                try: max_change = pdef_map[pname]["max_daily_change"]
                except Exception: max_change = None
            try: max_change_f = float(max_change) if max_change is not None else None
            except Exception: max_change_f = None
            days = 1
            if max_change_f and max_change_f > 0 and delta > max_change_f: days = int(math.ceil(delta / max_change_f))
            per_day_change = delta / float(days)
            adds = q(db, "SELECT * FROM additives WHERE active=1 AND parameter=? ORDER BY name", (pname,))
            if not adds:
                tank_rows.append({"parameter": pname, "latest": cur_val_f, "target": target_val, "change": delta, "unit": unit, "days": days, "per_day_change": per_day_change, "max_daily_change": max_change_f, "additives": [], "note": "No additive linked."})
                continue
            add_rows = []
            for a in adds:
                strength = a["strength"]
                if strength in (None, 0) or eff_vol_l is None: total_ml = None
                else:
                    try: total_ml = (float(delta) / float(strength)) * (float(eff_vol_l) / 100.0)
                    except Exception: total_ml = None
                per_day_ml = None if total_ml is None else (float(total_ml) / float(days))
                schedule = []
                for i in range(int(days)):
                    d = (today + timedelta(days=i)).isoformat()
                    schedule.append({
                        "day": i + 1,  # Day 1, Day 2...
                        "date": d,
                        "when": parse_dt_any(d),
                        "ml": per_day_ml,
                        "checked": check_map.get((tank_id, pname, a["id"], d), 0),
                        "tank_id": tank_id,
                        "additive_id": a["id"],
                        "parameter": pname,
                        "key": f"{tank_id}|{pname}|{a['id']}|{d}",
                    })
                add_rows.append({"additive_id": a["id"], "additive_name": a["name"], "strength": a["strength"], "max_daily_change": max_change_f, "total_ml": total_ml, "per_day_ml": per_day_ml, "schedule": schedule})
            tank_rows.append({"parameter": pname, "latest": cur_val_f, "target": target_val, "change": delta, "unit": unit, "days": days, "per_day_change": per_day_change, "additives": add_rows, "note": ""})
        plan_total_ml = 0.0
        for _r in tank_rows:
            for _a in (_r.get("additives") or []):
                try: plan_total_ml += float(_a.get("total_ml") or 0)
                except Exception: pass
        grand_total_ml += plan_total_ml
        plans.append({"tank": t, "latest_taken": latest_taken, "eff_vol_l": eff_vol_l, "net_pct": net_pct, "rows": tank_rows, "total_ml": plan_total_ml})
    db.close()
    return templates.TemplateResponse("dose_plan.html", {"request": request, "plans": plans, "grand_total_ml": grand_total_ml, "format_value": format_value})

@app.post("/tools/dose-plan/check")
async def dose_plan_check(request: Request):
    form = await request.form()
    key = (form.get("key") or "").strip()
    if key and (not form.get("tank_id")):
        parts = key.split("|", 3)
        if len(parts) == 4: form = {**form, "tank_id": parts[0], "parameter": parts[1], "additive_id": parts[2], "planned_date": parts[3]}
    try:
        tank_id = int(form.get("tank_id") or 0)
        additive_id = int(form.get("additive_id") or 0)
    except Exception: raise HTTPException(status_code=400, detail="Invalid tank/additive id")
    parameter = (form.get("parameter") or "").strip()
    planned_date = (form.get("planned_date") or "").strip()
    checked = 1 if str(form.get("checked") or "").lower() in ("1", "true", "on", "yes") else 0
    try: amount_ml = float(form.get("amount_ml") or 0)
    except Exception: amount_ml = 0.0
    if not tank_id or not additive_id or not parameter or not planned_date: raise HTTPException(status_code=400, detail="Missing fields")
    db = get_db()
    now_iso = datetime.utcnow().isoformat()
    db.execute("INSERT INTO dose_plan_checks (tank_id, parameter, additive_id, planned_date, checked, checked_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(tank_id, parameter, additive_id, planned_date) DO UPDATE SET checked=excluded.checked, checked_at=excluded.checked_at", (tank_id, parameter, additive_id, planned_date, checked, now_iso))
    try:
        logged_at = f"{planned_date}T00:00:00"
        reason = f"Dose plan: {parameter} ({planned_date})"
        if checked and amount_ml > 0:
            existing = one(db, "SELECT id FROM dose_logs WHERE tank_id=? AND additive_id=? AND logged_at=? AND reason=? LIMIT 1", (tank_id, additive_id, logged_at, reason))
            if not existing: db.execute("INSERT INTO dose_logs (tank_id, additive_id, amount_ml, reason, logged_at) VALUES (?, ?, ?, ?, ?)", (tank_id, additive_id, amount_ml, reason, logged_at))
        elif not checked: db.execute("DELETE FROM dose_logs WHERE tank_id=? AND additive_id=? AND logged_at=? AND reason=?", (tank_id, additive_id, logged_at, reason))
    except Exception: pass
    db.commit()
    db.close()
    return {"ok": True, "checked": checked}

@app.get("/admin/import-excel", response_class=HTMLResponse)
def import_excel_page(request: Request):
    return templates.TemplateResponse("simple_message.html", {"request": request, "title": "Import Tank Parameters Sheet", "message": "This will import tank volumes and historical readings from the bundled Tank Parameters Sheet.xlsx.", "actions": [{"label": "Run import", "method": "post", "href": "/admin/import-excel"}]})

@app.post("/admin/import-excel")
async def import_excel_run(request: Request):
    db = get_db()
    candidates = []
    env_path = os.environ.get("REEF_EXCEL_PATH")
    if env_path: candidates.append(env_path)
    here = os.path.dirname(__file__)
    candidates += [os.path.join(here, "Tank Parameters Sheet.xlsx"), os.path.join(here, "..", "Tank Parameters Sheet.xlsx"), os.path.join(here, "..", "..", "Tank Parameters Sheet.xlsx"), os.path.join(os.getcwd(), "Tank Parameters Sheet.xlsx"), os.path.join(os.getcwd(), "reef-params", "Tank Parameters Sheet.xlsx")]
    xlsx_path = next((p for p in candidates if p and os.path.exists(p)), None)
    if not xlsx_path:
        db.close()
        return templates.TemplateResponse("simple_message.html", {"request": request, "title": "Import failed", "message": "Could not find 'Tank Parameters Sheet.xlsx'. Place it in /app (same folder as main.py), or set REEF_EXCEL_PATH to the full path inside the container.", "actions": [{"label": "Back", "method": "get", "href": "/"}]})
    try:
        from import_from_excel import import_from_tank_parameters_sheet
        stats = import_from_tank_parameters_sheet(db, xlsx_path)
    except Exception as e:
        db.close()
        return templates.TemplateResponse("simple_message.html", {"request": request, "title": "Import failed", "message": f"Error while importing from Excel: {type(e).__name__}: {e}", "actions": [{"label": "Back", "method": "get", "href": "/admin/import-excel"}]})
    finally:
        try: db.close()
        except Exception: pass
    return templates.TemplateResponse("simple_message.html", {"request": request, "title": "Import complete", "message": f"Sheets processed: {stats.get('sheets_processed', 0)} | Tanks created: {stats.get('tanks_created', 0)} | Samples: {stats.get('samples_created', 0)} | Values: {stats.get('values_created', 0)}", "actions": [{"label": "Go to dashboard", "method": "get", "href": "/"}]})
