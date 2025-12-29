import os
import sqlite3
import re
import math
import json
import csv
import pandas as pd
from io import BytesIO
from datetime import datetime, date, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Form, Request, HTTPException, File, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import StreamingResponse

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
# Maps parameter names to the new DB columns we added
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
    
    # Updated: 2 decimal places standard
    s = f"{fv:.2f}" 
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s

def dtfmt(v: Any) -> str:
    dt = parse_dt_any(v) if not isinstance(v, datetime) else v
    if dt is None: return ""
    return dt.strftime("%H:%M - %d/%m/%Y")

def time_ago(v: Any) -> str:
    """Returns a string like '2 days ago' or 'Today'."""
    dt = parse_dt_any(v)
    if not dt: return ""
    now = datetime.now()
    diff = now - dt
    
    if diff.days == 0:
        return "Today"
    if diff.days == 1:
        return "Yesterday"
    if diff.days < 30:
        return f"{diff.days} days ago"
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

def collect_dosing_notifications(db: sqlite3.Connection, tank_id: int | None = None) -> List[Dict[str, Any]]:
    today = date.today().isoformat()
    params: List[Any] = []
    where_clause = ""
    if tank_id is not None:
        where_clause = "WHERE p.tank_id=?"
        params.append(tank_id)
    rows = q(
        db,
        f"""SELECT t.id AS tank_id, t.name AS tank_name, p.*
            FROM tank_profiles p
            JOIN tanks t ON t.id = p.tank_id
            {where_clause}""",
        tuple(params),
    )
    notifications: List[Dict[str, Any]] = []
    dosing_containers = [
        ("all_in_one", "all_in_one_container_ml", "all_in_one_remaining_ml", "all_in_one_daily_ml", "All-in-one"),
        ("alk", "alk_container_ml", "alk_remaining_ml", "alk_daily_ml", "Alkalinity"),
        ("ca", "ca_container_ml", "ca_remaining_ml", "ca_daily_ml", "Calcium"),
        ("mg", "mg_container_ml", "mg_remaining_ml", "mg_daily_ml", "Magnesium"),
        ("nitrate", "nitrate_container_ml", "nitrate_remaining_ml", "nitrate_daily_ml", "Nitrate"),
        ("phosphate", "phosphate_container_ml", "phosphate_remaining_ml", "phosphate_daily_ml", "Phosphate"),
        ("nopox", "nopox_container_ml", "nopox_remaining_ml", "nopox_daily_ml", "NoPox"),
    ]
    for row in rows:
        threshold_days = row_get(row, "dosing_low_days", 5)
        try:
            threshold_days = float(threshold_days) if threshold_days is not None else 5
        except Exception:
            threshold_days = 5
        for key, container_col, remaining_col, daily_col, label in dosing_containers:
            container_ml = row_get(row, container_col)
            if container_ml is None:
                continue
            remaining_ml = row_get(row, remaining_col)
            daily_ml = row_get(row, daily_col)
            if remaining_ml is None:
                remaining_ml = container_ml
            if not daily_ml:
                continue
            days_remaining = remaining_ml / float(daily_ml)
            if days_remaining <= threshold_days:
                notifications.append(
                    {
                        "tank_id": row_get(row, "tank_id"),
                        "tank_name": row_get(row, "tank_name"),
                        "label": label,
                        "days": days_remaining,
                        "threshold": threshold_days,
                    }
                )
                exists = one(
                    db,
                    "SELECT 1 FROM dosing_notifications WHERE tank_id=? AND container_key=? AND notified_on=?",
                    (row_get(row, "tank_id"), key, today),
                )
                if not exists:
                    db.execute(
                        "INSERT INTO dosing_notifications (tank_id, container_key, notified_on) VALUES (?, ?, ?)",
                        (row_get(row, "tank_id"), key, today),
                    )
                    db.commit()
    return notifications

def global_dosing_notifications() -> List[Dict[str, Any]]:
    db = get_db()
    try:
        return collect_dosing_notifications(db)
    finally:
        db.close()

templates.env.globals["global_dosing_notifications"] = global_dosing_notifications

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

def ensure_column(db: sqlite3.Connection, table: str, col: str, ddl: str) -> None:
    cur = db.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if col not in cols:
        db.execute(ddl)


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
        return q(db, """
            SELECT pd.name AS name,
                   sv.value AS value,
                   COALESCE(pd.unit, '') AS unit,
                   svk.test_kit_id AS test_kit_id,
                   tk.name AS test_kit_name
            FROM sample_values sv
            JOIN parameter_defs pd ON pd.id = sv.parameter_id
            LEFT JOIN sample_value_kits svk ON svk.sample_id = sv.sample_id AND svk.parameter_id = sv.parameter_id
            LEFT JOIN test_kits tk ON tk.id = svk.test_kit_id
            WHERE sv.sample_id=?
            ORDER BY COALESCE(pd.sort_order, 0), pd.name
        """, (sample_id,))
    return q(db, """
        SELECT p.name AS name,
               p.value AS value,
               COALESCE(pd.unit, p.unit, '') AS unit,
               p.test_kit_id AS test_kit_id,
               tk.name AS test_kit_name
        FROM parameters p
        LEFT JOIN parameter_defs pd ON pd.name = p.name
        LEFT JOIN test_kits tk ON tk.id = p.test_kit_id
        WHERE p.sample_id=?
        ORDER BY COALESCE(pd.sort_order, 0), p.name
    """, (sample_id,))

def insert_sample_reading(db: sqlite3.Connection, sample_id: int, pname: str, value: float, unit: str = "", test_kit_id: int | None = None) -> None:
    cur = db.cursor()
    mode = values_mode(db)
    if mode == "sample_values":
        pd = one(db, "SELECT id FROM parameter_defs WHERE name=?", (pname,))
        if not pd:
            cur.execute("INSERT INTO parameter_defs (name, unit, active, sort_order) VALUES (?, ?, 1, 0)", (pname, unit or None))
            pid = cur.lastrowid
        else: pid = pd["id"]
        cur.execute("INSERT INTO sample_values (sample_id, parameter_id, value) VALUES (?, ?, ?)", (sample_id, pid, value))
        if test_kit_id:
            cur.execute(
                "INSERT OR REPLACE INTO sample_value_kits (sample_id, parameter_id, test_kit_id) VALUES (?, ?, ?)",
                (sample_id, pid, test_kit_id),
            )
    else:
        cur.execute("INSERT INTO parameters (sample_id, name, value, unit, test_kit_id) VALUES (?, ?, ?, ?, ?)", (sample_id, pname, value, unit or None, test_kit_id))

def get_sample_kits(db: sqlite3.Connection, sample_id: int) -> Dict[str, int]:
    mode = values_mode(db)
    if mode == "sample_values":
        rows = q(db, """
            SELECT pd.name AS name, svk.test_kit_id AS test_kit_id
            FROM sample_value_kits svk
            JOIN parameter_defs pd ON pd.id = svk.parameter_id
            WHERE svk.sample_id=?
        """, (sample_id,))
    else:
        rows = q(db, """
            SELECT p.name AS name, p.test_kit_id AS test_kit_id
            FROM parameters p
            WHERE p.sample_id=?
        """, (sample_id,))
    return {r["name"]: r["test_kit_id"] for r in rows if r["test_kit_id"]}

def parse_conversion_table(text: str) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    if not text:
        return rows
    cleaned = text.replace("\t", " ")
    if "\n" not in cleaned and "," in cleaned:
        cleaned = cleaned.replace(" ", "\n")
    for line in cleaned.splitlines():
        if not line.strip():
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 2:
            continue
        remaining = to_float(parts[0])
        value = to_float(parts[1])
        if remaining is None or value is None:
            continue
        rows.append({"remaining": float(remaining), "value": float(value)})
    return rows

def compute_conversion_value(remaining: float, table: List[Dict[str, float]]) -> Optional[float]:
    if not table:
        return None
    sorted_rows = sorted(table, key=lambda r: r["remaining"])
    if remaining <= sorted_rows[0]["remaining"]:
        return sorted_rows[0]["value"]
    if remaining >= sorted_rows[-1]["remaining"]:
        return sorted_rows[-1]["value"]
    for idx in range(1, len(sorted_rows)):
        low = sorted_rows[idx - 1]
        high = sorted_rows[idx]
        if low["remaining"] <= remaining <= high["remaining"]:
            span = high["remaining"] - low["remaining"]
            if span == 0:
                return low["value"]
            ratio = (remaining - low["remaining"]) / span
            return low["value"] + ratio * (high["value"] - low["value"])
    return None

def get_test_kit_conversion(db: sqlite3.Connection, kit_id: int | None) -> Tuple[Optional[str], Optional[str]]:
    if not kit_id:
        return None, None
    kit = one(db, "SELECT conversion_type, conversion_data FROM test_kits WHERE id=?", (kit_id,))
    if not kit:
        return None, None
    return kit["conversion_type"], kit["conversion_data"]

# --- NEW HELPER: Get the Latest Reading per Parameter ---
def get_latest_per_parameter(db: sqlite3.Connection, tank_id: int) -> Dict[str, Dict[str, Any]]:
    """
    Returns a dictionary of the absolute most recent reading for each parameter.
    Format: {'Alkalinity': {'value': 8.5, 'taken_at': datetime(...)}, ...}
    """
    latest_map = {}
    mode = values_mode(db)
    
    if mode == "sample_values":
        rows = q(db, """
            SELECT pd.name, sv.value, s.taken_at, s.id AS sample_id
            FROM sample_values sv 
            JOIN samples s ON s.id = sv.sample_id 
            JOIN parameter_defs pd ON pd.id = sv.parameter_id
            WHERE s.tank_id=? 
            ORDER BY s.taken_at DESC
        """, (tank_id,))
    else:
        rows = q(db, """
            SELECT p.name, p.value, s.taken_at, s.id AS sample_id
            FROM parameters p
            JOIN samples s ON s.id = p.sample_id 
            WHERE s.tank_id=? 
            ORDER BY s.taken_at DESC
        """, (tank_id,))

    for r in rows:
        name = r["name"]
        if name not in latest_map:
            latest_map[name] = {
                "value": r["value"],
                "taken_at": parse_dt_any(r["taken_at"]),
                "sample_id": r["sample_id"],
            }
    
    return latest_map

def get_latest_and_previous_per_parameter(db: sqlite3.Connection, tank_id: int) -> Dict[str, Dict[str, Any]]:
    latest_map: Dict[str, Dict[str, Any]] = {}
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
            latest_map[name] = {
                "latest": {"value": r["value"], "taken_at": parse_dt_any(r["taken_at"])},
                "previous": None,
            }
        elif latest_map[name]["previous"] is None:
            latest_map[name]["previous"] = {
                "value": r["value"],
                "taken_at": parse_dt_any(r["taken_at"])
            }
    return latest_map

# --- DATABASE MODELS ---
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

    @property
    def values_dict(self):
        return {v.parameter_id: v.value for v in self.values}

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
        CREATE TABLE IF NOT EXISTS tank_profiles (tank_id INTEGER PRIMARY KEY, volume_l REAL, net_percent REAL DEFAULT 100, alk_solution TEXT, alk_daily_ml REAL, ca_solution TEXT, ca_daily_ml REAL, mg_solution TEXT, mg_daily_ml REAL, dosing_mode TEXT, all_in_one_solution TEXT, all_in_one_daily_ml REAL, nitrate_solution TEXT, nitrate_daily_ml REAL, phosphate_solution TEXT, phosphate_daily_ml REAL, nopox_daily_ml REAL, all_in_one_container_ml REAL, all_in_one_remaining_ml REAL, alk_container_ml REAL, alk_remaining_ml REAL, ca_container_ml REAL, ca_remaining_ml REAL, mg_container_ml REAL, mg_remaining_ml REAL, nitrate_container_ml REAL, nitrate_remaining_ml REAL, phosphate_container_ml REAL, phosphate_remaining_ml REAL, nopox_container_ml REAL, nopox_remaining_ml REAL, dosing_container_updated_at TEXT, dosing_low_days REAL, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS samples (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, taken_at TEXT NOT NULL, notes TEXT, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS parameter_defs (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, unit TEXT, active INTEGER DEFAULT 1, sort_order INTEGER DEFAULT 0, max_daily_change REAL);
        CREATE TABLE IF NOT EXISTS parameters (id INTEGER PRIMARY KEY AUTOINCREMENT, sample_id INTEGER NOT NULL, name TEXT NOT NULL, value REAL, unit TEXT, test_kit_id INTEGER, FOREIGN KEY (sample_id) REFERENCES samples(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS targets (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, parameter TEXT NOT NULL, low REAL, high REAL, unit TEXT, enabled INTEGER DEFAULT 1, UNIQUE(tank_id, parameter), FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS test_kits (id INTEGER PRIMARY KEY AUTOINCREMENT, parameter TEXT NOT NULL, name TEXT NOT NULL, unit TEXT, resolution REAL, min_value REAL, max_value REAL, notes TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS additives (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, parameter TEXT NOT NULL, strength REAL NOT NULL, unit TEXT NOT NULL, max_daily REAL, notes TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS dose_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, additive_id INTEGER, amount_ml REAL NOT NULL, reason TEXT, logged_at TEXT NOT NULL, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS dose_plan_checks (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, parameter TEXT NOT NULL, additive_id INTEGER NOT NULL, planned_date TEXT NOT NULL, checked INTEGER DEFAULT 0, checked_at TEXT, UNIQUE(tank_id, parameter, additive_id, planned_date), FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS sample_value_kits (sample_id INTEGER NOT NULL, parameter_id INTEGER NOT NULL, test_kit_id INTEGER NOT NULL, PRIMARY KEY (sample_id, parameter_id), FOREIGN KEY (sample_id) REFERENCES samples(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS dosing_notifications (id INTEGER PRIMARY KEY AUTOINCREMENT, tank_id INTEGER NOT NULL, container_key TEXT NOT NULL, notified_on TEXT NOT NULL, FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE);
    ''')
    ensure_column(db, "tanks", "volume_l", "ALTER TABLE tanks ADD COLUMN volume_l REAL")
    ensure_column(db, "parameter_defs", "max_daily_change", "ALTER TABLE parameter_defs ADD COLUMN max_daily_change REAL")
    ensure_column(db, "additives", "active", "ALTER TABLE additives ADD COLUMN active INTEGER DEFAULT 1")
    ensure_column(db, "test_kits", "active", "ALTER TABLE test_kits ADD COLUMN active INTEGER DEFAULT 1")
    ensure_column(db, "targets", "target_low", "ALTER TABLE targets ADD COLUMN target_low REAL")
    ensure_column(db, "targets", "target_high", "ALTER TABLE targets ADD COLUMN target_high REAL")
    ensure_column(db, "targets", "alert_low", "ALTER TABLE targets ADD COLUMN alert_low REAL")
    ensure_column(db, "targets", "alert_high", "ALTER TABLE targets ADD COLUMN alert_high REAL")
    ensure_column(db, "parameter_defs", "test_interval_days", "ALTER TABLE parameter_defs ADD COLUMN test_interval_days INTEGER")
    ensure_column(db, "parameters", "test_kit_id", "ALTER TABLE parameters ADD COLUMN test_kit_id INTEGER")
    ensure_column(db, "test_kits", "conversion_type", "ALTER TABLE test_kits ADD COLUMN conversion_type TEXT")
    ensure_column(db, "test_kits", "conversion_data", "ALTER TABLE test_kits ADD COLUMN conversion_data TEXT")
    
    # NEW: Add default target columns
    ensure_column(db, "parameter_defs", "default_target_low", "ALTER TABLE parameter_defs ADD COLUMN default_target_low REAL")
    ensure_column(db, "parameter_defs", "default_target_high", "ALTER TABLE parameter_defs ADD COLUMN default_target_high REAL")
    ensure_column(db, "parameter_defs", "default_alert_low", "ALTER TABLE parameter_defs ADD COLUMN default_alert_low REAL")
    ensure_column(db, "parameter_defs", "default_alert_high", "ALTER TABLE parameter_defs ADD COLUMN default_alert_high REAL")
    ensure_column(db, "tank_profiles", "alk_solution", "ALTER TABLE tank_profiles ADD COLUMN alk_solution TEXT")
    ensure_column(db, "tank_profiles", "alk_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN alk_daily_ml REAL")
    ensure_column(db, "tank_profiles", "ca_solution", "ALTER TABLE tank_profiles ADD COLUMN ca_solution TEXT")
    ensure_column(db, "tank_profiles", "ca_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN ca_daily_ml REAL")
    ensure_column(db, "tank_profiles", "mg_solution", "ALTER TABLE tank_profiles ADD COLUMN mg_solution TEXT")
    ensure_column(db, "tank_profiles", "mg_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN mg_daily_ml REAL")
    ensure_column(db, "tank_profiles", "dosing_mode", "ALTER TABLE tank_profiles ADD COLUMN dosing_mode TEXT")
    ensure_column(db, "tank_profiles", "all_in_one_solution", "ALTER TABLE tank_profiles ADD COLUMN all_in_one_solution TEXT")
    ensure_column(db, "tank_profiles", "all_in_one_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN all_in_one_daily_ml REAL")
    ensure_column(db, "tank_profiles", "nitrate_solution", "ALTER TABLE tank_profiles ADD COLUMN nitrate_solution TEXT")
    ensure_column(db, "tank_profiles", "nitrate_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN nitrate_daily_ml REAL")
    ensure_column(db, "tank_profiles", "phosphate_solution", "ALTER TABLE tank_profiles ADD COLUMN phosphate_solution TEXT")
    ensure_column(db, "tank_profiles", "phosphate_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN phosphate_daily_ml REAL")
    ensure_column(db, "tank_profiles", "nopox_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN nopox_daily_ml REAL")
    ensure_column(db, "tank_profiles", "all_in_one_container_ml", "ALTER TABLE tank_profiles ADD COLUMN all_in_one_container_ml REAL")
    ensure_column(db, "tank_profiles", "all_in_one_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN all_in_one_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "alk_container_ml", "ALTER TABLE tank_profiles ADD COLUMN alk_container_ml REAL")
    ensure_column(db, "tank_profiles", "alk_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN alk_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "ca_container_ml", "ALTER TABLE tank_profiles ADD COLUMN ca_container_ml REAL")
    ensure_column(db, "tank_profiles", "ca_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN ca_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "mg_container_ml", "ALTER TABLE tank_profiles ADD COLUMN mg_container_ml REAL")
    ensure_column(db, "tank_profiles", "mg_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN mg_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "nitrate_container_ml", "ALTER TABLE tank_profiles ADD COLUMN nitrate_container_ml REAL")
    ensure_column(db, "tank_profiles", "nitrate_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN nitrate_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "phosphate_container_ml", "ALTER TABLE tank_profiles ADD COLUMN phosphate_container_ml REAL")
    ensure_column(db, "tank_profiles", "phosphate_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN phosphate_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "nopox_container_ml", "ALTER TABLE tank_profiles ADD COLUMN nopox_container_ml REAL")
    ensure_column(db, "tank_profiles", "nopox_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN nopox_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "dosing_container_updated_at", "ALTER TABLE tank_profiles ADD COLUMN dosing_container_updated_at TEXT")
    ensure_column(db, "tank_profiles", "dosing_low_days", "ALTER TABLE tank_profiles ADD COLUMN dosing_low_days REAL")
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tank_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tank_id INTEGER NOT NULL,
            entry_date TEXT NOT NULL,
            entry_type TEXT,
            title TEXT,
            notes TEXT,
            FOREIGN KEY (tank_id) REFERENCES tanks(id) ON DELETE CASCADE
        );
    ''')

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
        latest_map = get_latest_and_previous_per_parameter(db, t["id"])
        pdefs = {p["name"]: p for p in get_active_param_defs(db)}
        targets = {tr["parameter"]: tr for tr in q(db, "SELECT * FROM targets WHERE tank_id=? AND enabled=1", (t["id"],))}
        latest = one(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT 1", (t["id"],))
        
        readings = []
        out_of_range = 0
        overdue_count = 0
        for pname, p in pdefs.items():
            data = latest_map.get(pname)
            if not data or not data.get("latest"):
                continue
            latest_data = data["latest"]
            previous = data.get("previous")
            latest_val = latest_data["value"]
            latest_taken = latest_data["taken_at"]
            status = "ok"
            target = targets.get(pname)
            if target and latest_val is not None:
                al, ah = row_get(target, "alert_low"), row_get(target, "alert_high")
                tl = row_get(target, "target_low") if row_get(target, "target_low") is not None else row_get(target, "low")
                th = row_get(target, "target_high") if row_get(target, "target_high") is not None else row_get(target, "high")
                try:
                    fv = float(latest_val)
                    if al is not None and fv < float(al):
                        status = "danger"
                    elif ah is not None and fv > float(ah):
                        status = "danger"
                    elif tl is not None and fv < float(tl):
                        status = "warn"
                    elif th is not None and fv > float(th):
                        status = "warn"
                except Exception:
                    pass
            trend_warning = False
            delta_per_day = None
            if previous and latest_taken and previous.get("taken_at"):
                try:
                    delta = float(latest_val) - float(previous.get("value"))
                    days = max((latest_taken - previous["taken_at"]).total_seconds() / 86400.0, 1e-6)
                    delta_per_day = delta / days
                    max_change = row_get(p, "max_daily_change")
                    if max_change is not None and abs(delta_per_day) > float(max_change):
                        trend_warning = True
                except Exception:
                    pass
            overdue = False
            if latest_taken:
                interval_days = row_get(p, "test_interval_days")
                if interval_days:
                    try:
                        overdue = (datetime.now() - latest_taken).days >= int(interval_days)
                    except Exception:
                        overdue = False
            if status in ("danger", "warn"):
                out_of_range += 1
            if overdue:
                overdue_count += 1
            readings.append({
                "name": pname,
                "value": latest_val,
                "unit": (row_get(p, "unit") or ""),
                "taken_at": latest_taken,
                "status": status,
                "trend_warning": trend_warning,
                "delta_per_day": delta_per_day,
                "overdue": overdue,
            })
        
        tank_data = dict(t)
        if "volume_l" not in tank_data:
            tank_data["volume_l"] = None 

        last_dose = one(db, "SELECT logged_at FROM dose_logs WHERE tank_id=? ORDER BY logged_at DESC LIMIT 1", (t["id"],))
        last_dose_at = parse_dt_any(last_dose["logged_at"]) if last_dose else None
        tank_cards.append({
            "tank": tank_data,
            "latest": latest,
            "readings": readings,
            "summary": {
                "out_of_range": out_of_range,
                "overdue": overdue_count,
                "last_dose_at": last_dose_at,
            },
        })
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
async def sample_delete(sample_id: int):
    db = get_db()
    try:
        # 1. Find the tank_id associated with this sample before we delete it
        sample = one(db, "SELECT tank_id FROM samples WHERE id = ?", (sample_id,))
        
        if not sample:
            return redirect("/")
            
        tank_id = sample["tank_id"]

        # 2. Perform the deletion
        db.execute("DELETE FROM samples WHERE id = ?", (sample_id,))
        db.execute("DELETE FROM sample_values WHERE sample_id = ?", (sample_id,))
        db.execute("DELETE FROM sample_value_kits WHERE sample_id = ?", (sample_id,))
        db.commit()

        # 3. Redirect back to the tank detail page we just came from
        return redirect(f"/tanks/{tank_id}")
    finally:
        db.close()

@app.get("/tanks/{tank_id}", response_class=HTMLResponse)
def tank_detail(request: Request, tank_id: int):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank:
        db.close()
        return templates.TemplateResponse("tank_detail.html", {"request": request, "tank": None, "params": [], "recent_samples": [], "sample_values": {}, "latest_vals": {}, "status_by_param_id": {}, "targets": [], "series": [], "chart_targets": [], "selected_parameter_id": "", "format_value": format_value, "target_map": {}})
    profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    tank_view = dict(tank)
    if profile:
        now = datetime.utcnow()
        last_update = parse_dt_any(row_get(profile, "dosing_container_updated_at"))
        if last_update is None:
            last_update = now
        days_since_update = max((now - last_update).days, 0)
        container_updates = {}
        if days_since_update:
            dosing_containers = [
                ("all_in_one_container_ml", "all_in_one_remaining_ml", "all_in_one_daily_ml"),
                ("alk_container_ml", "alk_remaining_ml", "alk_daily_ml"),
                ("ca_container_ml", "ca_remaining_ml", "ca_daily_ml"),
                ("mg_container_ml", "mg_remaining_ml", "mg_daily_ml"),
                ("nitrate_container_ml", "nitrate_remaining_ml", "nitrate_daily_ml"),
                ("phosphate_container_ml", "phosphate_remaining_ml", "phosphate_daily_ml"),
                ("nopox_container_ml", "nopox_remaining_ml", "nopox_daily_ml"),
            ]
            for container_col, remaining_col, daily_col in dosing_containers:
                container_ml = row_get(profile, container_col)
                daily_ml = row_get(profile, daily_col)
                if container_ml is None or daily_ml in (None, 0):
                    continue
                remaining_ml = row_get(profile, remaining_col)
                if remaining_ml is None:
                    remaining_ml = container_ml
                updated_remaining = max(remaining_ml - (float(daily_ml) * days_since_update), 0)
                if updated_remaining != remaining_ml:
                    container_updates[remaining_col] = updated_remaining
        if container_updates or row_get(profile, "dosing_container_updated_at") is None:
            container_updates["dosing_container_updated_at"] = now.isoformat()
            set_clause = ", ".join([f"{col}=?" for col in container_updates.keys()])
            db.execute(
                f"UPDATE tank_profiles SET {set_clause} WHERE tank_id=?",
                (*container_updates.values(), tank_id),
            )
            profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
        dosing_low_days = row_get(profile, "dosing_low_days")
        if dosing_low_days is None:
            dosing_low_days = 5
        tank_view.update({
            "volume_l": row_get(profile, "volume_l", tank_view.get("volume_l")),
            "net_percent": row_get(profile, "net_percent"),
            "alk_solution": row_get(profile, "alk_solution"),
            "alk_daily_ml": row_get(profile, "alk_daily_ml"),
            "ca_solution": row_get(profile, "ca_solution"),
            "ca_daily_ml": row_get(profile, "ca_daily_ml"),
            "mg_solution": row_get(profile, "mg_solution"),
            "mg_daily_ml": row_get(profile, "mg_daily_ml"),
            "dosing_mode": row_get(profile, "dosing_mode"),
            "all_in_one_solution": row_get(profile, "all_in_one_solution"),
            "all_in_one_daily_ml": row_get(profile, "all_in_one_daily_ml"),
            "nitrate_solution": row_get(profile, "nitrate_solution"),
            "nitrate_daily_ml": row_get(profile, "nitrate_daily_ml"),
            "phosphate_solution": row_get(profile, "phosphate_solution"),
            "phosphate_daily_ml": row_get(profile, "phosphate_daily_ml"),
            "nopox_daily_ml": row_get(profile, "nopox_daily_ml"),
            "all_in_one_container_ml": row_get(profile, "all_in_one_container_ml"),
            "all_in_one_remaining_ml": row_get(profile, "all_in_one_remaining_ml"),
            "alk_container_ml": row_get(profile, "alk_container_ml"),
            "alk_remaining_ml": row_get(profile, "alk_remaining_ml"),
            "ca_container_ml": row_get(profile, "ca_container_ml"),
            "ca_remaining_ml": row_get(profile, "ca_remaining_ml"),
            "mg_container_ml": row_get(profile, "mg_container_ml"),
            "mg_remaining_ml": row_get(profile, "mg_remaining_ml"),
            "nitrate_container_ml": row_get(profile, "nitrate_container_ml"),
            "nitrate_remaining_ml": row_get(profile, "nitrate_remaining_ml"),
            "phosphate_container_ml": row_get(profile, "phosphate_container_ml"),
            "phosphate_remaining_ml": row_get(profile, "phosphate_remaining_ml"),
            "nopox_container_ml": row_get(profile, "nopox_container_ml"),
            "nopox_remaining_ml": row_get(profile, "nopox_remaining_ml"),
            "dosing_container_updated_at": row_get(profile, "dosing_container_updated_at"),
            "dosing_low_days": dosing_low_days,
        })
    
    low_container_alerts = []
    if profile:
        all_notifications = collect_dosing_notifications(db, tank_id=tank_id)
        low_container_alerts = [
            {"label": alert["label"], "days": alert["days"]}
            for alert in all_notifications
            if alert.get("tank_id") == tank_id
        ]

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
            
            raw_date = r["taken_at"]
            dt_obj = parse_dt_any(raw_date)
            iso_date = dt_obj.isoformat() if dt_obj else str(raw_date)

            sid = int(r["sample_id"])
            if value is not None:
                try: y = float(value)
                except Exception: y = None
                if y is not None: series_map.setdefault(name, []).append({"x": iso_date, "y": y})
            
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
    
   # Build chart targets with distinct Alert and Target values
    chart_targets = []
    for t in targets:
        if (t["parameter"] or "") == selected_parameter_id:
            chart_targets.append({
                "parameter": t["parameter"],
                # These are the dashed red lines
                "alert_low": row_get(t, "alert_low"),
                "alert_high": row_get(t, "alert_high"),
                # These are the green box range
                "target_low": row_get(t, "target_low") if row_get(t, "target_low") is not None else row_get(t, "low"),
                "target_high": row_get(t, "target_high") if row_get(t, "target_high") is not None else row_get(t, "high"),
                "unit": row_get(t, "unit") or unit_by_name.get(selected_parameter_id, "")
            })
            
    params = [{"id": name, "name": name, "unit": unit_by_name.get(name, "")} for name in available_params]
    
    latest_by_param_id = get_latest_per_parameter(db, tank_id)
    latest_and_previous = get_latest_and_previous_per_parameter(db, tank_id)
    targets_by_param = {t["parameter"]: t for t in targets if row_get(t, "parameter") is not None}
    pdef_map = {p["name"]: p for p in get_active_param_defs(db)}
    
    status_by_param_id = {}
    trend_warning_by_param = {}
    overdue_by_param = {}
    for pname in available_params:
        data = latest_by_param_id.get(pname)
        v = data.get("value") if data else None
        
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

        prev_data = latest_and_previous.get(pname, {}).get("previous")
        latest_data = latest_and_previous.get(pname, {}).get("latest")
        trend_warning_by_param[pname] = False
        if latest_data and prev_data:
            try:
                delta = float(latest_data["value"]) - float(prev_data["value"])
                days = max((latest_data["taken_at"] - prev_data["taken_at"]).total_seconds() / 86400.0, 1e-6)
                delta_per_day = delta / days
                max_change = row_get(pdef_map.get(pname), "max_daily_change")
                if max_change is not None and abs(delta_per_day) > float(max_change):
                    trend_warning_by_param[pname] = True
            except Exception:
                trend_warning_by_param[pname] = False
        overdue_by_param[pname] = False
        latest_taken = latest_data["taken_at"] if latest_data else None
        interval_days = row_get(pdef_map.get(pname), "test_interval_days")
        if latest_taken and interval_days:
            try:
                overdue_by_param[pname] = (datetime.now() - latest_taken).days >= int(interval_days)
            except Exception:
                overdue_by_param[pname] = False
        
    recent_samples = samples[:10] if samples else []
    db.close()
    return templates.TemplateResponse("tank_detail.html", {"request": request, "tank": tank_view, "params": params, "recent_samples": recent_samples, "sample_values": sample_values, "latest_vals": latest_by_param_id, "status_by_param_id": status_by_param_id, "trend_warning_by_param": trend_warning_by_param, "overdue_by_param": overdue_by_param, "targets": targets, "target_map": targets_by_param, "series": series, "chart_targets": chart_targets, "selected_parameter_id": selected_parameter_id, "format_value": format_value, "low_container_alerts": low_container_alerts})

@app.get("/tanks/{tank_id}/journal", response_class=HTMLResponse)
def tank_journal(request: Request, tank_id: int):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    entries = q(db, "SELECT * FROM tank_journal WHERE tank_id=? ORDER BY entry_date DESC, id DESC", (tank_id,))
    db.close()
    return templates.TemplateResponse("tank_journal.html", {"request": request, "tank": tank, "entries": entries})

@app.post("/tanks/{tank_id}/journal")
async def tank_journal_add(request: Request, tank_id: int):
    form = await request.form()
    entry_date = (form.get("entry_date") or "").strip()
    if entry_date:
        try:
            entry_iso = datetime.fromisoformat(entry_date).isoformat()
        except ValueError:
            entry_iso = datetime.now().isoformat()
    else:
        entry_iso = datetime.now().isoformat()
    entry_type = (form.get("entry_type") or "").strip() or None
    title = (form.get("title") or "").strip() or None
    notes = (form.get("notes") or "").strip() or None
    db = get_db()
    db.execute(
        "INSERT INTO tank_journal (tank_id, entry_date, entry_type, title, notes) VALUES (?, ?, ?, ?, ?)",
        (tank_id, entry_iso, entry_type, title, notes),
    )
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/journal")

@app.post("/tanks/{tank_id}/journal/{entry_id}/delete")
def tank_journal_delete(tank_id: int, entry_id: int):
    db = get_db()
    db.execute("DELETE FROM tank_journal WHERE id=? AND tank_id=?", (entry_id, tank_id))
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/journal")

@app.get("/tanks/{tank_id}/export")
def tank_export(request: Request, tank_id: int):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    samples = q(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at ASC", (tank_id,))
    params = list_parameters(db)
    rows = []
    for s in samples:
        row = {
            "Tank": tank["name"],
            "Taken At": s["taken_at"],
            "Notes": s["notes"],
        }
        readings = {r["name"]: r["value"] for r in get_sample_readings(db, s["id"])}
        for p in params:
            row[p["name"]] = readings.get(p["name"])
        rows.append(row)
    db.close()
    df = pd.DataFrame(rows)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Samples")
    output.seek(0)
    return StreamingResponse(
        output,
        headers={"Content-Disposition": f'attachment; filename="{tank["name"]}_samples.xlsx"'},
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

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
            tank_view["alk_solution"] = profile["alk_solution"]
            tank_view["alk_daily_ml"] = profile["alk_daily_ml"]
            tank_view["ca_solution"] = profile["ca_solution"]
            tank_view["ca_daily_ml"] = profile["ca_daily_ml"]
            tank_view["mg_solution"] = profile["mg_solution"]
            tank_view["mg_daily_ml"] = profile["mg_daily_ml"]
            tank_view["dosing_mode"] = profile["dosing_mode"]
            tank_view["all_in_one_solution"] = profile["all_in_one_solution"]
            tank_view["all_in_one_daily_ml"] = profile["all_in_one_daily_ml"]
            tank_view["nitrate_solution"] = profile["nitrate_solution"]
            tank_view["nitrate_daily_ml"] = profile["nitrate_daily_ml"]
            tank_view["phosphate_solution"] = profile["phosphate_solution"]
            tank_view["phosphate_daily_ml"] = profile["phosphate_daily_ml"]
            tank_view["nopox_daily_ml"] = profile["nopox_daily_ml"]
            tank_view["all_in_one_container_ml"] = profile["all_in_one_container_ml"]
            tank_view["all_in_one_remaining_ml"] = profile["all_in_one_remaining_ml"]
            tank_view["alk_container_ml"] = profile["alk_container_ml"]
            tank_view["alk_remaining_ml"] = profile["alk_remaining_ml"]
            tank_view["ca_container_ml"] = profile["ca_container_ml"]
            tank_view["ca_remaining_ml"] = profile["ca_remaining_ml"]
            tank_view["mg_container_ml"] = profile["mg_container_ml"]
            tank_view["mg_remaining_ml"] = profile["mg_remaining_ml"]
            tank_view["nitrate_container_ml"] = profile["nitrate_container_ml"]
            tank_view["nitrate_remaining_ml"] = profile["nitrate_remaining_ml"]
            tank_view["phosphate_container_ml"] = profile["phosphate_container_ml"]
            tank_view["phosphate_remaining_ml"] = profile["phosphate_remaining_ml"]
            tank_view["nopox_container_ml"] = profile["nopox_container_ml"]
            tank_view["nopox_remaining_ml"] = profile["nopox_remaining_ml"]
            tank_view["dosing_container_updated_at"] = profile["dosing_container_updated_at"]
            tank_view["dosing_low_days"] = profile["dosing_low_days"] if profile["dosing_low_days"] is not None else 5
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
    alk_solution = (form.get("alk_solution") or "").strip() or None
    ca_solution = (form.get("ca_solution") or "").strip() or None
    mg_solution = (form.get("mg_solution") or "").strip() or None
    alk_daily_ml = to_float(form.get("alk_daily_ml"))
    ca_daily_ml = to_float(form.get("ca_daily_ml"))
    mg_daily_ml = to_float(form.get("mg_daily_ml"))
    dosing_mode = (form.get("dosing_mode") or "").strip() or None
    all_in_one_solution = (form.get("all_in_one_solution") or "").strip() or None
    all_in_one_daily_ml = to_float(form.get("all_in_one_daily_ml"))
    nitrate_solution = (form.get("nitrate_solution") or "").strip() or None
    nitrate_daily_ml = to_float(form.get("nitrate_daily_ml"))
    phosphate_solution = (form.get("phosphate_solution") or "").strip() or None
    phosphate_daily_ml = to_float(form.get("phosphate_daily_ml"))
    nopox_daily_ml = to_float(form.get("nopox_daily_ml"))
    all_in_one_container_ml = to_float(form.get("all_in_one_container_ml"))
    all_in_one_remaining_ml = to_float(form.get("all_in_one_remaining_ml"))
    alk_container_ml = to_float(form.get("alk_container_ml"))
    alk_remaining_ml = to_float(form.get("alk_remaining_ml"))
    ca_container_ml = to_float(form.get("ca_container_ml"))
    ca_remaining_ml = to_float(form.get("ca_remaining_ml"))
    mg_container_ml = to_float(form.get("mg_container_ml"))
    mg_remaining_ml = to_float(form.get("mg_remaining_ml"))
    nitrate_container_ml = to_float(form.get("nitrate_container_ml"))
    nitrate_remaining_ml = to_float(form.get("nitrate_remaining_ml"))
    phosphate_container_ml = to_float(form.get("phosphate_container_ml"))
    phosphate_remaining_ml = to_float(form.get("phosphate_remaining_ml"))
    nopox_container_ml = to_float(form.get("nopox_container_ml"))
    nopox_remaining_ml = to_float(form.get("nopox_remaining_ml"))
    dosing_low_days = to_float(form.get("dosing_low_days"))
    if dosing_low_days is None:
        dosing_low_days = 5
    if all_in_one_container_ml is not None and all_in_one_remaining_ml is None:
        all_in_one_remaining_ml = all_in_one_container_ml
    if alk_container_ml is not None and alk_remaining_ml is None:
        alk_remaining_ml = alk_container_ml
    if ca_container_ml is not None and ca_remaining_ml is None:
        ca_remaining_ml = ca_container_ml
    if mg_container_ml is not None and mg_remaining_ml is None:
        mg_remaining_ml = mg_container_ml
    if nitrate_container_ml is not None and nitrate_remaining_ml is None:
        nitrate_remaining_ml = nitrate_container_ml
    if phosphate_container_ml is not None and phosphate_remaining_ml is None:
        phosphate_remaining_ml = phosphate_container_ml
    if nopox_container_ml is not None and nopox_remaining_ml is None:
        nopox_remaining_ml = nopox_container_ml
    container_updated_at = None
    if any(
        value is not None
        for value in (
            all_in_one_container_ml,
            all_in_one_remaining_ml,
            alk_container_ml,
            alk_remaining_ml,
            ca_container_ml,
            ca_remaining_ml,
            mg_container_ml,
            mg_remaining_ml,
            nitrate_container_ml,
            nitrate_remaining_ml,
            phosphate_container_ml,
            phosphate_remaining_ml,
            nopox_container_ml,
            nopox_remaining_ml,
        )
    ):
        container_updated_at = datetime.utcnow().isoformat()
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    ensure_column(db, "tank_profiles", "alk_solution", "ALTER TABLE tank_profiles ADD COLUMN alk_solution TEXT")
    ensure_column(db, "tank_profiles", "alk_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN alk_daily_ml REAL")
    ensure_column(db, "tank_profiles", "ca_solution", "ALTER TABLE tank_profiles ADD COLUMN ca_solution TEXT")
    ensure_column(db, "tank_profiles", "ca_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN ca_daily_ml REAL")
    ensure_column(db, "tank_profiles", "mg_solution", "ALTER TABLE tank_profiles ADD COLUMN mg_solution TEXT")
    ensure_column(db, "tank_profiles", "mg_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN mg_daily_ml REAL")
    ensure_column(db, "tank_profiles", "dosing_mode", "ALTER TABLE tank_profiles ADD COLUMN dosing_mode TEXT")
    ensure_column(db, "tank_profiles", "all_in_one_solution", "ALTER TABLE tank_profiles ADD COLUMN all_in_one_solution TEXT")
    ensure_column(db, "tank_profiles", "all_in_one_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN all_in_one_daily_ml REAL")
    ensure_column(db, "tank_profiles", "nitrate_solution", "ALTER TABLE tank_profiles ADD COLUMN nitrate_solution TEXT")
    ensure_column(db, "tank_profiles", "nitrate_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN nitrate_daily_ml REAL")
    ensure_column(db, "tank_profiles", "phosphate_solution", "ALTER TABLE tank_profiles ADD COLUMN phosphate_solution TEXT")
    ensure_column(db, "tank_profiles", "phosphate_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN phosphate_daily_ml REAL")
    ensure_column(db, "tank_profiles", "nopox_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN nopox_daily_ml REAL")
    ensure_column(db, "tank_profiles", "all_in_one_container_ml", "ALTER TABLE tank_profiles ADD COLUMN all_in_one_container_ml REAL")
    ensure_column(db, "tank_profiles", "all_in_one_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN all_in_one_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "alk_container_ml", "ALTER TABLE tank_profiles ADD COLUMN alk_container_ml REAL")
    ensure_column(db, "tank_profiles", "alk_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN alk_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "ca_container_ml", "ALTER TABLE tank_profiles ADD COLUMN ca_container_ml REAL")
    ensure_column(db, "tank_profiles", "ca_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN ca_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "mg_container_ml", "ALTER TABLE tank_profiles ADD COLUMN mg_container_ml REAL")
    ensure_column(db, "tank_profiles", "mg_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN mg_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "nitrate_container_ml", "ALTER TABLE tank_profiles ADD COLUMN nitrate_container_ml REAL")
    ensure_column(db, "tank_profiles", "nitrate_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN nitrate_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "phosphate_container_ml", "ALTER TABLE tank_profiles ADD COLUMN phosphate_container_ml REAL")
    ensure_column(db, "tank_profiles", "phosphate_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN phosphate_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "nopox_container_ml", "ALTER TABLE tank_profiles ADD COLUMN nopox_container_ml REAL")
    ensure_column(db, "tank_profiles", "nopox_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN nopox_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "dosing_container_updated_at", "ALTER TABLE tank_profiles ADD COLUMN dosing_container_updated_at TEXT")
    ensure_column(db, "tank_profiles", "dosing_low_days", "ALTER TABLE tank_profiles ADD COLUMN dosing_low_days REAL")
    db.execute("UPDATE tanks SET volume_l=? WHERE id=?", (volume_l, tank_id))
    db.execute(
        """INSERT INTO tank_profiles (tank_id, volume_l, net_percent, alk_solution, alk_daily_ml, ca_solution, ca_daily_ml, mg_solution, mg_daily_ml, dosing_mode, all_in_one_solution, all_in_one_daily_ml, nitrate_solution, nitrate_daily_ml, phosphate_solution, phosphate_daily_ml, nopox_daily_ml, all_in_one_container_ml, all_in_one_remaining_ml, alk_container_ml, alk_remaining_ml, ca_container_ml, ca_remaining_ml, mg_container_ml, mg_remaining_ml, nitrate_container_ml, nitrate_remaining_ml, phosphate_container_ml, phosphate_remaining_ml, nopox_container_ml, nopox_remaining_ml, dosing_container_updated_at, dosing_low_days)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(tank_id) DO UPDATE SET
             volume_l=excluded.volume_l,
             net_percent=excluded.net_percent,
             alk_solution=excluded.alk_solution,
             alk_daily_ml=excluded.alk_daily_ml,
             ca_solution=excluded.ca_solution,
             ca_daily_ml=excluded.ca_daily_ml,
             mg_solution=excluded.mg_solution,
             mg_daily_ml=excluded.mg_daily_ml,
             dosing_mode=excluded.dosing_mode,
             all_in_one_solution=excluded.all_in_one_solution,
             all_in_one_daily_ml=excluded.all_in_one_daily_ml,
             nitrate_solution=excluded.nitrate_solution,
             nitrate_daily_ml=excluded.nitrate_daily_ml,
             phosphate_solution=excluded.phosphate_solution,
             phosphate_daily_ml=excluded.phosphate_daily_ml,
             nopox_daily_ml=excluded.nopox_daily_ml,
             all_in_one_container_ml=excluded.all_in_one_container_ml,
             all_in_one_remaining_ml=excluded.all_in_one_remaining_ml,
             alk_container_ml=excluded.alk_container_ml,
             alk_remaining_ml=excluded.alk_remaining_ml,
             ca_container_ml=excluded.ca_container_ml,
             ca_remaining_ml=excluded.ca_remaining_ml,
             mg_container_ml=excluded.mg_container_ml,
             mg_remaining_ml=excluded.mg_remaining_ml,
             nitrate_container_ml=excluded.nitrate_container_ml,
             nitrate_remaining_ml=excluded.nitrate_remaining_ml,
             phosphate_container_ml=excluded.phosphate_container_ml,
             phosphate_remaining_ml=excluded.phosphate_remaining_ml,
             nopox_container_ml=excluded.nopox_container_ml,
             nopox_remaining_ml=excluded.nopox_remaining_ml,
             dosing_container_updated_at=excluded.dosing_container_updated_at,
             dosing_low_days=excluded.dosing_low_days""",
        (tank_id, volume_l, float(net_percent), alk_solution, alk_daily_ml, ca_solution, ca_daily_ml, mg_solution, mg_daily_ml, dosing_mode, all_in_one_solution, all_in_one_daily_ml, nitrate_solution, nitrate_daily_ml, phosphate_solution, phosphate_daily_ml, nopox_daily_ml, all_in_one_container_ml, all_in_one_remaining_ml, alk_container_ml, alk_remaining_ml, ca_container_ml, ca_remaining_ml, mg_container_ml, mg_remaining_ml, nitrate_container_ml, nitrate_remaining_ml, phosphate_container_ml, phosphate_remaining_ml, nopox_container_ml, nopox_remaining_ml, container_updated_at, dosing_low_days),
    )
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}")

@app.post("/tanks/{tank_id}/dosing-containers")
async def dosing_container_action(request: Request, tank_id: int):
    form = await request.form()
    action = (form.get("action") or "").strip()
    container_key = (form.get("container_key") or "").strip()
    if action != "refill" or not container_key:
        return redirect(f"/tanks/{tank_id}")
    mapping = {
        "all_in_one": ("all_in_one_container_ml", "all_in_one_remaining_ml"),
        "alk": ("alk_container_ml", "alk_remaining_ml"),
        "ca": ("ca_container_ml", "ca_remaining_ml"),
        "mg": ("mg_container_ml", "mg_remaining_ml"),
        "nitrate": ("nitrate_container_ml", "nitrate_remaining_ml"),
        "phosphate": ("phosphate_container_ml", "phosphate_remaining_ml"),
        "nopox": ("nopox_container_ml", "nopox_remaining_ml"),
    }
    if container_key not in mapping:
        return redirect(f"/tanks/{tank_id}")
    capacity_col, remaining_col = mapping[container_key]
    db = get_db()
    profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    if not profile:
        db.close()
        return redirect(f"/tanks/{tank_id}")
    capacity = row_get(profile, capacity_col)
    if capacity is not None:
        db.execute(
            f"UPDATE tank_profiles SET {remaining_col}=? WHERE tank_id=?",
            (capacity, tank_id),
        )
        db.execute(
            "DELETE FROM dosing_notifications WHERE tank_id=? AND container_key=?",
            (tank_id, container_key),
        )
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
        kit_id = to_float(form.get(f"kit_{pid}"))
        if val is None:
            remaining = to_float(form.get(f"remaining_{pid}"))
            conv_type, conv_data = get_test_kit_conversion(db, int(kit_id) if kit_id else None)
            if remaining is not None and conv_type == "syringe_remaining_ml" and conv_data:
                try:
                    table = json.loads(conv_data)
                except Exception:
                    table = []
                val = compute_conversion_value(float(remaining), table)
        if val is None:
            continue
        insert_sample_reading(db, sample_id, pname, float(val), punit, int(kit_id) if kit_id else None)
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
    kits = q(db, "SELECT * FROM test_kits WHERE active=1 ORDER BY parameter, name")
    kits_by_param = {}
    for k in kits:
        kits_by_param.setdefault(k["parameter"], []).append(k)
    kit_map = get_sample_kits(db, sample_id)
    taken_dt = parse_dt_any(sample["taken_at"])
    taken_local = taken_dt.strftime("%Y-%m-%dT%H:%M") if taken_dt else ""
    db.close()
    return templates.TemplateResponse("sample_edit.html", {"request": request, "tank": tank, "tank_id": tank_id, "sample_id": sample_id, "sample": sample, "taken_local": taken_local, "pdefs": pdefs, "readings": readings, "kits_by_param": kits_by_param, "kit_map": kit_map})

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
    if mode == "sample_values" and table_exists(db, "sample_values"):
        cur.execute("DELETE FROM sample_values WHERE sample_id=?", (sample_id,))
        if table_exists(db, "sample_value_kits"):
            cur.execute("DELETE FROM sample_value_kits WHERE sample_id=?", (sample_id,))
    else:
        if table_exists(db, "parameters"): cur.execute("DELETE FROM parameters WHERE sample_id=?", (sample_id,))
    pdefs = get_active_param_defs(db)
    for p in pdefs:
        pid = p["id"]
        pname = p["name"]
        punit = (p.get("unit") or "")
        val = to_float(form.get(f"value_{pid}"))
        kit_id = to_float(form.get(f"kit_{pid}"))
        if val is None:
            remaining = to_float(form.get(f"remaining_{pid}"))
            conv_type, conv_data = get_test_kit_conversion(db, int(kit_id) if kit_id else None)
            if remaining is not None and conv_type == "syringe_remaining_ml" and conv_data:
                try:
                    table = json.loads(conv_data)
                except Exception:
                    table = []
                val = compute_conversion_value(float(remaining), table)
        if val is None: continue
        insert_sample_reading(db, sample_id, pname, float(val), punit, int(kit_id) if kit_id else None)
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
    request: Request,
    param_id: Optional[str] = Form(None), 
    name: str = Form(...), 
    unit: Optional[str] = Form(None), 
    sort_order: Optional[str] = Form(None), 
    max_daily_change: Optional[str] = Form(None), 
    test_interval_days: Optional[str] = Form(None),
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
    interval = int(to_float(test_interval_days) or 0) if test_interval_days else None
    dt_low = to_float(default_target_low)
    dt_high = to_float(default_target_high)
    da_low = to_float(default_alert_low)
    da_high = to_float(default_alert_high)
    
    clean_name = name.strip()
    
    if not clean_name:
        db.close()
        return redirect("/settings/parameters")

    data = (clean_name, (unit or "").strip() or None, mdc, interval, order, is_active, dt_low, dt_high, da_low, da_high)

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
                        SET name=?, unit=?, max_daily_change=?, test_interval_days=?, sort_order=?, active=?, 
                            default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=?
                        WHERE id=?""", (*data, existing_id))
                    
                else:
                    # RENAME SCENARIO (No conflict):
                    for tbl, col in (("parameters", "name"), ("targets", "parameter"), ("additives", "parameter"), ("test_kits", "parameter")):
                        if table_exists(db, tbl): 
                            cur.execute(f"UPDATE {tbl} SET {col}=? WHERE {col}=?", (clean_name, old_name))
                            
                    cur.execute("""
                        UPDATE parameter_defs 
                        SET name=?, unit=?, max_daily_change=?, test_interval_days=?, sort_order=?, active=?, 
                            default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=?
                        WHERE id=?""", (*data, pid))
            else:
                # SIMPLE UPDATE
                cur.execute("""
                    UPDATE parameter_defs 
                    SET name=?, unit=?, max_daily_change=?, test_interval_days=?, sort_order=?, active=?, 
                        default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=?
                    WHERE id=?""", (*data, pid))
        
        # 3. Logic for Insert
        else:
            existing = one(db, "SELECT id FROM parameter_defs WHERE name=?", (clean_name,))
            if existing:
                cur.execute("""
                    UPDATE parameter_defs 
                    SET unit=?, max_daily_change=?, test_interval_days=?, sort_order=?, active=?, 
                        default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=?
                    WHERE id=?""", (data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], existing["id"]))
            else:
                cur.execute("""
                    INSERT INTO parameter_defs 
                    (name, unit, max_daily_change, test_interval_days, sort_order, active, default_target_low, default_target_high, default_alert_low, default_alert_high) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", data)

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
def parameter_delete(request: Request, param_id: int):
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
    if kit:
        kit = dict(kit)
        conversion_data = kit.get("conversion_data")
        if conversion_data:
            try:
                rows = json.loads(conversion_data)
            except Exception:
                rows = []
            kit["conversion_table"] = "\n".join(
                f"{r.get('remaining')},{r.get('value')}" for r in rows if "remaining" in r and "value" in r
            )
    return templates.TemplateResponse("test_kit_edit.html", {"request": request, "kit": kit, "parameters": parameters})

@app.post("/settings/test-kits/save")
def test_kit_save(request: Request, kit_id: Optional[str] = Form(None), parameter: Optional[str] = Form(None), parameter_id: Optional[str] = Form(None), name: str = Form(...), unit: Optional[str] = Form(None), resolution: Optional[str] = Form(None), min_value: Optional[str] = Form(None), max_value: Optional[str] = Form(None), notes: Optional[str] = Form(None), conversion_type: Optional[str] = Form(None), conversion_table: Optional[str] = Form(None), active: Optional[str] = Form(None)):
    db = get_db()
    cur = db.cursor()
    is_active = 1 if (active in ("1", "on", "true", "True")) else 0
    conv_type = (conversion_type or "").strip() or None
    conversion_rows = parse_conversion_table(conversion_table or "") if conv_type else []
    conversion_json = json.dumps(conversion_rows) if conversion_rows else None
    data = (
        parameter.strip(),
        name.strip(),
        (unit or "").strip() or None,
        to_float(resolution),
        to_float(min_value),
        to_float(max_value),
        (notes or "").strip() or None,
        conv_type,
        conversion_json,
        is_active,
    )
    if kit_id and str(kit_id).strip().isdigit():
        cur.execute(
            "UPDATE test_kits SET parameter=?, name=?, unit=?, resolution=?, min_value=?, max_value=?, notes=?, conversion_type=?, conversion_data=?, active=? WHERE id=?",
            (*data, int(kit_id)),
        )
    else:
        cur.execute(
            "INSERT INTO test_kits (parameter, name, unit, resolution, min_value, max_value, notes, conversion_type, conversion_data, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            data,
        )
    db.commit()
    db.close()
    return redirect("/settings/test-kits")

@app.post("/settings/test-kits/{kit_id}/delete")
def test_kit_delete(request: Request, kit_id: int):
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
    grouped = []
    groups: Dict[str, List[sqlite3.Row]] = {}
    for r in rows:
        key = (r["parameter"] or "Uncategorized").strip() or "Uncategorized"
        groups.setdefault(key, []).append(r)
    for key in sorted(groups.keys(), key=lambda s: s.lower()):
        grouped.append({"parameter": key, "items": groups[key]})
    return templates.TemplateResponse("additives.html", {"request": request, "additives": rows, "rows": rows, "grouped_additives": grouped})

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
def additive_save(request: Request, additive_id: Optional[str] = Form(None), name: str = Form(...), parameter: Optional[str] = Form(None), parameter_id: Optional[str] = Form(None), strength: str = Form(...), unit: str = Form(...), max_daily: Optional[str] = Form(None), notes: Optional[str] = Form(None), active: Optional[str] = Form(None)):
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
def additive_delete(request: Request, additive_id: int):
    db = get_db()
    db.execute("DELETE FROM additives WHERE id=?", (additive_id,))
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
    logged_at = (form.get("logged_at") or "").strip()
    additive_id = form.get("additive_id")
    amount_ml = to_float(form.get("amount_ml"))
    reason = (form.get("reason") or "").strip() or None
    if amount_ml is None: return redirect(f"/tanks/{tank_id}/dosing-log")
    db = get_db()
    cur = db.cursor()
    when_dt = None
    if logged_at:
        try:
            when_dt = datetime.fromisoformat(logged_at)
        except ValueError:
            when_dt = parse_dt_any(logged_at)
    when_iso = (when_dt or datetime.now()).isoformat()
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
def merge_parameters_run(request: Request):
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
    grouped_additives: Dict[str, List[sqlite3.Row]] = {}
    for a in additives_rows:
        key = (a["parameter"] or "Uncategorized").strip() or "Uncategorized"
        grouped_additives.setdefault(key, []).append(a)
    profiles = q(db, "SELECT * FROM tank_profiles")
    profile_by_tank = {p["tank_id"]: p for p in profiles}
    db.close()
    return templates.TemplateResponse("calculators.html", {"request": request, "tanks": tanks, "additives": additives_rows, "grouped_additives": grouped_additives, "profiles": profile_by_tank})

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
    grouped_additives: Dict[str, List[sqlite3.Row]] = {}
    for a in additives_rows:
        key = (a["parameter"] or "Uncategorized").strip() or "Uncategorized"
        grouped_additives.setdefault(key, []).append(a)
    profiles = q(db, "SELECT * FROM tank_profiles")
    profile_by_tank = {p["tank_id"]: p for p in profiles}
    db.close()
    return templates.TemplateResponse("calculators.html", {"request": request, "result": {"dose_ml": None if dose_ml is None else round(dose_ml, 2), "days": days, "daily_ml": None if daily_ml is None else round(daily_ml, 2), "daily_change": None if daily_change is None else round(daily_change, 4), "unit": unit, "error": error, "tank": tank, "additive": additive, "desired_change": desired_change}, "tanks": tanks, "additives": additives_rows, "grouped_additives": grouped_additives, "profiles": profile_by_tank, "selected": {"tank_id": tank_id, "additive_id": additive_id}})

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
        
        latest_map = get_latest_per_parameter(db, tank_id)
        latest = one(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT 1", (tank_id,))
        latest_taken = parse_dt_any(latest["taken_at"]) if latest else None

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
            
            p_data = latest_map.get(pname)
            cur_val = p_data.get("value") if p_data else None
            
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
                # Keep 'no additive' rows so user knows they need one
                tank_rows.append({"parameter": pname, "latest": cur_val_f, "target": target_val, "change": delta, "unit": unit, "days": days, "per_day_change": per_day_change, "max_daily_change": max_change_f, "additives": [], "note": "No additive linked."})
                continue
            
            # --- NEW LOGIC: Determine Preferred Additive ---
            # 1. Try to find the last additive used for this parameter in this tank
            last_used = one(db, """
                SELECT dl.additive_id 
                FROM dose_logs dl 
                JOIN additives a ON a.id = dl.additive_id 
                WHERE dl.tank_id=? AND a.parameter=? 
                ORDER BY dl.logged_at DESC LIMIT 1
            """, (tank_id, pname))
            
            preferred_id = last_used["additive_id"] if last_used else None
            
            add_rows = []
            has_selected = False
            
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
                        "day": i + 1,
                        "date": d,
                        "when": parse_dt_any(d),
                        "ml": per_day_ml,
                        "checked": check_map.get((tank_id, pname, a["id"], d), 0),
                        "tank_id": tank_id,
                        "additive_id": a["id"],
                        "parameter": pname,
                        "key": f"{tank_id}|{pname}|{a['id']}|{d}",
                    })
                
                # Check if this should be the selected one
                is_selected = False
                if preferred_id:
                    if a["id"] == preferred_id: is_selected = True
                
                add_rows.append({
                    "additive_id": a["id"], 
                    "additive_name": a["name"], 
                    "strength": a["strength"], 
                    "max_daily_change": max_change_f, 
                    "total_ml": total_ml, 
                    "per_day_ml": per_day_ml, 
                    "schedule": schedule,
                    "selected": is_selected
                })
                if is_selected: has_selected = True

            # If no history (or history additive deleted), default to the first one
            if not has_selected and add_rows:
                add_rows[0]["selected"] = True
                
            tank_rows.append({"parameter": pname, "latest": cur_val_f, "target": target_val, "change": delta, "unit": unit, "days": days, "per_day_change": per_day_change, "additives": add_rows, "note": ""})
            
        plan_total_ml = 0.0
        for _r in tank_rows:
            # Sum only the selected additives for the total
            for _a in (_r.get("additives") or []):
                if _a.get("selected"):
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
        planned_dt = parse_dt_any(planned_date)
        if planned_dt:
            now_time = datetime.now().time()
            logged_at = datetime.combine(planned_dt.date(), now_time).isoformat()
        else:
            logged_at = datetime.now().isoformat()
        reason = f"Dose plan: {parameter} ({planned_date})"
        date_like = f"{planned_date}%"
        if checked and amount_ml > 0:
            existing = one(db, "SELECT id FROM dose_logs WHERE tank_id=? AND additive_id=? AND logged_at LIKE ? AND reason=? LIMIT 1", (tank_id, additive_id, date_like, reason))
            if not existing:
                db.execute("INSERT INTO dose_logs (tank_id, additive_id, amount_ml, reason, logged_at) VALUES (?, ?, ?, ?, ?)", (tank_id, additive_id, amount_ml, reason, logged_at))
        elif not checked:
            db.execute("DELETE FROM dose_logs WHERE tank_id=? AND additive_id=? AND logged_at LIKE ? AND reason=?", (tank_id, additive_id, date_like, reason))
    except Exception: pass
    db.commit()
    db.close()
    return {"ok": True, "checked": checked}

# --- NEW: ADVANCED EXCEL IMPORT CENTER ---

@app.get("/admin/import", response_class=HTMLResponse)
def import_page(request: Request):
    return templates.TemplateResponse("import_manager.html", {"request": request})

@app.get("/admin/download-template")
def download_template(request: Request):
    db = get_db()
    # 1. Get your actual parameters for column headers
    params = list_parameters(db)
    # 2. Get your actual tanks to pre-fill the rows
    tanks = q(db, "SELECT name, volume_l FROM tanks ORDER BY name")
    db.close()
    
    # Define columns
    cols = ["Tank Name", "Volume (L)", "Date (YYYY-MM-DD)", "Notes"]
    for p in params:
        cols.append(p["name"])
    
    # Create the DataFrame
    df = pd.DataFrame(columns=cols)
    
    # 3. Pre-fill with your existing tanks
    today_str = date.today().isoformat()
    for i, t in enumerate(tanks):
        # We create a pre-filled row for every tank
        row_data = [t["name"], t["volume_l"], today_str, "Manual Import"]
        # Add empty cells for the parameter values
        row_data += [None] * (len(cols) - 4)
        df.loc[i] = row_data
    
    # If you have no tanks yet, add one example row so the file isn't empty
    if len(tanks) == 0:
        df.loc[0] = ["My First Tank", 450, today_str, "Example Entry"] + [None] * (len(cols) - 4)
    
    # Write to memory buffer
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Log Entries')
        
        # Optional: Add some basic formatting to make it look professional
        workbook  = writer.book
        worksheet = writer.sheets['Log Entries']
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
            worksheet.set_column(col_num, col_num, 18) # Make columns wider
    
    output.seek(0)
    
    headers = {
        'Content-Disposition': 'attachment; filename="Reef_Log_Template.xlsx"'
    }
    
    return StreamingResponse(
        output, 
        headers=headers,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@app.get("/admin/export-all")
def export_all(request: Request):
    db = get_db()
    tanks = q(db, "SELECT * FROM tanks ORDER BY name")
    params = list_parameters(db)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for t in tanks:
            samples = q(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at ASC", (t["id"],))
            rows = []
            for s in samples:
                row = {
                    "Tank": t["name"],
                    "Taken At": s["taken_at"],
                    "Notes": s["notes"],
                }
                readings = {r["name"]: r["value"] for r in get_sample_readings(db, s["id"])}
                for p in params:
                    row[p["name"]] = readings.get(p["name"])
                rows.append(row)
            df = pd.DataFrame(rows)
            sheet_name = (t["name"] or f"Tank {t['id']}")[:31]
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    db.close()
    output.seek(0)
    return StreamingResponse(
        output,
        headers={"Content-Disposition": 'attachment; filename="reef_export_all.xlsx"'},
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

@app.get("/api/tanks")
def api_tanks():
    db = get_db()
    tanks = q(db, "SELECT * FROM tanks ORDER BY name")
    data = []
    for t in tanks:
        latest_map = get_latest_per_parameter(db, t["id"])
        readings = []
        for name, info in latest_map.items():
            taken_at = info.get("taken_at")
            if isinstance(taken_at, datetime):
                taken_at = taken_at.isoformat()
            readings.append({
                "parameter": name,
                "value": info.get("value"),
                "taken_at": taken_at,
            })
        data.append({
            "id": t["id"],
            "name": t["name"],
            "volume_l": t["volume_l"],
            "latest_readings": readings,
        })
    db.close()
    return {"tanks": data}

@app.get("/api/tanks/{tank_id}/samples")
def api_samples(tank_id: int, limit: int = 50):
    db = get_db()
    tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    samples = q(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT ?", (tank_id, limit))
    data = []
    for s in samples:
        readings = {r["name"]: r["value"] for r in get_sample_readings(db, s["id"])}
        data.append({
            "id": s["id"],
            "taken_at": s["taken_at"],
            "notes": s["notes"],
            "readings": readings,
        })
    db.close()
    return {"tank": {"id": tank["id"], "name": tank["name"]}, "samples": data}
    
@app.post("/admin/upload-excel")
async def upload_excel(request: Request, file: UploadFile = File(...)):
    if not file.filename.endswith(('.xlsx', '.xls')):
        return templates.TemplateResponse("import_manager.html", {"request": request, "error": "Invalid format. Please upload an Excel file."})
    
    db = get_db()
    stats = {"tanks": 0, "samples": 0}
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))
        cur = db.cursor()
        pdefs = list_parameters(db)

        for _, row in df.iterrows():
            t_name = str(row.get("Tank Name", "Unknown")).strip()
            if not t_name or t_name == "nan": continue

            # Resolve Tank
            tank = one(db, "SELECT id FROM tanks WHERE name=?", (t_name,))
            if not tank:
                vol = to_float(row.get("Volume (L)", 0))
                cur.execute("INSERT INTO tanks (name, volume_l) VALUES (?,?)", (t_name, vol))
                tid = cur.lastrowid
                cur.execute("INSERT INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?,?,100)", (tid, vol))
                stats["tanks"] += 1
            else:
                tid = tank["id"]
            
            # Resolve Date
            dt_raw = row.get("Date (YYYY-MM-DD)")
            dt_str = str(dt_raw.date()) if hasattr(dt_raw, 'date') else str(dt_raw)
            notes = str(row.get("Notes", "")) if row.get("Notes") and str(row.get("Notes")) != "nan" else ""
            
            cur.execute("INSERT INTO samples (tank_id, taken_at, notes) VALUES (?,?,?)", (tid, dt_str, notes))
            sid = cur.lastrowid
            stats["samples"] += 1
            
            # Resolve readings for all parameters
            for p in pdefs:
                val = to_float(row.get(p["name"]))
                if val is not None:
                    insert_sample_reading(db, sid, p["name"], val)
        
        db.commit()
        return templates.TemplateResponse("import_manager.html", {"request": request, "success": f"Imported {stats['samples']} samples across {stats['tanks']} new tanks."})
    except Exception as e:
        return templates.TemplateResponse("import_manager.html", {"request": request, "error": str(e)})
    finally:
        db.close()

# --- Legacy Import (kept for compatibility) ---
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
        return templates.TemplateResponse("simple_message.html", {"request": request, "title": "Import failed", "message": "Could not find 'Tank Parameters Sheet.xlsx'.", "actions": [{"label": "Back", "method": "get", "href": "/"}]})
    try:
        from import_from_excel import import_from_tank_parameters_sheet
        stats = import_from_tank_parameters_sheet(db, xlsx_path)
    except Exception as e:
        db.close()
        return templates.TemplateResponse("simple_message.html", {"request": request, "title": "Import failed", "message": f"Error: {str(e)}", "actions": [{"label": "Back", "method": "get", "href": "/admin/import-excel"}]})
    finally:
        db.close()
    return templates.TemplateResponse("simple_message.html", {"request": request, "title": "Import complete", "message": "Excel imported successfully.", "actions": [{"label": "Go to dashboard", "method": "get", "href": "/"}]})
