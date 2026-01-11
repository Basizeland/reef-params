import os
import fcntl
import psycopg
from sqlalchemy import text, inspect
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.exc import OperationalError, IntegrityError, SQLAlchemyError
import re
import math
import json
import time as time_module
import shutil
import secrets
import hashlib
import base64
import urllib.parse
import urllib.request
import csv
import gzip
import html
import importlib
import importlib.util
import smtplib
import threading
import logging
import uuid
import atexit
from contextlib import contextmanager
from functools import lru_cache
from email.message import EmailMessage
from io import BytesIO
from datetime import datetime, date, time as datetime_time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from integrations.apex import ApexClient, readings_from_payload
from database import engine, DB_PATH
import models  # noqa: F401
from html.parser import HTMLParser

from fastapi import FastAPI, Form, Request, HTTPException, File, UploadFile, Query, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import StreamingResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from cachetools import TTLCache, cached
from apscheduler.schedulers.background import BackgroundScheduler
from pythonjsonlogger import jsonlogger
from pydantic import BaseModel, Field, validator

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://reefmetrics.app")
PASSWORD_HASH_ITERATIONS = int(os.environ.get("PASSWORD_HASH_ITERATIONS", "200000"))
SESSION_LIFETIME_DAYS = int(os.environ.get("SESSION_LIFETIME_DAYS", "30"))
MIN_PASSWORD_LENGTH = int(os.environ.get("MIN_PASSWORD_LENGTH", "12"))
RUN_BACKGROUND_JOBS = os.environ.get("RUN_BACKGROUND_JOBS", "true").lower() in {"1", "true", "yes", "on"}
BACKGROUND_JOB_LOCK_PATH = os.environ.get("BACKGROUND_JOB_LOCK_PATH", "").strip() or None
SESSION_ROTATE_ON_LOGIN = os.environ.get("SESSION_ROTATE_ON_LOGIN", "true").lower() in {"1", "true", "yes", "on"}
ALLOW_RUNTIME_SCHEMA_CHANGES = os.environ.get("ALLOW_RUNTIME_SCHEMA_CHANGES", "false").lower() in {"1", "true", "yes", "on"}
CSRF_EXEMPT_PATHS = tuple(
    path.strip() for path in os.environ.get("CSRF_EXEMPT_PATHS", "").split(",") if path.strip()
)
R2_ENDPOINT = os.environ.get("R2_ENDPOINT", "")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET", "")
R2_REGION = os.environ.get("R2_REGION", "auto")
R2_BACKUP_RETENTION_DAYS = os.environ.get("R2_BACKUP_RETENTION_DAYS", "")
R2_ICP_RETENTION_DAYS = os.environ.get("R2_ICP_RETENTION_DAYS", "")

# Initialize FastAPI with OpenAPI documentation
app = FastAPI(
    title="Reef Metrics API",
    description="Water parameter tracking and dosing management for reef aquariums",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Setup structured JSON logging
logger = logging.getLogger("reef")
log_handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Setup rate limiter
limiter = Limiter(key_func=get_remote_address, default_limits=["200/minute"])
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Setup caching
param_cache = TTLCache(maxsize=100, ttl=300)  # 5 min cache for parameter definitions
profile_cache = TTLCache(maxsize=1000, ttl=60)  # 1 min cache for tank profiles
tank_cache = TTLCache(maxsize=500, ttl=120)  # 2 min cache for tanks

# Setup background scheduler
scheduler = BackgroundScheduler()
scheduler.start()
atexit.register(lambda: scheduler.shutdown())

sentry_dsn = os.environ.get("SENTRY_DSN")
if sentry_dsn and importlib.util.find_spec("sentry_sdk"):
    import sentry_sdk
    from sentry_sdk.integrations.fastapi import FastApiIntegration

    sentry_sdk.init(
        dsn=sentry_dsn,
        integrations=[FastApiIntegration()],
        send_default_pii=os.environ.get("SENTRY_SEND_DEFAULT_PII", "false").lower() in {"1", "true", "yes", "on"},
        enable_logs=os.environ.get("SENTRY_ENABLE_LOGS", "true").lower() in {"1", "true", "yes", "on"},
        traces_sample_rate=float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.1")),
        profile_session_sample_rate=float(os.environ.get("SENTRY_PROFILE_SESSION_SAMPLE_RATE", "0.0")),
        profile_lifecycle=os.environ.get("SENTRY_PROFILE_LIFECYCLE"),
    )

@lru_cache(maxsize=None)
def optional_module(module_name: str):
    if importlib.util.find_spec(module_name) is None:
        return None
    return importlib.import_module(module_name)

def get_pandas():
    return optional_module("pandas")

def get_webpush():
    return optional_module("pywebpush")

def is_csrf_exempt(path: str) -> bool:
    if not CSRF_EXEMPT_PATHS:
        return False
    for entry in CSRF_EXEMPT_PATHS:
        if entry.endswith("*"):
            if path.startswith(entry[:-1]):
                return True
        elif path == entry:
            return True
    return False

@contextmanager
def get_db_connection():
    """Context manager for database connections - ensures cleanup even on exceptions"""
    db = get_db()
    try:
        yield db
    finally:
        db.close()

def validate_password(password: str) -> Tuple[bool, str]:
    """Validate password meets security requirements"""
    if len(password) < MIN_PASSWORD_LENGTH:
        return False, f"Password must be at least {MIN_PASSWORD_LENGTH} characters"
    if not re.search(r'[A-Z]', password):
        return False, "Password must contain at least one uppercase letter"
    if not re.search(r'[a-z]', password):
        return False, "Password must contain at least one lowercase letter"
    if not re.search(r'[0-9]', password):
        return False, "Password must contain at least one number"
    return True, ""

async def get_authorized_tank(request: Request, tank_id: int) -> Dict[str, Any]:
    """FastAPI dependency to verify tank access and return tank or raise 404"""
    db = get_db()
    try:
        user = get_current_user(db, request)
        if not user:
            raise HTTPException(status_code=401, detail="Not authenticated")
        tank = get_tank_for_user(db, user, tank_id)
        if not tank:
            raise HTTPException(status_code=404, detail="Tank not found or access denied")
        return tank
    finally:
        db.close()

def get_cached_parameter_defs(db: Connection) -> List[Dict[str, Any]]:
    """Get active parameter definitions with caching"""
    cache_key = "active_params"
    if cache_key in param_cache:
        return param_cache[cache_key]
    params = q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY sort_order, name")
    param_cache[cache_key] = params
    return params

def get_cached_tank_profile(db: Connection, tank_id: int) -> Optional[Dict[str, Any]]:
    """Get tank profile with caching"""
    cache_key = f"tank_profile_{tank_id}"
    if cache_key in profile_cache:
        return profile_cache[cache_key]
    profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    if profile:
        profile_cache[cache_key] = profile
    return profile

def invalidate_tank_profile_cache(tank_id: int) -> None:
    """Invalidate tank profile cache for a specific tank"""
    cache_key = f"tank_profile_{tank_id}"
    profile_cache.pop(cache_key, None)

def require_pandas():
    pandas = get_pandas()
    if pandas is None:
        raise HTTPException(
            status_code=500,
            detail="Excel import/export requires pandas. Install pandas and restart the app.",
        )
    return pandas

def normalize_param_name(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "", name.lower())
    if cleaned in {"alkalinitykh", "alkalinity", "kh"}:
        return "Alkalinity/KH"
    if cleaned in {"calcium", "ca"}:
        return "Calcium"
    if cleaned in {"magnesium", "mg"}:
        return "Magnesium"
    if cleaned in {"nitrate", "no3"}:
        return "Nitrate"
    if cleaned in {"phosphate", "po4"}:
        return "Phosphate"
    if cleaned in {"traceelements", "trace"}:
        return "Trace Elements"
    return name.strip() or name

def additive_label(additive: Any) -> str:
    if not additive:
        return ""
    if isinstance(additive, str):
        return additive
    name = row_get(additive, "name") or ""
    brand = row_get(additive, "brand") or ""
    if brand and name.lower().startswith(brand.lower()):
        return name
    label = f"{brand} {name}".strip()
    return label or name

def build_daily_consumption(db: Connection, tank_view: Dict[str, Any], user: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    daily_consumption: Dict[str, Dict[str, Any]] = {}
    volume_l = row_get(tank_view, "volume_l")
    if not volume_l:
        return daily_consumption

    def add_consumption(param_name: str, value: float, unit: str | None = None, source_label: str | None = None) -> None:
        if not param_name:
            return
        canonical_param = normalize_param_name(param_name)
        entry = daily_consumption.setdefault(canonical_param, {"value": 0.0, "unit": unit or "", "sources": []})
        entry["value"] += float(value)
        if not entry.get("unit") and unit:
            entry["unit"] = unit
        if source_label:
            entry["sources"].append({"label": source_label, "value": float(value), "unit": unit or entry.get("unit") or ""})

    def is_enabled(flag: Any, has_data: bool) -> bool:
        if flag is None:
            return has_data
        return bool(flag)

    where_sql, params = build_additives_where(db, user, active_only=True)
    additives = q(db, f"SELECT name, parameter, strength, unit FROM additives{where_sql}", params)
    additive_by_name = {str(a["name"]).strip().lower(): a for a in additives}
    dosing_entries = [
        ("all_in_one", tank_view.get("all_in_one_solution"), tank_view.get("all_in_one_daily_ml"), tank_view.get("use_all_in_one")),
        ("alk", tank_view.get("alk_solution"), tank_view.get("alk_daily_ml"), tank_view.get("use_alk")),
        ("kalkwasser", tank_view.get("kalk_solution"), tank_view.get("kalk_daily_ml"), tank_view.get("use_kalkwasser")),
        ("ca", tank_view.get("ca_solution"), tank_view.get("ca_daily_ml"), tank_view.get("use_ca")),
        ("mg", tank_view.get("mg_solution"), tank_view.get("mg_daily_ml"), tank_view.get("use_mg")),
        ("nitrate", tank_view.get("nitrate_solution"), tank_view.get("nitrate_daily_ml"), tank_view.get("use_nitrate")),
        ("phosphate", tank_view.get("phosphate_solution"), tank_view.get("phosphate_daily_ml"), tank_view.get("use_phosphate")),
        ("trace", tank_view.get("trace_solution"), tank_view.get("trace_daily_ml"), tank_view.get("use_trace")),
    ]
    tank_id = row_get(tank_view, "tank_id") or row_get(tank_view, "id")
    extra_entries = q(
        db,
        "SELECT solution, daily_ml FROM dosing_entries WHERE tank_id=? AND active=1",
        (tank_id,),
    )
    for entry in extra_entries:
        dosing_entries.append(
            ("extra", row_get(entry, "solution"), row_get(entry, "daily_ml"), True)
        )
    for key, solution_name, daily_ml, enabled in dosing_entries:
        has_data = solution_name and daily_ml not in (None, 0)
        if not is_enabled(enabled, bool(has_data)):
            continue
        if not solution_name or daily_ml in (None, 0):
            continue
        additive = additive_by_name.get(str(solution_name).strip().lower())
        if not additive:
            continue
        strength = row_get(additive, "strength")
        if strength in (None, 0):
            continue
        daily_change = (float(daily_ml) * float(strength)) / (1000.0 * float(volume_l))
        additive_name = additive_label(additive) or solution_name
        additive_param = row_get(additive, "parameter") or ""
        additive_unit = row_get(additive, "unit") or ""
        add_consumption(additive_param, daily_change, additive_unit, additive_name)
        name_key = str(additive_name or "").strip().lower()
        if "all for reef" in name_key:
            ca_per_dkh = 7.14
            mg_per_dkh = 1.25
            add_consumption("Calcium", daily_change * ca_per_dkh, "ppm", f"{additive_name} (Ca est.)")
            add_consumption("Magnesium", daily_change * mg_per_dkh, "ppm", f"{additive_name} (Mg est.)")
        if "kalkwasser" in name_key or key == "kalkwasser":
            ca_per_dkh = 7.14
            add_consumption("Calcium", daily_change * ca_per_dkh, "ppm", f"{additive_name} (Ca est.)")

    reactor_daily_ml = row_get(tank_view, "calcium_reactor_daily_ml")
    reactor_effluent_dkh = row_get(tank_view, "calcium_reactor_effluent_dkh")
    reactor_enabled = is_enabled(
        row_get(tank_view, "use_calcium_reactor"),
        reactor_daily_ml not in (None, 0) and reactor_effluent_dkh not in (None, 0),
    )
    if reactor_enabled and reactor_daily_ml not in (None, 0) and reactor_effluent_dkh not in (None, 0):
        daily_change = float(reactor_effluent_dkh) * (float(reactor_daily_ml) / (float(volume_l) * 1000.0))
        add_consumption("Alkalinity/KH", daily_change, "dKH", "Calcium Reactor")

    return daily_consumption

def get_email_sender(kind: str | None = None) -> str:
    sender = os.environ.get("SMTP_FROM") or os.environ.get("SMTP_USERNAME") or ""
    if "@" in sender:
        return sender
    transactional = os.environ.get("EMAIL_FROM_TRANSACTIONAL") or "support@reefmetrics.app"
    alerts = os.environ.get("EMAIL_FROM_ALERTS") or "alerts@reefmetrics.app"
    billing = os.environ.get("EMAIL_FROM_BILLING") or "billing@reefmetrics.app"
    marketing = os.environ.get("EMAIL_FROM_MARKETING") or "hello@reefmetrics.app"
    if kind == "alerts":
        return alerts
    if kind == "billing":
        return billing
    if kind == "marketing":
        return marketing
    if kind == "transactional":
        return transactional
    return transactional

def send_email(recipient: str, subject: str, text_body: str, html_body: str | None = None, sender: str | None = None, sender_kind: str | None = None) -> Tuple[bool, str]:
    sender = sender or get_email_sender(sender_kind)
    if not recipient:
        msg = "missing recipient"
        logger.warning(f"Email skipped: {msg}")
        return False, msg
    mailjet_key = os.environ.get("MAILJET_API_KEY") or ""
    mailjet_secret = os.environ.get("MAILJET_API_SECRET") or ""
    if mailjet_key and mailjet_secret:
        payload = {
            "Messages": [
                {
                    "From": {"Email": sender},
                    "To": [{"Email": recipient}],
                    "Subject": subject,
                    "TextPart": text_body,
                    "HTMLPart": html_body or text_body,
                }
            ]
        }
        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            "https://api.mailjet.com/v3.1/send",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        auth = base64.b64encode(f"{mailjet_key}:{mailjet_secret}".encode("utf-8")).decode("utf-8")
        request.add_header("Authorization", f"Basic {auth}")
        timeout = float(os.environ.get("SMTP_TIMEOUT", "10"))
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                if response.status >= 400:
                    return False, f"Mailjet HTTP {response.status}"
            return True, ""
        except Exception as exc:
            msg = str(exc)
            logger.error(f"Email failed: {msg}")
            return False, msg
    host = os.environ.get("SMTP_HOST")
    if not host:
        msg = "missing SMTP_HOST"
        logger.warning(f"Email skipped: {msg}")
        return False, msg
    port = int(os.environ.get("SMTP_PORT", "587"))
    timeout = float(os.environ.get("SMTP_TIMEOUT", "10"))
    use_tls = os.environ.get("SMTP_USE_TLS", "true").lower() in {"1", "true", "yes"}
    use_ssl = os.environ.get("SMTP_USE_SSL", "false").lower() in {"1", "true", "yes"}
    smtp_username = os.environ.get("SMTP_USERNAME")
    smtp_password = os.environ.get("SMTP_PASSWORD")

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = recipient
    message.set_content(text_body)
    if html_body:
        message.add_alternative(html_body, subtype="html")
    try:
        if use_ssl:
            server = smtplib.SMTP_SSL(host, port, timeout=timeout)
        else:
            server = smtplib.SMTP(host, port, timeout=timeout)
        with server:
            if use_tls and not use_ssl:
                server.starttls()
            if smtp_username and smtp_password:
                server.login(smtp_username, smtp_password)
            server.send_message(message)
        return True, ""
    except Exception as exc:
        msg = str(exc)
        print(f"Email failed: {msg}")
        return False, msg

def send_welcome_email(recipient: str, username: str) -> Tuple[bool, str]:
    app_name = os.environ.get("APP_NAME", "Reef Metrics")
    base_url = PUBLIC_BASE_URL or "https://reefmetrics.app"
    subject = f"Welcome to {app_name}"
    text_body = (
        f"Hi {username or recipient},\n\n"
        f"Welcome to {app_name}! Your account is ready.\n\n"
        f"Get started here: {base_url}\n\n"
        "Thanks for joining!"
    )
    html_body = f"""
<!DOCTYPE html>
<html>
  <body style="margin:0; padding:0; background:#f8fafc; font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
    <table role="presentation" cellpadding="0" cellspacing="0" width="100%">
      <tr>
        <td align="center" style="padding:32px 12px;">
          <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="max-width:560px; background:#ffffff; border-radius:12px; border:1px solid #e5e7eb;">
            <tr>
              <td style="padding:28px 28px 8px;">
                <div style="font-size:18px; letter-spacing:0.08em; text-transform:uppercase; color:#0ea5e9; font-weight:700;">{app_name}</div>
              </td>
            </tr>
            <tr>
              <td style="padding:8px 28px 16px;">
                <h1 style="margin:0 0 12px; font-size:24px; color:#0f172a;">Welcome to {app_name}</h1>
                <p style="margin:0 0 16px; font-size:15px; color:#334155;">
                  Hi {username or recipient}, your account is ready. You can start tracking your reef metrics right away.
                </p>
                <a href="{base_url}" style="display:inline-block; padding:10px 18px; background:#0ea5e9; color:#ffffff; text-decoration:none; border-radius:8px; font-size:14px;">
                  Open {app_name}
                </a>
              </td>
            </tr>
            <tr>
              <td style="padding:0 28px 24px; font-size:12px; color:#94a3b8;">
                If you did not create this account, you can ignore this email.
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
"""
    return send_email(recipient, subject, text_body, html_body, sender_kind="transactional")

# --- 1. INITIAL SEED DEFAULTS (Used for DB Migration only) ---
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

# Pydantic Models for Request Validation
class LoginRequest(BaseModel):
    email: str = Field(min_length=3)
    password: str = Field(min_length=1)
    rotate_sessions: bool = False

    @validator('email')
    def validate_email(cls, v):
        v = v.strip().lower()
        if '@' not in v or '.' not in v.split('@')[-1]:
            raise ValueError('Invalid email format')
        return v

class RegisterRequest(BaseModel):
    email: str = Field(min_length=3)
    username: Optional[str] = None
    password: str = Field(min_length=MIN_PASSWORD_LENGTH)
    confirm_password: str

    @validator('email')
    def validate_email(cls, v):
        v = v.strip().lower()
        if '@' not in v or '.' not in v.split('@')[-1]:
            raise ValueError('Invalid email format')
        return v

    @validator('confirm_password')
    def passwords_match(cls, v, values):
        if 'password' in values and v != values['password']:
            raise ValueError('Passwords do not match')
        return v

class PasswordChangeRequest(BaseModel):
    current_password: str = Field(min_length=1)
    new_password: str = Field(min_length=MIN_PASSWORD_LENGTH)
    confirm_password: str

    @validator('confirm_password')
    def passwords_match(cls, v, values):
        if 'new_password' in values and v != values['new_password']:
            raise ValueError('Passwords do not match')
        return v

static_dir = os.path.join(BASE_DIR, "static")
templates_dir = os.path.join(BASE_DIR, "templates")
os.makedirs(static_dir, exist_ok=True)
os.makedirs(templates_dir, exist_ok=True)

app.mount("/static", StaticFiles(directory=static_dir), name="static")
templates = Jinja2Templates(directory=templates_dir)

# Serve service worker from root for proper scope
@app.get("/service-worker.js")
async def service_worker():
    from fastapi.responses import FileResponse
    sw_path = os.path.join(static_dir, "service-worker.js")
    return FileResponse(sw_path, media_type="application/javascript")

# --- Jinja Helpers ---
def fmt2(v: Any) -> str:
    if v is None: return ""
    try:
        if isinstance(v, bool): return "1" if v else "0"
        if isinstance(v, int): return str(v)
        fv = float(v)
    except (ValueError, TypeError):
        return str(v)
    
    # Updated: 2 decimal places standard
    s = f"{fv:.2f}" 
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s

def _tzinfo():
    tz_name = os.environ.get("APP_TIMEZONE") or os.environ.get("TZ") or "UTC"
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(tz_name)
    except Exception:
        return None

def _to_local(dt: datetime) -> datetime:
    tz = _tzinfo()
    if tz is None:
        return dt
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)

def oauth_cookie_settings(request: Request) -> Dict[str, Any]:
    base = PUBLIC_BASE_URL or str(request.base_url).rstrip("/")
    parsed = urllib.parse.urlparse(base)
    hostname = parsed.hostname or ""
    if hostname.startswith("www."):
        hostname = hostname[4:]
    domain = hostname or None
    secure = parsed.scheme == "https"
    return {"domain": domain, "secure": secure}

def get_google_redirect_uri(request: Request) -> str:
    mobile_redirect = os.environ.get("GOOGLE_REDIRECT_URI_MOBILE")
    base = PUBLIC_BASE_URL or str(request.base_url).rstrip("/")
    default_redirect = os.environ.get("GOOGLE_REDIRECT_URI") or f"{base.rstrip('/')}/auth/google/callback"
    platform = (request.query_params.get("platform") or "").strip().lower()
    if platform == "mobile" and mobile_redirect:
        return mobile_redirect
    return default_redirect

def get_vapid_settings() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    public_key = os.environ.get("VAPID_PUBLIC_KEY")
    private_key = os.environ.get("VAPID_PRIVATE_KEY")
    subject = os.environ.get("VAPID_SUBJECT", "mailto:admin@reefmetrics.app")
    if not public_key or not private_key:
        return None, None, None
    return public_key, private_key, subject

def send_web_push(db: Connection, user_ids: List[int], payload: Dict[str, Any]) -> None:
    webpush_module = get_webpush()
    if webpush_module is None:
        return
    public_key, private_key, subject = get_vapid_settings()
    if not public_key or not private_key or not subject:
        return
    if not user_ids:
        return
    rows = q(
        db,
        "SELECT id, endpoint, subscription_json FROM push_subscriptions WHERE user_id IN ({})".format(
            ",".join("?" for _ in user_ids)
        ),
        tuple(user_ids),
    )
    if not rows:
        return
    for row in rows:
        subscription_info = None
        try:
            subscription_info = json.loads(row["subscription_json"])
        except (json.JSONDecodeError, TypeError, KeyError):
            subscription_info = None
        if not subscription_info:
            db.execute("DELETE FROM push_subscriptions WHERE id=?", (row["id"],))
            continue
        try:
            webpush_module.webpush(
                subscription_info=subscription_info,
                data=json.dumps(payload),
                vapid_private_key=private_key,
                vapid_claims={"sub": subject},
            )
        except webpush_module.WebPushException:
            db.execute("DELETE FROM push_subscriptions WHERE id=?", (row["id"],))
    db.commit()

def get_tank_recipient_ids(db: Connection, tank_id: int) -> List[int]:
    rows = q(
        db,
        """SELECT DISTINCT u.id
           FROM tanks t
           LEFT JOIN user_tanks ut ON ut.tank_id = t.id
           LEFT JOIN users u ON u.id = ut.user_id
           WHERE t.id=?""",
        (tank_id,),
    )
    owner = one(db, "SELECT owner_user_id FROM tanks WHERE id=?", (tank_id,))
    recipient_ids = {row["id"] for row in rows if row.get("id") is not None}
    if owner and owner.get("owner_user_id"):
        recipient_ids.add(owner["owner_user_id"])
    return sorted(recipient_ids)

def dtfmt(v: Any) -> str:
    dt = parse_dt_any(v) if not isinstance(v, datetime) else v
    if dt is None: return ""
    dt = _to_local(dt)
    day = dt.day
    if 10 <= day % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{dt.strftime('%a')} {day}{suffix} {dt.strftime('%b %y')}"

def dtfmt_time(v: Any) -> str:
    dt = parse_dt_any(v) if not isinstance(v, datetime) else v
    if dt is None:
        return ""
    dt = _to_local(dt)
    return dt.strftime("%H:%M:%S")

def time_ago(v: Any) -> str:
    """Returns a string like '2 days ago' or 'Today'."""
    dt = parse_dt_any(v)
    if not dt: return ""
    now = _to_local(datetime.utcnow())
    diff = now - _to_local(dt)
    
    if diff.total_seconds() < 0:
        return "Today"
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

def audit_details(details: Any) -> List[Dict[str, Any]]:
    if details is None:
        return []
    text = str(details).strip()
    if not text:
        return []
    parsed_items: List[Dict[str, Any]] = []
    if (text.startswith("{") and text.endswith("}")) or (text.startswith("[") and text.endswith("]")):
        try:
            payload = json.loads(text)
        except (json.JSONDecodeError, TypeError):
            payload = None
        if isinstance(payload, dict):
            for key, value in payload.items():
                parsed_items.append({"label": key.replace("_", " ").title(), "value": value})
            return parsed_items
        if isinstance(payload, list):
            for entry in payload:
                parsed_items.append({"label": "Item", "value": entry})
            return parsed_items
    for part in re.split(r"[;,]\s*", text):
        if not part:
            continue
        if "=" in part:
            key, value = part.split("=", 1)
            key = key.strip()
            value = value.strip()
            parsed_items.append({"label": key.replace("_", " ").title(), "value": value})
        else:
            parsed_items.append({"label": "Detail", "value": part})
    return parsed_items

templates.env.filters["fmt2"] = fmt2
templates.env.filters["dtfmt"] = dtfmt
templates.env.filters["dtfmt_time"] = dtfmt_time
templates.env.filters["time_ago"] = time_ago
templates.env.filters["tojson"] = tojson_filter
templates.env.filters["additive_label"] = additive_label
templates.env.filters["audit_details"] = audit_details

def _finalize(v: Any) -> Any:
    if isinstance(v, bool) or v is None: return v
    if isinstance(v, int): return v
    if isinstance(v, float): return fmt2(v)
    return v

templates.env.finalize = _finalize

def collect_dosing_notifications(
    db: Connection,
    tank_id: int | None = None,
    owner_user_id: int | None = None,
    actor_user: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    today = date.today().isoformat()
    params: List[Any] = []
    where_clause = ""
    if tank_id is not None:
        where_clause = "WHERE p.tank_id=?"
        params.append(tank_id)
    elif owner_user_id is not None:
        where_clause = "WHERE t.owner_user_id=? OR ut.user_id=?"
        params.extend([owner_user_id, owner_user_id])
    rows = q(
        db,
        f"""SELECT DISTINCT t.id AS tank_id, t.name AS tank_name, p.*
            FROM tank_profiles p
            JOIN tanks t ON t.id = p.tank_id
            LEFT JOIN user_tanks ut ON ut.tank_id = t.id
            {where_clause}""",
        tuple(params),
    )
    extra_entries = q(
        db,
        "SELECT id, tank_id, solution, daily_ml, container_ml, remaining_ml FROM dosing_entries WHERE active=1",
    )
    extra_by_tank: Dict[int, List[Dict[str, Any]]] = {}
    for entry in extra_entries:
        extra_by_tank.setdefault(int(row_get(entry, "tank_id")), []).append(entry)
    dismissed_rows = q(
        db,
        "SELECT tank_id, container_key, notified_on FROM dosing_notifications WHERE dismissed_at IS NOT NULL",
    )
    dismissed_keys = {(row["tank_id"], row["container_key"], row["notified_on"]) for row in dismissed_rows}
    notifications: List[Dict[str, Any]] = []
    dosing_containers = [
        ("all_in_one", "all_in_one_container_ml", "all_in_one_remaining_ml", "all_in_one_daily_ml", "all_in_one_solution", "All-in-one"),
        ("alk", "alk_container_ml", "alk_remaining_ml", "alk_daily_ml", "alk_solution", "Alkalinity"),
        ("kalk", "kalk_container_ml", "kalk_remaining_ml", "kalk_daily_ml", "kalk_solution", "Kalkwasser"),
        ("ca", "ca_container_ml", "ca_remaining_ml", "ca_daily_ml", "ca_solution", "Calcium"),
        ("mg", "mg_container_ml", "mg_remaining_ml", "mg_daily_ml", "mg_solution", "Magnesium"),
        ("nitrate", "nitrate_container_ml", "nitrate_remaining_ml", "nitrate_daily_ml", "nitrate_solution", "Nitrate"),
        ("phosphate", "phosphate_container_ml", "phosphate_remaining_ml", "phosphate_daily_ml", "phosphate_solution", "Phosphate"),
        ("trace", "trace_container_ml", "trace_remaining_ml", "trace_daily_ml", "trace_solution", "Trace Elements"),
        ("nopox", "nopox_container_ml", "nopox_remaining_ml", "nopox_daily_ml", None, "NoPox"),
    ]
    for row in rows:
        threshold_days = row_get(row, "dosing_low_days", 5)
        try:
            threshold_days = float(threshold_days) if threshold_days is not None else 5
        except (ValueError, TypeError):
            threshold_days = 5
        for key, container_col, remaining_col, daily_col, solution_col, default_label in dosing_containers:
            container_ml = row_get(row, container_col)
            if container_ml is None:
                continue
            remaining_ml = row_get(row, remaining_col)
            daily_ml = row_get(row, daily_col)
            solution_name = row_get(row, solution_col) if solution_col else None
            label = solution_name or default_label
            if remaining_ml is None:
                remaining_ml = container_ml
            if not daily_ml:
                continue
            days_remaining = remaining_ml / float(daily_ml)
            if days_remaining <= threshold_days:
                if (row_get(row, "tank_id"), key, today) in dismissed_keys:
                    continue
                notifications.append(
                    {
                        "tank_id": row_get(row, "tank_id"),
                        "tank_name": row_get(row, "tank_name"),
                        "label": label,
                        "days": days_remaining,
                        "threshold": threshold_days,
                        "container_key": key,
                        "notified_on": today,
                    }
                )
                exists = one(
                    db,
                    "SELECT 1 FROM dosing_notifications WHERE tank_id=? AND container_key=? AND notified_on=? AND dismissed_at IS NULL",
                    (row_get(row, "tank_id"), key, today),
                )
                if not exists:
                    db.execute(
                        "INSERT INTO dosing_notifications (tank_id, container_key, notified_on) VALUES (?, ?, ?)",
                        (row_get(row, "tank_id"), key, today),
                    )
                    log_audit(
                        db,
                        actor_user,
                        "notification-sent",
                        {
                            "tank_id": row_get(row, "tank_id"),
                            "container_key": key,
                            "notified_on": today,
                        },
                    )
                    db.commit()
                    tank_id_value = int(row_get(row, "tank_id"))
                    recipients = get_tank_recipient_ids(db, tank_id_value)
                    send_web_push(
                        db,
                        recipients,
                        {
                            "title": f"{row_get(row, 'tank_name')}: {label}",
                            "body": f"{days_remaining:.1f} days remaining (≤ {threshold_days})",
                            "url": f"/tanks/{tank_id_value}",
                            "tag": f"dosing-{tank_id_value}-{key}-{today}",
                        },
                    )
        for entry in extra_by_tank.get(int(row_get(row, "tank_id")), []):
            container_ml = row_get(entry, "container_ml")
            if container_ml is None:
                continue
            remaining_ml = row_get(entry, "remaining_ml")
            daily_ml = row_get(entry, "daily_ml")
            solution_name = row_get(entry, "solution") or row_get(entry, "parameter") or "Dosing"
            if remaining_ml is None:
                remaining_ml = container_ml
            if not daily_ml:
                continue
            days_remaining = remaining_ml / float(daily_ml)
            key = f"extra_{row_get(entry, 'id')}"
            if days_remaining <= threshold_days:
                if (row_get(row, "tank_id"), key, today) in dismissed_keys:
                    continue
                notifications.append(
                    {
                        "tank_id": row_get(row, "tank_id"),
                        "tank_name": row_get(row, "tank_name"),
                        "label": solution_name,
                        "days": days_remaining,
                        "threshold": threshold_days,
                        "container_key": key,
                        "notified_on": today,
                    }
                )
                exists = one(
                    db,
                    "SELECT 1 FROM dosing_notifications WHERE tank_id=? AND container_key=? AND notified_on=? AND dismissed_at IS NULL",
                    (row_get(row, "tank_id"), key, today),
                )
                if not exists:
                    db.execute(
                        "INSERT INTO dosing_notifications (tank_id, container_key, notified_on) VALUES (?, ?, ?)",
                        (row_get(row, "tank_id"), key, today),
                    )
                    log_audit(
                        db,
                        actor_user,
                        "notification-sent",
                        {
                            "tank_id": row_get(row, "tank_id"),
                            "container_key": key,
                            "notified_on": today,
                        },
                    )
                    db.commit()
                    tank_id_value = int(row_get(row, "tank_id"))
                    recipients = get_tank_recipient_ids(db, tank_id_value)
                    send_web_push(
                        db,
                        recipients,
                        {
                            "title": f"{row_get(row, 'tank_name')}: {solution_name}",
                            "body": f"{days_remaining:.1f} days remaining (≤ {threshold_days})",
                            "url": f"/tanks/{tank_id_value}",
                            "tag": f"dosing-{tank_id_value}-{key}-{today}",
                        },
                    )
    return notifications

def build_daily_summary(db: Connection, user: Dict[str, Any]) -> Dict[str, Any]:
    today = date.today().isoformat()
    tanks = q(
        db,
        """SELECT DISTINCT ON (t.id) t.*
           FROM tanks t
           LEFT JOIN user_tanks ut ON ut.tank_id = t.id
           WHERE t.owner_user_id=? OR ut.user_id=?
           ORDER BY t.id, COALESCE(t.sort_order, 0), t.name""",
        (user["id"], user["id"]),
    )
    dosing_alerts = collect_dosing_notifications(db, owner_user_id=user["id"], actor_user=user)
    dosing_by_tank: Dict[int, List[Dict[str, Any]]] = {}
    for alert in dosing_alerts:
        dosing_by_tank.setdefault(alert["tank_id"], []).append(alert)
    overdue_by_tank: Dict[int, List[str]] = {}
    parameter_alerts_by_tank: Dict[int, List[Dict[str, Any]]] = {}
    trend_alerts_by_tank: Dict[int, List[Dict[str, Any]]] = {}
    maintenance_by_tank: Dict[int, List[Dict[str, Any]]] = {}
    for t in tanks:
        latest_map = get_latest_and_previous_per_parameter(db, t["id"])
        pdefs = {p["name"]: p for p in get_active_param_defs(db, user_id=user["id"])}
        overdue = []
        for pname, pdef in pdefs.items():
            latest = latest_map.get(pname, {}).get("latest")
            taken_at = latest.get("taken_at") if latest else None
            interval_days = row_get(pdef, "test_interval_days")
            if not interval_days:
                continue
            dt = parse_dt_any(taken_at)
            if not dt:
                overdue.append(pname)
                continue
            if (datetime.utcnow() - dt).days >= int(interval_days):
                overdue.append(pname)
        if overdue:
            overdue_by_tank[t["id"]] = overdue
        targets = q(db, "SELECT * FROM targets WHERE tank_id=? AND enabled=1", (t["id"],))
        alert_rows = []
        for target in targets:
            pname = target["parameter"]
            if not pname:
                continue
            latest = latest_map.get(pname, {}).get("latest")
            val = latest.get("value") if latest else None
            if val is None:
                continue
            try:
                val_f = float(val)
            except (ValueError, TypeError):
                continue
            al = row_get(target, "alert_low")
            ah = row_get(target, "alert_high")
            tl = row_get(target, "target_low")
            th = row_get(target, "target_high")
            if tl is None:
                tl = row_get(target, "low")
            if th is None:
                th = row_get(target, "high")
            severity = None
            label = None
            if al is not None and val_f < float(al):
                severity = "alert"
                label = "below alert"
            elif ah is not None and val_f > float(ah):
                severity = "alert"
                label = "above alert"
            elif tl is not None and val_f < float(tl):
                severity = "warn"
                label = "below target"
            elif th is not None and val_f > float(th):
                severity = "warn"
                label = "above target"
            if severity:
                alert_rows.append(
                    {
                        "parameter": pname,
                        "value": val_f,
                        "unit": row_get(target, "unit") or row_get(pdefs.get(pname), "unit") or "",
                        "severity": severity,
                        "label": label,
                    }
                )
        if alert_rows:
            parameter_alerts_by_tank[t["id"]] = alert_rows
        trend_rows = []
        for pname, pdef in pdefs.items():
            max_change = row_get(pdef, "max_daily_change")
            if max_change is None:
                continue
            latest_pair = latest_map.get(pname, {})
            latest = latest_pair.get("latest")
            previous = latest_pair.get("previous")
            if not latest or not previous:
                continue
            try:
                delta = float(latest["value"]) - float(previous["value"])
                days = max((latest["taken_at"] - previous["taken_at"]).total_seconds() / 86400.0, 1e-6)
                delta_per_day = abs(delta / days)
                if delta_per_day > float(max_change):
                    trend_rows.append(
                        {
                            "parameter": pname,
                            "delta_per_day": delta_per_day,
                            "unit": row_get(pdef, "unit") or "",
                        }
                    )
            except (ValueError, TypeError, AttributeError, KeyError):
                continue
        if trend_rows:
            trend_alerts_by_tank[t["id"]] = trend_rows
        tasks = q(db, "SELECT * FROM tank_maintenance_tasks WHERE tank_id=? AND active=1", (t["id"],))
        due_rows = []
        for task in tasks:
            due_at = parse_dt_any(task["next_due_at"])
            if not due_at:
                continue
            days_until = (due_at.date() - datetime.utcnow().date()).days
            if days_until <= 3:
                due_rows.append(
                    {
                        "title": task["title"],
                        "days_until": days_until,
                        "next_due_at": due_at,
                    }
                )
        if due_rows:
            maintenance_by_tank[t["id"]] = due_rows
    return {
        "date": today,
        "tanks": tanks,
        "dosing_by_tank": dosing_by_tank,
        "overdue_by_tank": overdue_by_tank,
        "parameter_alerts_by_tank": parameter_alerts_by_tank,
        "trend_alerts_by_tank": trend_alerts_by_tank,
        "maintenance_by_tank": maintenance_by_tank,
    }

def send_daily_summary_email(db: Connection, user: Dict[str, Any]) -> Tuple[bool, str]:
    app_name = os.environ.get("APP_NAME", "Reef Metrics")
    base_url = PUBLIC_BASE_URL or "https://reefmetrics.app"
    summary = build_daily_summary(db, user)
    subject = f"{app_name} Daily Summary"
    lines = [f"Daily summary for {summary['date']}:"]
    for t in summary["tanks"]:
        alerts = summary["dosing_by_tank"].get(t["id"], [])
        overdue = summary["overdue_by_tank"].get(t["id"], [])
        param_alerts = summary["parameter_alerts_by_tank"].get(t["id"], [])
        trend_alerts = summary["trend_alerts_by_tank"].get(t["id"], [])
        maintenance = summary["maintenance_by_tank"].get(t["id"], [])
        if not alerts and not overdue and not param_alerts and not trend_alerts and not maintenance:
            continue
        lines.append(f"- {t['name']}")
        for alert in alerts:
            lines.append(
                f"  • Low {alert['label']}: {alert['days']:.1f} days remaining (≤ {alert['threshold']})"
            )
        for pname in overdue:
            lines.append(f"  • Overdue test: {pname}")
        for alert in param_alerts:
            lines.append(
                f"  • {alert['parameter']} {alert['label']}: {alert['value']:.2f} {alert['unit']}"
            )
        for trend in trend_alerts:
            lines.append(
                f"  • Trend alert: {trend['parameter']} changed {trend['delta_per_day']:.2f} {trend['unit']}/day"
            )
        for task in maintenance:
            if task["days_until"] < 0:
                lines.append(f"  • Maintenance overdue: {task['title']} ({abs(task['days_until'])} days late)")
            else:
                lines.append(f"  • Maintenance due soon: {task['title']} ({task['days_until']} days)")
    if len(lines) == 1:
        lines.append("No alerts today.")
    text_body = "\n".join(lines)
    html_rows = ""
    for t in summary["tanks"]:
        alerts = summary["dosing_by_tank"].get(t["id"], [])
        overdue = summary["overdue_by_tank"].get(t["id"], [])
        param_alerts = summary["parameter_alerts_by_tank"].get(t["id"], [])
        trend_alerts = summary["trend_alerts_by_tank"].get(t["id"], [])
        maintenance = summary["maintenance_by_tank"].get(t["id"], [])
        if not alerts and not overdue and not param_alerts and not trend_alerts and not maintenance:
            continue
        html_rows += f"<tr><td style='padding:8px 0; font-weight:600;'>{t['name']}</td></tr>"
        for alert in alerts:
            html_rows += (
                f"<tr><td style='padding:2px 0; color:#b45309;'>Low {alert['label']}: "
                f"{alert['days']:.1f} days remaining (≤ {alert['threshold']})</td></tr>"
            )
        for pname in overdue:
            html_rows += f"<tr><td style='padding:2px 0; color:#b91c1c;'>Overdue test: {pname}</td></tr>"
        for alert in param_alerts:
            color = "#b91c1c" if alert["severity"] == "alert" else "#b45309"
            html_rows += (
                f"<tr><td style='padding:2px 0; color:{color};'>{alert['parameter']} {alert['label']}: "
                f"{alert['value']:.2f} {alert['unit']}</td></tr>"
            )
        for trend in trend_alerts:
            html_rows += (
                f"<tr><td style='padding:2px 0; color:#b45309;'>Trend alert: {trend['parameter']} "
                f"changed {trend['delta_per_day']:.2f} {trend['unit']}/day</td></tr>"
            )
        for task in maintenance:
            if task["days_until"] < 0:
                html_rows += (
                    f"<tr><td style='padding:2px 0; color:#b91c1c;'>Maintenance overdue: "
                    f"{task['title']} ({abs(task['days_until'])} days late)</td></tr>"
                )
            else:
                html_rows += (
                    f"<tr><td style='padding:2px 0; color:#b45309;'>Maintenance due soon: "
                    f"{task['title']} ({task['days_until']} days)</td></tr>"
                )
    if not html_rows:
        html_rows = "<tr><td style='padding:8px 0;'>No alerts today.</td></tr>"
    html_body = f"""
<!DOCTYPE html>
<html>
  <body style="margin:0; padding:0; background:#f8fafc; font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
    <table role="presentation" cellpadding="0" cellspacing="0" width="100%">
      <tr>
        <td align="center" style="padding:32px 12px;">
          <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="max-width:560px; background:#ffffff; border-radius:12px; border:1px solid #e5e7eb;">
            <tr>
              <td style="padding:28px 28px 8px;">
                <div style="font-size:18px; letter-spacing:0.08em; text-transform:uppercase; color:#0ea5e9; font-weight:700;">{app_name}</div>
              </td>
            </tr>
            <tr>
              <td style="padding:8px 28px 16px;">
                <h1 style="margin:0 0 12px; font-size:22px; color:#0f172a;">Daily Summary</h1>
                <p style="margin:0 0 16px; font-size:14px; color:#475569;">{summary['date']}</p>
                <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="font-size:14px; color:#334155;">
                  {html_rows}
                </table>
                <div style="margin-top:16px;">
                  <a href="{base_url}" style="display:inline-block; padding:10px 18px; background:#0ea5e9; color:#ffffff; text-decoration:none; border-radius:8px; font-size:14px;">Open {app_name}</a>
                </div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
    """
    return send_email(user["email"], subject, text_body, html_body, sender_kind="alerts")

def send_summaries_if_due() -> None:
    auto_send = os.environ.get("AUTO_SEND_DAILY_SUMMARIES", "false").lower() in {"1", "true", "yes"}
    if not auto_send:
        return
    db = get_db()
    try:
        now_local = _to_local(datetime.utcnow())
        today = now_local.date().isoformat()
        last_sent = get_setting(db, "daily_summary_last_sent")
        send_hour = int(os.environ.get("DAILY_SUMMARY_HOUR", "7"))
        if last_sent == today or now_local.hour < send_hour:
            return
        users = q(db, "SELECT id, email FROM users ORDER BY email")
        for u in users:
            send_daily_summary_email(db, u)
        set_setting(db, "daily_summary_last_sent", today)
    finally:
        db.close()

def run_daily_summary_check() -> None:
    """Check and send daily summaries if due - called by scheduler"""
    try:
        send_summaries_if_due()
    except Exception as exc:
        logger.error(f"Daily summary check error: {exc}", exc_info=True)

def send_push_notifications_if_due() -> None:
    auto_send = os.environ.get("AUTO_SEND_PUSH_NOTIFICATIONS", "true").lower() in {"1", "true", "yes"}
    if not auto_send:
        return
    db = get_db()
    try:
        collect_dosing_notifications(db)
    finally:
        db.close()

def start_push_notification_scheduler() -> None:
    """Add push notification job to scheduler"""
    try:
        interval_minutes = int(os.environ.get("PUSH_NOTIFICATION_INTERVAL_MINUTES", "15"))
    except ValueError:
        interval_minutes = 15

    def run_push_check():
        try:
            send_push_notifications_if_due()
        except Exception as exc:
            logger.error(f"Push notification check error: {exc}", exc_info=True)

    scheduler.add_job(
        run_push_check,
        'interval',
        minutes=interval_minutes,
        id='push_notifications',
        replace_existing=True
    )

def global_dosing_notifications(request: Request) -> List[Dict[str, Any]]:
    db = get_db()
    try:
        user = get_current_user(db, request)
        if user and row_get(user, "admin"):
            return collect_dosing_notifications(db, actor_user=user)
        if user:
            return collect_dosing_notifications(db, owner_user_id=user["id"], actor_user=user)
        return []
    finally:
        db.close()

templates.env.globals["global_dosing_notifications"] = global_dosing_notifications

def redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url, status_code=303)

def r2_enabled() -> bool:
    return all([R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET])

def get_r2_client():
    if importlib.util.find_spec("boto3") is None or importlib.util.find_spec("botocore") is None:
        raise RuntimeError("Cloudflare R2 requires boto3 and botocore to be installed.")
    boto3 = importlib.import_module("boto3")
    botocore_config = importlib.import_module("botocore.config")
    Config = botocore_config.Config
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name=R2_REGION,
        config=Config(s3={"addressing_style": "path"}),
    )

def upload_r2_file(file_path: str, key: str, content_type: str) -> None:
    client = get_r2_client()
    client.upload_file(
        file_path,
        R2_BUCKET,
        key,
        ExtraArgs={"ContentType": content_type},
    )

def upload_r2_bytes(payload: bytes, key: str, content_type: str) -> None:
    client = get_r2_client()
    client.put_object(
        Bucket=R2_BUCKET,
        Key=key,
        Body=payload,
        ContentType=content_type,
    )

def presign_r2_download(key: str, expires_in: int = 900) -> str:
    client = get_r2_client()
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": R2_BUCKET, "Key": key},
        ExpiresIn=expires_in,
    )

def cleanup_r2_backups() -> None:
    if not R2_BACKUP_RETENTION_DAYS:
        return
    try:
        retention_days = int(R2_BACKUP_RETENTION_DAYS)
    except ValueError:
        return
    if retention_days <= 0:
        return
    client = get_r2_client()
    cutoff = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=retention_days)
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix="backups/"):
        for item in page.get("Contents", []):
            last_modified = item.get("LastModified")
            if last_modified and last_modified < cutoff:
                client.delete_object(Bucket=R2_BUCKET, Key=item["Key"])

def cleanup_r2_icp_uploads() -> None:
    if not R2_ICP_RETENTION_DAYS:
        return
    try:
        retention_days = int(R2_ICP_RETENTION_DAYS)
    except ValueError:
        return
    if retention_days <= 0:
        return
    client = get_r2_client()
    cutoff = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=retention_days)
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix="icp-uploads/"):
        for item in page.get("Contents", []):
            last_modified = item.get("LastModified")
            if last_modified and last_modified < cutoff:
                client.delete_object(Bucket=R2_BUCKET, Key=item["Key"])

def ensure_csrf_token(token: str | None) -> str:
    return token or secrets.token_urlsafe(32)

async def extract_csrf_token(request: Request) -> str | None:
    header_token = request.headers.get("X-CSRF-Token") or request.headers.get("x-csrf-token")
    if header_token:
        return header_token
    content_type = request.headers.get("content-type", "")
    body = await request.body()
    if not body:
        return None
    if "application/json" in content_type:
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return None
        return payload.get("csrf_token")
    if "multipart/form-data" in content_type:
        match = re.search(r"boundary=([^;]+)", content_type)
        if not match:
            return None
        boundary = match.group(1).strip().strip('"')
        boundary_bytes = f"--{boundary}".encode()
        for part in body.split(boundary_bytes):
            if b'name="csrf_token"' not in part:
                continue
            _, _, value = part.partition(b"\r\n\r\n")
            if not value:
                return None
            return value.strip().decode("utf-8", errors="ignore")
    if "application/x-www-form-urlencoded" in content_type:
        payload = urllib.parse.parse_qs(body.decode("utf-8"), keep_blank_values=True)
        token_values = payload.get("csrf_token", [])
        return token_values[0] if token_values else None
    return None

def hash_password(password: str, salt: str | None = None) -> Tuple[str, str]:
    if not salt:
        salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        PASSWORD_HASH_ITERATIONS,
    )
    return digest.hex(), salt

def verify_password(password: str, password_hash: str, salt: str) -> bool:
    digest, _ = hash_password(password, salt)
    return secrets.compare_digest(digest, password_hash)

def get_current_user(db: Connection, request: Request) -> Optional[Dict[str, Any]]:
    token = request.cookies.get("session_token")
    if not token:
        return None
    row = one(db, """
        SELECT u.* FROM sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.session_token=? AND (s.expires_at IS NULL OR s.expires_at > ?)
        LIMIT 1
    """, (token, datetime.utcnow().isoformat()))
    return row

def get_visible_tanks(db: Connection, request: Request) -> List[Dict[str, Any]]:
    user = get_current_user(db, request)
    if user and row_get(user, "admin"):
        return q(db, "SELECT * FROM tanks ORDER BY COALESCE(sort_order, 0), name")
    if user:
        return q(
            db,
            """SELECT DISTINCT ON (t.id) t.*
               FROM tanks t
               LEFT JOIN user_tanks ut ON ut.tank_id = t.id
               WHERE t.owner_user_id=? OR ut.user_id=?
               ORDER BY t.id, COALESCE(t.sort_order, 0), t.name""",
            (user["id"], user["id"]),
        )
    return q(db, "SELECT * FROM tanks ORDER BY COALESCE(sort_order, 0), name")

def ensure_additives_owner_column(db: "DBConnection") -> None:
    ensure_column(db, "additives", "owner_user_id", "ALTER TABLE additives ADD COLUMN owner_user_id INTEGER")

def ensure_test_kits_owner_column(db: "DBConnection") -> None:
    ensure_column(db, "test_kits", "owner_user_id", "ALTER TABLE test_kits ADD COLUMN owner_user_id INTEGER")

def is_admin_user(user: Optional[Dict[str, Any]]) -> bool:
    return bool(user and (row_get(user, "admin") in (1, True) or row_get(user, "role") == "admin"))

def build_additives_where(db: "DBConnection", user: Optional[Dict[str, Any]], active_only: bool = True, extra_clause: Optional[str] = None, extra_params: Tuple[Any, ...] = ()) -> Tuple[str, Tuple[Any, ...]]:
    ensure_additives_owner_column(db)
    clauses = []
    params: List[Any] = []
    if active_only:
        clauses.append("active=1")
    if user:
        clauses.append("(owner_user_id IS NULL OR owner_user_id=?)")
        params.append(user["id"])
    else:
        clauses.append("owner_user_id IS NULL")
    if extra_clause:
        clauses.append(extra_clause)
        params.extend(extra_params)
    where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, tuple(params)

def get_visible_additives(db: "DBConnection", user: Optional[Dict[str, Any]], active_only: bool = True) -> List[Dict[str, Any]]:
    where_sql, params = build_additives_where(db, user, active_only=active_only)
    return q(db, f"SELECT * FROM additives{where_sql} ORDER BY parameter, name", params)

def get_visible_additive_by_id(db: "DBConnection", user: Optional[Dict[str, Any]], additive_id: int, active_only: bool = False) -> Optional[Dict[str, Any]]:
    where_sql, params = build_additives_where(db, user, active_only=active_only, extra_clause="id=?", extra_params=(additive_id,))
    return one(db, f"SELECT * FROM additives{where_sql}", params)

def build_test_kits_where(db: "DBConnection", user: Optional[Dict[str, Any]], active_only: bool = True, extra_clause: Optional[str] = None, extra_params: Tuple[Any, ...] = ()) -> Tuple[str, Tuple[Any, ...]]:
    ensure_test_kits_owner_column(db)
    clauses = []
    params: List[Any] = []
    if active_only:
        clauses.append("active=1")
    if user and is_admin_user(user):
        pass
    elif user:
        clauses.append("(owner_user_id IS NULL OR owner_user_id=?)")
        params.append(user["id"])
    else:
        clauses.append("owner_user_id IS NULL")
    if extra_clause:
        clauses.append(extra_clause)
        params.extend(extra_params)
    where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, tuple(params)

def get_visible_test_kits(db: "DBConnection", user: Optional[Dict[str, Any]], active_only: bool = True) -> List[Dict[str, Any]]:
    where_sql, params = build_test_kits_where(db, user, active_only=active_only)
    return q(db, f"SELECT * FROM test_kits{where_sql} ORDER BY parameter, name", params)

def get_visible_test_kit_by_id(db: "DBConnection", user: Optional[Dict[str, Any]], test_kit_id: int, active_only: bool = False) -> Optional[Dict[str, Any]]:
    where_sql, params = build_test_kits_where(db, user, active_only=active_only, extra_clause="id=?", extra_params=(test_kit_id,))
    return one(db, f"SELECT * FROM test_kits{where_sql}", params)

def group_additives_by_parameter(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped = []
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        group_name = (r["group_name"] or "").strip()
        if group_name:
            key = group_name
        else:
            key = (r["parameter"] or "Uncategorized").strip() or "Uncategorized"
        groups.setdefault(key, []).append(r)
    for key in sorted(groups.keys(), key=lambda s: s.lower()):
        grouped.append({"parameter": key, "items": groups[key]})
    return grouped

def group_test_kits_by_parameter(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped = []
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        key = (r["parameter"] or "Uncategorized").strip() or "Uncategorized"
        groups.setdefault(key, []).append(r)
    for key in sorted(groups.keys(), key=lambda s: s.lower()):
        grouped.append({"parameter": key, "items": groups[key]})
    return grouped

def get_visible_tank_ids(db: Connection, user: Optional[Dict[str, Any]]) -> List[int]:
    if user and row_get(user, "admin"):
        rows = q(db, "SELECT id FROM tanks ORDER BY id")
        return [row["id"] for row in rows]
    if user:
        rows = q(
            db,
            """SELECT DISTINCT t.id
               FROM tanks t
               LEFT JOIN user_tanks ut ON ut.tank_id = t.id
               WHERE t.owner_user_id=? OR ut.user_id=?
               ORDER BY t.id""",
            (user["id"], user["id"]),
        )
        return [row["id"] for row in rows]
    rows = q(db, "SELECT id FROM tanks ORDER BY id")
    return [row["id"] for row in rows]

def get_tank_for_user(db: Connection, user: Optional[Dict[str, Any]], tank_id: int) -> Optional[Dict[str, Any]]:
    if user and row_get(user, "admin"):
        return one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
    if user:
        return one(
            db,
            """SELECT DISTINCT ON (t.id) t.*
               FROM tanks t
               LEFT JOIN user_tanks ut ON ut.tank_id = t.id
               WHERE t.id=? AND (t.owner_user_id=? OR ut.user_id=?)
               ORDER BY t.id""",
            (tank_id, user["id"], user["id"]),
        )
    return one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))

def require_admin(user: Optional[Dict[str, Any]]) -> None:
    if not user or (row_get(user, "admin") not in (1, True) and row_get(user, "role") != "admin"):
        raise HTTPException(status_code=403, detail="Admin access required")

def create_session(db: Connection, user_id: int, rotate_existing: Optional[bool] = None) -> str:
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.utcnow() + timedelta(days=SESSION_LIFETIME_DAYS)).isoformat()
    rotate = SESSION_ROTATE_ON_LOGIN if rotate_existing is None else rotate_existing
    if rotate:
        db.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
    db.execute(
        "INSERT INTO sessions (user_id, session_token, created_at, expires_at) VALUES (?, ?, ?, ?)",
        (user_id, token, datetime.utcnow().isoformat(), expires_at),
    )
    db.commit()
    return token

def hash_api_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()

def create_api_token(db: Connection, user_id: int, label: str | None = None) -> str:
    token = secrets.token_urlsafe(32)
    token_hash = hash_api_token(token)
    token_prefix = token[:8]
    db.execute(
        "INSERT INTO api_tokens (user_id, token_hash, token_prefix, label, created_at) VALUES (?, ?, ?, ?, ?)",
        (user_id, token_hash, token_prefix, (label or "").strip() or None, datetime.utcnow().isoformat()),
    )
    db.commit()
    return token

def get_api_user(db: Connection, request: Request) -> Optional[Dict[str, Any]]:
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header.replace("Bearer ", "", 1).strip()
        if token:
            token_hash = hash_api_token(token)
            row = one(
                db,
                """SELECT u.*
                   FROM api_tokens t
                   JOIN users u ON u.id = t.user_id
                   WHERE t.token_hash=?""",
                (token_hash,),
            )
            if row:
                db.execute(
                    "UPDATE api_tokens SET last_used_at=? WHERE token_hash=?",
                    (datetime.utcnow().isoformat(), token_hash),
                )
                db.commit()
                return row
    return None

def get_authenticated_user(db: Connection, request: Request) -> Optional[Dict[str, Any]]:
    user = get_api_user(db, request)
    if user:
        return user
    return get_current_user(db, request)

def list_api_tokens(db: Connection, user_id: int) -> List[Dict[str, Any]]:
    return q(
        db,
        "SELECT id, token_prefix, label, created_at, last_used_at FROM api_tokens WHERE user_id=? ORDER BY created_at DESC",
        (user_id,),
    )

def get_setting(db: Connection, key: str) -> Optional[str]:
    row = one(db, "SELECT value FROM app_settings WHERE key=?", (key,))
    return row["value"] if row else None

def set_setting(db: Connection, key: str, value: str) -> None:
    db.execute(
        "INSERT INTO app_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    db.commit()

APEX_SETTINGS_KEY = "apex_integration_settings"
def _normalize_apex_integration(settings: Dict[str, Any]) -> Dict[str, Any]:
    settings.setdefault("id", "default")
    settings.setdefault("name", "Apex Integration")
    settings.setdefault("enabled", False)
    settings.setdefault("host", "")
    settings.setdefault("username", "")
    settings.setdefault("password", "")
    settings.setdefault("api_token", "")
    settings.setdefault("poll_interval_minutes", 15)
    settings.setdefault("mapping", {})
    settings.setdefault("tank_id", None)
    settings.setdefault("tank_ids", [])
    settings.setdefault("last_probe_list", [])
    settings.setdefault("last_probe_loaded_at", None)
    if not isinstance(settings.get("mapping"), dict):
        settings["mapping"] = {}
    if not isinstance(settings.get("last_probe_list"), list):
        settings["last_probe_list"] = []
    if not isinstance(settings.get("tank_ids"), list):
        settings["tank_ids"] = []
    return settings

def get_apex_integrations(db: Connection) -> List[Dict[str, Any]]:
    raw = get_setting(db, APEX_SETTINGS_KEY)
    if not raw:
        return [_normalize_apex_integration({})]
    try:
        stored = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        stored = {}
    if isinstance(stored, dict) and "integrations" in stored:
        integrations = stored.get("integrations") or []
    elif isinstance(stored, list):
        integrations = stored
    elif isinstance(stored, dict):
        integrations = [stored]
    else:
        integrations = []
    normalized = []
    for idx, entry in enumerate(integrations):
        if not isinstance(entry, dict):
            continue
        if not entry.get("id"):
            entry["id"] = f"apex-{idx + 1}"
        normalized.append(_normalize_apex_integration(entry))
    return normalized or [_normalize_apex_integration({})]

def get_apex_settings(db: Connection, integration_id: Optional[str] = None) -> Dict[str, Any]:
    integrations = get_apex_integrations(db)
    if integration_id:
        for integration in integrations:
            if integration.get("id") == integration_id:
                return integration
    return integrations[0]

def save_apex_settings(db: Connection, integration_id: str, settings: Dict[str, Any]) -> None:
    integrations = get_apex_integrations(db)
    updated = False
    for idx, integration in enumerate(integrations):
        if integration.get("id") == integration_id:
            integrations[idx] = _normalize_apex_integration(settings)
            updated = True
            break
    if not updated:
        integrations.append(_normalize_apex_integration(settings))
    set_setting(db, APEX_SETTINGS_KEY, json.dumps({"integrations": integrations}))

def parse_apex_mapping(mapping_text: str) -> Dict[str, str]:
    if not mapping_text:
        return {}
    try:
        payload = json.loads(mapping_text)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError("Mapping JSON must be valid JSON.") from exc
    if isinstance(payload, dict):
        return {str(k): str(v) for k, v in payload.items() if k and v}
    if isinstance(payload, list):
        mapping: Dict[str, str] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            probe = item.get("probe") or item.get("name")
            param = item.get("parameter") or item.get("param")
            if probe and param:
                mapping[str(probe)] = str(param)
        return mapping
    return {}

def apex_mapping_text(mapping: Dict[str, str]) -> str:
    if not mapping:
        return ""
    return json.dumps(mapping, indent=2, sort_keys=True)

def clean_apex_host(host: str) -> str:
    trimmed = host.strip()
    if not trimmed:
        return ""
    if "://" in trimmed:
        split = urllib.parse.urlsplit(trimmed)
        netloc = split.netloc or split.path.split("/")[0]
        if not netloc:
            return ""
        if split.path and split.path != "/":
            return f"{split.scheme}://{netloc}{split.path}"
        return f"{split.scheme}://{netloc}"
    return f"http://{trimmed.split('?', 1)[0]}"

def normalize_probe_label(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(label).lower())

def infer_apex_auto_mapping(probes: List[str], parameters: List[Dict[str, Any]]) -> Dict[str, str]:
    param_by_key = {
        normalize_probe_label(row_get(p, "name")): row_get(p, "name")
        for p in parameters
        if row_get(p, "name")
    }
    mapping_rules = {
        "temp": "Temperature",
        "temperature": "Temperature",
        "ph": "pH",
        "alk": "Alkalinity/KH",
        "alkalinity": "Alkalinity/KH",
        "dkh": "Alkalinity/KH",
        "calcium": "Calcium",
        "ca": "Calcium",
        "magnesium": "Magnesium",
        "mg": "Magnesium",
        "nitrate": "Nitrate",
        "no3": "Nitrate",
        "phosphate": "Phosphate",
        "po4": "Phosphate",
        "salinity": "Salinity",
        "sg": "Salinity",
        "orp": "ORP",
        "oxygen": "Oxygen",
        "o2": "Oxygen",
        "ammonia": "Ammonia",
        "nh3": "Ammonia",
        "nitrite": "Nitrite",
        "no2": "Nitrite",
    }
    auto_mapping: Dict[str, str] = {}
    for probe in probes:
        key = normalize_probe_label(probe)
        if not key:
            continue
        matched = None
        for rule_key, param_name in mapping_rules.items():
            if key == rule_key or key.startswith(rule_key):
                matched = param_name
                break
        if not matched:
            continue
        param_key = normalize_probe_label(matched)
        if param_key in param_by_key:
            auto_mapping[probe] = param_by_key[param_key]
    return auto_mapping

def build_apex_mapping_from_form(form: Any, fallback: Dict[str, str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    probe_names = form.getlist("probe_name")
    param_names = form.getlist("param_name")
    for probe_name, param_name in zip(probe_names, param_names):
        if probe_name and param_name:
            mapping[str(probe_name)] = str(param_name)
    if mapping:
        return mapping
    return fallback or {}

def build_sparkline_points(values: List[float], width: int = 84, height: int = 24) -> str:
    if len(values) < 2:
        return ""
    min_val = min(values)
    max_val = max(values)
    span = max(max_val - min_val, 1e-6)
    points = []
    for idx, value in enumerate(values):
        x = round((idx / (len(values) - 1)) * width, 1)
        y = round(height - ((value - min_val) / span) * height, 1)
        points.append(f"{x},{y}")
    return " ".join(points)

TRACE_ELEMENT_NAMES = {
    "iron",
    "iodine",
    "strontium",
    "potassium",
    "boron",
    "fluoride",
    "manganese",
    "molybdenum",
    "zinc",
    "cobalt",
}

def is_trace_element(name: str) -> bool:
    if not name:
        return False
    return normalize_probe_label(name) in TRACE_ELEMENT_NAMES

def filter_trace_parameters(parameters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [p for p in parameters if not is_trace_element(row_get(p, "name"))]

ICP_ELEMENT_ALIASES = {
    "al": "aluminium",
    "aluminum": "aluminium",
    "b": "boron",
    "ba": "barium",
    "be": "beryllium",
    "br": "bromine",
    "ca": "calcium",
    "cd": "cadmium",
    "co": "cobalt",
    "cr": "chromium",
    "cu": "copper",
    "fe": "iron",
    "f": "fluoride",
    "i": "iodine",
    "k": "potassium",
    "li": "lithium",
    "mg": "magnesium",
    "mn": "manganese",
    "mo": "molybdenum",
    "na": "sodium",
    "ni": "nickel",
    "p": "phosphorus",
    "po4": "phosphate",
    "s": "sulfur",
    "se": "selenium",
    "si": "silicon",
    "sr": "strontium",
    "zn": "zinc",
}

class TritonTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_row = False
        self.in_cell = False
        self.current_row: List[str] = []
        self.rows: List[List[str]] = []
        self.buffer: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag == "tr":
            self.in_row = True
            self.current_row = []
        if self.in_row and tag in {"td", "th"}:
            self.in_cell = True
            self.buffer = []

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self.in_cell:
            text = " ".join("".join(self.buffer).split())
            self.current_row.append(text)
            self.in_cell = False
        if tag == "tr" and self.in_row:
            if self.current_row:
                self.rows.append(self.current_row)
            self.in_row = False

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.buffer.append(data)

def fetch_triton_html(url: str) -> Tuple[str, Dict[str, Any]]:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; ReefMetrics/1.0)"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read()
        encoding = resp.getheader("Content-Encoding", "")
        if encoding.lower() == "gzip":
            raw = gzip.decompress(raw)
        content_type = resp.getheader("Content-Type", "")
        meta = {
            "status": getattr(resp, "status", None),
            "content_type": content_type,
            "content_length": len(raw),
        }
        return raw.decode("utf-8", errors="ignore"), meta

def summarize_triton_html(content: str, meta: Dict[str, Any]) -> str:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", content, re.IGNORECASE | re.DOTALL)
    title = html.unescape(title_match.group(1).strip()) if title_match else "unknown"
    lower = content.lower()
    blocked_markers = ["access denied", "forbidden", "captcha", "cloudflare", "attention required"]
    blocked = any(marker in lower for marker in blocked_markers)
    notes = []
    if blocked:
        notes.append("response looks blocked (access denied/captcha)")
    if meta.get("content_type"):
        notes.append(f"type: {meta['content_type']}")
    if meta.get("content_length") is not None:
        notes.append(f"bytes: {meta['content_length']}")
    extra = f" ({'; '.join(notes)})" if notes else ""
    return f"Response title: {title}{extra}"

def extract_numeric_value(raw: str) -> Optional[float]:
    if raw is None:
        return None
    match = re.search(r"-?\d+(?:[.,]\d+)?", str(raw))
    if not match:
        return None
    return float(match.group(0).replace(",", "."))

def parse_triton_rows(rows: List[List[str]]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for row in rows:
        cleaned = [cell.strip() for cell in row if cell and cell.strip()]
        if len(cleaned) < 2:
            continue
        name_idx = None
        for idx, cell in enumerate(cleaned):
            if re.search(r"[A-Za-z]", cell):
                name_idx = idx
                break
        if name_idx is None:
            name_idx = 0
        name = cleaned[name_idx]
        if not name:
            continue
        numeric = None
        for cell in cleaned[name_idx + 1 :]:
            numeric = extract_numeric_value(cell)
            if numeric is not None:
                break
        if numeric is None:
            for idx, cell in enumerate(cleaned):
                if idx == name_idx:
                    continue
                numeric = extract_numeric_value(cell)
                if numeric is not None:
                    break
        if numeric is None:
            continue
        results.append({"name": name, "value": numeric, "unit": None})
    return results

def parse_triton_html(content: str) -> List[Dict[str, Any]]:
    parser = TritonTableParser()
    parser.feed(content)
    results = parse_triton_rows(parser.rows)
    if results:
        return results
    results = parse_triton_json_like(content)
    if results:
        return results
    return parse_triton_data_attributes(content)

def parse_triton_csv(content: str) -> List[Dict[str, Any]]:
    text = content.decode("utf-8", errors="ignore")
    sniffer = csv.Sniffer()
    sample = "\n".join(text.splitlines()[:20])
    try:
        dialect = sniffer.sniff(sample, delimiters=",;\t")
    except Exception:
        if ";" in sample and "," not in sample:
            dialect = csv.excel
            dialect.delimiter = ";"
        else:
            dialect = csv.excel
    reader = csv.reader(text.splitlines(), dialect)
    rows = list(reader)
    if not rows:
        return []
    header = [h.strip().lower() for h in rows[0]]
    name_idx = None
    value_idx = None
    for idx, col in enumerate(header):
        if col in {"element", "parameter", "name"}:
            name_idx = idx
        if col in {"value", "result", "measured"}:
            value_idx = idx
    if name_idx is not None and value_idx is not None:
        data_rows = rows[1:]
        slim_rows = [
            [row[name_idx], row[value_idx]]
            for row in data_rows
            if len(row) > max(name_idx, value_idx)
        ]
        return parse_triton_rows(slim_rows)
    return parse_triton_rows(rows)

def parse_triton_pdf(content: bytes) -> List[Dict[str, Any]]:
    try:
        PdfReader = importlib.import_module("PyPDF2").PdfReader
    except (ImportError, ModuleNotFoundError, AttributeError) as exc:
        raise ValueError("PDF parsing requires PyPDF2. Install it and rebuild the container.") from exc
    reader = PdfReader(BytesIO(content))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    results: List[Dict[str, Any]] = []
    for line in text.splitlines():
        parts = [p for p in line.split() if p]
        if len(parts) < 2:
            continue
        results.extend(parse_triton_rows([parts]))
    return results

def parse_triton_data_attributes(content: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    pattern = re.compile(
        r'data-(?:element|name)=\"([^\"]+)\"[^>]*?data-(?:value|result)=\"([^\"]+)\"',
        re.IGNORECASE,
    )
    for name, value in pattern.findall(content):
        numeric = extract_numeric_value(value)
        if numeric is None:
            continue
        results.append({"name": name.strip(), "value": numeric, "unit": None})
    return results

def parse_triton_json_like(content: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    pattern = re.compile(
        r'[{,]\s*"(?:element|name)"\s*:\s*"([^"]+)"[^}]*?"(?:value|result)"\s*:\s*"?(.*?)"?(?:[},])',
        re.IGNORECASE | re.DOTALL,
    )
    for name, value in pattern.findall(content):
        numeric = extract_numeric_value(value)
        if numeric is None:
            continue
        results.append({"name": name.strip(), "value": numeric, "unit": None})
    return results

def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").strip()

def extract_json_candidates(content: str) -> List[Any]:
    candidates: List[Any] = []
    script_pattern = re.compile(
        r'<script[^>]*type="application/(?:json|ld\+json)"[^>]*>(.*?)</script>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in script_pattern.findall(content):
        raw = match.strip()
        if not raw:
            continue
        try:
            candidates.append(json.loads(raw))
        except (json.JSONDecodeError, TypeError):
            continue

    inline_pattern = re.compile(
        r"(?:window\.__NUXT__|__NUXT__|__NEXT_DATA__)\s*=\s*(\{.*?\})\s*;",
        re.DOTALL,
    )
    for match in inline_pattern.findall(content):
        raw = match.strip()
        try:
            candidates.append(json.loads(raw))
        except (json.JSONDecodeError, TypeError):
            continue

    return candidates

def extract_dose_tab_recommendations(content: str) -> List[Dict[str, Any]]:
    blocks = re.findall(
        r"<(?:div|section)[^>]*(?:id|class)=\"[^\"]*(?:dose|dosage)[^\"]*\"[^>]*>(.*?)</(?:div|section)>",
        content,
        re.IGNORECASE | re.DOTALL,
    )
    recommendations: List[Dict[str, Any]] = []
    ignored_headings = {
        "very important for your aquarium",
        "important for your aquarium",
        "not important but beneficial for your aquarium",
        "fine-tuning level 1 ( basic )",
        "fine-tuning level 1 ( advanced )",
        "products to improve your aquarium",
    }
    for block in blocks:
        text = strip_html(block)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        current_label = None
        buffer: List[str] = []

        def flush() -> None:
            nonlocal buffer, current_label
            if not current_label:
                buffer = []
                return
            notes_text = " ".join(buffer).strip()
            if notes_text:
                recommendations.append(
                    {
                        "label": current_label,
                        "value": None,
                        "unit": None,
                        "notes": notes_text,
                    }
                )
            buffer = []

        idx = 0
        while idx < len(lines):
            line = lines[idx]
            lower_line = line.lower()
            if lower_line in ignored_headings:
                idx += 1
                continue
            if re.fullmatch(r"[A-Za-z0-9]{1,4}", line):
                symbol = line
                next_line = lines[idx + 1] if idx + 1 < len(lines) else ""
                label = symbol
                idx += 1
                if next_line and not re.fullmatch(r"[A-Za-z0-9]{1,4}", next_line):
                    next_lower = next_line.lower()
                    if not next_lower.startswith("we have detected"):
                        label = f"{symbol} {next_line}"
                        idx += 1
                flush()
                current_label = label
                continue
            if current_label and lower_line.startswith("we have detected"):
                idx += 1
                continue
            buffer.append(line)
            idx += 1
        flush()
    return recommendations

def parse_triton_recommendations(content: str) -> Dict[str, List[Dict[str, Any]]]:
    def normalize_help_item(item: Any) -> Optional[Dict[str, Any]]:
        if isinstance(item, str):
            text = strip_html(item)
            return {"label": text, "severity": None, "notes": None} if text else None
        if isinstance(item, dict):
            text = (
                item.get("message")
                or item.get("text")
                or item.get("title")
                or item.get("description")
                or item.get("label")
            )
            text = strip_html(str(text)) if text else ""
            if not text:
                return None
            severity = item.get("severity") or item.get("level") or item.get("status")
            return {"label": text, "severity": severity, "notes": None}
        return None

    def normalize_dose_item(item: Any) -> Optional[Dict[str, Any]]:
        if isinstance(item, str):
            text = strip_html(item)
            return {"label": text, "value": None, "unit": None, "notes": None} if text else None
        if isinstance(item, dict):
            label = (
                item.get("name")
                or item.get("label")
                or item.get("product")
                or item.get("additive")
            )
            value = item.get("dose") or item.get("dosage") or item.get("amount") or item.get("value")
            unit = item.get("unit") or item.get("uom")
            notes = item.get("notes") or item.get("message") or item.get("description")
            numeric = extract_numeric_value(value) if value is not None else None
            label = strip_html(str(label)) if label else ""
            notes = strip_html(str(notes)) if notes else None
            if not label and numeric is None:
                return None
            return {"label": label, "value": numeric, "unit": unit, "notes": notes}
        return None

    recommendations: Dict[str, List[Dict[str, Any]]] = {"help": [], "dose": []}
    dose_tab_recommendations = extract_dose_tab_recommendations(content)
    if dose_tab_recommendations:
        recommendations["dose"] = dose_tab_recommendations
    for candidate in extract_json_candidates(content):
        stack = [candidate]
        while stack:
            current = stack.pop()
            if isinstance(current, dict):
                for key, value in current.items():
                    lowered = str(key).lower()
                    if lowered in {"help", "warnings", "alerts"} and isinstance(value, list):
                        for item in value:
                            normalized = normalize_help_item(item)
                            if normalized:
                                recommendations["help"].append(normalized)
                    if "dose" in lowered and isinstance(value, list):
                        for item in value:
                            normalized = normalize_dose_item(item)
                            if normalized:
                                recommendations["dose"].append(normalized)
                    if isinstance(value, (dict, list)):
                        stack.append(value)
            elif isinstance(current, list):
                stack.extend(current)

    if not recommendations["help"]:
        for match in re.findall(r"help[^:]*:\s*([^\n<]+)", content, re.IGNORECASE):
            text = strip_html(match)
            if text:
                recommendations["help"].append({"label": text, "severity": None, "notes": None})

    if not recommendations["dose"]:
        table_pattern = re.compile(
            r"<tr[^>]*>\s*<t[dh][^>]*>(.*?)</t[dh]>\s*<t[dh][^>]*>(.*?)</t[dh]>\s*<t[dh][^>]*>(.*?)</t[dh]>\s*</tr>",
            re.IGNORECASE | re.DOTALL,
        )
        for name, amount, notes in table_pattern.findall(content):
            label = strip_html(name)
            amount_text = strip_html(amount)
            numeric = extract_numeric_value(amount_text)
            unit_match = re.search(r"[a-zA-Z]+/?[a-zA-Z]*", amount_text)
            unit = unit_match.group(0) if unit_match else None
            notes_text = strip_html(notes)
            if label or numeric is not None:
                recommendations["dose"].append(
                    {
                        "label": label,
                        "value": numeric,
                        "unit": unit,
                        "notes": notes_text or None,
                    }
                )

    if not recommendations["dose"]:
        dosage_block_pattern = re.compile(
            r"<(?:div|section)[^>]*(?:id|class)=\"[^\"]*dosage[^\"]*\"[^>]*>(.*?)</(?:div|section)>",
            re.IGNORECASE | re.DOTALL,
        )
        for block in dosage_block_pattern.findall(content):
            for name, amount, notes in re.findall(
                r"<tr[^>]*>\s*<t[dh][^>]*>(.*?)</t[dh]>\s*<t[dh][^>]*>(.*?)</t[dh]>\s*<t[dh][^>]*>(.*?)</t[dh]>\s*</tr>",
                block,
                re.IGNORECASE | re.DOTALL,
            ):
                label = strip_html(name)
                amount_text = strip_html(amount)
                numeric = extract_numeric_value(amount_text)
                unit_match = re.search(r"[a-zA-Z]+/?[a-zA-Z]*", amount_text)
                unit = unit_match.group(0) if unit_match else None
                notes_text = strip_html(notes)
                if label or numeric is not None:
                    recommendations["dose"].append(
                        {
                            "label": label,
                            "value": numeric,
                            "unit": unit,
                            "notes": notes_text or None,
                        }
                    )

    if not recommendations["dose"]:
        dose_pattern = re.compile(
            r"(?:dose|dosage)\s*[:\-]\s*([^<\n]+)",
            re.IGNORECASE,
        )
        for match in dose_pattern.findall(content):
            text = strip_html(match)
            numeric = extract_numeric_value(text)
            if numeric is None:
                continue
            recommendations["dose"].append({"label": "", "value": numeric, "unit": None, "notes": None})

    if recommendations["dose"]:
        filtered = []
        for item in recommendations["dose"]:
            label = (item.get("label") or "").strip()
            value = item.get("value")
            notes = item.get("notes") or ""
            if value is None and notes:
                value = extract_numeric_value(notes)
                if value is not None:
                    item["value"] = value
                unit_match = re.search(r"[a-zA-Z]+/?[a-zA-Z]*", notes)
                if unit_match and not item.get("unit"):
                    item["unit"] = unit_match.group(0)
            if value is None and notes:
                schedule_info = parse_icp_dose_schedule(notes)
                if schedule_info.get("total") is not None:
                    item["value"] = schedule_info["total"]
                    if not item.get("unit"):
                        item["unit"] = schedule_info.get("unit")
            if not label and notes:
                item["label"] = notes.split(":", 1)[0].strip()
            if not label and value is None:
                continue
            filtered.append(item)
        recommendations["dose"] = filtered

    return recommendations

def normalize_icp_name(name: str) -> str:
    normalized = normalize_probe_label(name)
    if normalized in ICP_ELEMENT_ALIASES:
        return ICP_ELEMENT_ALIASES[normalized]

    stripped = re.sub(r"\([^)]*\)", "", name).strip()
    stripped_normalized = normalize_probe_label(stripped)
    if stripped_normalized in ICP_ELEMENT_ALIASES:
        return ICP_ELEMENT_ALIASES[stripped_normalized]

    tokens: List[str] = []
    tokens.extend(re.findall(r"\(([^)]+)\)", name))
    tokens.append(name)
    for text in tokens:
        for token in re.split(r"[\s/,-]+", text):
            token_normalized = normalize_probe_label(token)
            if token_normalized in ICP_ELEMENT_ALIASES:
                return ICP_ELEMENT_ALIASES[token_normalized]

    return ICP_ELEMENT_ALIASES.get(stripped_normalized or normalized, stripped_normalized or normalized)

def map_icp_to_parameters(
    db: Connection,
    results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    params = q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY name")
    param_lookup = {normalize_probe_label(row_get(p, "name")): row_get(p, "name") for p in params}
    for param in params:
        symbol = row_get(param, "chemical_symbol")
        if symbol:
            param_lookup[normalize_probe_label(symbol)] = row_get(param, "name")
    mapped: List[Dict[str, Any]] = []
    for row in results:
        raw_name = row.get("name") or ""
        normalized = normalize_icp_name(raw_name)
        param_name = param_lookup.get(normalized)
        if not param_name:
            continue
        mapped.append(
            {
                "parameter": param_name,
                "value": row.get("value"),
                "unit": row.get("unit"),
                "source": raw_name,
            }
        )
    return mapped

def insert_icp_recommendations(
    db: Connection,
    sample_id: int,
    recommendations: Dict[str, List[Dict[str, Any]]],
) -> None:
    rows: List[Tuple[Any, ...]] = []
    for category, items in recommendations.items():
        for item in items:
            label = (item.get("label") or "").strip() or None
            value = item.get("value")
            unit = (item.get("unit") or "").strip() or None
            notes = (item.get("notes") or "").strip() or None
            severity = (item.get("severity") or "").strip() or None
            if not label and value is None and not notes:
                continue
            rows.append((sample_id, category, label, value, unit, notes, severity))
    if not rows:
        return
    db.executemany(
        """
        INSERT INTO icp_recommendations
        (sample_id, category, label, value, unit, notes, severity)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

def parse_icp_notes_range(notes: str) -> Dict[str, Optional[float]]:
    if not notes:
        return {"current": None, "target_low": None, "target_high": None, "unit": None}
    pattern = re.compile(
        r"(?P<current>-?[\d.,]+)\s*(?P<unit>[a-zA-Zµ/%]+)?\s*/\s*"
        r"(?P<low>-?[\d.,]+)\s*-\s*(?P<high>-?[\d.,]+)\s*(?P=unit)?",
        re.IGNORECASE,
    )
    match = pattern.search(notes)
    if not match:
        return {"current": None, "target_low": None, "target_high": None, "unit": None}
    current = extract_numeric_value(match.group("current"))
    low = extract_numeric_value(match.group("low"))
    high = extract_numeric_value(match.group("high"))
    unit = match.group("unit")
    return {"current": current, "target_low": low, "target_high": high, "unit": unit}

def parse_icp_dose_schedule(notes: str) -> Dict[str, Any]:
    if not notes:
        return {"days": 1, "doses": [], "total": None, "unit": None}
    days = 1
    days_match = re.search(r"over\s+(\d+)\s+days", notes, re.IGNORECASE)
    if days_match:
        days = int(days_match.group(1))
    doses: List[Tuple[int, float]] = []
    dose_pattern = r"(?:day\s*(\d+)|first day|last day).*?(\d+(?:[.,]\d+)?)\s*(ml|g|mg)"
    for match in re.finditer(dose_pattern, notes, re.IGNORECASE):
        day_raw = match.group(1)
        amount = extract_numeric_value(match.group(2))
        if amount is None:
            continue
        if day_raw:
            day = int(day_raw)
        else:
            text = match.group(0).lower()
            if "last day" in text:
                day = days
            else:
                day = 1
        doses.append((day, amount))
    doses = sorted({day: amount for day, amount in doses}.items())
    total_match = re.search(r"total[^\\d]*(\\d+(?:[.,]\\d+)?)\\s*(ml|g|mg)", notes, re.IGNORECASE)
    total = extract_numeric_value(total_match.group(1)) if total_match else None
    if total is None and doses:
        total = sum(amount for _, amount in doses)
    unit_match = re.search(r"\b(ml|g|mg)\b", notes.lower())
    unit = unit_match.group(1) if unit_match else None
    if not doses:
        single_match = re.search(r"corrective dosage of\s*(\d+(?:[.,]\d+)?)\s*(ml|g|mg)", notes, re.IGNORECASE)
        if single_match:
            amount = extract_numeric_value(single_match.group(1))
            if amount is not None:
                unit = single_match.group(2)
                doses = [(1, amount)]
                total = amount
                days = 1
    return {"days": days, "doses": doses, "total": total, "unit": unit}

def get_recent_param_values(
    db: Connection,
    tank_id: int,
    param_names: List[str],
    limit: int = 6,
) -> Dict[str, List[float]]:
    if not param_names:
        return {}
    samples = q(
        db,
        """
        SELECT id
        FROM samples
        WHERE tank_id=?
        ORDER BY taken_at DESC, id DESC
        LIMIT ?
        """,
        (tank_id, limit),
    )
    if not samples:
        return {}
    sample_ids = [row["id"] for row in samples]
    mode = values_mode(db)
    placeholders = ",".join("?" for _ in sample_ids)
    param_placeholders = ",".join("?" for _ in param_names)
    if mode == "sample_values":
        rows = q(
            db,
            f"""
            SELECT sv.sample_id, pd.name AS name, sv.value
            FROM sample_values sv
            JOIN parameter_defs pd ON pd.id = sv.parameter_id
            WHERE sv.sample_id IN ({placeholders}) AND pd.name IN ({param_placeholders})
            """,
            (*sample_ids, *param_names),
        )
    else:
        rows = q(
            db,
            f"""
            SELECT p.sample_id, p.name AS name, p.value
            FROM parameters p
            WHERE p.sample_id IN ({placeholders}) AND p.name IN ({param_placeholders})
            """,
            (*sample_ids, *param_names),
        )
    values_by_param: Dict[str, Dict[int, float]] = {name: {} for name in param_names}
    for row in rows:
        name = row_get(row, "name")
        value = row_get(row, "value")
        sample_id = row_get(row, "sample_id")
        if name is None or value is None or sample_id is None:
            continue
        try:
            values_by_param[str(name)][int(sample_id)] = float(value)
        except (ValueError, TypeError, KeyError):
            continue
    ordered_ids = list(reversed(sample_ids))
    series: Dict[str, List[float]] = {}
    for name, values_map in values_by_param.items():
        series_values = [values_map[sid] for sid in ordered_ids if sid in values_map]
        if series_values:
            series[name] = series_values
    return series

def get_overdue_tests(db: Connection, tank_id: int, user_id: Optional[int] = None) -> List[Dict[str, Any]]:
    now = datetime.now()
    overdue: List[Dict[str, Any]] = []
    pdefs = get_active_param_defs(db, user_id=user_id)
    latest_map = get_latest_and_previous_per_parameter(db, tank_id)
    for p in pdefs:
        name = row_get(p, "name")
        if not name:
            continue
        interval_days = row_get(p, "test_interval_days")
        if not interval_days:
            continue
        data = latest_map.get(name)
        if not data or not data.get("latest"):
            continue
        latest_taken = data["latest"].get("taken_at")
        if not latest_taken:
            continue
        try:
            if (now - latest_taken).days >= int(interval_days):
                overdue.append(
                    {
                        "parameter": name,
                        "last_tested": latest_taken,
                        "days_overdue": (now - latest_taken).days,
                    }
                )
        except Exception:
            continue
    return overdue

def users_exist(db: Connection) -> bool:
    row = one(db, "SELECT COUNT(*) AS count FROM users")
    return bool(row and row["count"] > 0)

def _prepare_sql(sql: str, params: Tuple[Any, ...] | Dict[str, Any] | None) -> Tuple[str, Dict[str, Any]]:
    if params is None:
        return sql, {}
    if isinstance(params, dict):
        return sql, params
    params_list = list(params)
    prepared_sql = sql
    bound_params: Dict[str, Any] = {}
    for idx, value in enumerate(params_list):
        key = f"p{idx}"
        prepared_sql = prepared_sql.replace("?", f":{key}", 1)
        bound_params[key] = value
    return prepared_sql, bound_params

class DBConnection:
    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._needs_rollback = False

    def _is_failed_transaction(self, exc: SQLAlchemyError) -> bool:
        orig = getattr(exc, "orig", None)
        return isinstance(orig, psycopg.errors.InFailedSqlTransaction)

    def execute(self, sql: str | TextClause, params: Tuple[Any, ...] | Dict[str, Any] | None = None):
        if self._needs_rollback:
            self._conn.rollback()
            self._needs_rollback = False
        try:
            if isinstance(sql, TextClause):
                return self._conn.execute(sql, params or {})
            prepared_sql, bound = _prepare_sql(sql, params)
            return self._conn.execute(text(prepared_sql), bound)
        except SQLAlchemyError as exc:
            self._needs_rollback = True
            self._conn.rollback()
            if self._is_failed_transaction(exc):
                self._needs_rollback = False
                if isinstance(sql, TextClause):
                    return self._conn.execute(sql, params or {})
                prepared_sql, bound = _prepare_sql(sql, params)
                return self._conn.execute(text(prepared_sql), bound)
            raise

    def commit(self) -> None:
        try:
            self._conn.commit()
            self._needs_rollback = False
        except SQLAlchemyError:
            self._needs_rollback = True
            self._conn.rollback()
            raise

    def rollback(self) -> None:
        self._conn.rollback()
        self._needs_rollback = False

    def close(self) -> None:
        self._conn.close()

    def executemany(self, sql: str, seq_of_params: List[Tuple[Any, ...]]) -> None:
        for params in seq_of_params:
            self.execute(sql, params)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        self.close()
        return False

    def __getattr__(self, name: str):
        return getattr(self._conn, name)

class CursorProxy:
    def __init__(self, db: DBConnection) -> None:
        self.db = db
        self.lastrowid: Optional[int] = None

    def execute(self, sql: str, params: Tuple[Any, ...] | Dict[str, Any] | None = None):
        result = self.db.execute(sql, params)
        try:
            lastrowid = result.lastrowid
        except AttributeError:
            lastrowid = None
        if lastrowid is not None:
            self.lastrowid = lastrowid
        return result

    def executemany(self, sql: str, seq_of_params: List[Tuple[Any, ...]]):
        for params in seq_of_params:
            self.execute(sql, params)

def cursor(db: DBConnection) -> CursorProxy:
    return CursorProxy(db)

def execute_with_retry(db: DBConnection, sql: str, params: Tuple[Any, ...] | Dict[str, Any] | None = None, attempts: int = 5) -> None:
    for idx in range(attempts):
        try:
            db.execute(sql, params)
            return
        except OperationalError as exc:
            if "locked" not in str(exc).lower() or idx == attempts - 1:
                raise
            time_module.sleep(0.2 * (idx + 1))

def execute_insert_returning_id(db: DBConnection, sql: str, params: Tuple[Any, ...] | Dict[str, Any] | None = None) -> Optional[int]:
    if engine.dialect.name == "postgresql":
        try:
            result = db.execute(f"{sql} RETURNING id", params)
        except IntegrityError as exc:
            orig = getattr(exc, "orig", None)
            constraint = getattr(getattr(orig, "diag", None), "constraint_name", None)
            if engine.dialect.name == "postgresql" and isinstance(orig, psycopg.errors.UniqueViolation):
                if "INSERT INTO samples" in sql and (constraint in (None, "samples_pkey")):
                    reset_samples_sequence(db)
                    result = db.execute(f"{sql} RETURNING id", params)
                elif "INSERT INTO tanks" in sql and (constraint in (None, "tanks_pkey")):
                    reset_tanks_sequence(db)
                    result = db.execute(f"{sql} RETURNING id", params)
                elif "INSERT INTO users" in sql and (constraint in (None, "users_pkey")):
                    reset_users_sequence(db)
                    result = db.execute(f"{sql} RETURNING id", params)
                else:
                    raise
            else:
                raise
        row = result.mappings().first()
        return row["id"] if row else None
    result = db.execute(sql, params)
    return result.lastrowid

def get_db() -> DBConnection:
    conn = engine.connect()
    if engine.dialect.name == "sqlite":
        conn.execute(text("PRAGMA journal_mode=WAL"))
        conn.execute(text("PRAGMA busy_timeout=30000"))
    return DBConnection(conn)

def q(db: DBConnection, sql: str, params: Tuple[Any, ...] | Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    result = db.execute(sql, params)
    return list(result.mappings().all())

def one(db: DBConnection, sql: str, params: Tuple[Any, ...] | Dict[str, Any] | None = None) -> Optional[Dict[str, Any]]:
    result = db.execute(sql, params)
    return result.mappings().first()

def row_get(row: Any, key: str, default: Any = None) -> Any:
    try:
        if row is None: return default
        if isinstance(row, dict): return row.get(key, default)
        if hasattr(row, 'keys') and key in row.keys(): return row[key]
    except Exception: pass
    return default

def table_exists(db: DBConnection, name: str) -> bool:
    inspector = inspect(db._conn)
    return inspector.has_table(name)

def reset_additives_sequence(db: DBConnection) -> None:
    if engine.dialect.name != "postgresql":
        return
    db.execute(
        """
        WITH max_id AS (
            SELECT MAX(id) AS max_id FROM additives
        )
        SELECT setval(
            pg_get_serial_sequence('additives', 'id'),
            COALESCE(max_id, 1),
            max_id IS NOT NULL
        )
        FROM max_id
        """
    )

def reset_parameter_defs_sequence(db: DBConnection) -> None:
    if engine.dialect.name != "postgresql":
        return
    db.execute(
        """
        WITH max_id AS (
            SELECT MAX(id) AS max_id FROM parameter_defs
        )
        SELECT setval(
            pg_get_serial_sequence('parameter_defs', 'id'),
            COALESCE(max_id, 1),
            max_id IS NOT NULL
        )
        FROM max_id
        """
    )

def reset_tanks_sequence(db: DBConnection) -> None:
    if engine.dialect.name != "postgresql":
        return
    db.execute(
        """
        WITH max_id AS (
            SELECT MAX(id) AS max_id FROM tanks
        )
        SELECT setval(
            pg_get_serial_sequence('tanks', 'id'),
            COALESCE(max_id, 1),
            max_id IS NOT NULL
        )
        FROM max_id
        """
    )

def reset_targets_sequence(db: DBConnection) -> None:
    if engine.dialect.name != "postgresql":
        return
    db.execute(
        """
        WITH max_id AS (
            SELECT MAX(id) AS max_id FROM targets
        )
        SELECT setval(
            pg_get_serial_sequence('targets', 'id'),
            COALESCE(max_id, 1),
            max_id IS NOT NULL
        )
        FROM max_id
        """
    )

def reset_samples_sequence(db: DBConnection) -> None:
    if engine.dialect.name != "postgresql":
        return
    db.execute(
        """
        WITH max_id AS (
            SELECT MAX(id) AS max_id FROM samples
        )
        SELECT setval(
            pg_get_serial_sequence('samples', 'id'),
            COALESCE(max_id, 1),
            max_id IS NOT NULL
        )
        FROM max_id
        """
    )

def reset_tank_journal_sequence(db: DBConnection) -> None:
    if engine.dialect.name != "postgresql":
        return
    db.execute(
        """
        WITH max_id AS (
            SELECT MAX(id) AS max_id FROM tank_journal
        )
        SELECT setval(
            pg_get_serial_sequence('tank_journal', 'id'),
            COALESCE(max_id, 1),
            max_id IS NOT NULL
        )
        FROM max_id
        """
    )

def reset_audit_logs_sequence(db: DBConnection) -> None:
    if engine.dialect.name != "postgresql":
        return
    db.execute(
        """
        WITH max_id AS (
            SELECT MAX(id) AS max_id FROM audit_logs
        )
        SELECT setval(
            pg_get_serial_sequence('audit_logs', 'id'),
            COALESCE(max_id, 1),
            max_id IS NOT NULL
        )
        FROM max_id
        """
    )

def reset_users_sequence(db: DBConnection) -> None:
    if engine.dialect.name != "postgresql":
        return
    db.execute(
        """
        WITH max_id AS (
            SELECT MAX(id) AS max_id FROM users
        )
        SELECT setval(
            pg_get_serial_sequence('users', 'id'),
            COALESCE(max_id, 1),
            max_id IS NOT NULL
        )
        FROM max_id
        """
    )

def insert_tank_journal(db: DBConnection, tank_id: int, entry_date: str, entry_type: str, title: str, notes: str) -> None:
    try:
        db.execute(
            "INSERT INTO tank_journal (tank_id, entry_date, entry_type, title, notes) VALUES (?, ?, ?, ?, ?)",
            (tank_id, entry_date, entry_type, title, notes),
        )
    except IntegrityError as exc:
        orig = getattr(exc, "orig", None)
        constraint = getattr(getattr(orig, "diag", None), "constraint_name", None)
        if engine.dialect.name == "postgresql" and isinstance(orig, psycopg.errors.UniqueViolation) and (constraint in (None, "tank_journal_pkey")):
            reset_tank_journal_sequence(db)
            db.execute(
                "INSERT INTO tank_journal (tank_id, entry_date, entry_type, title, notes) VALUES (?, ?, ?, ?, ?)",
                (tank_id, entry_date, entry_type, title, notes),
            )
        else:
            raise

def insert_audit_log(db: DBConnection, user_id: int, action: str, details: str, created_at: str) -> None:
    try:
        db.execute(
            "INSERT INTO audit_logs (actor_user_id, action, details, created_at) VALUES (?, ?, ?, ?)",
            (user_id, action, details, created_at),
        )
    except IntegrityError as exc:
        orig = getattr(exc, "orig", None)
        constraint = getattr(getattr(orig, "diag", None), "constraint_name", None)
        if engine.dialect.name == "postgresql" and isinstance(orig, psycopg.errors.UniqueViolation) and (constraint in (None, "audit_logs_pkey")):
            reset_audit_logs_sequence(db)
            db.execute(
                "INSERT INTO audit_logs (actor_user_id, action, details, created_at) VALUES (?, ?, ?, ?)",
                (user_id, action, details, created_at),
            )
        else:
            raise

def insert_parameter_def(db: DBConnection, name: str, unit: Optional[str], active: int, sort_order: int) -> None:
    try:
        db.execute(
            "INSERT INTO parameter_defs (name, unit, active, sort_order) VALUES (?, ?, ?, ?)",
            (name, unit, active, sort_order),
        )
    except IntegrityError as exc:
        orig = getattr(exc, "orig", None)
        constraint = getattr(getattr(orig, "diag", None), "constraint_name", None)
        if engine.dialect.name == "postgresql" and isinstance(orig, psycopg.errors.UniqueViolation) and constraint == "parameter_defs_pkey":
            reset_parameter_defs_sequence(db)
            db.execute(
                "INSERT INTO parameter_defs (name, unit, active, sort_order) VALUES (?, ?, ?, ?)",
                (name, unit, active, sort_order),
            )
        else:
            raise

def log_audit(db: Connection, user: Optional[Dict[str, Any]], action: str, details: Any = "") -> None:
    if not user:
        return
    if isinstance(details, (dict, list)):
        details = json.dumps(details, default=str)
    try:
        insert_audit_log(db, user["id"], action, str(details), datetime.utcnow().isoformat())
        db.commit()
    except Exception as exc:
        logger.warning(
            "Audit log write failed",
            extra={"audit_action": action, "audit_user_id": user.get("id") if user else None, "error": str(exc)},
        )

def ensure_column(db: DBConnection, table: str, col: str, ddl: str) -> None:
    inspector = inspect(db._conn)
    cols = {column["name"] for column in inspector.get_columns(table)}
    if col in cols:
        return  # Column already exists, no action needed
    if not ALLOW_RUNTIME_SCHEMA_CHANGES:
        raise RuntimeError(f"Runtime schema changes are disabled; missing {table}.{col}")
    db.execute(text(ddl))


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

USER_PARAMETER_FIELDS = (
    "max_daily_change",
    "test_interval_days",
    "default_target_low",
    "default_target_high",
    "default_alert_low",
    "default_alert_high",
)

def get_user_parameter_settings(db: Connection, user_id: Optional[int]) -> Dict[int, Dict[str, Any]]:
    if not user_id or not table_exists(db, "user_parameter_settings"):
        return {}
    rows = q(db, "SELECT * FROM user_parameter_settings WHERE user_id=?", (user_id,))
    return {int(row["parameter_id"]): row for row in rows if row_get(row, "parameter_id") is not None}

def apply_user_parameter_overrides(
    rows: List[Dict[str, Any]],
    overrides: Dict[int, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not overrides:
        return rows
    merged_rows = []
    for row in rows:
        merged = dict(row)
        override = overrides.get(row_get(row, "id"))
        if override:
            for field in USER_PARAMETER_FIELDS:
                value = row_get(override, field)
                if value is not None:
                    merged[field] = value
        merged_rows.append(merged)
    return merged_rows

def list_parameters(db: Connection, user_id: Optional[int] = None) -> List[Dict[str, Any]]:
    if table_exists(db, "parameter_defs"):
        rows = q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY sort_order, name")
        if user_id:
            overrides = get_user_parameter_settings(db, user_id)
            rows = apply_user_parameter_overrides(rows, overrides)
        return rows
    if table_exists(db, "parameters"):
        rows = q(db, "SELECT DISTINCT name, COALESCE(unit,'') AS unit FROM parameters ORDER BY name")
        return [{"id": slug_key(r["name"]), "name": r["name"], "unit": r["unit"]} for r in rows]
    return []

def get_active_param_defs(db: Connection, user_id: Optional[int] = None) -> List[Dict[str, Any]]:
    return list_parameters(db, user_id=user_id)

def parse_dt_any(v):
    if v is None: return None
    if isinstance(v, datetime): return v
    if isinstance(v, date) and not isinstance(v, datetime): return datetime.combine(v, datetime_time.min)
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

def fetch_apex_readings(settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    client = ApexClient(
        host=settings.get("host", ""),
        username=settings.get("username") or None,
        password=settings.get("password") or None,
        api_token=settings.get("api_token") or None,
    )
    readings = client.fetch_readings()
    normalized: List[Dict[str, Any]] = []
    for reading in readings:
        normalized.append(
            {
                "name": reading.name,
                "value": reading.value,
                "unit": reading.unit or "",
                "timestamp": reading.timestamp,
            }
        )
    return normalized

def map_apex_readings(settings: Dict[str, Any], readings: List[Dict[str, Any]]) -> List[Tuple[str, Dict[str, Any]]]:
    mapping_raw = settings.get("mapping") or {}
    if not mapping_raw:
        raise ValueError("Apex mapping is required.")
    mapping = {str(k).strip().lower(): str(v).strip() for k, v in mapping_raw.items() if k and v}
    if not mapping:
        raise ValueError("Apex mapping is required.")
    mapped: List[Tuple[str, Dict[str, Any]]] = []
    for reading in readings:
        probe_name = str(reading.get("name", "")).strip()
        if not probe_name:
            continue
        param_name = mapping.get(probe_name.lower())
        if not param_name:
            continue
        mapped.append((param_name, reading))
    if not mapped:
        raise ValueError("No Apex probes matched the mapping.")
    return mapped

def normalize_apex_readings(readings: List[Any]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for reading in readings:
        normalized.append(
            {
                "name": reading.name,
                "value": reading.value,
                "unit": reading.unit or "",
                "timestamp": reading.timestamp,
            }
        )
    return normalized

def map_apex_readings_with_mapping(readings: List[Dict[str, Any]], mapping_raw: Dict[str, str]) -> List[Tuple[str, Dict[str, Any]]]:
    if not mapping_raw:
        raise ValueError("Apex mapping is required.")
    mapping = {str(k).strip().lower(): str(v).strip() for k, v in mapping_raw.items() if k and v}
    if not mapping:
        raise ValueError("Apex mapping is required.")
    mapped: List[Tuple[str, Dict[str, Any]]] = []
    for reading in readings:
        probe_name = str(reading.get("name", "")).strip()
        if not probe_name:
            continue
        param_name = mapping.get(probe_name.lower())
        if not param_name:
            continue
        mapped.append((param_name, reading))
    if not mapped:
        raise ValueError("No Apex probes matched the mapping.")
    return mapped

def preview_apex_mapping(settings: Dict[str, Any], mapping_raw: Dict[str, str]) -> List[Dict[str, Any]]:
    if not settings.get("host"):
        raise ValueError("Apex host is required.")
    readings = fetch_apex_readings(settings)
    mapping = {str(k).strip().lower(): str(v).strip() for k, v in mapping_raw.items() if k and v}
    if not mapping:
        raise ValueError("Apex mapping is required.")
    preview: List[Dict[str, Any]] = []
    for reading in readings:
        probe_name = str(reading.get("name", "")).strip()
        if not probe_name:
            continue
        param_name = mapping.get(probe_name.lower())
        if not param_name:
            continue
        preview.append(
            {
                "probe": probe_name,
                "parameter": param_name,
                "value": reading.get("value"),
                "unit": reading.get("unit", ""),
                "timestamp": reading.get("timestamp"),
            }
        )
    if not preview:
        raise ValueError("No Apex probes matched the mapping.")
    return preview

def values_mode(db: Connection) -> str:
    try:
        inspector = inspect(db._conn)
        if inspector.has_table("sample_values") and inspector.has_table("parameter_defs"):
            return "sample_values"
    except Exception: pass
    return "parameters"

def format_value(p: Any, v: Any) -> str:
    if v is None: return ""
    try: fv = float(v)
    except Exception: return str(v)
    s = f"{fv:.2f}"
    if "." in s: s = s.rstrip("0").rstrip(".")
    return s

def get_sample_readings(db: Connection, sample_id: int) -> List[Dict[str, Any]]:
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

def insert_sample_reading(db: Connection, sample_id: int, pname: str, value: float, unit: str = "", test_kit_id: int | None = None) -> None:
    mode = values_mode(db)
    if mode == "sample_values":
        pd = one(
            db,
            "SELECT id FROM parameter_defs WHERE LOWER(TRIM(name))=LOWER(TRIM(?))",
            (pname,),
        )
        if not pd:
            return
        pid = pd["id"]
        if pid is None:
            return
        execute_with_retry(
            db,
            "INSERT INTO sample_values (sample_id, parameter_id, value) VALUES (?, ?, ?)",
            (sample_id, pid, value),
        )
        if test_kit_id:
            execute_with_retry(
                db,
                "INSERT INTO sample_value_kits (sample_id, parameter_id, test_kit_id) VALUES (?, ?, ?) "
                "ON CONFLICT (sample_id, parameter_id) DO UPDATE SET test_kit_id=excluded.test_kit_id",
                (sample_id, pid, test_kit_id),
            )
    else:
        execute_with_retry(
            db,
            "INSERT INTO parameters (sample_id, name, value, unit, test_kit_id) VALUES (?, ?, ?, ?, ?)",
            (sample_id, pname, value, unit or None, test_kit_id),
        )

def get_latest_sample_values(db: Connection, tank_id: int, param_names: List[str]) -> Dict[str, float]:
    if not param_names:
        return {}
    mode = values_mode(db)
    latest = one(db, "SELECT id FROM samples WHERE tank_id=? ORDER BY taken_at DESC, id DESC LIMIT 1", (tank_id,))
    if not latest:
        return {}
    sample_id = latest["id"]
    if mode == "sample_values":
        placeholders = ",".join("?" for _ in param_names)
        rows = q(
            db,
            f"""
            SELECT pd.name AS name, sv.value AS value
            FROM sample_values sv
            JOIN parameter_defs pd ON pd.id = sv.parameter_id
            WHERE sv.sample_id=? AND pd.name IN ({placeholders})
            """,
            (sample_id, *param_names),
        )
    else:
        placeholders = ",".join("?" for _ in param_names)
        rows = q(
            db,
            f"""
            SELECT name, value
            FROM parameters
            WHERE sample_id=? AND name IN ({placeholders})
            """,
            (sample_id, *param_names),
        )
    latest_values: Dict[str, float] = {}
    for row in rows:
        name = row_get(row, "name")
        value = row_get(row, "value")
        if name is None or value is None:
            continue
        try:
            latest_values[str(name)] = float(value)
        except Exception:
            continue
    return latest_values

def should_import_apex_sample(
    db: Connection,
    tank_id: int,
    mapped: List[Tuple[str, Dict[str, Any]]],
) -> Tuple[bool, str]:
    tracked_params = {"Alkalinity/KH", "Calcium", "Magnesium"}
    tracked_readings = [(param, reading) for param, reading in mapped if param in tracked_params]
    if not tracked_readings:
        return True, ""
    latest_values = get_latest_sample_values(db, tank_id, list(tracked_params))
    if not latest_values:
        return True, ""
    for param_name, reading in tracked_readings:
        value = reading.get("value")
        if value is None:
            continue
        try:
            current_value = float(value)
        except Exception:
            continue
        previous_value = latest_values.get(param_name)
        if previous_value is None:
            return True, ""
        if abs(current_value - previous_value) > 1e-6:
            return True, ""
    return False, "Alkalinity, Calcium, and Magnesium unchanged from last sample."

def import_apex_sample(db: Connection, settings: Dict[str, Any], user: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not settings.get("host"):
        raise ValueError("Apex host is required.")
    readings = fetch_apex_readings(settings)
    mapped = map_apex_readings(settings, readings)
    return import_apex_sample_from_mapped(db, settings, mapped, user)

def import_apex_sample_from_mapped(
    db: Connection,
    settings: Dict[str, Any],
    mapped: List[Tuple[str, Dict[str, Any]]],
    user: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    tank_ids = []
    if settings.get("tank_ids"):
        tank_ids = [int(tid) for tid in settings.get("tank_ids") if str(tid).isdigit()]
    elif settings.get("tank_id"):
        tank_ids = [int(settings.get("tank_id"))]
    if not tank_ids:
        tank = one(db, "SELECT * FROM tanks ORDER BY id LIMIT 1")
        if not tank:
            raise ValueError("No tank available to import Apex readings.")
        tank_ids = [tank["id"]]
    results = []
    taken_at = None
    for _, reading in mapped:
        dt = parse_dt_any(reading.get("timestamp"))
        if dt:
            taken_at = dt
            break
    when_iso = (taken_at or datetime.utcnow()).isoformat()
    for tank_id in tank_ids:
        tank = one(db, "SELECT * FROM tanks WHERE id=?", (tank_id,))
        if not tank:
            continue
        should_import, reason = should_import_apex_sample(db, tank["id"], mapped)
        if not should_import:
            results.append({"tank_id": tank["id"], "skipped": True, "reason": reason})
            continue
        sample_id = execute_insert_returning_id(
            db,
            "INSERT INTO samples (tank_id, taken_at, notes) VALUES (?, ?, ?)",
            (tank["id"], when_iso, "Imported from Apex"),
        )
        if sample_id is None:
            continue
        for param_name, reading in mapped:
            value = reading.get("value")
            if value is None:
                continue
            insert_sample_reading(db, sample_id, param_name, float(value), reading.get("unit", ""))
        db.commit()
        log_audit(db, user, "apex-import", {"tank_id": tank["id"], "sample_id": sample_id})
        results.append({"sample_id": sample_id, "tank_id": tank["id"], "mapped_count": len(mapped), "skipped": False})
    return {"imports": results, "mapped_count": len(mapped)}

def run_apex_polling() -> None:
    """Run one iteration of Apex polling - called by scheduler"""
    db = get_db()
    try:
        integrations = get_apex_integrations(db)
        for settings in integrations:
            enabled = bool(settings.get("enabled"))
            if enabled and settings.get("host") and settings.get("mapping"):
                try:
                    import_apex_sample(db, settings, None)
                except Exception as exc:
                    logger.error(f"Apex polling error: {exc}", exc_info=True)
    finally:
        db.close()

def get_sample_kits(db: Connection, sample_id: int) -> Dict[str, int]:
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

def parse_duration_seconds(raw: str) -> Optional[int]:
    if not raw:
        return None
    cleaned = raw.strip().lower()
    if not cleaned:
        return None
    if ":" in cleaned:
        parts = [p for p in cleaned.split(":") if p.strip()]
        if len(parts) == 2:
            minutes = to_float(parts[0])
            seconds = to_float(parts[1])
            if minutes is None or seconds is None:
                return None
            return int(minutes * 60 + seconds)
    match = re.match(r"^(\d+(?:\.\d+)?)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes)?$", cleaned)
    if match:
        value = to_float(match.group(1))
        unit = match.group(2) or "s"
        if value is None:
            return None
        if unit.startswith("m"):
            return int(value * 60)
        return int(value)
    value = to_float(cleaned)
    if value is None:
        return None
    return int(value)

def parse_workflow_steps(text: str) -> List[Dict[str, Any]]:
    steps: List[Dict[str, Any]] = []
    if not text:
        return steps
    cleaned = text.replace("\t", " ")
    for line in cleaned.splitlines():
        if not line.strip():
            continue
        if "|" in line:
            parts = [p.strip() for p in line.split("|", 1)]
        elif "," in line:
            parts = [p.strip() for p in line.split(",", 1)]
        else:
            parts = [line.strip()]
        label = parts[0]
        if not label:
            continue
        seconds = None
        if len(parts) > 1 and parts[1]:
            seconds = parse_duration_seconds(parts[1])
        steps.append({"label": label, "seconds": seconds})
    return steps

def format_workflow_steps(steps: List[Dict[str, Any]]) -> str:
    lines = []
    for step in steps:
        label = str(step.get("label") or "").strip()
        if not label:
            continue
        seconds = step.get("seconds")
        if seconds is None:
            lines.append(label)
            continue
        minutes = int(seconds) // 60
        remainder = int(seconds) % 60
        if minutes and remainder:
            duration = f"{minutes}m {remainder}s"
        elif minutes:
            duration = f"{minutes}m"
        else:
            duration = f"{remainder}s"
        lines.append(f"{label} | {duration}")
    return "\n".join(lines)

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

def get_test_kit_conversion(db: Connection, kit_id: int | None) -> Tuple[Optional[str], Optional[str]]:
    if not kit_id:
        return None, None
    kit = one(db, "SELECT conversion_type, conversion_data FROM test_kits WHERE id=?", (kit_id,))
    if not kit:
        return None, None
    return kit["conversion_type"], kit["conversion_data"]

# --- NEW HELPER: Get the Latest Reading per Parameter ---
def get_latest_per_parameter(db: Connection, tank_id: int) -> Dict[str, Dict[str, Any]]:
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

def get_latest_and_previous_per_parameter(db: Connection, tank_id: int) -> Dict[str, Dict[str, Any]]:
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

def init_db() -> None:
    db = get_db()
    try:
        from database import Base

        Base.metadata.create_all(engine)
        if engine.dialect.name == "postgresql" and table_exists(db, "icp_uploads"):
            execute_with_retry(db, "CREATE SEQUENCE IF NOT EXISTS icp_uploads_id_seq")
            execute_with_retry(
                db,
                "ALTER TABLE icp_uploads ALTER COLUMN id SET DEFAULT nextval('icp_uploads_id_seq')",
            )
            execute_with_retry(
                db,
                "SELECT setval('icp_uploads_id_seq', COALESCE((SELECT MAX(id) FROM icp_uploads), 0) + 1, false)",
            )
        if engine.dialect.name == "postgresql":
            if table_exists(db, "additives"):
                reset_additives_sequence(db)
            if table_exists(db, "parameter_defs"):
                reset_parameter_defs_sequence(db)
            if table_exists(db, "tanks"):
                reset_tanks_sequence(db)
            if table_exists(db, "targets"):
                reset_targets_sequence(db)
            if table_exists(db, "samples"):
                reset_samples_sequence(db)
            if table_exists(db, "tank_journal"):
                reset_tank_journal_sequence(db)
            if table_exists(db, "audit_logs"):
                reset_audit_logs_sequence(db)

        if table_exists(db, "user_tanks"):
            execute_with_retry(
                db,
                "INSERT INTO user_tanks (user_id, tank_id) "
                "SELECT owner_user_id, id FROM tanks WHERE owner_user_id IS NOT NULL "
                "ON CONFLICT (user_id, tank_id) DO NOTHING",
            )

        count_row = one(db, "SELECT COUNT(1) AS count FROM parameter_defs")
        count = count_row["count"] if count_row else 0
        if count == 0:
            defaults = [
                ("Alkalinity/KH", "dKH", 1, 10),
                ("Calcium", "ppm", 1, 20),
                ("Magnesium", "ppm", 1, 30),
                ("Phosphate", "ppm", 1, 40),
                ("Nitrate", "ppm", 1, 50),
                ("Salinity", "ppt", 1, 60),
                ("Temperature", "°C", 1, 70),
                ("Trace Elements", None, 1, 80),
            ]
            for entry in defaults:
                insert_parameter_def(db, entry[0], entry[1], entry[2], entry[3])
        else:
            trace = one(db, "SELECT id FROM parameter_defs WHERE name=?", ("Trace Elements",))
            if trace is None:
                insert_parameter_def(db, "Trace Elements", None, 1, 80)

        for name, d in INITIAL_DEFAULTS.items():
            execute_with_retry(
                db,
                """
                UPDATE parameter_defs
                SET default_target_low=?, default_target_high=?, default_alert_low=?, default_alert_high=?
                WHERE name=? AND default_target_low IS NULL
                """,
                (
                    d["default_target_low"],
                    d["default_target_high"],
                    d["default_alert_low"],
                    d["default_alert_high"],
                    name,
                ),
            )

        db.commit()
    finally:
        db.close()

init_db()

@app.on_event("startup")
def start_background_jobs() -> None:
    logger.info(
        "Security settings: PASSWORD_HASH_ITERATIONS=%s SESSION_ROTATE_ON_LOGIN=%s",
        PASSWORD_HASH_ITERATIONS,
        SESSION_ROTATE_ON_LOGIN,
    )
    if not RUN_BACKGROUND_JOBS:
        logger.info("Background jobs disabled by RUN_BACKGROUND_JOBS setting.")
        return
    if BACKGROUND_JOB_LOCK_PATH:
        lock_handle = open(BACKGROUND_JOB_LOCK_PATH, "a+")
        try:
            fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            logger.info("Background job lock is held; skipping scheduler startup.")
            lock_handle.close()
            return
        app.state.background_job_lock = lock_handle

    # Schedule recurring jobs
    scheduler.add_job(
        run_daily_summary_check,
        'interval',
        hours=1,
        id='daily_summaries',
        replace_existing=True
    )
    start_push_notification_scheduler()
    scheduler.add_job(
        run_apex_polling,
        'interval',
        minutes=15,
        id='apex_polling',
        replace_existing=True
    )

@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    """Add security headers to all responses"""
    response = await call_next(request)

    # Cache control for static assets
    if request.url.path.startswith("/static/"):
        # Cache static assets for 1 year (immutable resources)
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    else:
        # No caching for dynamic content
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, private"

    # Prevent MIME type sniffing
    response.headers["X-Content-Type-Options"] = "nosniff"

    # Prevent clickjacking
    response.headers["X-Frame-Options"] = "SAMEORIGIN"

    # Enable browser XSS protection
    response.headers["X-XSS-Protection"] = "1; mode=block"

    # Control referrer information
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

    # Restrict feature access
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"

    # HSTS for HTTPS connections
    if request.url.scheme == "https":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

    # Content Security Policy
    csp = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.jsdelivr.net https://www.googletagmanager.com; "
        "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "img-src 'self' data: https:; "
        "font-src 'self' data:; "
        "connect-src 'self' https://www.google-analytics.com; "
        "frame-ancestors 'self'; "
        "base-uri 'self'; "
        "form-action 'self'"
    )
    response.headers["Content-Security-Policy"] = csp

    return response

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    request.state.request_id = request_id
    path = request.url.path
    csrf_token = ensure_csrf_token(request.cookies.get("csrf_token"))
    if request.method in {"POST", "PUT", "PATCH", "DELETE"} and not path.startswith("/static"):
        csrf_exempt = is_csrf_exempt(path)
        if not csrf_exempt:
            if request.headers.get("Authorization", "").startswith("Bearer "):
                pass
            else:
                request_token = await extract_csrf_token(request)
                if not request_token or request_token != csrf_token:
                    response = JSONResponse({"detail": "Invalid CSRF token"}, status_code=403)
                    response.headers["X-Request-Id"] = request_id
                    response.set_cookie("csrf_token", csrf_token, httponly=False, samesite="lax")
                    return response
    if path.startswith(("/static", "/auth")) or path.startswith("/favicon"):
        start_time = time_module.time()
        response = await call_next(request)
        log_payload = {
            "event": "request",
            "method": request.method,
            "path": path,
            "status": response.status_code,
            "duration_ms": round((time_module.time() - start_time) * 1000, 2),
            "request_id": request_id,
            "user_id": None,
            "tank_id": request.query_params.get("tank_id"),
        }
        logger.info(json.dumps(log_payload))
        response.headers["X-Request-Id"] = request_id
        response.set_cookie("csrf_token", csrf_token, httponly=False, samesite="lax")
        return response
    db = get_db()
    user = None
    start_time = time_module.time()
    try:
        user = get_current_user(db, request)
        request.state.user = user
        if users_exist(db) and user is None:
            response = redirect("/auth/login")
            log_payload = {
                "event": "request",
                "method": request.method,
                "path": path,
                "status": response.status_code,
                "duration_ms": round((time_module.time() - start_time) * 1000, 2),
                "request_id": request_id,
                "user_id": None,
                "tank_id": request.path_params.get("tank_id") or request.query_params.get("tank_id"),
            }
            logger.info(json.dumps(log_payload))
            response.headers["X-Request-Id"] = request_id
            return response
        response = await call_next(request)
        if user:
            log_audit(
                db,
                user,
                f"{request.method} {path}",
                {"status": response.status_code},
            )
        tank_id = request.path_params.get("tank_id") or request.query_params.get("tank_id")
        log_payload = {
            "event": "request",
            "method": request.method,
            "path": path,
            "status": response.status_code,
            "duration_ms": round((time_module.time() - start_time) * 1000, 2),
            "request_id": request_id,
            "user_id": user["id"] if user else None,
            "tank_id": tank_id,
        }
        logger.info(json.dumps(log_payload))
        response.headers["X-Request-Id"] = request_id
        response.set_cookie("csrf_token", csrf_token, httponly=False, samesite="lax")
        return response
    finally:
        db.close()

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    tanks = get_visible_tanks(db, request)
    tank_cards = []
    reminders: List[Dict[str, Any]] = []
    pdefs = {p["name"]: p for p in get_active_param_defs(db, user_id=user["id"] if user else None)}
    available_params = [
        {
            "name": name,
            "unit": (row_get(pdef, "unit") or ""),
            "key": slug_key(name),
        }
        for name, pdef in pdefs.items()
    ]
    available_params.sort(key=lambda item: item["name"].lower())

    # Bulk-fetch data for all tanks to avoid N+1 queries
    tank_ids = [t["id"] for t in tanks]
    if tank_ids:
        placeholders = ",".join("?" for _ in tank_ids)
        # Fetch all targets for all tanks
        all_targets = q(db, f"SELECT * FROM targets WHERE tank_id IN ({placeholders}) AND enabled=1", tuple(tank_ids))
        targets_by_tank = {}
        for tr in all_targets:
            tank_id = tr["tank_id"]
            if tank_id not in targets_by_tank:
                targets_by_tank[tank_id] = {}
            targets_by_tank[tank_id][tr["parameter"]] = tr

        # Fetch latest samples for all tanks
        latest_samples = q(db, f"""
            SELECT s.* FROM samples s
            INNER JOIN (
                SELECT tank_id, MAX(taken_at) as max_taken
                FROM samples
                WHERE tank_id IN ({placeholders})
                GROUP BY tank_id
            ) latest ON s.tank_id = latest.tank_id AND s.taken_at = latest.max_taken
        """, tuple(tank_ids))
        latest_by_tank = {s["tank_id"]: s for s in latest_samples}
    else:
        targets_by_tank = {}
        latest_by_tank = {}

    for t in tanks:
        latest_map = get_latest_and_previous_per_parameter(db, t["id"])
        targets = targets_by_tank.get(t["id"], {})
        latest = latest_by_tank.get(t["id"])
        history_map = get_recent_param_values(db, t["id"], list(pdefs.keys()))
        
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
                reminders.append(
                    {
                        "tank_id": t["id"],
                        "tank_name": t["name"],
                        "parameter": pname,
                        "last_tested": latest_taken,
                    }
                )
            sparkline_values = history_map.get(pname, [])
            readings.append({
                "name": pname,
                "key": slug_key(pname),
                "value": latest_val,
                "unit": (row_get(p, "unit") or ""),
                "taken_at": latest_taken,
                "status": status,
                "trend_warning": trend_warning,
                "delta_per_day": delta_per_day,
                "overdue": overdue,
                "sparkline": build_sparkline_points(sparkline_values),
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
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "tank_cards": tank_cards,
            "reminders": reminders,
            "available_params": available_params,
            "extra_css": ["/static/dashboard.css"],
        },
    )

@app.get("/health")
def health_check() -> JSONResponse:
    try:
        # Check database connectivity
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        # Get scheduler status
        scheduler_running = scheduler.running if hasattr(scheduler, 'running') else False
        scheduled_jobs = len(scheduler.get_jobs()) if scheduler_running else 0

        return JSONResponse({
            "status": "ok",
            "database": "connected",
            "scheduler": {
                "running": scheduler_running,
                "jobs": scheduled_jobs
            }
        })
    except Exception as exc:
        logger.error(json.dumps({"event": "health_check_failed", "error": str(exc)}))
        return JSONResponse({"status": "error", "detail": "database unavailable"}, status_code=503)

@app.get("/debug/sentry")
def sentry_test() -> JSONResponse:
    allow_test = os.environ.get("ALLOW_SENTRY_TEST", "").lower() in {"1", "true", "yes", "on"}
    if not allow_test:
        raise HTTPException(status_code=404, detail="Not found")
    raise RuntimeError("Sentry test endpoint triggered")

@app.get("/insights", response_class=HTMLResponse)
def insights(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    tanks = get_visible_tanks(db, request)
    tank_ids = [t["id"] for t in tanks]
    total_tanks = len(tanks)
    total_samples_week = 0
    overdue_items: List[Dict[str, Any]] = []
    volatility: List[Dict[str, Any]] = []
    tank_summaries: List[Dict[str, Any]] = []
    samples_week_map: Dict[int, int] = {}
    last_sample_map: Dict[int, Optional[str]] = {}
    volatility_count_map: Dict[int, int] = {}
    if tank_ids:
        placeholders = ",".join("?" for _ in tank_ids)
        week_start = (datetime.utcnow() - timedelta(days=7)).isoformat()
        total_samples_week = one(
            db,
            f"SELECT COUNT(*) AS count FROM samples WHERE tank_id IN ({placeholders}) AND taken_at>=?",
            (*tank_ids, week_start),
        )["count"]
        sample_rows = q(
            db,
            f"""SELECT tank_id, COUNT(*) AS count
                FROM samples
                WHERE tank_id IN ({placeholders}) AND taken_at>=?
                GROUP BY tank_id""",
            (*tank_ids, week_start),
        )
        for row in sample_rows:
            tank_id_value = row_get(row, "tank_id")
            if tank_id_value is None:
                continue
            samples_week_map[int(tank_id_value)] = int(row_get(row, "count") or 0)
        last_sample_rows = q(
            db,
            f"""SELECT tank_id, MAX(taken_at) AS last_taken
                FROM samples
                WHERE tank_id IN ({placeholders})
                GROUP BY tank_id""",
            tuple(tank_ids),
        )
        for row in last_sample_rows:
            tank_id_value = row_get(row, "tank_id")
            if tank_id_value is None:
                continue
            last_sample_map[int(tank_id_value)] = row_get(row, "last_taken")
        for tank in tanks:
            overdue_list = get_overdue_tests(db, tank["id"], user_id=user["id"] if user else None)
            overdue_items.extend(
                [
                    {
                        "tank_id": tank["id"],
                        "tank_name": tank["name"],
                        **item,
                    }
                    for item in overdue_list
                ]
            )
            volatility_count_map[int(tank["id"])] = 0
            latest_map = get_latest_and_previous_per_parameter(db, tank["id"])
            for param_name, data in latest_map.items():
                latest = data.get("latest")
                previous = data.get("previous")
                if not latest or not previous:
                    continue
                latest_taken = latest.get("taken_at")
                previous_taken = previous.get("taken_at")
                if not latest_taken or not previous_taken:
                    continue
                try:
                    delta = float(latest.get("value")) - float(previous.get("value"))
                    days = max((latest_taken - previous_taken).total_seconds() / 86400.0, 1e-6)
                    volatility.append(
                        {
                            "tank_id": tank["id"],
                            "tank_name": tank["name"],
                            "parameter": param_name,
                            "delta_per_day": delta / days,
                        }
                    )
                    volatility_count_map[int(tank["id"])] += 1
                except Exception:
                    continue
            tank_id_value = int(tank["id"])
            tank_summaries.append(
                {
                    "tank_id": tank_id_value,
                    "tank_name": tank["name"],
                    "last_sample": last_sample_map.get(tank_id_value),
                    "samples_week": samples_week_map.get(tank_id_value, 0),
                    "overdue_count": len(overdue_list),
                    "volatility_count": volatility_count_map.get(tank_id_value, 0),
                }
            )
    volatility_sorted = sorted(volatility, key=lambda v: abs(v["delta_per_day"]), reverse=True)[:5]
    db.close()
    return templates.TemplateResponse(
        "insights.html",
        {
            "request": request,
            "total_tanks": total_tanks,
            "total_samples_week": total_samples_week,
            "overdue_items": overdue_items,
            "volatility": volatility_sorted,
            "tank_summaries": tank_summaries,
        },
    )

@app.get("/alerts", response_class=HTMLResponse)
def alerts_center(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    if row_get(user, "admin"):
        alerts = collect_dosing_notifications(db, actor_user=user)
    else:
        alerts = collect_dosing_notifications(db, owner_user_id=user["id"], actor_user=user)
    tanks = q(db, "SELECT id, name FROM tanks ORDER BY name")
    db.close()
    return templates.TemplateResponse(
        "alerts.html",
        {
            "request": request,
            "alerts": alerts,
            "tanks": tanks,
        },
    )

@app.get("/tanks/{tank_id}/snapshot", response_class=HTMLResponse)
def tank_snapshot(request: Request, tank_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    latest_sample = one(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC LIMIT 1", (tank_id,))
    readings: List[Dict[str, Any]] = []
    if latest_sample:
        for row in get_sample_readings(db, latest_sample["id"]):
            readings.append(
                {
                    "name": row_get(row, "name"),
                    "value": row_get(row, "value"),
                    "unit": row_get(row, "unit"),
                }
            )
    db.close()
    return templates.TemplateResponse(
        "tank_snapshot.html",
        {
            "request": request,
            "tank": tank,
            "latest_sample": latest_sample,
            "readings": readings,
        },
    )

@app.get("/pwa/checklist", response_class=HTMLResponse)
def pwa_checklist(request: Request):
    return templates.TemplateResponse("pwa_checklist.html", {"request": request})

@app.get("/auth/login", response_class=HTMLResponse)
def login_form(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/auth/login")
@limiter.limit("10/minute")
async def login_submit(request: Request):
    form = await request.form()
    email = (form.get("email") or "").strip().lower()
    password = form.get("password") or ""
    rotate_requested = "rotate_sessions" in form
    rotate_sessions = (form.get("rotate_sessions") or "").lower() in {"1", "true", "on", "yes"}
    db = get_db()
    user = one(db, "SELECT * FROM users WHERE email=?", (email,))
    if not user or not user["password_hash"]:
        db.close()
        return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid email or password."})
    if not verify_password(password, user["password_hash"], user["password_salt"]):
        db.close()
        return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid email or password."})
    rotate_existing = rotate_sessions if rotate_requested else None
    token = create_session(db, user["id"], rotate_existing=rotate_existing)
    db.close()
    response = redirect("/")
    response.set_cookie("session_token", token, httponly=True, samesite="lax", max_age=60 * 60 * 24 * SESSION_LIFETIME_DAYS)
    return response

@app.get("/auth/register", response_class=HTMLResponse)
def register_form(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})

@app.post("/auth/register")
@limiter.limit("5/minute")
async def register_submit(request: Request):
    form = await request.form()
    email = (form.get("email") or "").strip().lower()
    username = (form.get("username") or "").strip()
    if not username:
        username = (email.split("@")[0] if email else "").strip() or email
    password = form.get("password") or ""
    confirm = form.get("confirm_password") or ""
    if not email or not password:
        return templates.TemplateResponse("register.html", {"request": request, "error": "Email and password are required."})
    if password != confirm:
        return templates.TemplateResponse("register.html", {"request": request, "error": "Passwords do not match."})
    # Validate password strength
    is_valid, error_msg = validate_password(password)
    if not is_valid:
        return templates.TemplateResponse("register.html", {"request": request, "error": error_msg})
    db = get_db()
    first_user = not users_exist(db)
    existing = one(db, "SELECT id FROM users WHERE email=?", (email,))
    if existing:
        db.close()
        return templates.TemplateResponse("register.html", {"request": request, "error": "Email already registered."})
    password_hash, password_salt = hash_password(password)
    role = "admin" if first_user else "user"
    user_id = execute_insert_returning_id(
        db,
        "INSERT INTO users (email, username, role, password_hash, password_salt, created_at, admin) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (email, username, role, password_hash, password_salt, datetime.utcnow().isoformat(), 1 if first_user else 0),
    )
    user = one(db, "SELECT id FROM users WHERE id=?", (user_id,)) if user_id else None
    if not user:
        user = one(db, "SELECT id FROM users WHERE email=?", (email,))
    sent, reason = send_welcome_email(email, username)
    log_audit(
        db,
        user,
        "welcome-email",
        {"email": email, "sent": sent, "reason": reason} if reason else {"email": email, "sent": sent},
    )
    token = create_session(db, user["id"])
    db.close()
    response = redirect("/")
    response.set_cookie("session_token", token, httponly=True, samesite="lax", max_age=60 * 60 * 24 * SESSION_LIFETIME_DAYS)
    return response

@app.get("/auth/logout")
def logout(request: Request):
    token = request.cookies.get("session_token")
    if token:
        db = get_db()
        db.execute("DELETE FROM sessions WHERE session_token=?", (token,))
        db.commit()
        db.close()
    response = redirect("/auth/login")
    response.delete_cookie("session_token")
    return response

@app.get("/account", response_class=HTMLResponse)
def account_settings(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    tokens = list_api_tokens(db, user["id"])
    db.close()
    return templates.TemplateResponse("account.html", {"request": request, "tokens": tokens, "user": user})

@app.post("/account/password")
@limiter.limit("5/minute")
async def account_change_password(request: Request):
    form = await request.form()
    current_password = form.get("current_password") or ""
    new_password = form.get("new_password") or ""
    confirm_password = form.get("confirm_password") or ""
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    if not current_password or not new_password:
        db.close()
        return templates.TemplateResponse(
            "account.html",
            {
                "request": request,
                "error": "Current and new password are required.",
                "tokens": list_api_tokens(db, user["id"]),
                "user": user,
            },
        )
    if new_password != confirm_password:
        db.close()
        return templates.TemplateResponse(
            "account.html",
            {
                "request": request,
                "error": "New passwords do not match.",
                "tokens": list_api_tokens(db, user["id"]),
                "user": user,
            },
        )
    if not verify_password(current_password, user["password_hash"], user["password_salt"]):
        db.close()
        return templates.TemplateResponse(
            "account.html",
            {
                "request": request,
                "error": "Current password is incorrect.",
                "tokens": list_api_tokens(db, user["id"]),
                "user": user,
            },
        )
    # Validate new password strength
    is_valid, error_msg = validate_password(new_password)
    if not is_valid:
        db.close()
        return templates.TemplateResponse(
            "account.html",
            {
                "request": request,
                "error": error_msg,
                "tokens": list_api_tokens(db, user["id"]),
                "user": user,
            },
        )
    password_hash, password_salt = hash_password(new_password)
    db.execute(
        "UPDATE users SET password_hash=?, password_salt=? WHERE id=?",
        (password_hash, password_salt, user["id"]),
    )
    log_audit(db, user, "password-change", {"mode": "self-service"})
    db.commit()
    tokens = list_api_tokens(db, user["id"])
    db.close()
    return templates.TemplateResponse(
        "account.html",
        {"request": request, "success": "Password updated successfully.", "tokens": tokens, "user": user},
    )

@app.post("/account/api-tokens")
async def account_create_api_token(request: Request):
    form = await request.form()
    label = (form.get("label") or "").strip() or None
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    token = create_api_token(db, user["id"], label)
    log_audit(db, user, "api-token-create", {"label": label or "API Token"})
    tokens = list_api_tokens(db, user["id"])
    db.close()
    return templates.TemplateResponse(
        "account.html",
        {
            "request": request,
            "success": "API token created. Copy it now; it won’t be shown again.",
            "api_token": token,
            "tokens": tokens,
            "user": user,
        },
    )

@app.post("/account/api-tokens/{token_id}/revoke")
async def account_revoke_api_token(request: Request, token_id: int):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    db.execute("DELETE FROM api_tokens WHERE id=? AND user_id=?", (token_id, user["id"]))
    log_audit(db, user, "api-token-revoke", {"token_id": token_id})
    db.commit()
    tokens = list_api_tokens(db, user["id"])
    db.close()
    return templates.TemplateResponse(
        "account.html",
        {"request": request, "success": "API token revoked.", "tokens": tokens, "user": user},
    )

@app.post("/admin/send-daily-summaries")
async def admin_send_daily_summaries(request: Request):
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    users = q(db, "SELECT id, email FROM users ORDER BY email")
    sent_count = 0
    for u in users:
        success, reason = send_daily_summary_email(db, u)
        log_audit(
            db,
            current_user,
            "daily-summary-send",
            {"user_id": u["id"], "email": u["email"], "sent": success, "reason": reason}
            if reason
            else {"user_id": u["id"], "email": u["email"], "sent": success},
        )
        if success:
            sent_count += 1
    db.commit()
    db.close()
    return redirect(f"/admin/audit?success=Sent {sent_count} summaries")

@app.get("/auth/google/start")
def google_start(request: Request):
    client_id = os.environ.get("GOOGLE_CLIENT_ID")
    if not client_id:
        return templates.TemplateResponse("login.html", {"request": request, "error": "Google OAuth is not configured."})
    state = secrets.token_urlsafe(16)
    redirect_uri = get_google_redirect_uri(request)
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "offline",
        "prompt": "consent",
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    response = redirect(url)
    cookie_settings = oauth_cookie_settings(request)
    response.set_cookie(
        "oauth_state",
        state,
        httponly=True,
        samesite="lax",
        secure=cookie_settings["secure"],
        domain=cookie_settings["domain"],
        path="/",
    )
    return response

@app.get("/auth/google/callback", name="google_callback")
def google_callback(request: Request, code: str | None = None, state: str | None = None):
    client_id = os.environ.get("GOOGLE_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET")
    redirect_uri = get_google_redirect_uri(request)
    expected_state = request.cookies.get("oauth_state")
    if not client_id or not client_secret:
        return templates.TemplateResponse("login.html", {"request": request, "error": "Google OAuth is not configured."})
    if not code or not state or state != expected_state:
        return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid OAuth state."})
    data = urllib.parse.urlencode({
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": redirect_uri,
    }).encode("utf-8")
    try:
        with urllib.request.urlopen("https://oauth2.googleapis.com/token", data=data) as resp:
            token_data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        return templates.TemplateResponse("login.html", {"request": request, "error": f"OAuth token exchange failed: {exc}"})
    access_token = token_data.get("access_token")
    if not access_token:
        return templates.TemplateResponse("login.html", {"request": request, "error": "OAuth token exchange failed."})
    req = urllib.request.Request(
        "https://openidconnect.googleapis.com/v1/userinfo",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            info = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        return templates.TemplateResponse("login.html", {"request": request, "error": f"OAuth user info failed: {exc}"})
    email = (info.get("email") or "").lower()
    sub = info.get("sub")
    if not email or not sub:
        return templates.TemplateResponse("login.html", {"request": request, "error": "Google account did not return email."})
    db = get_db()
    user = one(db, "SELECT * FROM users WHERE google_sub=? OR email=?", (sub, email))
    if not user:
        username = (email.split("@")[0] if email else "") or email
        first_user = not users_exist(db)
        role = "admin" if first_user else "user"
        oauth_password_hash, oauth_password_salt = hash_password(secrets.token_urlsafe(24))
        user_id = execute_insert_returning_id(
            db,
            "INSERT INTO users (email, username, role, password_hash, password_salt, google_sub, created_at, admin) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                email,
                username,
                role,
                oauth_password_hash,
                oauth_password_salt,
                sub,
                datetime.utcnow().isoformat(),
                1 if first_user else 0,
            ),
        )
        user = one(db, "SELECT * FROM users WHERE id=?", (user_id,)) if user_id else None
        if not user:
            user = one(db, "SELECT * FROM users WHERE email=?", (email,))
        sent, reason = send_welcome_email(email, username)
        log_audit(
            db,
            user,
            "welcome-email",
            {"email": email, "sent": sent, "reason": reason} if reason else {"email": email, "sent": sent},
        )
    token = create_session(db, user["id"])
    db.close()
    response = redirect("/")
    response.set_cookie("session_token", token, httponly=True, samesite="lax", max_age=60 * 60 * 24 * SESSION_LIFETIME_DAYS)
    cookie_settings = oauth_cookie_settings(request)
    response.delete_cookie("oauth_state", domain=cookie_settings["domain"], path="/")
    return response

@app.get("/tanks/reorder", response_class=HTMLResponse)
def tank_reorder(request: Request):
    db = get_db()
    tanks = get_visible_tanks(db, request)
    db.close()
    return templates.TemplateResponse("tank_reorder.html", {"request": request, "tanks": tanks})

@app.post("/tanks/order")
async def tank_order_save(request: Request):
    payload = await request.json()
    order = payload.get("order", []) if isinstance(payload, dict) else []
    if not order:
        return {"ok": False}
    db = get_db()
    user = get_current_user(db, request)
    allowed_ids = set(get_visible_tank_ids(db, user))
    for idx, tank_id in enumerate(order, start=1):
        tank_id = int(tank_id)
        if allowed_ids and tank_id not in allowed_ids:
            continue
        execute_with_retry(db, "UPDATE tanks SET sort_order=? WHERE id=?", (idx, tank_id))
    db.commit()
    db.close()
    return {"ok": True}

@app.get("/add", response_class=HTMLResponse)
def add_reading_selector(request: Request):
    db = get_db()
    tanks = get_visible_tanks(db, request)
    db.close()
    html = """<html><head><title>Add Reading</title></head><body style="font-family: sans-serif; max-width: 720px; margin: 40px auto;"><h2>Add Reading</h2><form method="get" action="/tanks/0/add" onsubmit="event.preventDefault(); window.location = '/tanks/' + document.getElementById('tank').value + '/add';"><label>Select tank</label><br/><select id="tank" name="tank" style="width:100%; padding:10px; margin:10px 0;" required>{options}</select><button type="submit" style="padding:10px 16px;">Continue</button></form><p><a href="/tanks/multi-add">Log one parameter across multiple tanks</a></p><p><a href="/">Back to dashboard</a></p></body></html>"""
    options = "\n".join([f'<option value="{t["id"]}">{t["name"]}</option>' for t in tanks])
    return HTMLResponse(html.format(options=options))

@app.get("/tanks/multi-add", response_class=HTMLResponse)
def multi_add_form(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    tanks = get_visible_tanks(db, request)
    params_rows = get_active_param_defs(db, user_id=user["id"] if user else None)
    kits = get_visible_test_kits(db, user, active_only=True)
    kits_by_param = {}
    for k in kits:
        kits_by_param.setdefault(k["parameter"], []).append(k)
    db.close()
    return templates.TemplateResponse(
        "multi_add.html",
        {
            "request": request,
            "tanks": tanks,
            "parameters": params_rows,
            "kits_by_param": kits_by_param,
        },
    )

@app.post("/tanks/multi-add")
async def multi_add_save(request: Request):
    form = await request.form()
    param_id = to_float(form.get("parameter_id"))
    kit_id = to_float(form.get("kit_id"))
    notes = (form.get("notes") or "").strip() or None
    taken_at = (form.get("taken_at") or "").strip()
    if param_id is None:
        return redirect("/tanks/multi-add")
    db = get_db()
    user = get_current_user(db, request)
    allowed_ids = set(get_visible_tank_ids(db, user))
    param = one(db, "SELECT * FROM parameter_defs WHERE id=? AND active=1", (int(param_id),))
    if not param:
        db.close()
        return redirect("/tanks/multi-add")
    conv_type, conv_data = get_test_kit_conversion(db, int(kit_id) if kit_id else None)
    if conv_type == "syringe_remaining_ml" and conv_data:
        try:
            conversion_table = json.loads(conv_data)
        except Exception:
            conversion_table = []
    else:
        conversion_table = []
    if taken_at:
        try:
            when_iso = datetime.fromisoformat(taken_at).isoformat()
        except ValueError:
            when_iso = datetime.utcnow().isoformat()
    else:
        when_iso = datetime.utcnow().isoformat()
    saved_ids = []
    for tank_id in sorted(allowed_ids):
        value = to_float(form.get(f"value_{tank_id}"))
        remaining = to_float(form.get(f"remaining_{tank_id}"))
        if value is None and remaining is not None and conversion_table:
            value = compute_conversion_value(float(remaining), conversion_table)
        if value is None:
            continue
        sample_id = execute_insert_returning_id(
            db,
            "INSERT INTO samples (tank_id, taken_at, notes) VALUES (?, ?, ?)",
            (tank_id, when_iso, notes),
        )
        if sample_id is None:
            continue
        insert_sample_reading(
            db,
            sample_id,
            param["name"],
            float(value),
            row_get(param, "unit") or "",
            int(kit_id) if kit_id else None,
        )
        saved_ids.append(tank_id)
    db.commit()
    db.close()
    if not saved_ids:
        return redirect("/tanks/multi-add")
    if len(saved_ids) == 1:
        return redirect(f"/tanks/{saved_ids[0]}")
    return redirect("/")

@app.get("/tanks/new", response_class=HTMLResponse)
def tank_new_form(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    params = list_parameters(db, user_id=user["id"] if user else None)
    recommended_targets = {
        row_get(p, "name"): {
            "target_low": row_get(p, "default_target_low"),
            "target_high": row_get(p, "default_target_high"),
            "alert_low": row_get(p, "default_alert_low"),
            "alert_high": row_get(p, "default_alert_high"),
        }
        for p in params
    }
    additives_rows = get_visible_additives(db, user, active_only=True)
    grouped_additives: Dict[str, List[Dict[str, Any]]] = {}
    for a in additives_rows:
        parameter = (a["parameter"] or "other").strip().lower()
        if "alk" in parameter or "kh" in parameter:
            key = "alkalinity"
        elif "calcium" in parameter or parameter == "ca":
            key = "calcium"
        elif "magnesium" in parameter or parameter == "mg":
            key = "magnesium"
        elif "nitrate" in parameter or "no3" in parameter:
            key = "nitrate"
        elif "phosphate" in parameter or "po4" in parameter:
            key = "phosphate"
        elif "trace" in parameter:
            key = "trace"
        else:
            key = parameter
        grouped_additives.setdefault(key, []).append(a)
    grouped_additives.setdefault("all", []).extend(additives_rows)
    db.close()
    return templates.TemplateResponse(
        "tank_new.html",
        {
            "request": request,
            "params": params,
            "recommended_targets": recommended_targets,
            "additives": additives_rows,
            "grouped_additives": grouped_additives,
        },
    )

@app.post("/tanks/new")
async def tank_new(request: Request):
    form = await request.form()
    name = (form.get("name") or "").strip()
    volume_l = to_float(form.get("volume_l"))
    net_percent = to_float(form.get("net_percent")) or 100
    dosing_mode = (form.get("dosing_mode") or "").strip() or None
    db = get_db()
    user = get_current_user(db, request)
    max_order = one(db, "SELECT MAX(sort_order) AS max_order FROM tanks")
    next_order = ((max_order["max_order"] or 0) if max_order else 0) + 1
    try:
        tank_id = execute_insert_returning_id(
            db,
            "INSERT INTO tanks (name, volume_l, sort_order, owner_user_id) VALUES (?, ?, ?, ?)",
            (name, volume_l, next_order, user["id"] if user else None),
        )
    except IntegrityError as exc:
        orig = getattr(exc, "orig", None)
        constraint = getattr(getattr(orig, "diag", None), "constraint_name", None)
        if engine.dialect.name == "postgresql" and isinstance(orig, psycopg.errors.UniqueViolation) and constraint == "tanks_pkey":
            reset_tanks_sequence(db)
            tank_id = execute_insert_returning_id(
                db,
                "INSERT INTO tanks (name, volume_l, sort_order, owner_user_id) VALUES (?, ?, ?, ?)",
                (name, volume_l, next_order, user["id"] if user else None),
            )
        else:
            db.close()
            raise
    if tank_id is None:
        db.close()
        raise HTTPException(status_code=500, detail="Failed to create tank")
    if user:
        execute_with_retry(
            db,
            "INSERT INTO user_tanks (user_id, tank_id) VALUES (?, ?) "
            "ON CONFLICT (user_id, tank_id) DO NOTHING",
            (user["id"], tank_id),
        )
    db.commit()
    profile_fields = {
        "tank_id": tank_id,
        "volume_l": volume_l,
        "net_percent": net_percent,
        "dosing_mode": dosing_mode,
        "all_in_one_solution": form.get("all_in_one_solution") or None,
        "all_in_one_daily_ml": to_float(form.get("all_in_one_daily_ml")),
        "alk_solution": form.get("alk_solution") or None,
        "alk_daily_ml": to_float(form.get("alk_daily_ml")),
        "kalk_solution": form.get("kalk_solution") or None,
        "kalk_daily_ml": to_float(form.get("kalk_daily_ml")),
        "ca_solution": form.get("ca_solution") or None,
        "ca_daily_ml": to_float(form.get("ca_daily_ml")),
        "mg_solution": form.get("mg_solution") or None,
        "mg_daily_ml": to_float(form.get("mg_daily_ml")),
        "nitrate_solution": form.get("nitrate_solution") or None,
        "nitrate_daily_ml": to_float(form.get("nitrate_daily_ml")),
        "phosphate_solution": form.get("phosphate_solution") or None,
        "phosphate_daily_ml": to_float(form.get("phosphate_daily_ml")),
        "nopox_daily_ml": to_float(form.get("nopox_daily_ml")),
        "calcium_reactor_daily_ml": to_float(form.get("calcium_reactor_daily_ml")),
        "calcium_reactor_effluent_dkh": to_float(form.get("calcium_reactor_effluent_dkh")),
        "use_all_in_one": 1 if form.get("use_all_in_one") else 0,
        "use_alk": 1 if form.get("use_alk") else 0,
        "use_ca": 1 if form.get("use_ca") else 0,
        "use_mg": 1 if form.get("use_mg") else 0,
        "use_nitrate": 1 if form.get("use_nitrate") else 0,
        "use_phosphate": 1 if form.get("use_phosphate") else 0,
        "use_nopox": 1 if form.get("use_nopox") else 0,
        "use_calcium_reactor": 1 if form.get("use_calcium_reactor") else 0,
        "use_kalkwasser": 1 if form.get("use_kalkwasser") else 0,
        "dosing_low_days": to_float(form.get("dosing_low_days")),
        "all_in_one_container_ml": to_float(form.get("all_in_one_container_ml")),
        "all_in_one_remaining_ml": to_float(form.get("all_in_one_remaining_ml")),
        "alk_container_ml": to_float(form.get("alk_container_ml")),
        "alk_remaining_ml": to_float(form.get("alk_remaining_ml")),
        "kalk_container_ml": to_float(form.get("kalk_container_ml")),
        "kalk_remaining_ml": to_float(form.get("kalk_remaining_ml")),
        "ca_container_ml": to_float(form.get("ca_container_ml")),
        "ca_remaining_ml": to_float(form.get("ca_remaining_ml")),
        "mg_container_ml": to_float(form.get("mg_container_ml")),
        "mg_remaining_ml": to_float(form.get("mg_remaining_ml")),
        "nitrate_container_ml": to_float(form.get("nitrate_container_ml")),
        "nitrate_remaining_ml": to_float(form.get("nitrate_remaining_ml")),
        "phosphate_container_ml": to_float(form.get("phosphate_container_ml")),
        "phosphate_remaining_ml": to_float(form.get("phosphate_remaining_ml")),
        "nopox_container_ml": to_float(form.get("nopox_container_ml")),
        "nopox_remaining_ml": to_float(form.get("nopox_remaining_ml")),
    }
    columns = ", ".join(profile_fields.keys())
    placeholders = ", ".join(["?"] * len(profile_fields))
    execute_with_retry(
        db,
        f"INSERT INTO tank_profiles ({columns}) VALUES ({placeholders}) ON CONFLICT (tank_id) DO NOTHING",
        tuple(profile_fields.values()),
    )
    if table_exists(db, "targets"):
        cur = cursor(db)
        inspector = inspect(db._conn)
        cols = {column["name"] for column in inspector.get_columns("targets")}
        has_target_cols = "target_low" in cols
        params = list_parameters(db, user_id=user["id"] if user else None)
        for p in params:
            pname = p["name"]
            pid = p["id"]
            target_low = to_float(form.get(f"target_low_{pid}"))
            target_high = to_float(form.get(f"target_high_{pid}"))
            alert_low = to_float(form.get(f"alert_low_{pid}"))
            alert_high = to_float(form.get(f"alert_high_{pid}"))
            if target_low is None and target_high is None and alert_low is None and alert_high is None:
                continue
            unit = row_get(p, "unit") or ""
            if has_target_cols:
                try:
                    cur.execute(
                        "INSERT INTO targets (tank_id, parameter, target_low, target_high, alert_low, alert_high, unit, enabled) VALUES (?, ?, ?, ?, ?, ?, ?, 1)",
                        (tank_id, pname, target_low, target_high, alert_low, alert_high, unit),
                    )
                except IntegrityError as exc:
                    orig = getattr(exc, "orig", None)
                    constraint = getattr(getattr(orig, "diag", None), "constraint_name", None)
                    if engine.dialect.name == "postgresql" and isinstance(orig, psycopg.errors.UniqueViolation) and constraint == "targets_pkey":
                        reset_targets_sequence(db)
                        cur.execute(
                            "INSERT INTO targets (tank_id, parameter, target_low, target_high, alert_low, alert_high, unit, enabled) VALUES (?, ?, ?, ?, ?, ?, ?, 1)",
                            (tank_id, pname, target_low, target_high, alert_low, alert_high, unit),
                        )
                    else:
                        raise
            else:
                try:
                    cur.execute(
                        "INSERT INTO targets (tank_id, parameter, low, high, unit, enabled) VALUES (?, ?, ?, ?, ?, 1)",
                        (tank_id, pname, target_low, target_high, unit),
                    )
                except IntegrityError as exc:
                    orig = getattr(exc, "orig", None)
                    constraint = getattr(getattr(orig, "diag", None), "constraint_name", None)
                    if engine.dialect.name == "postgresql" and isinstance(orig, psycopg.errors.UniqueViolation) and constraint == "targets_pkey":
                        reset_targets_sequence(db)
                        cur.execute(
                            "INSERT INTO targets (tank_id, parameter, low, high, unit, enabled) VALUES (?, ?, ?, ?, ?, 1)",
                            (tank_id, pname, target_low, target_high, unit),
                        )
                    else:
                        raise
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}")

@app.post("/tanks/{tank_id}/order")
async def tank_order(request: Request, tank_id: int):
    form = await request.form()
    direction = (form.get("direction") or "").strip().lower()
    db = get_db()
    tanks = q(db, "SELECT id, sort_order FROM tanks ORDER BY COALESCE(sort_order, 0), name")
    if not tanks:
        db.close()
        return redirect("/")
    missing = any(t["sort_order"] is None for t in tanks)
    if missing:
        for idx, t in enumerate(tanks):
            execute_with_retry(db, "UPDATE tanks SET sort_order=? WHERE id=?", (idx + 1, t["id"]))
        tanks = q(db, "SELECT id, sort_order FROM tanks ORDER BY COALESCE(sort_order, 0), name")
    index_by_id = {t["id"]: idx for idx, t in enumerate(tanks)}
    current_index = index_by_id.get(tank_id)
    if current_index is None:
        db.close()
        return redirect("/")
    swap_index = None
    if direction == "up" and current_index > 0:
        swap_index = current_index - 1
    elif direction == "down" and current_index < len(tanks) - 1:
        swap_index = current_index + 1
    if swap_index is not None:
        current = tanks[current_index]
        swap = tanks[swap_index]
        execute_with_retry(db, "UPDATE tanks SET sort_order=? WHERE id=?", (swap["sort_order"], current["id"]))
        execute_with_retry(db, "UPDATE tanks SET sort_order=? WHERE id=?", (current["sort_order"], swap["id"]))
        db.commit()
    db.close()
    return redirect("/")

@app.post("/tanks/{tank_id}/delete")
async def tank_delete(tank_id: int):
    db = get_db()
    try:
        if table_exists(db, "user_tanks"):
            db.execute("DELETE FROM user_tanks WHERE tank_id=?", (tank_id,))
        if table_exists(db, "tank_profiles"):
            db.execute("DELETE FROM tank_profiles WHERE tank_id=?", (tank_id,))
        if table_exists(db, "targets"):
            db.execute("DELETE FROM targets WHERE tank_id=?", (tank_id,))
        if table_exists(db, "dose_logs"):
            db.execute("DELETE FROM dose_logs WHERE tank_id=?", (tank_id,))
        if table_exists(db, "dosing_entries"):
            db.execute("DELETE FROM dosing_entries WHERE tank_id=?", (tank_id,))
        if table_exists(db, "dosing_notifications"):
            db.execute("DELETE FROM dosing_notifications WHERE tank_id=?", (tank_id,))
        if table_exists(db, "tank_maintenance_tasks"):
            db.execute("DELETE FROM tank_maintenance_tasks WHERE tank_id=?", (tank_id,))
        if table_exists(db, "tank_journal"):
            db.execute("DELETE FROM tank_journal WHERE tank_id=?", (tank_id,))
        if table_exists(db, "samples"):
            sample_ids = [row["id"] for row in q(db, "SELECT id FROM samples WHERE tank_id=?", (tank_id,))]
            if sample_ids:
                placeholders = ",".join(["?"] * len(sample_ids))
                if table_exists(db, "sample_value_kits"):
                    db.execute(f"DELETE FROM sample_value_kits WHERE sample_id IN ({placeholders})", tuple(sample_ids))
                if table_exists(db, "sample_values"):
                    db.execute(f"DELETE FROM sample_values WHERE sample_id IN ({placeholders})", tuple(sample_ids))
                if table_exists(db, "parameters"):
                    db.execute(f"DELETE FROM parameters WHERE sample_id IN ({placeholders})", tuple(sample_ids))
            db.execute("DELETE FROM samples WHERE tank_id=?", (tank_id,))
        db.commit()
        db.execute("DELETE FROM tanks WHERE id=?", (tank_id,))
        db.commit()
        return redirect("/")
    except IntegrityError:
        db.rollback()
        return redirect("/?error=Unable+to+delete+tank+with+existing+references.")
    finally:
        db.close()

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
        if table_exists(db, "sample_value_kits"):
            db.execute("DELETE FROM sample_value_kits WHERE sample_id = ?", (sample_id,))
        if table_exists(db, "sample_values"):
            db.execute("DELETE FROM sample_values WHERE sample_id = ?", (sample_id,))
        if table_exists(db, "parameters"):
            db.execute("DELETE FROM parameters WHERE sample_id = ?", (sample_id,))
        db.execute("DELETE FROM samples WHERE id = ?", (sample_id,))
        db.commit()

        # 3. Redirect back to the tank detail page we just came from
        return redirect(f"/tanks/{tank_id}")
    finally:
        db.close()

@app.get("/tanks/{tank_id}", response_class=HTMLResponse)
def tank_detail(request: Request, tank_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        return templates.TemplateResponse(
            "tank_detail.html",
            {
                "request": request,
                "tank": None,
                "params": [],
                "recent_samples": [],
                "sample_values": {},
                "latest_vals": {},
                "status_by_param_id": {},
                "targets": [],
                "series": [],
                "series_map": {},
                "chart_targets_map": {},
                "chart_targets": [],
                "selected_parameter_id": "",
                "format_value": format_value,
                "target_map": {},
                "dose_log_events": [],
            },
        )
    profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    extra_dosing_entries = q(
        db,
        "SELECT id, parameter, solution, daily_ml, container_ml, remaining_ml FROM dosing_entries WHERE tank_id=? AND active=1 ORDER BY id",
        (tank_id,),
    )
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
                ("kalk_container_ml", "kalk_remaining_ml", "kalk_daily_ml"),
                ("ca_container_ml", "ca_remaining_ml", "ca_daily_ml"),
                ("mg_container_ml", "mg_remaining_ml", "mg_daily_ml"),
                ("nitrate_container_ml", "nitrate_remaining_ml", "nitrate_daily_ml"),
                ("phosphate_container_ml", "phosphate_remaining_ml", "phosphate_daily_ml"),
                ("trace_container_ml", "trace_remaining_ml", "trace_daily_ml"),
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
            extra_updates = []
            for entry in extra_dosing_entries:
                container_ml = row_get(entry, "container_ml")
                daily_ml = row_get(entry, "daily_ml")
                if container_ml is None or daily_ml in (None, 0):
                    continue
                remaining_ml = row_get(entry, "remaining_ml")
                if remaining_ml is None:
                    remaining_ml = container_ml
                updated_remaining = max(remaining_ml - (float(daily_ml) * days_since_update), 0)
                if updated_remaining != remaining_ml:
                    extra_updates.append((updated_remaining, row_get(entry, "id")))
            if extra_updates:
                db.executemany(
                    "UPDATE dosing_entries SET remaining_ml=? WHERE id=?",
                    extra_updates,
                )
                extra_dosing_entries = q(
                    db,
                    "SELECT id, parameter, solution, daily_ml, container_ml, remaining_ml FROM dosing_entries WHERE tank_id=? AND active=1 ORDER BY id",
                    (tank_id,),
                )
        if container_updates or row_get(profile, "dosing_container_updated_at") is None:
            container_updates["dosing_container_updated_at"] = now.isoformat()
            set_clause = ", ".join([f"{col}=?" for col in container_updates.keys()])
            db.execute(
                f"UPDATE tank_profiles SET {set_clause} WHERE tank_id=?",
                (*container_updates.values(), tank_id),
            )
            profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
            log_audit(
                db,
                user,
                "dosing-volume-update",
                {"tank_id": tank_id, "updates": container_updates},
            )
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
            "calcium_reactor_daily_ml": row_get(profile, "calcium_reactor_daily_ml"),
            "calcium_reactor_effluent_dkh": row_get(profile, "calcium_reactor_effluent_dkh"),
            "kalk_solution": row_get(profile, "kalk_solution"),
            "kalk_daily_ml": row_get(profile, "kalk_daily_ml"),
            "use_all_in_one": row_get(profile, "use_all_in_one"),
            "use_alk": row_get(profile, "use_alk"),
            "use_ca": row_get(profile, "use_ca"),
            "use_mg": row_get(profile, "use_mg"),
            "use_nitrate": row_get(profile, "use_nitrate"),
            "use_phosphate": row_get(profile, "use_phosphate"),
            "use_trace": row_get(profile, "use_trace"),
            "use_nopox": row_get(profile, "use_nopox"),
            "use_calcium_reactor": row_get(profile, "use_calcium_reactor"),
            "use_kalkwasser": row_get(profile, "use_kalkwasser"),
            "nitrate_solution": row_get(profile, "nitrate_solution"),
            "nitrate_daily_ml": row_get(profile, "nitrate_daily_ml"),
            "phosphate_solution": row_get(profile, "phosphate_solution"),
            "phosphate_daily_ml": row_get(profile, "phosphate_daily_ml"),
            "trace_solution": row_get(profile, "trace_solution"),
            "trace_daily_ml": row_get(profile, "trace_daily_ml"),
            "nopox_daily_ml": row_get(profile, "nopox_daily_ml"),
            "all_in_one_container_ml": row_get(profile, "all_in_one_container_ml"),
            "all_in_one_remaining_ml": row_get(profile, "all_in_one_remaining_ml"),
            "alk_container_ml": row_get(profile, "alk_container_ml"),
            "alk_remaining_ml": row_get(profile, "alk_remaining_ml"),
            "kalk_container_ml": row_get(profile, "kalk_container_ml"),
            "kalk_remaining_ml": row_get(profile, "kalk_remaining_ml"),
            "ca_container_ml": row_get(profile, "ca_container_ml"),
            "ca_remaining_ml": row_get(profile, "ca_remaining_ml"),
            "mg_container_ml": row_get(profile, "mg_container_ml"),
            "mg_remaining_ml": row_get(profile, "mg_remaining_ml"),
            "nitrate_container_ml": row_get(profile, "nitrate_container_ml"),
            "nitrate_remaining_ml": row_get(profile, "nitrate_remaining_ml"),
            "phosphate_container_ml": row_get(profile, "phosphate_container_ml"),
            "phosphate_remaining_ml": row_get(profile, "phosphate_remaining_ml"),
            "trace_container_ml": row_get(profile, "trace_container_ml"),
            "trace_remaining_ml": row_get(profile, "trace_remaining_ml"),
            "nopox_container_ml": row_get(profile, "nopox_container_ml"),
            "nopox_remaining_ml": row_get(profile, "nopox_remaining_ml"),
            "dosing_container_updated_at": row_get(profile, "dosing_container_updated_at"),
            "dosing_low_days": dosing_low_days,
        })

    daily_consumption = build_daily_consumption(db, tank_view, user) if profile else {}
    if profile:
        try:
            additive_rows = get_visible_additives(db, user, active_only=False)
            additive_map = {str(a["name"]).strip(): additive_label(a) for a in additive_rows}
            for key in (
                "all_in_one_solution",
                "alk_solution",
                "kalk_solution",
                "ca_solution",
                "mg_solution",
                "nitrate_solution",
                "phosphate_solution",
                "trace_solution",
            ):
                current = tank_view.get(key)
                if current and current in additive_map:
                    tank_view[key] = additive_map[current]
        except Exception:
            pass
    
    low_container_alerts = []
    if profile:
        all_notifications = collect_dosing_notifications(db, tank_id=tank_id)
        seen_alerts = set()
        for alert in all_notifications:
            if alert.get("tank_id") != tank_id:
                continue
            key = alert.get("container_key") or alert.get("label")
            if key in seen_alerts:
                continue
            seen_alerts.add(key)
            low_container_alerts.append({"label": alert["label"], "days": alert["days"]})

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
    def normalize_param_key(value: str) -> str:
        return re.sub(r"[\s/_-]+", "", str(value or "").lower())

    chart_targets_map = {}
    for t in targets:
        param_name = t["parameter"] or ""
        if not param_name:
            continue
        chart_targets_map[normalize_param_key(param_name)] = {
            "parameter": param_name,
            # These are the dashed red lines
            "alert_low": row_get(t, "alert_low"),
            "alert_high": row_get(t, "alert_high"),
            # These are the green box range
            "target_low": row_get(t, "target_low") if row_get(t, "target_low") is not None else row_get(t, "low"),
            "target_high": row_get(t, "target_high") if row_get(t, "target_high") is not None else row_get(t, "high"),
            "unit": row_get(t, "unit") or unit_by_name.get(param_name, "")
        }

    dose_log_rows = q(
        db,
        """SELECT dl.logged_at, dl.amount_ml, dl.reason, a.name AS additive_name, a.parameter AS parameter
           FROM dose_logs dl
           LEFT JOIN additives a ON a.id = dl.additive_id
           WHERE dl.tank_id=?
           ORDER BY dl.logged_at DESC
           LIMIT 50""",
        (tank_id,),
    )
    dose_log_events = []
    for row in dose_log_rows:
        param_name = row_get(row, "parameter") or ""
        if not param_name:
            continue
        logged_at = parse_dt_any(row_get(row, "logged_at"))
        if not logged_at:
            continue
        dose_log_events.append(
            {
                "parameter": param_name,
                "key": normalize_param_key(param_name),
                "logged_at": logged_at.isoformat(),
                "label": row_get(row, "additive_name") or row_get(row, "reason") or "Dose",
                "amount_ml": row_get(row, "amount_ml"),
            }
        )
            
    params = [
        {
            "id": name,
            "name": name,
            "unit": unit_by_name.get(name, ""),
            "key": normalize_param_key(name),
        }
        for name in available_params
    ]
    
    latest_by_param_id = get_latest_per_parameter(db, tank_id)
    latest_and_previous = get_latest_and_previous_per_parameter(db, tank_id)
    targets_by_param = {t["parameter"]: t for t in targets if row_get(t, "parameter") is not None}
    pdef_map = {p["name"]: p for p in get_active_param_defs(db, user_id=user["id"] if user else None)}

    latest_sample_ids = {data.get("sample_id") for data in latest_by_param_id.values() if data.get("sample_id")}
    icp_sample_ids: set[int] = set()
    if latest_sample_ids:
        placeholders = ",".join("?" for _id in latest_sample_ids)
        rows = q(
            db,
            f"SELECT id, notes FROM samples WHERE id IN ({placeholders})",
            tuple(latest_sample_ids),
        )
        for row in rows:
            notes = (row_get(row, "notes") or "").lower()
            if "imported from icp" in notes:
                icp_sample_ids.add(row_get(row, "id"))

    core_params = []
    trace_params = []
    icp_params = []
    for param in params:
        latest = latest_by_param_id.get(param["name"])
        sample_id = latest.get("sample_id") if latest else None
        if sample_id and sample_id in icp_sample_ids:
            icp_params.append(param)
        elif is_trace_element(param["name"]):
            trace_params.append(param)
        else:
            core_params.append(param)
    
    status_by_param_id = {}
    trend_warning_by_param = {}
    stability_score_by_param = {}
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
        series_vals = [point.get("y") for point in series_map.get(pname, [])][-10:]
        series_vals = [float(val) for val in series_vals if val is not None]
        if len(series_vals) >= 2:
            mean_val = sum(series_vals) / len(series_vals)
            if mean_val != 0:
                variance = sum((val - mean_val) ** 2 for val in series_vals) / len(series_vals)
                std_dev = variance ** 0.5
                coeff = abs(std_dev / mean_val) * 100.0
                stability_score_by_param[pname] = max(0.0, 100.0 - min(100.0, coeff))
        overdue_by_param[pname] = False
        latest_taken = latest_data["taken_at"] if latest_data else None
        interval_days = row_get(pdef_map.get(pname), "test_interval_days")
        if latest_taken and interval_days:
            try:
                overdue_by_param[pname] = (datetime.now() - latest_taken).days >= int(interval_days)
            except Exception:
                overdue_by_param[pname] = False
        
    trend_alert_params = sorted([name for name, flagged in trend_warning_by_param.items() if flagged], key=lambda s: s.lower())

    recent_samples = samples[:10] if samples else []
    db.close()
    return templates.TemplateResponse(
        "tank_detail.html",
        {
            "request": request,
            "tank": tank_view,
            "extra_dosing_entries": extra_dosing_entries,
            "daily_consumption": daily_consumption,
            "params": params,
            "recent_samples": recent_samples,
            "sample_values": sample_values,
            "latest_vals": latest_by_param_id,
            "status_by_param_id": status_by_param_id,
            "trend_warning_by_param": trend_warning_by_param,
            "trend_alert_params": trend_alert_params,
            "stability_score_by_param": stability_score_by_param,
            "overdue_by_param": overdue_by_param,
            "targets": targets,
            "target_map": targets_by_param,
            "series": series,
            "series_map": series_map,
            "chart_targets_map": chart_targets_map,
            "dose_log_events": dose_log_events,
            "selected_parameter_id": selected_parameter_id,
            "format_value": format_value,
            "low_container_alerts": low_container_alerts,
            "core_params": core_params,
            "trace_params": trace_params,
            "icp_params": icp_params,
        },
    )

@app.get("/tanks/{tank_id}/journal", response_class=HTMLResponse)
def tank_journal(request: Request, tank_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    entries = q(db, "SELECT * FROM tank_journal WHERE tank_id=? ORDER BY entry_date DESC, id DESC", (tank_id,))
    tasks = q(db, "SELECT * FROM tank_maintenance_tasks WHERE tank_id=? AND active=1 ORDER BY next_due_at ASC", (tank_id,))
    task_views = []
    today = datetime.utcnow().date()
    for task in tasks:
        due_at = parse_dt_any(task["next_due_at"])
        days_until = None
        if due_at:
            days_until = (due_at.date() - today).days
        status = "ok"
        if days_until is not None:
            if days_until < 0:
                status = "overdue"
            elif days_until <= 3:
                status = "due_soon"
        task_views.append(
            {
                **dict(task),
                "due_at": due_at,
                "days_until": days_until,
                "status": status,
            }
        )
    error = (request.query_params.get("error") or "").strip()
    db.close()
    return templates.TemplateResponse(
        "tank_journal.html",
        {"request": request, "tank": tank, "entries": entries, "tasks": task_views, "error": error},
    )

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
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    insert_tank_journal(db, tank_id, entry_iso, entry_type, title, notes)
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/journal")

@app.post("/tanks/{tank_id}/maintenance")
async def maintenance_task_add(request: Request, tank_id: int):
    form = await request.form()
    title = (form.get("title") or "").strip()
    notes = (form.get("notes") or "").strip() or None
    first_due = (form.get("first_due_date") or "").strip()
    try:
        interval_days = int(form.get("interval_days") or 0)
    except Exception:
        interval_days = 0
    if not title or interval_days <= 0:
        return redirect(f"/tanks/{tank_id}/journal?error=maintenance")
    if first_due:
        try:
            next_due = datetime.fromisoformat(first_due)
        except ValueError:
            next_due = datetime.utcnow() + timedelta(days=interval_days)
    else:
        next_due = datetime.utcnow() + timedelta(days=interval_days)
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    db.execute(
        """INSERT INTO tank_maintenance_tasks
           (tank_id, title, interval_days, next_due_at, last_completed_at, notes, active, created_at)
           VALUES (?, ?, ?, ?, ?, ?, 1, ?)""",
        (
            tank_id,
            title,
            interval_days,
            next_due.isoformat(),
            None,
            notes,
            datetime.utcnow().isoformat(),
        ),
    )
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/journal")

@app.post("/tanks/{tank_id}/maintenance/{task_id}/complete")
def maintenance_task_complete(request: Request, tank_id: int, task_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    task = one(db, "SELECT * FROM tank_maintenance_tasks WHERE id=? AND tank_id=?", (task_id, tank_id))
    if task:
        try:
            interval_days = int(task["interval_days"] or 0)
        except Exception:
            interval_days = 0
        now = datetime.utcnow()
        next_due = now + timedelta(days=interval_days) if interval_days > 0 else now
        db.execute(
            "UPDATE tank_maintenance_tasks SET last_completed_at=?, next_due_at=? WHERE id=? AND tank_id=?",
            (now.isoformat(), next_due.isoformat(), task_id, tank_id),
        )
        insert_tank_journal(
            db,
            tank_id,
            now.isoformat(),
            "maintenance",
            f"Completed: {task['title']}",
            "Scheduled maintenance task completed.",
        )
        db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/journal")

@app.post("/tanks/{tank_id}/maintenance/{task_id}/delete")
def maintenance_task_delete(request: Request, tank_id: int, task_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    db.execute("DELETE FROM tank_maintenance_tasks WHERE id=? AND tank_id=?", (task_id, tank_id))
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/journal")

@app.post("/tanks/{tank_id}/journal/{entry_id}/delete")
def tank_journal_delete(request: Request, tank_id: int, entry_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    db.execute("DELETE FROM tank_journal WHERE id=? AND tank_id=?", (entry_id, tank_id))
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/journal")

@app.get("/tanks/{tank_id}/export")
def tank_export(request: Request, tank_id: int):
    pd = require_pandas()
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
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

@app.get("/api/tanks")
def api_tanks(request: Request):
    db = get_db()
    user = get_authenticated_user(db, request)
    if not user:
        db.close()
        raise HTTPException(status_code=401, detail="Authentication required")
    tanks = get_visible_tanks(db, request)
    payload = [
        {
            "id": t["id"],
            "name": t["name"],
            "volume_l": row_get(t, "volume_l"),
        }
        for t in tanks
    ]
    db.close()
    return JSONResponse(payload)

@app.get("/api/samples")
def api_samples(request: Request, tank_id: Optional[int] = None):
    db = get_db()
    user = get_authenticated_user(db, request)
    if not user:
        db.close()
        raise HTTPException(status_code=401, detail="Authentication required")
    tank_ids: List[int] = []
    if tank_id is not None:
        tank = get_tank_for_user(db, user, tank_id)
        if not tank:
            db.close()
            raise HTTPException(status_code=404, detail="Tank not found")
        tank_ids = [tank_id]
    else:
        tank_ids = get_visible_tank_ids(db, user)
    if not tank_ids:
        db.close()
        return JSONResponse([])
    placeholders = ",".join(["?"] * len(tank_ids))
    samples = q(
        db,
        f"SELECT * FROM samples WHERE tank_id IN ({placeholders}) ORDER BY taken_at DESC",
        tuple(tank_ids),
    )
    payload = []
    for sample in samples:
        readings = []
        for r in get_sample_readings(db, sample["id"]):
            readings.append(
                {
                    "name": r["name"],
                    "value": r["value"],
                    "unit": r["unit"] or "",
                    "test_kit_id": row_get(r, "test_kit_id"),
                    "test_kit_name": row_get(r, "test_kit_name"),
                }
            )
        payload.append(
            {
                "id": sample["id"],
                "tank_id": sample["tank_id"],
                "taken_at": sample["taken_at"],
                "notes": sample["notes"],
                "readings": readings,
            }
        )
    db.close()
    return JSONResponse(payload)

@app.get("/api/targets")
def api_targets(request: Request, tank_id: Optional[int] = None):
    db = get_db()
    user = get_authenticated_user(db, request)
    if not user:
        db.close()
        raise HTTPException(status_code=401, detail="Authentication required")
    tank_ids: List[int] = []
    if tank_id is not None:
        tank = get_tank_for_user(db, user, tank_id)
        if not tank:
            db.close()
            raise HTTPException(status_code=404, detail="Tank not found")
        tank_ids = [tank_id]
    else:
        tank_ids = get_visible_tank_ids(db, user)
    if not tank_ids:
        db.close()
        return JSONResponse([])
    placeholders = ",".join(["?"] * len(tank_ids))
    targets = q(
        db,
        f"SELECT * FROM targets WHERE tank_id IN ({placeholders}) ORDER BY tank_id, parameter",
        tuple(tank_ids),
    )
    payload = []
    for target in targets:
        payload.append(
            {
                "id": target["id"],
                "tank_id": target["tank_id"],
                "parameter": target["parameter"],
                "target_low": row_get(target, "target_low") if "target_low" in target.keys() else row_get(target, "low"),
                "target_high": row_get(target, "target_high") if "target_high" in target.keys() else row_get(target, "high"),
                "alert_low": row_get(target, "alert_low"),
                "alert_high": row_get(target, "alert_high"),
                "unit": row_get(target, "unit") or "",
                "enabled": row_get(target, "enabled"),
            }
        )
    db.close()
    return JSONResponse(payload)

@app.get("/tanks/{tank_id}/profile", response_class=HTMLResponse)
@app.get("/tanks/{tank_id}/edit", response_class=HTMLResponse, include_in_schema=False)
def tank_profile(request: Request, tank_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    if not profile:
        try: vol = tank["volume_l"] if "volume_l" in tank.keys() else None
        except Exception: vol = None
        execute_with_retry(
            db,
            "INSERT INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?, ?, 100) "
            "ON CONFLICT (tank_id) DO NOTHING",
            (tank_id, vol),
        )
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

@app.get("/tanks/{tank_id}/dosing-settings", response_class=HTMLResponse)
def tank_dosing_settings(request: Request, tank_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    additives_rows = get_visible_additives(db, user, active_only=True)
    grouped_additives: Dict[str, List[Dict[str, Any]]] = {}
    for a in additives_rows:
        parameter = (a["parameter"] or "other").strip().lower()
        if "alk" in parameter or "kh" in parameter:
            key = "alkalinity"
        elif "calcium" in parameter or parameter == "ca":
            key = "calcium"
        elif "magnesium" in parameter or parameter == "mg":
            key = "magnesium"
        elif "nitrate" in parameter or "no3" in parameter:
            key = "nitrate"
        elif "phosphate" in parameter or "po4" in parameter:
            key = "phosphate"
        elif "trace" in parameter:
            key = "trace"
        else:
            key = parameter
        grouped_additives.setdefault(key, []).append(a)
    grouped_additives.setdefault("all", []).extend(additives_rows)
    extra_dosing_entries = q(
        db,
        "SELECT id, parameter, solution, daily_ml, container_ml, remaining_ml FROM dosing_entries WHERE tank_id=? AND active=1 ORDER BY id",
        (tank_id,),
    )
    profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    if not profile:
        try: vol = tank["volume_l"] if "volume_l" in tank.keys() else None
        except Exception: vol = None
        execute_with_retry(
            db,
            "INSERT INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?, ?, 100) "
            "ON CONFLICT (tank_id) DO NOTHING",
            (tank_id, vol),
        )
        db.commit()
        profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    tank_view = dict(tank)
    if profile:
        try:
            tank_view["dosing_mode"] = profile["dosing_mode"]
            tank_view["all_in_one_solution"] = profile["all_in_one_solution"]
            tank_view["all_in_one_daily_ml"] = profile["all_in_one_daily_ml"]
            tank_view["calcium_reactor_daily_ml"] = profile["calcium_reactor_daily_ml"]
            tank_view["calcium_reactor_effluent_dkh"] = profile["calcium_reactor_effluent_dkh"]
            tank_view["kalk_solution"] = profile["kalk_solution"]
            tank_view["kalk_daily_ml"] = profile["kalk_daily_ml"]
            tank_view["use_all_in_one"] = profile["use_all_in_one"]
            tank_view["use_alk"] = profile["use_alk"]
            tank_view["use_ca"] = profile["use_ca"]
            tank_view["use_mg"] = profile["use_mg"]
            tank_view["use_nitrate"] = profile["use_nitrate"]
            tank_view["use_phosphate"] = profile["use_phosphate"]
            tank_view["use_trace"] = profile["use_trace"]
            tank_view["use_nopox"] = profile["use_nopox"]
            tank_view["use_calcium_reactor"] = profile["use_calcium_reactor"]
            tank_view["use_kalkwasser"] = profile["use_kalkwasser"]
            tank_view["alk_solution"] = profile["alk_solution"]
            tank_view["alk_daily_ml"] = profile["alk_daily_ml"]
            tank_view["ca_solution"] = profile["ca_solution"]
            tank_view["ca_daily_ml"] = profile["ca_daily_ml"]
            tank_view["mg_solution"] = profile["mg_solution"]
            tank_view["mg_daily_ml"] = profile["mg_daily_ml"]
            tank_view["nitrate_solution"] = profile["nitrate_solution"]
            tank_view["nitrate_daily_ml"] = profile["nitrate_daily_ml"]
            tank_view["phosphate_solution"] = profile["phosphate_solution"]
            tank_view["phosphate_daily_ml"] = profile["phosphate_daily_ml"]
            tank_view["trace_solution"] = profile["trace_solution"]
            tank_view["trace_daily_ml"] = profile["trace_daily_ml"]
            tank_view["nopox_daily_ml"] = profile["nopox_daily_ml"]
            tank_view["all_in_one_container_ml"] = profile["all_in_one_container_ml"]
            tank_view["all_in_one_remaining_ml"] = profile["all_in_one_remaining_ml"]
            tank_view["alk_container_ml"] = profile["alk_container_ml"]
            tank_view["alk_remaining_ml"] = profile["alk_remaining_ml"]
            tank_view["kalk_container_ml"] = profile["kalk_container_ml"]
            tank_view["kalk_remaining_ml"] = profile["kalk_remaining_ml"]
            tank_view["ca_container_ml"] = profile["ca_container_ml"]
            tank_view["ca_remaining_ml"] = profile["ca_remaining_ml"]
            tank_view["mg_container_ml"] = profile["mg_container_ml"]
            tank_view["mg_remaining_ml"] = profile["mg_remaining_ml"]
            tank_view["nitrate_container_ml"] = profile["nitrate_container_ml"]
            tank_view["nitrate_remaining_ml"] = profile["nitrate_remaining_ml"]
            tank_view["phosphate_container_ml"] = profile["phosphate_container_ml"]
            tank_view["phosphate_remaining_ml"] = profile["phosphate_remaining_ml"]
            tank_view["trace_container_ml"] = profile["trace_container_ml"]
            tank_view["trace_remaining_ml"] = profile["trace_remaining_ml"]
            tank_view["nopox_container_ml"] = profile["nopox_container_ml"]
            tank_view["nopox_remaining_ml"] = profile["nopox_remaining_ml"]
            tank_view["dosing_low_days"] = profile["dosing_low_days"] if profile["dosing_low_days"] is not None else 5
        except Exception: pass
    param_limits = {}
    try:
        pdefs = q(db, "SELECT id, name, max_daily_change, unit FROM parameter_defs WHERE active=1")
        if user:
            pdefs = apply_user_parameter_overrides(pdefs, get_user_parameter_settings(db, user["id"]))
        for row in pdefs:
            name = row_get(row, "name")
            if not name:
                continue
            param_limits[name] = {
                "max_daily_change": row_get(row, "max_daily_change"),
                "unit": row_get(row, "unit") or "",
            }
    except Exception:
        param_limits = {}
    daily_consumption = build_daily_consumption(db, tank_view, user) if profile else {}
    suggested_dosing: Dict[str, float] = {}
    volume_l = row_get(tank_view, "volume_l")
    if volume_l and daily_consumption:
        for additive in additives_rows:
            strength = row_get(additive, "strength")
            parameter = row_get(additive, "parameter") or ""
            if strength in (None, 0):
                continue
            entry = daily_consumption.get(normalize_param_name(parameter))
            if not entry:
                continue
            suggested_ml = (float(entry["value"]) / float(strength)) * (float(volume_l) / 100.0)
            suggested_dosing[additive["name"]] = round(suggested_ml, 2)
    db.close()
    dosing_entries_view = []
    type_map = {
        "all_in_one": ("All-in-one", "all_in_one_solution", "all_in_one_daily_ml", "all_in_one_container_ml", "all_in_one_remaining_ml"),
        "alk": ("Alkalinity", "alk_solution", "alk_daily_ml", "alk_container_ml", "alk_remaining_ml"),
        "kalkwasser": ("Kalkwasser", "kalk_solution", "kalk_daily_ml", "kalk_container_ml", "kalk_remaining_ml"),
        "ca": ("Calcium", "ca_solution", "ca_daily_ml", "ca_container_ml", "ca_remaining_ml"),
        "mg": ("Magnesium", "mg_solution", "mg_daily_ml", "mg_container_ml", "mg_remaining_ml"),
        "nitrate": ("Nitrate", "nitrate_solution", "nitrate_daily_ml", "nitrate_container_ml", "nitrate_remaining_ml"),
        "phosphate": ("Phosphate", "phosphate_solution", "phosphate_daily_ml", "phosphate_container_ml", "phosphate_remaining_ml"),
        "trace": ("Trace Elements", "trace_solution", "trace_daily_ml", "trace_container_ml", "trace_remaining_ml"),
        "nopox": ("NoPox", None, "nopox_daily_ml", "nopox_container_ml", "nopox_remaining_ml"),
    }
    for key, (label, solution_col, daily_col, container_col, remaining_col) in type_map.items():
        daily_val = row_get(profile, daily_col) if profile else None
        solution_val = row_get(profile, solution_col) if solution_col and profile else None
        if key == "nopox" and daily_val is not None and not solution_val:
            solution_val = "NoPox"
        if daily_val is None and not solution_val:
            continue
        dosing_entries_view.append(
            {
                "label": label,
                "solution": solution_val or "",
                "daily_ml": daily_val,
                "container_ml": row_get(profile, container_col) if profile else None,
                "remaining_ml": row_get(profile, remaining_col) if profile else None,
            }
        )
    for entry in extra_dosing_entries:
        label = (row_get(entry, "parameter") or "").strip()
        dosing_entries_view.append(
            {
                "label": label or "Trace Elements",
                "solution": row_get(entry, "solution") or "",
                "daily_ml": row_get(entry, "daily_ml"),
                "container_ml": row_get(entry, "container_ml"),
                "remaining_ml": row_get(entry, "remaining_ml"),
            }
        )
    return templates.TemplateResponse(
        "dosing_settings.html",
        {
            "request": request,
            "tank": tank_view,
            "additives": additives_rows,
            "grouped_additives": grouped_additives,
            "suggested_dosing": suggested_dosing,
            "param_limits": param_limits,
            "dosing_entries": dosing_entries_view,
        },
    )

@app.post("/tanks/{tank_id}/dosing-settings")
async def tank_dosing_settings_save(request: Request, tank_id: int):
    form = await request.form()
    dosing_mode = (form.get("dosing_mode") or "").strip() or None
    entry_solutions = form.getlist("dosing_solution[]")
    entry_daily_ml = form.getlist("dosing_daily_ml[]")
    entry_container_ml = form.getlist("dosing_container_ml[]")
    entry_remaining_ml = form.getlist("dosing_remaining_ml[]")
    nopox_daily_ml = None
    calcium_reactor_daily_ml = to_float(form.get("calcium_reactor_daily_ml"))
    calcium_reactor_effluent_dkh = to_float(form.get("calcium_reactor_effluent_dkh"))
    type_config = {
        "all_in_one": ("All-in-one", "all_in_one_solution", "all_in_one_daily_ml", "all_in_one_container_ml", "all_in_one_remaining_ml", "use_all_in_one"),
        "alk": ("Alkalinity", "alk_solution", "alk_daily_ml", "alk_container_ml", "alk_remaining_ml", "use_alk"),
        "kalkwasser": ("Kalkwasser", "kalk_solution", "kalk_daily_ml", "kalk_container_ml", "kalk_remaining_ml", "use_kalkwasser"),
        "ca": ("Calcium", "ca_solution", "ca_daily_ml", "ca_container_ml", "ca_remaining_ml", "use_ca"),
        "mg": ("Magnesium", "mg_solution", "mg_daily_ml", "mg_container_ml", "mg_remaining_ml", "use_mg"),
        "nitrate": ("Nitrate", "nitrate_solution", "nitrate_daily_ml", "nitrate_container_ml", "nitrate_remaining_ml", "use_nitrate"),
        "phosphate": ("Phosphate", "phosphate_solution", "phosphate_daily_ml", "phosphate_container_ml", "phosphate_remaining_ml", "use_phosphate"),
        "trace": ("Trace Elements", "trace_solution", "trace_daily_ml", "trace_container_ml", "trace_remaining_ml", "use_trace"),
        "nopox": ("NoPox", None, "nopox_daily_ml", "nopox_container_ml", "nopox_remaining_ml", "use_nopox"),
    }
    db = get_db()
    user = get_current_user(db, request)
    additives_visible = get_visible_additives(db, user, active_only=True)
    additives_by_name = {
        row_get(row, "name"): row for row in additives_visible
    }

    def type_key_for_additive(solution_name: str) -> str | None:
        if not solution_name:
            return None
        if solution_name == "NoPox":
            return "nopox"
        additive = additives_by_name.get(solution_name)
        if not additive:
            return None
        group_name = (row_get(additive, "group_name") or "").lower()
        if "all-in-one" in group_name or "all in one" in group_name:
            return "all_in_one"
        parameter = (row_get(additive, "parameter") or "").lower()
        if "alk" in parameter or "kh" in parameter:
            return "alk"
        if "calcium" in parameter or parameter == "ca":
            return "ca"
        if "magnesium" in parameter or parameter == "mg":
            return "mg"
        if "nitrate" in parameter or "no3" in parameter:
            return "nitrate"
        if "phosphate" in parameter or "po4" in parameter:
            return "phosphate"
        if "trace" in parameter:
            return "trace"
        return "all_in_one"
    primary_entries = {}
    extra_entries = []
    for idx, solution in enumerate(entry_solutions):
        solution = (solution or "").strip()
        type_key = type_key_for_additive(solution)
        if not type_key or type_key not in type_config:
            continue
        _, solution_col, daily_col, container_col, remaining_col, _ = type_config[type_key]
        daily_val = to_float(entry_daily_ml[idx]) if idx < len(entry_daily_ml) else None
        if daily_val is None:
            continue
        container_val = to_float(entry_container_ml[idx]) if idx < len(entry_container_ml) else None
        remaining_val = to_float(entry_remaining_ml[idx]) if idx < len(entry_remaining_ml) else None
        if container_val is not None and remaining_val is None:
            remaining_val = container_val
        entry_data = {
            "type_key": type_key,
            "solution": solution if solution_col else None,
            "daily_ml": daily_val,
            "container_ml": container_val,
            "remaining_ml": remaining_val,
        }
        if type_key not in primary_entries:
            primary_entries[type_key] = entry_data
        else:
            extra_entries.append(entry_data)
    def primary_value(type_key, key, default=None):
        entry = primary_entries.get(type_key) or {}
        return entry.get(key, default)
    all_in_one_solution = primary_value("all_in_one", "solution")
    all_in_one_daily_ml = primary_value("all_in_one", "daily_ml")
    all_in_one_container_ml = primary_value("all_in_one", "container_ml")
    all_in_one_remaining_ml = primary_value("all_in_one", "remaining_ml")
    alk_solution = primary_value("alk", "solution")
    alk_daily_ml = primary_value("alk", "daily_ml")
    alk_container_ml = primary_value("alk", "container_ml")
    alk_remaining_ml = primary_value("alk", "remaining_ml")
    kalk_solution = primary_value("kalkwasser", "solution")
    kalk_daily_ml = primary_value("kalkwasser", "daily_ml")
    kalk_container_ml = primary_value("kalkwasser", "container_ml")
    kalk_remaining_ml = primary_value("kalkwasser", "remaining_ml")
    ca_solution = primary_value("ca", "solution")
    ca_daily_ml = primary_value("ca", "daily_ml")
    ca_container_ml = primary_value("ca", "container_ml")
    ca_remaining_ml = primary_value("ca", "remaining_ml")
    mg_solution = primary_value("mg", "solution")
    mg_daily_ml = primary_value("mg", "daily_ml")
    mg_container_ml = primary_value("mg", "container_ml")
    mg_remaining_ml = primary_value("mg", "remaining_ml")
    nitrate_solution = primary_value("nitrate", "solution")
    nitrate_daily_ml = primary_value("nitrate", "daily_ml")
    nitrate_container_ml = primary_value("nitrate", "container_ml")
    nitrate_remaining_ml = primary_value("nitrate", "remaining_ml")
    phosphate_solution = primary_value("phosphate", "solution")
    phosphate_daily_ml = primary_value("phosphate", "daily_ml")
    phosphate_container_ml = primary_value("phosphate", "container_ml")
    phosphate_remaining_ml = primary_value("phosphate", "remaining_ml")
    trace_solution = primary_value("trace", "solution")
    trace_daily_ml = primary_value("trace", "daily_ml")
    trace_container_ml = primary_value("trace", "container_ml")
    trace_remaining_ml = primary_value("trace", "remaining_ml")
    nopox_daily_ml = primary_value("nopox", "daily_ml")
    nopox_container_ml = primary_value("nopox", "container_ml")
    nopox_remaining_ml = primary_value("nopox", "remaining_ml")
    use_all_in_one = 1 if all_in_one_daily_ml is not None or all_in_one_solution else 0
    use_alk = 1 if alk_daily_ml is not None or alk_solution else 0
    use_kalkwasser = 1 if kalk_daily_ml is not None or kalk_solution else 0
    use_ca = 1 if ca_daily_ml is not None or ca_solution else 0
    use_mg = 1 if mg_daily_ml is not None or mg_solution else 0
    use_nitrate = 1 if nitrate_daily_ml is not None or nitrate_solution else 0
    use_phosphate = 1 if phosphate_daily_ml is not None or phosphate_solution else 0
    use_trace = 1 if trace_daily_ml is not None or trace_solution else 0
    use_nopox = 1 if nopox_daily_ml is not None else 0
    use_calcium_reactor = 1 if calcium_reactor_daily_ml is not None or calcium_reactor_effluent_dkh is not None else 0
    dosing_low_days = to_float(form.get("dosing_low_days"))
    if dosing_low_days is None:
        dosing_low_days = 5
    if all_in_one_container_ml is not None and all_in_one_remaining_ml is None:
        all_in_one_remaining_ml = all_in_one_container_ml
    if alk_container_ml is not None and alk_remaining_ml is None:
        alk_remaining_ml = alk_container_ml
    if kalk_container_ml is not None and kalk_remaining_ml is None:
        kalk_remaining_ml = kalk_container_ml
    if ca_container_ml is not None and ca_remaining_ml is None:
        ca_remaining_ml = ca_container_ml
    if mg_container_ml is not None and mg_remaining_ml is None:
        mg_remaining_ml = mg_container_ml
    if nitrate_container_ml is not None and nitrate_remaining_ml is None:
        nitrate_remaining_ml = nitrate_container_ml
    if phosphate_container_ml is not None and phosphate_remaining_ml is None:
        phosphate_remaining_ml = phosphate_container_ml
    if trace_container_ml is not None and trace_remaining_ml is None:
        trace_remaining_ml = trace_container_ml
    if nopox_container_ml is not None and nopox_remaining_ml is None:
        nopox_remaining_ml = nopox_container_ml
    container_updated_at = None
    if not use_trace:
        extra_entries = [entry for entry in extra_entries if entry.get("type_key") != "trace"]

    if any(
        value is not None
        for value in (
            all_in_one_container_ml,
            all_in_one_remaining_ml,
            alk_container_ml,
            alk_remaining_ml,
            kalk_container_ml,
            kalk_remaining_ml,
            ca_container_ml,
            ca_remaining_ml,
            mg_container_ml,
            mg_remaining_ml,
            nitrate_container_ml,
            nitrate_remaining_ml,
            phosphate_container_ml,
            phosphate_remaining_ml,
            trace_container_ml,
            trace_remaining_ml,
            nopox_container_ml,
            nopox_remaining_ml,
        )
    ):
        container_updated_at = datetime.utcnow().isoformat()
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    if not profile:
        db.close()
        raise HTTPException(status_code=404, detail="Tank profile not found")
    previous_values = dict(profile)
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
    ensure_column(db, "tank_profiles", "trace_solution", "ALTER TABLE tank_profiles ADD COLUMN trace_solution TEXT")
    ensure_column(db, "tank_profiles", "trace_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN trace_daily_ml REAL")
    ensure_column(db, "tank_profiles", "nopox_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN nopox_daily_ml REAL")
    ensure_column(db, "tank_profiles", "calcium_reactor_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN calcium_reactor_daily_ml REAL")
    ensure_column(db, "tank_profiles", "calcium_reactor_effluent_dkh", "ALTER TABLE tank_profiles ADD COLUMN calcium_reactor_effluent_dkh REAL")
    ensure_column(db, "tank_profiles", "kalk_solution", "ALTER TABLE tank_profiles ADD COLUMN kalk_solution TEXT")
    ensure_column(db, "tank_profiles", "kalk_daily_ml", "ALTER TABLE tank_profiles ADD COLUMN kalk_daily_ml REAL")
    ensure_column(db, "tank_profiles", "use_all_in_one", "ALTER TABLE tank_profiles ADD COLUMN use_all_in_one INTEGER")
    ensure_column(db, "tank_profiles", "use_alk", "ALTER TABLE tank_profiles ADD COLUMN use_alk INTEGER")
    ensure_column(db, "tank_profiles", "use_ca", "ALTER TABLE tank_profiles ADD COLUMN use_ca INTEGER")
    ensure_column(db, "tank_profiles", "use_mg", "ALTER TABLE tank_profiles ADD COLUMN use_mg INTEGER")
    ensure_column(db, "tank_profiles", "use_nitrate", "ALTER TABLE tank_profiles ADD COLUMN use_nitrate INTEGER")
    ensure_column(db, "tank_profiles", "use_phosphate", "ALTER TABLE tank_profiles ADD COLUMN use_phosphate INTEGER")
    ensure_column(db, "tank_profiles", "use_trace", "ALTER TABLE tank_profiles ADD COLUMN use_trace INTEGER")
    ensure_column(db, "tank_profiles", "use_nopox", "ALTER TABLE tank_profiles ADD COLUMN use_nopox INTEGER")
    ensure_column(db, "tank_profiles", "use_calcium_reactor", "ALTER TABLE tank_profiles ADD COLUMN use_calcium_reactor INTEGER")
    ensure_column(db, "tank_profiles", "use_kalkwasser", "ALTER TABLE tank_profiles ADD COLUMN use_kalkwasser INTEGER")
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
    ensure_column(db, "tank_profiles", "trace_container_ml", "ALTER TABLE tank_profiles ADD COLUMN trace_container_ml REAL")
    ensure_column(db, "tank_profiles", "trace_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN trace_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "nopox_container_ml", "ALTER TABLE tank_profiles ADD COLUMN nopox_container_ml REAL")
    ensure_column(db, "tank_profiles", "nopox_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN nopox_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "kalk_container_ml", "ALTER TABLE tank_profiles ADD COLUMN kalk_container_ml REAL")
    ensure_column(db, "tank_profiles", "kalk_remaining_ml", "ALTER TABLE tank_profiles ADD COLUMN kalk_remaining_ml REAL")
    ensure_column(db, "tank_profiles", "dosing_container_updated_at", "ALTER TABLE tank_profiles ADD COLUMN dosing_container_updated_at TEXT")
    ensure_column(db, "tank_profiles", "dosing_low_days", "ALTER TABLE tank_profiles ADD COLUMN dosing_low_days REAL")
    db.execute(
        """UPDATE tank_profiles SET
             dosing_mode=?,
             all_in_one_solution=?,
             all_in_one_daily_ml=?,
             alk_solution=?,
             alk_daily_ml=?,
             kalk_solution=?,
             kalk_daily_ml=?,
             ca_solution=?,
             ca_daily_ml=?,
             mg_solution=?,
             mg_daily_ml=?,
             nitrate_solution=?,
             nitrate_daily_ml=?,
             phosphate_solution=?,
             phosphate_daily_ml=?,
             trace_solution=?,
             trace_daily_ml=?,
             nopox_daily_ml=?,
             calcium_reactor_daily_ml=?,
             calcium_reactor_effluent_dkh=?,
             use_all_in_one=?,
             use_alk=?,
             use_ca=?,
             use_mg=?,
             use_nitrate=?,
             use_phosphate=?,
             use_trace=?,
             use_nopox=?,
             use_calcium_reactor=?,
             use_kalkwasser=?,
             all_in_one_container_ml=?,
             all_in_one_remaining_ml=?,
             alk_container_ml=?,
             alk_remaining_ml=?,
             kalk_container_ml=?,
             kalk_remaining_ml=?,
             ca_container_ml=?,
             ca_remaining_ml=?,
             mg_container_ml=?,
             mg_remaining_ml=?,
             nitrate_container_ml=?,
             nitrate_remaining_ml=?,
             phosphate_container_ml=?,
             phosphate_remaining_ml=?,
             trace_container_ml=?,
             trace_remaining_ml=?,
             nopox_container_ml=?,
             nopox_remaining_ml=?,
             dosing_container_updated_at=?,
             dosing_low_days=?
           WHERE tank_id=?""",
        (
            dosing_mode,
            all_in_one_solution,
            all_in_one_daily_ml,
            alk_solution,
            alk_daily_ml,
            kalk_solution,
            kalk_daily_ml,
            ca_solution,
            ca_daily_ml,
            mg_solution,
            mg_daily_ml,
            nitrate_solution,
            nitrate_daily_ml,
            phosphate_solution,
            phosphate_daily_ml,
            trace_solution,
            trace_daily_ml,
            nopox_daily_ml,
            calcium_reactor_daily_ml,
            calcium_reactor_effluent_dkh,
            use_all_in_one,
            use_alk,
            use_ca,
            use_mg,
            use_nitrate,
            use_phosphate,
            use_trace,
            use_nopox,
            use_calcium_reactor,
            use_kalkwasser,
            all_in_one_container_ml,
            all_in_one_remaining_ml,
            alk_container_ml,
            alk_remaining_ml,
            kalk_container_ml,
            kalk_remaining_ml,
            ca_container_ml,
            ca_remaining_ml,
            mg_container_ml,
            mg_remaining_ml,
            nitrate_container_ml,
            nitrate_remaining_ml,
            phosphate_container_ml,
            phosphate_remaining_ml,
            trace_container_ml,
            trace_remaining_ml,
            nopox_container_ml,
            nopox_remaining_ml,
            container_updated_at,
            dosing_low_days,
            tank_id,
        ),
    )
    db.execute(
        "DELETE FROM dosing_entries WHERE tank_id=?",
        (tank_id,),
    )
    if extra_entries:
        now_iso = datetime.utcnow().isoformat()
        for entry in extra_entries:
            label = type_config[entry["type_key"]][0]
            db.execute(
                """INSERT INTO dosing_entries
                   (tank_id, parameter, solution, daily_ml, container_ml, remaining_ml, active, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, 1, ?)""",
                (
                    tank_id,
                    label,
                    entry["solution"],
                    entry["daily_ml"],
                    entry["container_ml"],
                    entry["remaining_ml"],
                    now_iso,
                ),
            )
        if container_updated_at is None and any(
            entry.get("container_ml") is not None or entry.get("remaining_ml") is not None
            for entry in extra_entries
        ):
            db.execute(
                "UPDATE tank_profiles SET dosing_container_updated_at=? WHERE tank_id=?",
                (now_iso, tank_id),
            )
    updated_values = {
        "dosing_mode": dosing_mode,
        "all_in_one_solution": all_in_one_solution,
        "all_in_one_daily_ml": all_in_one_daily_ml,
        "alk_solution": alk_solution,
        "alk_daily_ml": alk_daily_ml,
        "kalk_solution": kalk_solution,
        "kalk_daily_ml": kalk_daily_ml,
        "ca_solution": ca_solution,
        "ca_daily_ml": ca_daily_ml,
        "mg_solution": mg_solution,
        "mg_daily_ml": mg_daily_ml,
        "nitrate_solution": nitrate_solution,
        "nitrate_daily_ml": nitrate_daily_ml,
        "phosphate_solution": phosphate_solution,
        "phosphate_daily_ml": phosphate_daily_ml,
        "trace_solution": trace_solution,
        "trace_daily_ml": trace_daily_ml,
        "nopox_daily_ml": nopox_daily_ml,
        "calcium_reactor_daily_ml": calcium_reactor_daily_ml,
        "calcium_reactor_effluent_dkh": calcium_reactor_effluent_dkh,
        "use_all_in_one": use_all_in_one,
        "use_alk": use_alk,
        "use_ca": use_ca,
        "use_mg": use_mg,
        "use_nitrate": use_nitrate,
        "use_phosphate": use_phosphate,
        "use_trace": use_trace,
        "use_nopox": use_nopox,
        "use_calcium_reactor": use_calcium_reactor,
        "use_kalkwasser": use_kalkwasser,
        "dosing_low_days": dosing_low_days,
    }
    changed = []
    for key, new_value in updated_values.items():
        old_value = previous_values.get(key)
        if old_value != new_value:
            changed.append(f"{key}: {old_value} → {new_value}")
    if changed:
        insert_tank_journal(
            db,
            tank_id,
            datetime.utcnow().isoformat(),
            "dosing",
            "Updated dosing settings",
            "; ".join(changed),
        )
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}")

@app.post("/tanks/{tank_id}/profile")
@app.post("/tanks/{tank_id}/edit", include_in_schema=False)
async def tank_profile_save(request: Request, tank_id: int):
    form = await request.form()
    volume_l = to_float(form.get("volume_l"))
    net_percent = to_float(form.get("net_percent"))
    if net_percent is None: net_percent = 100
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    ensure_column(db, "tank_profiles", "volume_l", "ALTER TABLE tank_profiles ADD COLUMN volume_l REAL")
    ensure_column(db, "tank_profiles", "net_percent", "ALTER TABLE tank_profiles ADD COLUMN net_percent REAL")
    db.execute("UPDATE tanks SET volume_l=? WHERE id=?", (volume_l, tank_id))
    execute_with_retry(
        db,
        "INSERT INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?, ?, ?) "
        "ON CONFLICT (tank_id) DO NOTHING",
        (tank_id, volume_l, float(net_percent)),
    )
    db.execute(
        "UPDATE tank_profiles SET volume_l=?, net_percent=? WHERE tank_id=?",
        (volume_l, float(net_percent), tank_id),
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
        "kalk": ("kalk_container_ml", "kalk_remaining_ml"),
        "ca": ("ca_container_ml", "ca_remaining_ml"),
        "mg": ("mg_container_ml", "mg_remaining_ml"),
        "nitrate": ("nitrate_container_ml", "nitrate_remaining_ml"),
        "phosphate": ("phosphate_container_ml", "phosphate_remaining_ml"),
        "trace": ("trace_container_ml", "trace_remaining_ml"),
        "nopox": ("nopox_container_ml", "nopox_remaining_ml"),
    }
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    if not profile:
        db.close()
        return redirect(f"/tanks/{tank_id}")
    if container_key.startswith("extra_"):
        entry_id = container_key.replace("extra_", "")
        if not entry_id.isdigit():
            db.close()
            return redirect(f"/tanks/{tank_id}")
        entry = one(
            db,
            "SELECT id, solution, container_ml FROM dosing_entries WHERE id=? AND tank_id=?",
            (int(entry_id), tank_id),
        )
        if entry and row_get(entry, "container_ml") is not None:
            capacity = row_get(entry, "container_ml")
            db.execute(
                "UPDATE dosing_entries SET remaining_ml=? WHERE id=?",
                (capacity, row_get(entry, "id")),
            )
            db.execute(
                "UPDATE tank_profiles SET dosing_container_updated_at=? WHERE tank_id=?",
                (datetime.utcnow().isoformat(), tank_id),
            )
            db.execute(
                "DELETE FROM dosing_notifications WHERE tank_id=? AND container_key=?",
                (tank_id, container_key),
            )
            label = row_get(entry, "solution") or row_get(entry, "parameter") or "Dosing"
            insert_tank_journal(
                db,
                tank_id,
                datetime.utcnow().isoformat(),
                "dosing",
                f"Refilled {label} container",
                f"Reset remaining volume to {capacity} ml.",
            )
            db.commit()
    else:
        if container_key not in mapping:
            db.close()
            return redirect(f"/tanks/{tank_id}")
        capacity_col, remaining_col = mapping[container_key]
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
            label_map = {
                "all_in_one": "All-in-one",
                "alk": "Alkalinity",
                "kalk": "Kalkwasser",
                "ca": "Calcium",
                "mg": "Magnesium",
                "nitrate": "Nitrate",
                "phosphate": "Phosphate",
                "trace": "Trace Elements",
                "nopox": "NoPox",
            }
            label = label_map.get(container_key, container_key)
            insert_tank_journal(
                db,
                tank_id,
                datetime.utcnow().isoformat(),
                "dosing",
                f"Refilled {label} container",
                f"Reset remaining volume to {capacity} ml.",
            )
            db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}")

@app.post("/notifications/dismiss")
async def dismiss_notification(request: Request):
    form = await request.form()
    tank_id = to_float(form.get("tank_id"))
    container_key = (form.get("container_key") or "").strip()
    notified_on = (form.get("notified_on") or "").strip()
    if tank_id is None or not container_key or not notified_on:
        return redirect("/")
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, int(tank_id))
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    db.execute(
        "UPDATE dosing_notifications SET dismissed_at=? WHERE tank_id=? AND container_key=? AND notified_on=?",
        (datetime.utcnow().isoformat(), int(tank_id), container_key, notified_on),
    )
    log_audit(
        db,
        user,
        "notification-dismiss",
        {"tank_id": tank_id, "container_key": container_key, "notified_on": notified_on},
    )
    db.commit()
    db.close()
    return redirect(request.headers.get("referer") or "/")

@app.get("/api/notifications")
def api_notifications(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return JSONResponse({"alerts": []})
    if row_get(user, "admin"):
        alerts = collect_dosing_notifications(db, actor_user=user)
    else:
        alerts = collect_dosing_notifications(db, owner_user_id=user["id"], actor_user=user)
    db.close()
    return JSONResponse({"alerts": alerts})

@app.get("/api/push/public-key")
def push_public_key():
    public_key, _, _ = get_vapid_settings()
    if not public_key:
        raise HTTPException(status_code=404, detail="Push notifications not configured.")
    return JSONResponse({"public_key": public_key})

@app.post("/api/push/subscribe")
@limiter.limit("20/minute")
async def push_subscribe(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        raise HTTPException(status_code=401, detail="Not authenticated")
    payload = await request.json()
    endpoint = payload.get("endpoint") if isinstance(payload, dict) else None
    if not endpoint:
        db.close()
        raise HTTPException(status_code=400, detail="Missing subscription endpoint")
    try:
        execute_with_retry(
            db,
            "INSERT INTO push_subscriptions (user_id, endpoint, subscription_json, created_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT (user_id, endpoint) DO UPDATE "
            "SET subscription_json=excluded.subscription_json, created_at=excluded.created_at",
            (user["id"], endpoint, json.dumps(payload), datetime.utcnow().isoformat()),
        )
        db.commit()
    finally:
        db.close()
    return JSONResponse({"ok": True})

@app.post("/api/push/unsubscribe")
async def push_unsubscribe(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        raise HTTPException(status_code=401, detail="Not authenticated")
    payload = await request.json()
    endpoint = payload.get("endpoint") if isinstance(payload, dict) else None
    if not endpoint:
        db.close()
        raise HTTPException(status_code=400, detail="Missing subscription endpoint")
    try:
        db.execute(
            "DELETE FROM push_subscriptions WHERE user_id=? AND endpoint=?",
            (user["id"], endpoint),
        )
        db.commit()
    finally:
        db.close()
    return JSONResponse({"ok": True})

@app.post("/api/push/test")
@limiter.limit("10/minute")
async def push_test(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        raise HTTPException(status_code=401, detail="Not authenticated")
    payload = await request.json()
    subscription = payload.get("subscription") if isinstance(payload, dict) else None
    if subscription and isinstance(subscription, dict):
        endpoint = subscription.get("endpoint")
        if endpoint:
            try:
                execute_with_retry(
                    db,
                    "INSERT INTO push_subscriptions (user_id, endpoint, subscription_json, created_at) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT (user_id, endpoint) DO UPDATE "
                    "SET subscription_json=excluded.subscription_json, created_at=excluded.created_at",
                    (user["id"], endpoint, json.dumps(subscription), datetime.utcnow().isoformat()),
                )
                db.commit()
            except Exception as e:
                logger.error(f"Error saving subscription: {e}", exc_info=True)
                db.rollback()
    public_key, private_key, subject = get_vapid_settings()
    if not public_key or not private_key or not subject:
        db.close()
        raise HTTPException(status_code=404, detail="Push notifications not configured.")
    if subscription and isinstance(subscription, dict):
        webpush_module = get_webpush()
        if webpush_module is None:
            db.close()
            raise HTTPException(status_code=503, detail="Push notifications are unavailable.")
        try:
            webpush_module.webpush(
                subscription_info=subscription,
                data=json.dumps(
                    {
                        "title": "Reef Metrics",
                        "body": "Test notification sent successfully.",
                        "url": "/account",
                        "tag": f"test-{user['id']}",
                        "require_interaction": True,
                        "renotify": True,
                        "vibrate": [200, 100, 200],
                    }
                ),
                vapid_private_key=private_key,
                vapid_claims={"sub": subject},
            )
        except webpush_module.WebPushException as e:
            logger.error(f"WebPushException: {e}", exc_info=True)
            db.close()
            raise HTTPException(status_code=400, detail=f"Unable to send test notification: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in webpush: {type(e).__name__}: {e}", exc_info=True)
            import traceback
            traceback.print_exc()
            db.close()
            raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")
        db.close()
        return JSONResponse({"ok": True})
    has_subscription = one(
        db,
        "SELECT 1 FROM push_subscriptions WHERE user_id=?",
        (user["id"],),
    )
    if not has_subscription:
        db.close()
        raise HTTPException(status_code=400, detail="No push subscription found.")
    payload = {
        "title": "Reef Metrics",
        "body": "Test notification sent successfully.",
        "url": "/account",
        "tag": f"test-{user['id']}",
        "require_interaction": True,
        "renotify": True,
        "vibrate": [200, 100, 200],
    }
    try:
        send_web_push(db, [user["id"]], payload)
    finally:
        db.close()
    return JSONResponse({"ok": True})

@app.get("/api/tanks/{tank_id}/summary")
def tank_summary(request: Request, tank_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    latest_map = get_latest_per_parameter(db, tank_id)
    unit_by_name = {p["name"]: row_get(p, "unit") or "" for p in get_active_param_defs(db, user_id=user["id"] if user else None)}
    latest_values = []
    for name, data in latest_map.items():
        taken_at = data.get("taken_at")
        latest_values.append(
            {
                "parameter": name,
                "value": data.get("value"),
                "unit": unit_by_name.get(name, ""),
                "taken_at": taken_at.isoformat() if isinstance(taken_at, datetime) else taken_at,
                "sample_id": data.get("sample_id"),
            }
        )
    profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    extra_entries = q(
        db,
        "SELECT id, parameter, solution, daily_ml, container_ml, remaining_ml FROM dosing_entries WHERE tank_id=? AND active=1 ORDER BY id",
        (tank_id,),
    )
    db.close()
    dosing_entries = []
    if profile:
        dosing_entries = [
            {
                "key": "all_in_one",
                "solution": row_get(profile, "all_in_one_solution"),
                "daily_ml": row_get(profile, "all_in_one_daily_ml"),
                "container_ml": row_get(profile, "all_in_one_container_ml"),
                "remaining_ml": row_get(profile, "all_in_one_remaining_ml"),
            },
            {
                "key": "alk",
                "solution": row_get(profile, "alk_solution"),
                "daily_ml": row_get(profile, "alk_daily_ml"),
                "container_ml": row_get(profile, "alk_container_ml"),
                "remaining_ml": row_get(profile, "alk_remaining_ml"),
            },
            {
                "key": "kalk",
                "solution": row_get(profile, "kalk_solution"),
                "daily_ml": row_get(profile, "kalk_daily_ml"),
                "container_ml": row_get(profile, "kalk_container_ml"),
                "remaining_ml": row_get(profile, "kalk_remaining_ml"),
            },
            {
                "key": "ca",
                "solution": row_get(profile, "ca_solution"),
                "daily_ml": row_get(profile, "ca_daily_ml"),
                "container_ml": row_get(profile, "ca_container_ml"),
                "remaining_ml": row_get(profile, "ca_remaining_ml"),
            },
            {
                "key": "mg",
                "solution": row_get(profile, "mg_solution"),
                "daily_ml": row_get(profile, "mg_daily_ml"),
                "container_ml": row_get(profile, "mg_container_ml"),
                "remaining_ml": row_get(profile, "mg_remaining_ml"),
            },
            {
                "key": "nitrate",
                "solution": row_get(profile, "nitrate_solution"),
                "daily_ml": row_get(profile, "nitrate_daily_ml"),
                "container_ml": row_get(profile, "nitrate_container_ml"),
                "remaining_ml": row_get(profile, "nitrate_remaining_ml"),
            },
            {
                "key": "phosphate",
                "solution": row_get(profile, "phosphate_solution"),
                "daily_ml": row_get(profile, "phosphate_daily_ml"),
                "container_ml": row_get(profile, "phosphate_container_ml"),
                "remaining_ml": row_get(profile, "phosphate_remaining_ml"),
            },
            {
                "key": "trace",
                "solution": row_get(profile, "trace_solution"),
                "daily_ml": row_get(profile, "trace_daily_ml"),
                "container_ml": row_get(profile, "trace_container_ml"),
                "remaining_ml": row_get(profile, "trace_remaining_ml"),
            },
            {
                "key": "nopox",
                "solution": row_get(profile, "nopox_solution"),
                "daily_ml": row_get(profile, "nopox_daily_ml"),
                "container_ml": row_get(profile, "nopox_container_ml"),
                "remaining_ml": row_get(profile, "nopox_remaining_ml"),
            },
        ]
    dosing_entries = [entry for entry in dosing_entries if entry.get("daily_ml") or entry.get("container_ml") or entry.get("remaining_ml")]
    extra_payload = [
        {
            "key": f"extra_{row_get(entry, 'id')}",
            "solution": row_get(entry, "solution") or row_get(entry, "parameter"),
            "daily_ml": row_get(entry, "daily_ml"),
            "container_ml": row_get(entry, "container_ml"),
            "remaining_ml": row_get(entry, "remaining_ml"),
        }
        for entry in extra_entries
    ]
    return JSONResponse(
        {
            "tank": {"id": tank_id, "name": row_get(tank, "name")},
            "latest_readings": latest_values,
            "dosing_entries": dosing_entries,
            "extra_dosing_entries": extra_payload,
        }
    )

@app.get("/tanks/{tank_id}/add", response_class=HTMLResponse)
def add_sample_form(request: Request, tank_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    params_rows = filter_trace_parameters(get_active_param_defs(db, user_id=user["id"] if user else None))
    kits = get_visible_test_kits(db, user, active_only=True)
    kits_by_param = {}
    for k in kits:
        if is_trace_element(row_get(k, "parameter") or ""):
            continue
        kits_by_param.setdefault(k["parameter"], []).append(k)
    db.close()
    return templates.TemplateResponse("add_sample.html", {"request": request, "tank_id": tank_id, "tank": tank, "parameters": params_rows, "kits_by_param": kits_by_param, "kits": kits})

@app.post("/tanks/{tank_id}/add")
async def add_sample(request: Request, tank_id: int):
    form = await request.form()
    notes = (form.get("notes") or "").strip() or None
    taken_at = (form.get("taken_at") or "").strip()
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    if taken_at:
        try: when_iso = datetime.fromisoformat(taken_at).isoformat()
        except ValueError: when_iso = datetime.utcnow().isoformat()
    else: when_iso = datetime.utcnow().isoformat()
    sample_id = execute_insert_returning_id(
        db,
        "INSERT INTO samples (tank_id, taken_at, notes) VALUES (?, ?, ?)",
        (tank_id, when_iso, notes),
    )
    if sample_id is None:
        db.close()
        raise HTTPException(status_code=500, detail="Failed to save sample")
    pdefs = filter_trace_parameters(get_active_param_defs(db, user_id=user["id"] if user else None))
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
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
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
    return templates.TemplateResponse(
        "sample_detail.html",
        {
            "request": request,
            "tank": tank,
            "sample": s,
            "readings": readings,
            "recommendations": [],
        },
    )

@app.get("/tanks/{tank_id}/samples/{sample_id}/edit", response_class=HTMLResponse)
def sample_edit(request: Request, tank_id: int, sample_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    sample = one(db, "SELECT * FROM samples WHERE id=? AND tank_id=?", (sample_id, tank_id))
    if not tank or not sample:
        db.close()
        raise HTTPException(status_code=404, detail="Sample not found")
    pdefs = get_active_param_defs(db, user_id=user["id"] if user else None)
    readings = {r["name"]: r["value"] for r in get_sample_readings(db, sample_id)}
    kits = get_visible_test_kits(db, user, active_only=True)
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
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    sample = one(db, "SELECT * FROM samples WHERE id=? AND tank_id=?", (sample_id, tank_id))
    if not tank or not sample:
        db.close()
        raise HTTPException(status_code=404, detail="Sample not found")
    when_iso = None
    if taken_at:
        try: when_iso = datetime.fromisoformat(taken_at).isoformat()
        except Exception: when_iso = None
    if not when_iso: when_iso = (parse_dt_any(sample["taken_at"]) or datetime.utcnow()).isoformat()
    execute_with_retry(db, "UPDATE samples SET taken_at=?, notes=? WHERE id=? AND tank_id=?", (when_iso, notes, sample_id, tank_id))
    mode = values_mode(db)
    if mode == "sample_values" and table_exists(db, "sample_values"):
        execute_with_retry(db, "DELETE FROM sample_values WHERE sample_id=?", (sample_id,))
        if table_exists(db, "sample_value_kits"):
            execute_with_retry(db, "DELETE FROM sample_value_kits WHERE sample_id=?", (sample_id,))
    else:
        if table_exists(db, "parameters"):
            execute_with_retry(db, "DELETE FROM parameters WHERE sample_id=?", (sample_id,))
    pdefs = get_active_param_defs(db, user_id=user["id"] if user else None)
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
        if val is None: continue
        insert_sample_reading(db, sample_id, pname, float(val), punit, int(kit_id) if kit_id else None)
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/samples/{sample_id}")

@app.get("/tanks/{tank_id}/samples", response_class=HTMLResponse)
def tank_samples(request: Request, tank_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    samples_rows = q(db, "SELECT * FROM samples WHERE tank_id=? ORDER BY taken_at DESC", (tank_id,))
    samples = []
    for _row in samples_rows:
        _s = dict(_row)
        dt = parse_dt_any(_s.get("taken_at"))
        if dt is not None:
            _s["taken_at"] = dt
        samples.append(_s)
    sample_values, unit_by_name = {}, {}
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
            if name is None:
                continue
            try:
                value = float(r["value"]) if r["value"] is not None else None
            except Exception:
                value = r["value"]
            sid = int(r["sample_id"])
            sample_values.setdefault(sid, {})[name] = value
            try:
                u = r["unit"] or ""
            except Exception:
                u = ""
            if name not in unit_by_name and u:
                unit_by_name[name] = u
    all_param_names = set(unit_by_name.keys())
    if samples:
        all_param_names.update(sample_values.get(int(samples[0]["id"]), {}).keys())
    available_params = sorted(all_param_names, key=lambda s: s.lower())
    params = [{"id": name, "name": name, "unit": unit_by_name.get(name, "")} for name in available_params]
    deduped = request.query_params.get("deduped")
    db.close()
    return templates.TemplateResponse(
        "tank_samples.html",
        {
            "request": request,
            "tank": tank,
            "samples": samples,
            "params": params,
            "sample_values": sample_values,
            "deduped_count": int(deduped) if deduped and deduped.isdigit() else None,
        },
    )

@app.post("/tanks/{tank_id}/samples/dedupe")
async def tank_samples_dedupe(request: Request, tank_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    rows = q(db, "SELECT id, taken_at FROM samples WHERE tank_id=? ORDER BY taken_at ASC, id ASC", (tank_id,))
    sample_ids = [row["id"] for row in rows]
    samples_by_id: Dict[int, datetime.date] = {}
    for row in rows:
        dt = parse_dt_any(row_get(row, "taken_at"))
        if dt is None:
            continue
        samples_by_id[row["id"]] = dt.date()
    sample_values: Dict[int, Dict[str, Any]] = {}
    if sample_ids:
        placeholders = ",".join(["?"] * len(sample_ids))
        mode = values_mode(db)
        if mode == "sample_values":
            rows = q(
                db,
                f"SELECT pd.name AS name, sv.value AS value, s.id AS sample_id "
                f"FROM sample_values sv "
                f"JOIN parameter_defs pd ON pd.id = sv.parameter_id "
                f"JOIN samples s ON s.id = sv.sample_id "
                f"WHERE sv.sample_id IN ({placeholders})",
                tuple(sample_ids),
            )
        else:
            rows = q(
                db,
                f"SELECT p.name AS name, p.value AS value, s.id AS sample_id "
                f"FROM parameters p "
                f"JOIN samples s ON s.id = p.sample_id "
                f"WHERE p.sample_id IN ({placeholders})",
                tuple(sample_ids),
            )
        for row in rows:
            name = row_get(row, "name")
            if not name:
                continue
            sid = int(row["sample_id"])
            sample_values.setdefault(sid, {})[name] = row_get(row, "value")

    duplicates = []
    grouped_by_date: Dict[datetime.date, List[int]] = {}
    for sample_id, sample_date in samples_by_id.items():
        grouped_by_date.setdefault(sample_date, []).append(sample_id)

    for sample_date, ids in grouped_by_date.items():
        if len(ids) < 2:
            continue
        kept: List[int] = []
        for sid in ids:
            values = sample_values.get(sid, {})
            matched = False
            for kept_id in kept:
                kept_values = sample_values.get(kept_id, {})
                if any(
                    name in kept_values and kept_values[name] == value
                    for name, value in values.items()
                ):
                    matched = True
                    break
            if matched:
                duplicates.append(sid)
            else:
                kept.append(sid)

    removed = 0
    if duplicates:
        mode = values_mode(db)
        for sample_id in duplicates:
            execute_with_retry(db, "DELETE FROM samples WHERE id=? AND tank_id=?", (sample_id, tank_id))
            if mode == "sample_values":
                if table_exists(db, "sample_values"):
                    execute_with_retry(db, "DELETE FROM sample_values WHERE sample_id=?", (sample_id,))
                if table_exists(db, "sample_value_kits"):
                    execute_with_retry(db, "DELETE FROM sample_value_kits WHERE sample_id=?", (sample_id,))
            else:
                if table_exists(db, "parameters"):
                    execute_with_retry(db, "DELETE FROM parameters WHERE sample_id=?", (sample_id,))
            removed += 1
        log_audit(db, user, "samples-dedupe", {"tank_id": tank_id, "removed": removed})
        db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/samples?deduped={removed}")

@app.get("/tanks/{tank_id}/targets", response_class=HTMLResponse)
def edit_targets(request: Request, tank_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
        
    params = list_parameters(db, user_id=user["id"] if user else None)
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
    form = await request.form()
    user = get_current_user(db, request)
    params = list_parameters(db, user_id=user["id"] if user else None)
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
            execute_with_retry(db, "UPDATE targets SET enabled=0 WHERE tank_id=? AND parameter=?", (tank_id, name))
            continue
        execute_with_retry(
            db,
            """
            INSERT INTO targets (tank_id, parameter, target_low, target_high, alert_low, alert_high, unit, enabled)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (tank_id, parameter) DO UPDATE
            SET target_low=excluded.target_low,
                target_high=excluded.target_high,
                alert_low=excluded.alert_low,
                alert_high=excluded.alert_high,
                unit=excluded.unit,
                enabled=excluded.enabled
            """,
            (tank_id, name, t_low, t_high, a_low, a_high, unit, enabled),
        )
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
    user = get_current_user(db, request)
    rows = q(db, "SELECT * FROM parameter_defs ORDER BY sort_order, name")
    if user:
        rows = apply_user_parameter_overrides(rows, get_user_parameter_settings(db, user["id"]))
    db.close()
    error = request.query_params.get("error")
    return templates.TemplateResponse(
        "parameters.html",
        {"request": request, "parameters": rows, "error": error},
    )

@app.get("/settings/parameters/new", response_class=HTMLResponse)
@app.get("/settings/parameters/new/", response_class=HTMLResponse, include_in_schema=False)
def parameter_new(request: Request):
    return templates.TemplateResponse("parameter_edit.html", {"request": request, "param": None})

@app.get("/settings/parameters/{param_id}/edit", response_class=HTMLResponse)
def parameter_edit(request: Request, param_id: int):
    db = get_db()
    row = one(db, "SELECT * FROM parameter_defs WHERE id=?", (param_id,))
    user = get_current_user(db, request)
    if row and user and table_exists(db, "user_parameter_settings"):
        override = one(
            db,
            "SELECT * FROM user_parameter_settings WHERE user_id=? AND parameter_id=?",
            (user["id"], param_id),
        )
        if override:
            for field in USER_PARAMETER_FIELDS:
                value = row_get(override, field)
                if value is not None:
                    row[field] = value
    db.close()
    return templates.TemplateResponse("parameter_edit.html", {"request": request, "param": row})

@app.post("/settings/parameters/save")
def parameter_save(
    request: Request,
    param_id: Optional[str] = Form(None), 
    name: str = Form(...), 
    chemical_symbol: Optional[str] = Form(None),
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
    cur = cursor(db)
    user = get_current_user(db, request)
    if not user:
        db.close()
        raise HTTPException(status_code=401, detail="Not authenticated")
    
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

    existing_row = None
    if param_id and str(param_id).strip().isdigit():
        existing_row = one(db, "SELECT * FROM parameter_defs WHERE id=?", (int(param_id),))
        if not existing_row:
            db.close()
            return redirect("/settings/parameters")
        clean_name = (row_get(existing_row, "name") or clean_name).strip()
        chemical_symbol = row_get(existing_row, "chemical_symbol")
        unit = row_get(existing_row, "unit")

    try:
        # 2. Update existing definition (name/unit locked)
        if existing_row:
            pid = int(param_id)
            cur.execute(
                """
                UPDATE parameter_defs
                SET name=?, chemical_symbol=?, unit=?, sort_order=?, active=?
                WHERE id=?
                """,
                (
                    clean_name,
                    (chemical_symbol or "").strip() or None,
                    (unit or "").strip() or None,
                    order,
                    is_active,
                    pid,
                ),
            )
            param_id_value = pid

        # 3. Insert new definition
        else:
            existing = one(
                db,
                "SELECT id FROM parameter_defs WHERE LOWER(TRIM(name))=LOWER(TRIM(?))",
                (clean_name,),
            )
            if existing:
                message = "Parameter name already exists."
                param_payload = {
                    "id": None,
                    "name": clean_name,
                    "chemical_symbol": (chemical_symbol or "").strip() or None,
                    "unit": (unit or "").strip() or None,
                    "sort_order": order,
                    "max_daily_change": mdc,
                    "test_interval_days": interval,
                    "active": is_active,
                    "default_target_low": dt_low,
                    "default_target_high": dt_high,
                    "default_alert_low": da_low,
                    "default_alert_high": da_high,
                }
                return templates.TemplateResponse(
                    "parameter_edit.html",
                    {"request": request, "param": param_payload, "error": message},
                    status_code=400,
                )
            else:
                try:
                    cur.execute("""
                        INSERT INTO parameter_defs 
                        (name, chemical_symbol, unit, max_daily_change, test_interval_days, sort_order, active, default_target_low, default_target_high, default_alert_low, default_alert_high) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            clean_name,
                            (chemical_symbol or "").strip() or None,
                            (unit or "").strip() or None,
                            mdc,
                            interval,
                            order,
                            is_active,
                            dt_low,
                            dt_high,
                            da_low,
                            da_high,
                        ),
                    )
                except IntegrityError as exc:
                    orig = getattr(exc, "orig", None)
                    constraint = getattr(getattr(orig, "diag", None), "constraint_name", None)
                    if engine.dialect.name == "postgresql" and isinstance(orig, psycopg.errors.UniqueViolation) and constraint == "parameter_defs_pkey":
                        reset_parameter_defs_sequence(db)
                        cur.execute("""
                            INSERT INTO parameter_defs 
                            (name, chemical_symbol, unit, max_daily_change, test_interval_days, sort_order, active, default_target_low, default_target_high, default_alert_low, default_alert_high) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                clean_name,
                                (chemical_symbol or "").strip() or None,
                                (unit or "").strip() or None,
                                mdc,
                                interval,
                                order,
                                is_active,
                                dt_low,
                                dt_high,
                                da_low,
                                da_high,
                            ),
                        )
                    else:
                        raise
                param_id_value = cur.lastrowid
                if not param_id_value:
                    inserted = one(
                        db,
                        "SELECT id FROM parameter_defs WHERE LOWER(TRIM(name))=LOWER(TRIM(?))",
                        (clean_name,),
                    )
                    if inserted:
                        param_id_value = int(inserted["id"])

        if table_exists(db, "user_parameter_settings") and param_id_value:
            if all(
                value is None
                for value in (mdc, interval, dt_low, dt_high, da_low, da_high)
            ):
                db.execute(
                    "DELETE FROM user_parameter_settings WHERE user_id=? AND parameter_id=?",
                    (user["id"], param_id_value),
                )
            else:
                execute_with_retry(
                    db,
                    """
                    INSERT INTO user_parameter_settings
                    (user_id, parameter_id, max_daily_change, test_interval_days, default_target_low, default_target_high, default_alert_low, default_alert_high)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (user_id, parameter_id) DO UPDATE
                    SET max_daily_change=excluded.max_daily_change,
                        test_interval_days=excluded.test_interval_days,
                        default_target_low=excluded.default_target_low,
                        default_target_high=excluded.default_target_high,
                        default_alert_low=excluded.default_alert_low,
                        default_alert_high=excluded.default_alert_high
                    """,
                    (
                        user["id"],
                        param_id_value,
                        mdc,
                        interval,
                        dt_low,
                        dt_high,
                        da_low,
                        da_high,
                    ),
                )

        db.commit()
        
    except IntegrityError as exc:
        db.rollback()
        message = "Unable to save parameter."
        error_text = str(getattr(exc, "orig", exc)).lower()
        if "unique" in error_text or "duplicate" in error_text:
            message = "Parameter name already exists."
        param_payload = {
            "id": int(param_id) if param_id and str(param_id).strip().isdigit() else None,
            "name": clean_name,
            "chemical_symbol": (chemical_symbol or "").strip() or None,
            "unit": (unit or "").strip() or None,
            "sort_order": order,
            "max_daily_change": mdc,
            "test_interval_days": interval,
            "active": is_active,
            "default_target_low": dt_low,
            "default_target_high": dt_high,
            "default_alert_low": da_low,
            "default_alert_high": da_high,
        }
        return templates.TemplateResponse(
            "parameter_edit.html",
            {"request": request, "param": param_payload, "error": message},
            status_code=400,
        )
    except Exception:
        db.rollback()
        param_payload = {
            "id": int(param_id) if param_id and str(param_id).strip().isdigit() else None,
            "name": clean_name,
            "chemical_symbol": (chemical_symbol or "").strip() or None,
            "unit": (unit or "").strip() or None,
            "sort_order": order,
            "max_daily_change": mdc,
            "test_interval_days": interval,
            "active": is_active,
            "default_target_low": dt_low,
            "default_target_high": dt_high,
            "default_alert_low": da_low,
            "default_alert_high": da_high,
        }
        return templates.TemplateResponse(
            "parameter_edit.html",
            {"request": request, "param": param_payload, "error": "Unable to save parameter."},
            status_code=400,
        )
    finally:
        db.close()

    return redirect("/settings/parameters")

@app.post("/settings/parameters/{param_id}/delete")
def parameter_delete(request: Request, param_id: int):
    db = get_db()
    db.close()
    return redirect("/settings/parameters?error=Parameter+deletion+is+disabled.")

@app.post("/settings/parameters/bulk-add")
async def parameter_bulk_add(request: Request):
    form = await request.form()
    raw = (form.get("bulk_parameters") or "").strip()
    default_unit = (form.get("bulk_unit") or "").strip() or None
    if not raw:
        return redirect("/settings/parameters")
    db = get_db()
    cur = cursor(db)
    for line in raw.splitlines():
        cleaned = line.strip()
        if not cleaned:
            continue
        parts = [p.strip() for p in cleaned.split(",", 1)]
        name = parts[0]
        if not name:
            continue
        unit = parts[1] if len(parts) > 1 and parts[1] else default_unit
        existing = one(
            db,
            "SELECT id FROM parameter_defs WHERE LOWER(TRIM(name))=LOWER(TRIM(?))",
            (name,),
        )
        if existing:
            cur.execute(
                "UPDATE parameter_defs SET unit=COALESCE(unit, ?) WHERE id=?",
                (unit, existing["id"]),
            )
        else:
            insert_parameter_def(db, name.strip(), unit, 1, 0)
    db.commit()
    db.close()
    return redirect("/settings/parameters")

@app.get("/settings/test-kits", response_class=HTMLResponse)
@app.get("/settings/test-kits/", response_class=HTMLResponse, include_in_schema=False)
def test_kits(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    ensure_test_kits_owner_column(db)
    is_admin = is_admin_user(user)
    where_sql, params = build_test_kits_where(db, user, active_only=False)
    if is_admin:
        rows = q(
            db,
            f"SELECT tk.*, u.email AS owner_email FROM test_kits tk LEFT JOIN users u ON u.id = tk.owner_user_id{where_sql} ORDER BY tk.parameter, tk.name",
            params,
        )
    else:
        rows = q(db, f"SELECT * FROM test_kits{where_sql} ORDER BY parameter, name", params)
    standard_rows = [row for row in rows if row_get(row, "owner_user_id") is None]
    personal_rows = [row for row in rows if row_get(row, "owner_user_id") is not None]
    db.close()
    return templates.TemplateResponse(
        "test_kits.html",
        {
            "request": request,
            "standard_groups": group_test_kits_by_parameter(standard_rows),
            "personal_groups": group_test_kits_by_parameter(personal_rows),
            "is_admin": is_admin,
            "error": request.query_params.get("error"),
            "success": request.query_params.get("success"),
        },
    )

@app.get("/settings/system", response_class=HTMLResponse)
def system_settings(request: Request):
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    base_url = PUBLIC_BASE_URL or str(request.base_url).rstrip("/")
    redirect_uri = os.environ.get("GOOGLE_REDIRECT_URI") or f"{base_url.rstrip('/')}/auth/google/callback"
    vapid_public, _, _ = get_vapid_settings()
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = os.environ.get("SMTP_PORT", "587")
    smtp_tls = os.environ.get("SMTP_USE_TLS", "true").lower() in {"1", "true", "yes"}
    smtp_ssl = os.environ.get("SMTP_USE_SSL", "false").lower() in {"1", "true", "yes"}
    db.close()
    return templates.TemplateResponse(
        "system_settings.html",
        {
            "request": request,
            "base_url": base_url,
            "google_client_id": os.environ.get("GOOGLE_CLIENT_ID"),
            "google_redirect_uri": redirect_uri,
            "vapid_public_key": vapid_public,
            "smtp_host": smtp_host,
            "smtp_port": smtp_port,
            "smtp_tls": smtp_tls,
            "smtp_ssl": smtp_ssl,
            "smtp_sender_transactional": os.environ.get("EMAIL_FROM_TRANSACTIONAL") or "support@reefmetrics.app",
            "smtp_sender_alerts": os.environ.get("EMAIL_FROM_ALERTS") or "alerts@reefmetrics.app",
            "smtp_sender_billing": os.environ.get("EMAIL_FROM_BILLING") or "billing@reefmetrics.app",
            "smtp_sender_marketing": os.environ.get("EMAIL_FROM_MARKETING") or "hello@reefmetrics.app",
            "smtp_test_recipient": row_get(current_user, "email"),
        },
    )

@app.post("/admin/smtp-test")
async def admin_smtp_test(request: Request):
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    form = await request.form()
    recipient = (form.get("recipient") or row_get(current_user, "email") or "").strip()
    if not recipient:
        db.close()
        return templates.TemplateResponse(
            "simple_message.html",
            {
                "request": request,
                "title": "SMTP Test",
                "message": "Recipient email is required to run the SMTP test.",
                "actions": [{"label": "Back to system settings", "href": "/settings/system"}],
            },
        )
    subject = "Reef Metrics SMTP Test"
    text_body = "This is a test email to verify SMTP settings."
    html_body = "<p>This is a test email to verify SMTP settings.</p>"
    success, reason = send_email(recipient, subject, text_body, html_body, sender_kind="transactional")
    log_audit(
        db,
        current_user,
        "smtp-test",
        {"recipient": recipient, "sent": success, "reason": reason} if reason else {"recipient": recipient, "sent": success},
    )
    db.commit()
    db.close()
    message = f"SMTP test email sent to {recipient}." if success else f"SMTP test failed: {reason}"
    return templates.TemplateResponse(
        "simple_message.html",
        {
            "request": request,
            "title": "SMTP Test",
            "message": message,
            "actions": [{"label": "Back to system settings", "href": "/settings/system"}],
        },
    )

@app.get("/settings/integrations/apex", response_class=HTMLResponse)
def apex_settings(request: Request):
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    integration_id = request.query_params.get("integration_id") or None
    settings = get_apex_settings(db, integration_id)
    integrations = get_apex_integrations(db)
    tanks = q(db, "SELECT id, name FROM tanks ORDER BY name")
    parameters = list_parameters(db)
    mapping = settings.get("mapping") or {}
    probes = settings.get("last_probe_list") or []
    auto_mapping = infer_apex_auto_mapping(probes, parameters)
    last_probe_loaded_at = settings.get("last_probe_loaded_at")
    db.close()
    return templates.TemplateResponse(
        "apex_settings.html",
        {
            "request": request,
            "settings": settings,
            "integrations": integrations,
            "integration_id": settings.get("id"),
            "parameters": parameters,
            "probes": probes,
            "mapping": mapping,
            "auto_mapping": auto_mapping,
            "last_probe_loaded_at": last_probe_loaded_at,
            "preview": [],
            "tanks": tanks,
            "success": request.query_params.get("success"),
            "error": request.query_params.get("error"),
        },
    )

@app.post("/settings/integrations/apex/save")
async def apex_settings_save(request: Request):
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    form = await request.form()
    integration_id = (form.get("integration_id") or "").strip() or None
    if integration_id == "new":
        integration_id = None
    settings = get_apex_settings(db, integration_id)
    host = clean_apex_host(form.get("host") or "")
    name = (form.get("name") or "").strip()
    username = (form.get("username") or "").strip()
    password = (form.get("password") or "").strip() or settings.get("password", "")
    api_token = (form.get("api_token") or "").strip() or settings.get("api_token", "")
    poll_interval = int(to_float(form.get("poll_interval_minutes")) or 15)
    mapping = build_apex_mapping_from_form(form, settings.get("mapping") or {})
    tank_ids = [int(tid) for tid in form.getlist("tank_ids") if str(tid).isdigit()]
    tank_id = form.get("tank_id")
    settings.update(
        {
            "id": settings.get("id") or (integration_id or secrets.token_hex(6)),
            "enabled": form.get("enabled") in ("on", "true", "1", True),
            "name": name or settings.get("name") or "Apex Integration",
            "host": host,
            "username": username,
            "password": password,
            "api_token": api_token,
            "poll_interval_minutes": poll_interval,
            "mapping": mapping,
            "tank_id": int(tank_id) if tank_id and str(tank_id).isdigit() else None,
            "tank_ids": tank_ids,
        }
    )
    save_apex_settings(db, settings["id"], settings)
    db.close()
    return redirect(f"/settings/integrations/apex?integration_id={settings['id']}&success=Saved Apex settings.")

@app.post("/settings/integrations/apex/test")
async def apex_settings_test(request: Request):
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    form = await request.form()
    integration_id = (form.get("integration_id") or "").strip() or None
    if integration_id == "new":
        integration_id = None
    settings = get_apex_settings(db, integration_id)
    settings["host"] = clean_apex_host(form.get("host") or "")
    settings["username"] = (form.get("username") or "").strip()
    settings["password"] = (form.get("password") or "").strip() or settings.get("password", "")
    settings["api_token"] = (form.get("api_token") or "").strip() or settings.get("api_token", "")
    try:
        readings = fetch_apex_readings(settings)
        message = f"Connected successfully. Found {len(readings)} probes."
        db.close()
        return redirect(f"/settings/integrations/apex?integration_id={settings.get('id')}&success={urllib.parse.quote(message)}")
    except Exception as exc:
        db.close()
        return redirect(f"/settings/integrations/apex?integration_id={settings.get('id')}&error={urllib.parse.quote(str(exc))}")

@app.post("/settings/integrations/apex/import")
async def apex_settings_import(request: Request):
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    form = await request.form()
    integration_id = (form.get("integration_id") or "").strip() or None
    if integration_id == "new":
        integration_id = None
    settings = get_apex_settings(db, integration_id)
    try:
        result = import_apex_sample(db, settings, current_user)
        imports = result.get("imports") or []
        imported = [row for row in imports if not row.get("skipped")]
        if not imported:
            message = "No changes detected; import skipped."
        else:
            message = f"Imported {len(imported)} sample(s) from {result.get('mapped_count', 0)} readings."
        db.close()
        return redirect(f"/settings/integrations/apex?integration_id={settings.get('id')}&success={urllib.parse.quote(message)}")
    except Exception as exc:
        db.close()
        return redirect(f"/settings/integrations/apex?integration_id={settings.get('id')}&error={urllib.parse.quote(str(exc))}")

@app.post("/settings/integrations/apex/client-import")
async def apex_client_import(request: Request):
    payload = await request.json()
    integration_id = (payload.get("integration_id") or "").strip() or None
    if integration_id == "new":
        integration_id = None
    content = payload.get("payload") or ""
    mapping_override = payload.get("mapping") or None
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    settings = get_apex_settings(db, integration_id)
    if not content:
        db.close()
        return JSONResponse({"detail": "Apex payload is required."}, status_code=400)
    try:
        readings = readings_from_payload(content)
        normalized = normalize_apex_readings(readings)
        if mapping_override:
            mapped = map_apex_readings_with_mapping(normalized, mapping_override)
        else:
            mapped = map_apex_readings(settings, normalized)
        result = import_apex_sample_from_mapped(db, settings, mapped, current_user)
        db.close()
        message = f"Imported {result.get('mapped_count', 0)} readings."
        return JSONResponse({"success": message, "result": result})
    except Exception as exc:
        db.close()
        return JSONResponse({"detail": str(exc)}, status_code=400)

@app.post("/settings/integrations/apex/client-test")
async def apex_client_test(request: Request):
    payload = await request.json()
    integration_id = (payload.get("integration_id") or "").strip() or None
    if integration_id == "new":
        integration_id = None
    content = payload.get("payload") or ""
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    get_apex_settings(db, integration_id)
    if not content:
        db.close()
        return JSONResponse({"detail": "Apex payload is required."}, status_code=400)
    try:
        readings = readings_from_payload(content)
        db.close()
        return JSONResponse({"success": f"Connected successfully. Found {len(readings)} probes."})
    except Exception as exc:
        db.close()
        return JSONResponse({"detail": str(exc)}, status_code=400)

@app.post("/settings/integrations/apex/client-probes")
async def apex_client_probes(request: Request):
    payload = await request.json()
    integration_id = (payload.get("integration_id") or "").strip() or None
    if integration_id == "new":
        integration_id = None
    content = payload.get("payload") or ""
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    settings = get_apex_settings(db, integration_id)
    if not content:
        db.close()
        return JSONResponse({"detail": "Apex payload is required."}, status_code=400)
    try:
        readings = readings_from_payload(content)
        probes = sorted({reading.name for reading in readings if reading.name})
        settings["last_probe_list"] = probes
        settings["last_probe_loaded_at"] = datetime.utcnow().isoformat()
        save_apex_settings(db, settings["id"], settings)
        db.close()
        return JSONResponse({"success": f"Loaded {len(probes)} probes.", "probes": probes})
    except Exception as exc:
        db.close()
        return JSONResponse({"detail": str(exc)}, status_code=400)

@app.post("/settings/integrations/apex/client-preview")
async def apex_client_preview(request: Request):
    payload = await request.json()
    integration_id = (payload.get("integration_id") or "").strip() or None
    if integration_id == "new":
        integration_id = None
    content = payload.get("payload") or ""
    mapping_override = payload.get("mapping") or None
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    settings = get_apex_settings(db, integration_id)
    if not content:
        db.close()
        return JSONResponse({"detail": "Apex payload is required."}, status_code=400)
    try:
        readings = readings_from_payload(content)
        normalized = normalize_apex_readings(readings)
        mapping = mapping_override or settings.get("mapping") or {}
        mapped = map_apex_readings_with_mapping(normalized, mapping)
        preview = []
        for param_name, reading in mapped:
            preview.append(
                {
                    "probe": reading.get("name"),
                    "parameter": param_name,
                    "value": reading.get("value"),
                    "unit": reading.get("unit", ""),
                    "timestamp": reading.get("timestamp"),
                }
            )
        db.close()
        return JSONResponse({"success": f"Previewed {len(preview)} mapped probes.", "preview": preview})
    except Exception as exc:
        db.close()
        return JSONResponse({"detail": str(exc)}, status_code=400)

@app.post("/settings/integrations/apex/probes", response_class=HTMLResponse)
async def apex_settings_probes(request: Request):
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    form = await request.form()
    integration_id = (form.get("integration_id") or "").strip() or None
    if integration_id == "new":
        integration_id = None
    settings = get_apex_settings(db, integration_id)
    settings["host"] = clean_apex_host(form.get("host") or "")
    settings["username"] = (form.get("username") or "").strip()
    settings["password"] = (form.get("password") or "").strip() or settings.get("password", "")
    settings["api_token"] = (form.get("api_token") or "").strip() or settings.get("api_token", "")
    settings["poll_interval_minutes"] = int(to_float(form.get("poll_interval_minutes")) or 15)
    settings["enabled"] = form.get("enabled") in ("on", "true", "1", True)
    tank_id = form.get("tank_id")
    settings["tank_id"] = int(tank_id) if tank_id and str(tank_id).isdigit() else None
    settings["tank_ids"] = [int(tid) for tid in form.getlist("tank_ids") if str(tid).isdigit()]
    mapping = settings.get("mapping") or {}
    parameters = list_parameters(db)
    tanks = q(db, "SELECT id, name FROM tanks ORDER BY name")
    probes: List[str] = []
    error = None
    try:
        readings = fetch_apex_readings(settings)
        probes = sorted({str(r["name"]) for r in readings if r.get("name")})
        settings["last_probe_list"] = probes
        settings["last_probe_loaded_at"] = datetime.utcnow().isoformat()
        save_apex_settings(db, settings["id"], settings)
    except Exception as exc:
        error = str(exc)
    auto_mapping = infer_apex_auto_mapping(probes, parameters)
    last_probe_loaded_at = settings.get("last_probe_loaded_at")
    integrations = get_apex_integrations(db)
    db.close()
    return templates.TemplateResponse(
        "apex_settings.html",
        {
            "request": request,
            "settings": settings,
            "integrations": integrations,
            "integration_id": settings.get("id"),
            "parameters": parameters,
            "probes": probes,
            "mapping": mapping,
            "auto_mapping": auto_mapping,
            "last_probe_loaded_at": last_probe_loaded_at,
            "preview": [],
            "tanks": tanks,
            "success": None,
            "error": error,
        },
    )

@app.post("/settings/integrations/apex/preview", response_class=HTMLResponse)
async def apex_settings_preview(request: Request):
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    form = await request.form()
    integration_id = (form.get("integration_id") or "").strip() or None
    if integration_id == "new":
        integration_id = None
    settings = get_apex_settings(db, integration_id)
    settings["host"] = clean_apex_host(form.get("host") or "")
    settings["username"] = (form.get("username") or "").strip()
    settings["password"] = (form.get("password") or "").strip() or settings.get("password", "")
    settings["api_token"] = (form.get("api_token") or "").strip() or settings.get("api_token", "")
    settings["poll_interval_minutes"] = int(to_float(form.get("poll_interval_minutes")) or 15)
    settings["enabled"] = form.get("enabled") in ("on", "true", "1", True)
    tank_id = form.get("tank_id")
    settings["tank_id"] = int(tank_id) if tank_id and str(tank_id).isdigit() else None
    settings["tank_ids"] = [int(tid) for tid in form.getlist("tank_ids") if str(tid).isdigit()]
    parameters = list_parameters(db)
    tanks = q(db, "SELECT id, name FROM tanks ORDER BY name")
    probes = settings.get("last_probe_list") or []
    auto_mapping = infer_apex_auto_mapping(probes, parameters)
    mapping = build_apex_mapping_from_form(form, settings.get("mapping") or {})
    preview: List[Dict[str, Any]] = []
    error = None
    try:
        preview = preview_apex_mapping(settings, mapping or auto_mapping)
    except Exception as exc:
        error = str(exc)
    integrations = get_apex_integrations(db)
    db.close()
    return templates.TemplateResponse(
        "apex_settings.html",
        {
            "request": request,
            "settings": settings,
            "integrations": integrations,
            "integration_id": settings.get("id"),
            "parameters": parameters,
            "probes": probes,
            "mapping": mapping,
            "auto_mapping": auto_mapping,
            "last_probe_loaded_at": settings.get("last_probe_loaded_at"),
            "preview": preview,
            "tanks": tanks,
            "success": None,
            "error": error,
        },
    )

@app.get("/settings/test-kits/new", response_class=HTMLResponse)
@app.get("/settings/test-kits/new/", response_class=HTMLResponse, include_in_schema=False)
def test_kit_new(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    ensure_test_kits_owner_column(db)
    parameters = q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY sort_order, name")
    db.close()
    return templates.TemplateResponse(
        "test_kit_edit.html",
        {
            "request": request,
            "kit": None,
            "parameters": parameters,
            "is_admin": is_admin_user(user),
        },
    )

@app.get("/settings/test-kits/{kit_id}/edit", response_class=HTMLResponse)
def test_kit_edit(request: Request, kit_id: int):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    ensure_test_kits_owner_column(db)
    kit = get_visible_test_kit_by_id(db, user, kit_id, active_only=False)
    if not kit:
        db.close()
        return redirect("/settings/test-kits?error=Test%20kit%20not%20found")
    is_admin = is_admin_user(user)
    owner_id = row_get(kit, "owner_user_id")
    if owner_id is None and not is_admin:
        db.close()
        return redirect("/settings/test-kits?error=Only%20admins%20can%20edit%20standard%20test%20kits")
    if owner_id is not None and not is_admin and owner_id != user["id"]:
        db.close()
        return redirect("/settings/test-kits?error=You%20can%20only%20edit%20your%20own%20test%20kits")
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
        workflow_data = kit.get("workflow_data")
        if workflow_data:
            try:
                workflow_rows = json.loads(workflow_data)
            except Exception:
                workflow_rows = []
            kit["workflow_steps"] = format_workflow_steps(workflow_rows)
    return templates.TemplateResponse(
        "test_kit_edit.html",
        {
            "request": request,
            "kit": kit,
            "parameters": parameters,
            "is_admin": is_admin,
        },
    )

@app.post("/settings/test-kits/save")
def test_kit_save(request: Request, kit_id: Optional[str] = Form(None), parameter: Optional[str] = Form(None), parameter_id: Optional[str] = Form(None), name: str = Form(...), unit: Optional[str] = Form(None), resolution: Optional[str] = Form(None), manufacturer_accuracy: Optional[str] = Form(None), min_value: Optional[str] = Form(None), max_value: Optional[str] = Form(None), notes: Optional[str] = Form(None), workflow_steps: Optional[str] = Form(None), conversion_type: Optional[str] = Form(None), conversion_table: Optional[str] = Form(None), active: Optional[str] = Form(None), is_global: Optional[str] = Form(None)):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    ensure_test_kits_owner_column(db)
    cur = cursor(db)
    is_admin = is_admin_user(user)
    is_active = 1 if (active in ("1", "on", "true", "True")) else 0
    conv_type = (conversion_type or "").strip() or None
    conversion_rows = parse_conversion_table(conversion_table or "") if conv_type else []
    conversion_json = json.dumps(conversion_rows) if conversion_rows else None
    workflow_rows = parse_workflow_steps(workflow_steps or "")
    workflow_json = json.dumps(workflow_rows) if workflow_rows else None
    data_base = (
        parameter.strip(),
        name.strip(),
        (unit or "").strip() or None,
        to_float(resolution),
        to_float(manufacturer_accuracy),
        to_float(min_value),
        to_float(max_value),
        (notes or "").strip() or None,
        workflow_json,
        conv_type,
        conversion_json,
        is_active,
    )
    if kit_id and str(kit_id).strip().isdigit():
        existing = one(db, "SELECT * FROM test_kits WHERE id=?", (int(kit_id),))
        if not existing:
            db.close()
            return redirect("/settings/test-kits")
        existing_owner = row_get(existing, "owner_user_id")
        if existing_owner is None and not is_admin:
            db.close()
            return redirect("/settings/test-kits?error=Only%20admins%20can%20edit%20standard%20test%20kits")
        if existing_owner is not None and not is_admin and existing_owner != user["id"]:
            db.close()
            return redirect("/settings/test-kits?error=You%20can%20only%20edit%20your%20own%20test%20kits")
        owner_user_id = None if (is_admin and is_global in ("1", "on", "true", "True")) else (existing_owner or user["id"])
        data = (*data_base, owner_user_id)
        cur.execute(
            "UPDATE test_kits SET parameter=?, name=?, unit=?, resolution=?, manufacturer_accuracy=?, min_value=?, max_value=?, notes=?, workflow_data=?, conversion_type=?, conversion_data=?, active=?, owner_user_id=? WHERE id=?",
            (*data, int(kit_id)),
        )
        log_audit(
            db,
            user,
            "test-kit-update",
            {"kit_id": int(kit_id), "name": name.strip(), "parameter": parameter.strip()},
        )
    else:
        owner_user_id = None if (is_admin and is_global in ("1", "on", "true", "True")) else user["id"]
        data = (*data_base, owner_user_id)
        cur.execute(
            "INSERT INTO test_kits (parameter, name, unit, resolution, manufacturer_accuracy, min_value, max_value, notes, workflow_data, conversion_type, conversion_data, active, owner_user_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            data,
        )
        new_id = cur.lastrowid
        log_audit(
            db,
            user,
            "test-kit-create",
            {"kit_id": new_id, "name": name.strip(), "parameter": parameter.strip()},
        )
    db.commit()
    db.close()
    return redirect("/settings/test-kits")

@app.post("/settings/test-kits/{kit_id}/delete")
def test_kit_delete(request: Request, kit_id: int):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    ensure_test_kits_owner_column(db)
    kit = one(db, "SELECT * FROM test_kits WHERE id=?", (kit_id,))
    if not kit:
        db.close()
        return redirect("/settings/test-kits")
    is_admin = is_admin_user(user)
    owner_id = row_get(kit, "owner_user_id")
    if owner_id is None and not is_admin:
        db.close()
        return redirect("/settings/test-kits?error=Only%20admins%20can%20delete%20standard%20test%20kits")
    if owner_id is not None and not is_admin and owner_id != user["id"]:
        db.close()
        return redirect("/settings/test-kits?error=You%20can%20only%20delete%20your%20own%20test%20kits")
    log_audit(db, user, "test-kit-delete", {"kit_id": kit_id, "name": row_get(kit, "name")})
    db.execute("DELETE FROM test_kits WHERE id=?", (kit_id,))
    db.commit()
    db.close()
    return redirect("/settings/test-kits")

@app.post("/settings/test-kits/{kit_id}/make-standard")
def test_kit_make_standard(request: Request, kit_id: int):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    if not is_admin_user(user):
        db.close()
        return redirect("/settings/test-kits?error=Only%20admins%20can%20modify%20standard%20test%20kits")
    ensure_test_kits_owner_column(db)
    kit = one(db, "SELECT * FROM test_kits WHERE id=?", (kit_id,))
    if not kit:
        db.close()
        return redirect("/settings/test-kits?error=Test%20kit%20not%20found")
    if row_get(kit, "owner_user_id") is None:
        db.close()
        return redirect("/settings/test-kits")
    db.execute("UPDATE test_kits SET owner_user_id=NULL WHERE id=?", (kit_id,))
    log_audit(db, user, "test-kit-make-standard", {"kit_id": kit_id, "name": row_get(kit, "name")})
    db.commit()
    db.close()
    return redirect("/settings/test-kits?success=Test%20kit%20is%20now%20standard")

@app.get("/additives", response_class=HTMLResponse)
@app.get("/additives/", response_class=HTMLResponse, include_in_schema=False)
def additives(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    rows = get_visible_additives(db, user, active_only=False)
    standard_rows = [row for row in rows if row_get(row, "owner_user_id") is None]
    personal_rows = [row for row in rows if row_get(row, "owner_user_id") == user["id"]]
    db.close()
    return templates.TemplateResponse(
        "additives.html",
        {
            "request": request,
            "standard_groups": group_additives_by_parameter(standard_rows),
            "personal_groups": group_additives_by_parameter(personal_rows),
            "is_admin": row_get(user, "admin") in (1, True) or row_get(user, "role") == "admin",
            "error": request.query_params.get("error"),
        },
    )

@app.get("/settings/additives", response_class=HTMLResponse, include_in_schema=False)
@app.get("/settings/additives/", response_class=HTMLResponse, include_in_schema=False)
def additives_settings_redirect(): return redirect("/additives")

@app.get("/additives/new", response_class=HTMLResponse)
@app.get("/additives/new/", response_class=HTMLResponse, include_in_schema=False)
def additive_new(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    ensure_additives_owner_column(db)
    parameters = q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY sort_order, name")
    db.close()
    selected_parameter = (request.query_params.get("parameter") or "").strip()
    return templates.TemplateResponse(
        "additive_edit.html",
        {
            "request": request,
            "additive": None,
            "parameters": parameters,
            "selected_parameter": selected_parameter,
            "is_admin": row_get(user, "admin") in (1, True) or row_get(user, "role") == "admin",
        },
    )

@app.get("/additives/{additive_id}/edit", response_class=HTMLResponse)
def additive_edit(request: Request, additive_id: int):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    ensure_additives_owner_column(db)
    additive = one(db, "SELECT * FROM additives WHERE id=?", (additive_id,))
    if not additive:
        db.close()
        return redirect("/additives")
    is_admin = row_get(user, "admin") in (1, True) or row_get(user, "role") == "admin"
    owner_id = row_get(additive, "owner_user_id")
    if owner_id is None and not is_admin:
        db.close()
        return redirect("/additives?error=Only%20admins%20can%20edit%20standard%20additives")
    if owner_id is not None and not is_admin and owner_id != user["id"]:
        db.close()
        return redirect("/additives?error=You%20can%20only%20edit%20your%20own%20additives")
    parameters = q(db, "SELECT * FROM parameter_defs WHERE active=1 ORDER BY sort_order, name")
    db.close()
    return templates.TemplateResponse(
        "additive_edit.html",
        {
            "request": request,
            "additive": additive,
            "parameters": parameters,
            "is_admin": is_admin,
        },
    )

@app.post("/additives/save")
def additive_save(request: Request, additive_id: Optional[str] = Form(None), name: str = Form(...), brand: Optional[str] = Form(None), parameter: Optional[str] = Form(None), parameter_id: Optional[str] = Form(None), strength: str = Form(...), unit: str = Form(...), max_daily: Optional[str] = Form(None), notes: Optional[str] = Form(None), active: Optional[str] = Form(None), is_global: Optional[str] = Form(None)):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    ensure_additives_owner_column(db)
    cur = cursor(db)
    is_admin = row_get(user, "admin") in (1, True) or row_get(user, "role") == "admin"
    is_active = 1 if (active in ("1", "on", "true", "True")) else 0
    owner_user_id = None if (is_admin and is_global in ("1", "on", "true", "True")) else user["id"]
    data = (
        name.strip(),
        (brand or "").strip() or None,
        parameter.strip(),
        float(to_float(strength) or 0),
        unit.strip(),
        to_float(max_daily),
        (notes or "").strip() or None,
        is_active,
        owner_user_id,
    )
    if additive_id and str(additive_id).strip().isdigit():
        existing = one(db, "SELECT * FROM additives WHERE id=?", (int(additive_id),))
        if not existing:
            db.close()
            return redirect("/additives")
        existing_owner = row_get(existing, "owner_user_id")
        if existing_owner is None and not is_admin:
            db.close()
            return redirect("/additives?error=Only%20admins%20can%20edit%20standard%20additives")
        if existing_owner is not None and not is_admin and existing_owner != user["id"]:
            db.close()
            return redirect("/additives?error=You%20can%20only%20edit%20your%20own%20additives")
        cur.execute(
            "UPDATE additives SET name=?, brand=?, parameter=?, strength=?, unit=?, max_daily=?, notes=?, active=?, owner_user_id=? WHERE id=?",
            (*data, int(additive_id)),
        )
    else:
        try:
            cur.execute(
                "INSERT INTO additives (name, brand, parameter, strength, unit, max_daily, notes, active, owner_user_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                data,
            )
        except IntegrityError as exc:
            orig = getattr(exc, "orig", None)
            if engine.dialect.name == "postgresql" and isinstance(orig, psycopg.errors.UniqueViolation):
                reset_additives_sequence(db)
                cur.execute(
                    "INSERT INTO additives (name, brand, parameter, strength, unit, max_daily, notes, active, owner_user_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    data,
                )
            else:
                raise
    db.commit()
    db.close()
    return redirect("/additives")

@app.post("/additives/{additive_id}/delete")
def additive_delete(request: Request, additive_id: int):
    db = get_db()
    user = get_current_user(db, request)
    if not user:
        db.close()
        return redirect("/auth/login")
    ensure_additives_owner_column(db)
    additive = one(db, "SELECT * FROM additives WHERE id=?", (additive_id,))
    if not additive:
        db.close()
        return redirect("/additives")
    is_admin = row_get(user, "admin") in (1, True) or row_get(user, "role") == "admin"
    owner_id = row_get(additive, "owner_user_id")
    if owner_id is None and not is_admin:
        db.close()
        return redirect("/additives?error=Only%20admins%20can%20delete%20standard%20additives")
    if owner_id is not None and not is_admin and owner_id != user["id"]:
        db.close()
        return redirect("/additives?error=You%20can%20only%20delete%20your%20own%20additives")
    db.execute("DELETE FROM additives WHERE id=?", (additive_id,))
    db.commit()
    db.close()
    return redirect("/additives")

@app.get("/tanks/{tank_id}/dosing-log", response_class=HTMLResponse)
@app.get("/tank/{tank_id}/dosing-log", response_class=HTMLResponse, include_in_schema=False)
def dosing_log(request: Request, tank_id: int):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    additives_rows = get_visible_additives(db, user, active_only=True)
    logs = q(
        db,
        "SELECT dl.*, COALESCE(NULLIF(TRIM(a.brand), '') || ' ' || a.name, a.name) AS additive_name FROM dose_logs dl LEFT JOIN additives a ON a.id = dl.additive_id WHERE dl.tank_id=? ORDER BY dl.logged_at DESC, dl.id DESC",
        (tank_id,),
    )
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
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    cur = cursor(db)
    when_dt = None
    if logged_at:
        try:
            when_dt = datetime.fromisoformat(logged_at)
        except ValueError:
            when_dt = parse_dt_any(logged_at)
    when_iso = (when_dt or datetime.now()).isoformat()
    aid = int(additive_id) if additive_id and str(additive_id).isdigit() else None
    cur.execute("INSERT INTO dose_logs (tank_id, additive_id, amount_ml, reason, logged_at) VALUES (?, ?, ?, ?, ?)", (tank_id, aid, float(amount_ml), reason, when_iso))
    if aid is not None:
        profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
        additive = get_visible_additive_by_id(db, user, aid, active_only=False)
        if profile and additive:
            additive_name = (row_get(additive, "name") or "").strip().lower()
            additive_label_name = additive_label(additive).strip().lower()
            updates = {}
            extra_updates = {}
            for solution_col, container_col, remaining_col, default_label in (
                ("all_in_one_solution", "all_in_one_container_ml", "all_in_one_remaining_ml", "All-in-one"),
                ("alk_solution", "alk_container_ml", "alk_remaining_ml", "Alkalinity"),
                ("kalk_solution", "kalk_container_ml", "kalk_remaining_ml", "Kalkwasser"),
                ("ca_solution", "ca_container_ml", "ca_remaining_ml", "Calcium"),
                ("mg_solution", "mg_container_ml", "mg_remaining_ml", "Magnesium"),
                ("nitrate_solution", "nitrate_container_ml", "nitrate_remaining_ml", "Nitrate"),
                ("phosphate_solution", "phosphate_container_ml", "phosphate_remaining_ml", "Phosphate"),
                ("trace_solution", "trace_container_ml", "trace_remaining_ml", "Trace Elements"),
                (None, "nopox_container_ml", "nopox_remaining_ml", "NoPox"),
            ):
                if solution_col:
                    solution_name = (row_get(profile, solution_col) or "").strip().lower()
                    if not solution_name or solution_name not in {additive_name, additive_label_name}:
                        continue
                else:
                    default_name = default_label.strip().lower()
                    if additive_name != default_name and additive_label_name != default_name:
                        continue
                container_ml = row_get(profile, container_col)
                remaining_ml = row_get(profile, remaining_col)
                if remaining_ml is None:
                    remaining_ml = container_ml
                if remaining_ml is None:
                    continue
                updated_remaining = max(float(remaining_ml) - float(amount_ml), 0.0)
                updates[remaining_col] = updated_remaining
            extra_entries = q(
                db,
                "SELECT id, solution, container_ml, remaining_ml FROM dosing_entries WHERE tank_id=? AND active=1",
                (tank_id,),
            )
            for entry in extra_entries:
                solution_name = (row_get(entry, "solution") or "").strip().lower()
                if solution_name and solution_name not in {additive_name, additive_label_name}:
                    continue
                container_ml = row_get(entry, "container_ml")
                remaining_ml = row_get(entry, "remaining_ml")
                if remaining_ml is None:
                    remaining_ml = container_ml
                if remaining_ml is None:
                    continue
                updated_remaining = max(float(remaining_ml) - float(amount_ml), 0.0)
                extra_updates[row_get(entry, "id")] = updated_remaining
            if updates:
                updates["dosing_container_updated_at"] = datetime.utcnow().isoformat()
                set_clause = ", ".join([f"{col}=?" for col in updates.keys()])
                db.execute(
                    f"UPDATE tank_profiles SET {set_clause} WHERE tank_id=?",
                    (*updates.values(), tank_id),
                )
            if extra_updates:
                db.executemany(
                    "UPDATE dosing_entries SET remaining_ml=? WHERE id=?",
                    [(val, key) for key, val in extra_updates.items()],
                )
                db.execute(
                    "UPDATE tank_profiles SET dosing_container_updated_at=? WHERE tank_id=?",
                    (datetime.utcnow().isoformat(), tank_id),
                )
    db.commit()
    db.close()
    return redirect(f"/tanks/{tank_id}/dosing-log")

@app.post("/dose-logs/{log_id}/delete")
async def dosing_log_delete(request: Request, log_id: int, tank_id: int = Form(...)):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    if not tank:
        db.close()
        raise HTTPException(status_code=404, detail="Tank not found")
    log_row = one(db, "SELECT additive_id, amount_ml FROM dose_logs WHERE id=? AND tank_id=?", (log_id, tank_id))
    if log_row:
        aid = row_get(log_row, "additive_id")
        amount_ml = row_get(log_row, "amount_ml")
        if aid is not None and amount_ml is not None:
            profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
            additive = get_visible_additive_by_id(db, user, aid, active_only=False)
            if profile and additive:
                additive_name = (row_get(additive, "name") or "").strip().lower()
                additive_label_name = additive_label(additive).strip().lower()
                updates = {}
                extra_updates = {}
                for solution_col, container_col, remaining_col, default_label in (
                    ("all_in_one_solution", "all_in_one_container_ml", "all_in_one_remaining_ml", "All-in-one"),
                    ("alk_solution", "alk_container_ml", "alk_remaining_ml", "Alkalinity"),
                    ("kalk_solution", "kalk_container_ml", "kalk_remaining_ml", "Kalkwasser"),
                    ("ca_solution", "ca_container_ml", "ca_remaining_ml", "Calcium"),
                    ("mg_solution", "mg_container_ml", "mg_remaining_ml", "Magnesium"),
                    ("nitrate_solution", "nitrate_container_ml", "nitrate_remaining_ml", "Nitrate"),
                    ("phosphate_solution", "phosphate_container_ml", "phosphate_remaining_ml", "Phosphate"),
                    ("trace_solution", "trace_container_ml", "trace_remaining_ml", "Trace Elements"),
                    (None, "nopox_container_ml", "nopox_remaining_ml", "NoPox"),
                ):
                    if solution_col:
                        solution_name = (row_get(profile, solution_col) or "").strip().lower()
                        if not solution_name or solution_name not in {additive_name, additive_label_name}:
                            continue
                    else:
                        default_name = default_label.strip().lower()
                        if additive_name != default_name and additive_label_name != default_name:
                            continue
                    container_ml = row_get(profile, container_col)
                    remaining_ml = row_get(profile, remaining_col)
                    if remaining_ml is None:
                        remaining_ml = container_ml
                    if remaining_ml is None:
                        continue
                    updated_remaining = float(remaining_ml) + float(amount_ml)
                    if container_ml is not None:
                        updated_remaining = min(updated_remaining, float(container_ml))
                    updates[remaining_col] = updated_remaining
                extra_entries = q(
                    db,
                    "SELECT id, solution, container_ml, remaining_ml FROM dosing_entries WHERE tank_id=? AND active=1",
                    (tank_id,),
                )
                for entry in extra_entries:
                    solution_name = (row_get(entry, "solution") or "").strip().lower()
                    if solution_name and solution_name not in {additive_name, additive_label_name}:
                        continue
                    container_ml = row_get(entry, "container_ml")
                    remaining_ml = row_get(entry, "remaining_ml")
                    if remaining_ml is None:
                        remaining_ml = container_ml
                    if remaining_ml is None:
                        continue
                    updated_remaining = float(remaining_ml) + float(amount_ml)
                    if container_ml is not None:
                        updated_remaining = min(updated_remaining, float(container_ml))
                    extra_updates[row_get(entry, "id")] = updated_remaining
                if updates:
                    updates["dosing_container_updated_at"] = datetime.utcnow().isoformat()
                    set_clause = ", ".join([f"{col}=?" for col in updates.keys()])
                    db.execute(
                        f"UPDATE tank_profiles SET {set_clause} WHERE tank_id=?",
                        (*updates.values(), tank_id),
                    )
                if extra_updates:
                    db.executemany(
                        "UPDATE dosing_entries SET remaining_ml=? WHERE id=?",
                        [(val, key) for key, val in extra_updates.items()],
                    )
                    db.execute(
                        "UPDATE tank_profiles SET dosing_container_updated_at=? WHERE tank_id=?",
                        (datetime.utcnow().isoformat(), tank_id),
                    )
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
    cur = cursor(db)
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
    tanks = get_visible_tanks(db, request)
    user = get_current_user(db, request)
    additives_rows = get_visible_additives(db, user, active_only=True)
    grouped_additives: Dict[str, List[Dict[str, Any]]] = {}
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
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
    tank_profile = one(db, "SELECT * FROM tank_profiles WHERE tank_id=?", (tank_id,))
    additive = get_visible_additive_by_id(db, user, additive_id, active_only=True)
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
        dose_ml = (float(desired_change) * volume * 1000.0) / float(strength)
        pname = (additive['parameter'] or '').strip()
        pdef = one(db, "SELECT * FROM parameter_defs WHERE name=?", (pname,))
        if pdef and user and table_exists(db, "user_parameter_settings"):
            override = one(
                db,
                "SELECT * FROM user_parameter_settings WHERE user_id=? AND parameter_id=?",
                (user["id"], pdef["id"]),
            )
            if override:
                for field in USER_PARAMETER_FIELDS:
                    value = row_get(override, field)
                    if value is not None:
                        pdef[field] = value
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
                daily_ml = (daily_change * volume * 1000.0) / float(strength)
        except Exception: pass
    tanks = get_visible_tanks(db, request)
    additives_rows = get_visible_additives(db, user, active_only=True)
    grouped_additives: Dict[str, List[Dict[str, Any]]] = {}
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
    user = get_current_user(db, request)
    today = date.today()
    try:
        chk_rows = q(
            db,
            "SELECT tank_id, parameter, additive_id, planned_date, checked FROM dose_plan_checks WHERE planned_date>=? AND planned_date<=?",
            (today.isoformat(), (today + timedelta(days=60)).isoformat()),
        )
        check_map = {
            (r["tank_id"], r["parameter"], r["additive_id"], r["planned_date"]): int(r["checked"] or 0)
            for r in chk_rows
        }
        latest_rows = q(
            db,
            "SELECT tank_id, parameter, additive_id, checked_at FROM dose_plan_checks WHERE checked=1",
        )
        latest_check_map: Dict[Tuple[int, str, int], datetime] = {}
        for row in latest_rows:
            checked_at = parse_dt_any(row_get(row, "checked_at"))
            if not checked_at:
                continue
            key = (int(row["tank_id"]), row["parameter"], int(row["additive_id"]))
            existing = latest_check_map.get(key)
            if not existing or checked_at > existing:
                latest_check_map[key] = checked_at
    except Exception:
        check_map = {}
        latest_check_map = {}
    tanks = get_visible_tanks(db, request)
    pdefs = q(db, "SELECT id, name, unit, max_daily_change, default_target_low, default_target_high FROM parameter_defs")
    if user:
        pdefs = apply_user_parameter_overrides(pdefs, get_user_parameter_settings(db, user["id"]))
    pdef_map = {r["name"]: r for r in pdefs}
    all_additives = [
        {"id": row_get(row, "id"), "name": row_get(row, "name"), "parameter": row_get(row, "parameter")}
        for row in get_visible_additives(db, user, active_only=True)
    ]
    additives_by_group: Dict[str, List[Dict[str, Any]]] = {}
    def group_key(value: str) -> str:
        cleaned = re.sub(r"[^a-z0-9]+", "", str(value or "").lower())
        if "alk" in cleaned or "kh" in cleaned:
            return "alkalinity"
        if "calcium" in cleaned or cleaned == "ca":
            return "calcium"
        if "magnesium" in cleaned or cleaned == "mg":
            return "magnesium"
        if "nitrate" in cleaned or "no3" in cleaned:
            return "nitrate"
        if "phosphate" in cleaned or "po4" in cleaned:
            return "phosphate"
        if "trace" in cleaned:
            return "trace"
        return cleaned
    for additive in all_additives:
        additives_by_group.setdefault(group_key(row_get(additive, "parameter")), []).append(additive)
    available_parameters = set()
    total_needed = 0
    top_deficit = None
    plans = []
    grand_total_ml = 0.0
    latest_icp_by_tank: Dict[int, Dict[str, Any]] = {}
    icp_sample_ids: List[int] = []
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
        icp_sample = one(
            db,
            "SELECT id, taken_at FROM samples WHERE tank_id=? AND lower(notes) LIKE ? ORDER BY taken_at DESC LIMIT 1",
            (tank_id, "%imported from icp%"),
        )
        if icp_sample:
            latest_icp_by_tank[tank_id] = {
                "id": icp_sample["id"],
                "taken_at": parse_dt_any(icp_sample["taken_at"]),
            }
            icp_sample_ids.append(icp_sample["id"])

        targets = q(db, "SELECT * FROM targets WHERE tank_id=? AND enabled=1 ORDER BY parameter", (tank_id,))
        targets_by_param = {row_get(t, "parameter"): t for t in targets if row_get(t, "parameter")}
        for pname, pdef in pdef_map.items():
            if pname in targets_by_param:
                continue
            if not is_trace_element(pname):
                continue
            default_low = row_get(pdef, "default_target_low")
            default_high = row_get(pdef, "default_target_high")
            if default_low is None and default_high is None:
                continue
            targets_by_param[pname] = {
                "parameter": pname,
                "target_low": default_low,
                "target_high": default_high,
                "unit": row_get(pdef, "unit"),
                "enabled": 1,
            }
        targets = list(targets_by_param.values())
        tank_rows = []
        for tr in targets:
            pname = tr["parameter"]
            if not pname: continue
            available_parameters.add(pname)
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
            total_needed += 1
            
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
            where_sql, params = build_additives_where(db, user, active_only=True, extra_clause="parameter=?", extra_params=(pname,))
            adds = q(db, f"SELECT * FROM additives{where_sql} ORDER BY name", params)
            if not adds:
                suggestion = None
                for candidate in additives_by_group.get(group_key(pname), []):
                    suggestion = candidate
                    break
                if top_deficit is None or (top_deficit and delta > top_deficit["delta"]):
                    top_deficit = {"tank": t["name"], "parameter": pname, "delta": delta, "unit": unit}
                # Keep 'no additive' rows so user knows they need one
                tank_rows.append({
                    "parameter": pname,
                    "latest": cur_val_f,
                    "target": target_val,
                    "change": delta,
                    "unit": unit,
                    "days": days,
                    "per_day_change": per_day_change,
                    "max_daily_change": max_change_f,
                    "additives": [],
                    "note": "No additive linked.",
                    "suggested_additive": suggestion,
                })
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
                    try: total_ml = (float(delta) * float(eff_vol_l) * 1000.0) / float(strength)
                    except Exception: total_ml = None
                per_day_ml = None if total_ml is None else (float(total_ml) / float(days))
                schedule = []
                latest_sample = latest_map.get(pname)
                latest_sample_taken = latest_sample.get("taken_at") if latest_sample else None
                last_checked_at = latest_check_map.get((tank_id, pname, a["id"]))
                for i in range(int(days)):
                    d = (today + timedelta(days=i)).isoformat()
                    planned_key = (tank_id, pname, a["id"], d)
                    if planned_key in check_map:
                        checked_value = check_map[planned_key]
                    elif last_checked_at and (not latest_sample_taken or last_checked_at > latest_sample_taken):
                        checked_value = 1
                    else:
                        checked_value = 0
                    schedule.append({
                        "day": i + 1,
                        "date": d,
                        "when": parse_dt_any(d),
                        "ml": per_day_ml,
                        "checked": checked_value,
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
                    "additive_name": additive_label(a), 
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
            if top_deficit is None or (top_deficit and delta > top_deficit["delta"]):
                top_deficit = {"tank": t["name"], "parameter": pname, "delta": delta, "unit": unit}
                
            tank_rows.append({"parameter": pname, "latest": cur_val_f, "target": target_val, "change": delta, "unit": unit, "days": days, "per_day_change": per_day_change, "additives": add_rows, "note": ""})
            
        plan_total_ml = 0.0
        for _r in tank_rows:
            # Sum only the selected additives for the total
            for _a in (_r.get("additives") or []):
                if _a.get("selected"):
                    try: plan_total_ml += float(_a.get("total_ml") or 0)
                    except Exception: pass
        grand_total_ml += plan_total_ml
        icp_info = latest_icp_by_tank.get(tank_id)
        plans.append(
            {
                "tank": t,
                "latest_taken": latest_taken,
                "eff_vol_l": eff_vol_l,
                "net_pct": net_pct,
                "rows": tank_rows,
                "total_ml": plan_total_ml,
                "icp_recommendations": [],
                "icp_sample": icp_info,
                "targets_by_param": targets_by_param,
            }
        )
    icp_check_map: Dict[Tuple[int, str], int] = {}
    if icp_sample_ids:
        placeholders = ",".join("?" for _ in icp_sample_ids)
        icp_check_rows = q(
            db,
            f"SELECT sample_id, label, checked FROM icp_dose_checks WHERE sample_id IN ({placeholders})",
            tuple(icp_sample_ids),
        )
        icp_check_map = {
            (row_get(r, "sample_id"), row_get(r, "label")): int(row_get(r, "checked") or 0)
            for r in icp_check_rows
        }
    for plan in plans:
        icp_info = plan.get("icp_sample")
        if not icp_info:
            continue
        targets_by_param = plan.get("targets_by_param") or {}
        sample_values = { }
        for reading in get_sample_readings(db, icp_info["id"]):
            name = row_get(reading, "name") or ""
            normalized = normalize_icp_name(name)
            sample_values[normalized] = {
                "name": name,
                "value": row_get(reading, "value"),
                "unit": row_get(reading, "unit"),
            }
        rec_rows = q(
            db,
            """
            SELECT label, value, unit, notes
            FROM icp_recommendations
            WHERE sample_id=? AND category='dose'
            ORDER BY id ASC
            """,
            (icp_info["id"],),
        )
        for row in rec_rows:
            label = row_get(row, "label") or "Dose"
            notes = row_get(row, "notes") or ""
            range_info = parse_icp_notes_range(notes)
            schedule_info = parse_icp_dose_schedule(notes)
            normalized_label = normalize_icp_name(label)
            matched_reading = sample_values.get(normalized_label)
            parameter_name = matched_reading["name"] if matched_reading else label
            current_value = None
            current_unit = None
            if matched_reading:
                current_value = matched_reading["value"]
                current_unit = matched_reading["unit"]
            elif range_info["current"] is not None and (range_info["target_low"] is not None or range_info["target_high"] is not None):
                current_value = range_info["current"]
                current_unit = range_info["unit"]
            target_low = None
            target_high = None
            target_unit = current_unit or range_info["unit"]
            target_row = targets_by_param.get(parameter_name) or targets_by_param.get(label)
            if target_row:
                target_low = row_get(target_row, "target_low") or row_get(target_row, "low")
                target_high = row_get(target_row, "target_high") or row_get(target_row, "high")
                if row_get(target_row, "unit"):
                    target_unit = row_get(target_row, "unit")
                elif parameter_name in pdef_map and pdef_map[parameter_name]["unit"]:
                    target_unit = pdef_map[parameter_name]["unit"]
            days = schedule_info["days"]
            total_value = row_get(row, "value")
            if total_value is None:
                total_value = schedule_info["total"]
            per_day_value = None
            if total_value is not None and days:
                try:
                    per_day_value = float(total_value) / float(days)
                except Exception:
                    per_day_value = None
            additive_options = [{"id": a["id"], "name": a["name"]} for a in all_additives]
            selected_additive_id = None
            for additive in additive_options:
                if label.lower() in additive["name"].lower() or additive["name"].lower() in label.lower():
                    selected_additive_id = additive["id"]
                    break
            plan["icp_recommendations"].append(
                {
                    "label": label,
                    "parameter_name": parameter_name,
                    "value": total_value,
                    "unit": row_get(row, "unit") or schedule_info["unit"],
                    "notes": notes,
                    "current": current_value,
                    "target_low": range_info["target_low"] if range_info["target_low"] is not None else target_low,
                    "target_high": range_info["target_high"] if range_info["target_high"] is not None else target_high,
                    "target_unit": target_unit,
                    "days": days,
                    "per_day_value": per_day_value,
                    "schedule_doses": schedule_info["doses"],
                    "additives": additive_options,
                    "selected_additive_id": selected_additive_id,
                    "checked": icp_check_map.get((icp_info["id"], label), 0),
                    "sample_id": icp_info["id"],
                }
            )
    db.close()
    available_parameters_sorted = sorted(available_parameters, key=lambda s: s.lower())
    dose_plan_summary = {
        "parameters_needing_dose": total_needed,
        "top_deficit": top_deficit,
    }
    return templates.TemplateResponse(
        "dose_plan.html",
        {
            "request": request,
            "plans": plans,
            "grand_total_ml": grand_total_ml,
            "format_value": format_value,
            "available_parameters": available_parameters_sorted,
            "available_tanks": tanks,
            "dose_plan_summary": dose_plan_summary,
        },
    )

@app.get("/tools/icp", response_class=HTMLResponse)
def icp_import(request: Request):
    db = get_db()
    tanks = q(db, "SELECT id, name FROM tanks ORDER BY name")
    parameters = q(db, "SELECT name FROM parameter_defs WHERE active=1 ORDER BY name")
    pdf_available = optional_module("PyPDF2") is not None
    db.close()
    return templates.TemplateResponse(
        "icp_import.html",
        {
            "request": request,
            "tanks": tanks,
            "parameters": parameters,
            "mapped": [],
            "mapped_payload": "",
            "raw_count": 0,
            "raw_preview": [],
            "raw_results": [],
            "error": request.query_params.get("error"),
            "success": request.query_params.get("success"),
            "selected_tank": "",
            "pdf_available": pdf_available,
            "recommendations": {"help": [], "dose": []},
            "recommendations_payload": "",
        },
    )

@app.post("/tools/icp/preview", response_class=HTMLResponse)
async def icp_preview(request: Request):
    form = await request.form()
    url = (form.get("icp_url") or "").strip()
    upload = form.get("icp_file")
    selected_tank = (form.get("tank_id") or "").strip()
    db = get_db()
    current_user = get_current_user(db, request)
    tanks = q(db, "SELECT id, name FROM tanks ORDER BY name")
    parameters = q(db, "SELECT name FROM parameter_defs WHERE active=1 ORDER BY name")
    pdf_available = optional_module("PyPDF2") is not None
    icp_upload_note = None
    try:
        if url:
            content, meta = fetch_triton_html(url)
            results = parse_triton_html(content)
            recommendations = {"help": [], "dose": []}
        elif upload and getattr(upload, "filename", ""):
            # File size limit: 10MB for security (prevent DoS via large files)
            MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB

            # Check content type (belt and suspenders with extension check)
            content_type = getattr(upload, "content_type", "").lower()
            filename = upload.filename.lower()

            # Validate file type before reading
            if not (filename.endswith((".csv", ".pdf"))):
                raise ValueError("Unsupported file type. Upload CSV or PDF.")

            # Read file with size limit
            data = await upload.read(MAX_FILE_SIZE + 1)
            if len(data) > MAX_FILE_SIZE:
                raise ValueError(f"File too large. Maximum size is {MAX_FILE_SIZE // (1024*1024)}MB.")

            if filename.endswith(".csv"):
                # Additional MIME type check for CSV
                if content_type and not content_type.startswith(("text/", "application/csv")):
                    raise ValueError("Invalid file type. Expected CSV file.")
                results = parse_triton_csv(data)
                recommendations = {"help": [], "dose": []}
            elif filename.endswith(".pdf"):
                # Additional MIME type check for PDF
                if content_type and not content_type.startswith("application/pdf"):
                    raise ValueError("Invalid file type. Expected PDF file.")
                if not pdf_available:
                    raise ValueError("PDF parsing requires PyPDF2. Install it and rebuild the container.")
                results = parse_triton_pdf(data)
                recommendations = {"help": [], "dose": []}
            if r2_enabled():
                safe_name = re.sub(r"[^a-z0-9_.-]+", "_", os.path.basename(upload.filename).lower())
                timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                key = f"icp-uploads/{timestamp}_{safe_name or 'icp_upload'}"
                content_type = upload.content_type or ("application/pdf" if filename.endswith(".pdf") else "text/csv")
                try:
                    upload_r2_bytes(data, key, content_type)
                except Exception as exc:
                    try:
                        log_audit(
                            db,
                            current_user,
                            "icp-upload-failed",
                            {
                                "tank_id": selected_tank,
                                "filename": upload.filename,
                                "error": str(exc),
                            },
                        )
                    except Exception:
                        pass
                    raise ValueError(f"R2 upload failed: {exc}") from exc
                tank_id_value = int(selected_tank) if str(selected_tank).isdigit() else None
                user_id_value = current_user["id"] if current_user else None
                execute_insert_returning_id(
                    db,
                    """
                    INSERT INTO icp_uploads (user_id, tank_id, filename, content_type, r2_key, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        user_id_value,
                        tank_id_value,
                        upload.filename,
                        content_type,
                        key,
                        datetime.utcnow().isoformat(),
                    ),
                )
                db.commit()
                cleanup_r2_icp_uploads()
                icp_upload_note = f"ICP file uploaded to Cloudflare R2 ({key})."
        else:
            raise ValueError("Provide a Triton URL or upload a CSV/PDF.")
        if not results:
            if url:
                summary = summarize_triton_html(content, meta)
                raise ValueError(
                    "No ICP values found. Please verify the URL or file. "
                    f"{summary}."
                )
            raise ValueError("No ICP values found. Please verify the URL or file.")
        mapped = map_icp_to_parameters(db, results)
        enhanced_results = []
        for row in results:
            raw_name = row.get("name") or ""
            enhanced_results.append({**row, "normalized": normalize_icp_name(raw_name)})
        mapped_lookup = {}
        for row in mapped:
            source = row.get("source") or ""
            mapped_lookup[normalize_icp_name(source)] = row.get("parameter")
        raw_preview = enhanced_results[:10]
        recommendations_payload = ""
    except Exception as exc:
        db.close()
        return templates.TemplateResponse(
            "icp_import.html",
            {
                "request": request,
                "tanks": tanks,
                "parameters": parameters,
                "mapped": [],
                "mapped_payload": "",
                "raw_count": 0,
                "raw_preview": [],
                "raw_results": [],
                "mapped_lookup": {},
                "recommendations": {"help": [], "dose": []},
                "recommendations_payload": "",
                "error": str(exc),
                "success": None,
                "selected_tank": selected_tank,
                "pdf_available": pdf_available,
            },
        )
    db.close()
    return templates.TemplateResponse(
        "icp_import.html",
        {
            "request": request,
            "tanks": tanks,
            "parameters": parameters,
            "mapped": mapped,
            "mapped_payload": json.dumps(mapped),
            "raw_count": len(results),
            "raw_preview": raw_preview,
            "raw_results": enhanced_results,
            "mapped_lookup": mapped_lookup,
            "recommendations": recommendations,
            "recommendations_payload": recommendations_payload,
            "error": None,
            "success": icp_upload_note,
            "selected_tank": selected_tank,
            "pdf_available": pdf_available,
        },
    )

@app.post("/tools/icp/import")
async def icp_import_submit(request: Request):
    form = await request.form()
    tank_id = form.get("tank_id")
    payload = form.get("mapped_payload")
    recommendations_payload = form.get("recommendations_payload")
    if not tank_id or not payload:
        map_count = form.get("map_count")
        if not map_count:
            return redirect("/tools/icp?error=Missing tank or preview data.")
    try:
        tank_id_int = int(tank_id)
    except Exception:
        return redirect("/tools/icp?error=Invalid tank selected.")
    mapped: List[Dict[str, Any]] = []
    if payload:
        try:
            mapped = json.loads(payload)
        except Exception:
            return redirect("/tools/icp?error=Unable to read preview payload.")
    else:
        try:
            map_count_int = int(map_count or 0)
        except Exception:
            return redirect("/tools/icp?error=Invalid mapping data.")
        for idx in range(map_count_int):
            param_name = (form.get(f"map_{idx}") or "").strip()
            if not param_name:
                continue
            raw_name = (form.get(f"raw_name_{idx}") or "").strip()
            raw_value = form.get(f"raw_value_{idx}")
            raw_unit = (form.get(f"raw_unit_{idx}") or "").strip()
            if raw_value is None:
                continue
            mapped.append(
                {
                    "parameter": param_name,
                    "value": raw_value,
                    "unit": raw_unit,
                    "source": raw_name,
                }
            )
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id_int)
    if not tank:
        db.close()
        return redirect("/tools/icp?error=Tank not found.")
    if not mapped:
        db.close()
        return redirect("/tools/icp?error=No mapped ICP values found.")
    when_iso = datetime.utcnow().isoformat()
    sample_id = execute_insert_returning_id(
        db,
        "INSERT INTO samples (tank_id, taken_at, notes) VALUES (?, ?, ?)",
        (tank_id_int, when_iso, "Imported from ICP"),
    )
    if sample_id is None:
        db.close()
        return redirect("/tools/icp?error=Failed to save ICP sample.")
    for row in mapped:
        pname = row.get("parameter")
        value = row.get("value")
        unit = row.get("unit") or ""
        if pname is None or value is None:
            continue
        try:
            insert_sample_reading(db, sample_id, pname, float(value), unit)
        except Exception:
            continue
    db.commit()
    db.close()
    return redirect(f"/tools/icp?success=Imported ICP sample {sample_id}.")

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
    existing_check = one(
        db,
        "SELECT id FROM dose_plan_checks WHERE tank_id=? AND parameter=? AND additive_id=? AND planned_date=? LIMIT 1",
        (tank_id, parameter, additive_id, planned_date),
    )
    if existing_check:
        db.execute(
            "UPDATE dose_plan_checks SET checked=?, checked_at=? WHERE id=?",
            (checked, now_iso, existing_check["id"]),
        )
    else:
        try:
            db.execute(
                "INSERT INTO dose_plan_checks (tank_id, parameter, additive_id, planned_date, checked, checked_at) VALUES (?, ?, ?, ?, ?, ?)",
                (tank_id, parameter, additive_id, planned_date, checked, now_iso),
            )
        except IntegrityError:
            next_id = one(db, "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM dose_plan_checks")["next_id"]
            db.execute(
                "INSERT INTO dose_plan_checks (id, tank_id, parameter, additive_id, planned_date, checked, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (next_id, tank_id, parameter, additive_id, planned_date, checked, now_iso),
            )
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

@app.post("/tools/icp-dose/check")
async def icp_dose_check(request: Request):
    form = await request.form()
    sample_id_raw = (form.get("sample_id") or "").strip()
    label = (form.get("label") or "").strip()
    checked = 1 if str(form.get("checked") or "").lower() in ("1", "true", "on", "yes") else 0
    try:
        sample_id = int(sample_id_raw)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid sample id")
    if not label:
        raise HTTPException(status_code=400, detail="Missing label")
    db = get_db()
    now_iso = datetime.utcnow().isoformat()
    db.execute(
        "INSERT INTO icp_dose_checks (sample_id, label, checked, checked_at) VALUES (?, ?, ?, ?) ON CONFLICT(sample_id, label) DO UPDATE SET checked=excluded.checked, checked_at=excluded.checked_at",
        (sample_id, label, checked, now_iso),
    )
    db.commit()
    db.close()
    return {"ok": True, "checked": checked}

# --- NEW: ADVANCED EXCEL IMPORT CENTER ---

def render_import_manager(request: Request, **context: Any) -> HTMLResponse:
    backup_supported = engine.dialect.name == "sqlite"
    return templates.TemplateResponse(
        "import_manager.html",
        {
            "request": request,
            "backup_supported": backup_supported,
            "r2_enabled": r2_enabled(),
            "r2_bucket": R2_BUCKET,
            **context,
        },
    )

@app.get("/admin/import", response_class=HTMLResponse)
def import_page(request: Request):
    db = get_db()
    require_admin(get_current_user(db, request))
    db.close()
    return render_import_manager(request)

@app.get("/admin/users", response_class=HTMLResponse)
def admin_users(request: Request):
    db = get_db()
    user = get_current_user(db, request)
    require_admin(user)
    users = q(db, "SELECT id, email, admin, role, created_at FROM users ORDER BY created_at DESC")
    tanks = q(db, "SELECT id, name, owner_user_id FROM tanks ORDER BY name")
    assignments = q(db, "SELECT user_id, tank_id FROM user_tanks")
    user_tanks_map: Dict[int, List[int]] = {}
    for row in assignments:
        user_id = row_get(row, "user_id")
        tank_id = row_get(row, "tank_id")
        if user_id is None or tank_id is None:
            continue
        try:
            user_id_int = int(user_id)
            tank_id_int = int(tank_id)
        except Exception:
            continue
        user_tanks_map.setdefault(user_id_int, []).append(tank_id_int)
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    db.close()
    return templates.TemplateResponse(
        "admin_users.html",
        {
            "request": request,
            "users": users,
            "tanks": tanks,
            "user_tanks_map": user_tanks_map,
            "error": error,
            "success": success,
        },
    )

@app.get("/admin/icp-uploads", response_class=HTMLResponse)
def admin_icp_uploads(request: Request):
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    tank_id = (request.query_params.get("tank_id") or "").strip()
    user_id = (request.query_params.get("user_id") or "").strip()
    date_from = (request.query_params.get("from") or "").strip()
    date_to = (request.query_params.get("to") or "").strip()
    search = (request.query_params.get("q") or "").strip()
    where = []
    params: List[Any] = []
    if tank_id:
        where.append("iu.tank_id=?")
        params.append(int(tank_id))
    if user_id:
        where.append("iu.user_id=?")
        params.append(int(user_id))
    if date_from:
        where.append("iu.created_at >= ?")
        params.append(date_from)
    if date_to:
        where.append("iu.created_at <= ?")
        params.append(date_to)
    if search:
        # Escape special LIKE characters to prevent pattern injection
        escaped_search = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        where.append("(iu.filename LIKE ? ESCAPE '\\' OR iu.r2_key LIKE ? ESCAPE '\\')")
        params.extend([f"%{escaped_search}%", f"%{escaped_search}%"])
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = q(
        db,
        f"""
        SELECT iu.*, u.email AS user_email, t.name AS tank_name
        FROM icp_uploads iu
        LEFT JOIN users u ON u.id = iu.user_id
        LEFT JOIN tanks t ON t.id = iu.tank_id
        {where_sql}
        ORDER BY iu.created_at DESC
        LIMIT 200
        """,
        tuple(params),
    )
    users = q(db, "SELECT id, email FROM users ORDER BY email")
    tanks = q(db, "SELECT id, name FROM tanks ORDER BY name")
    uploads = []
    for row in rows:
        entry = dict(row)
        r2_key = row_get(row, "r2_key")
        if r2_key and r2_enabled():
            try:
                entry["download_url"] = presign_r2_download(r2_key)
            except Exception:
                entry["download_url"] = None
        else:
            entry["download_url"] = None
        uploads.append(entry)
    db.close()
    return templates.TemplateResponse(
        "admin_icp_uploads.html",
        {
            "request": request,
            "uploads": uploads,
            "r2_enabled": r2_enabled(),
            "users": users,
            "tanks": tanks,
            "filters": {
                "tank_id": tank_id,
                "user_id": user_id,
                "from": date_from,
                "to": date_to,
                "q": search,
            },
        },
    )

@app.post("/admin/users/{user_id}/role")
async def admin_user_role(request: Request, user_id: int):
    form = await request.form()
    make_admin = (form.get("admin") or "").lower() in {"1", "true", "on", "yes"}
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    role = "admin" if make_admin else "user"
    db.execute("UPDATE users SET admin=?, role=? WHERE id=?", (1 if make_admin else 0, role, user_id))
    log_audit(db, current_user, "user-role-update", {"user_id": user_id, "admin": make_admin})
    db.commit()
    db.close()
    return redirect("/admin/users")

@app.post("/admin/users/{user_id}/delete")
async def admin_user_delete(request: Request, user_id: int):
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    if current_user and int(current_user["id"]) == user_id:
        db.close()
        return redirect("/admin/users?error=Cannot delete your own account")
    target = one(db, "SELECT id, admin, role FROM users WHERE id=?", (user_id,))
    if not target:
        db.close()
        return redirect("/admin/users?error=User not found")
    is_admin = row_get(target, "admin") in (1, True) or row_get(target, "role") == "admin"
    if is_admin:
        admin_count_row = one(db, "SELECT COUNT(*) AS count FROM users WHERE admin=1 OR role='admin'")
        admin_count = admin_count_row["count"] if admin_count_row else 0
        if admin_count <= 1:
            db.close()
            return redirect("/admin/users?error=Cannot delete the last admin")
    ensure_additives_owner_column(db)
    ensure_test_kits_owner_column(db)
    db.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
    db.execute("DELETE FROM api_tokens WHERE user_id=?", (user_id,))
    db.execute("DELETE FROM push_subscriptions WHERE user_id=?", (user_id,))
    if table_exists(db, "user_parameter_settings"):
        db.execute("DELETE FROM user_parameter_settings WHERE user_id=?", (user_id,))
    db.execute("DELETE FROM user_tanks WHERE user_id=?", (user_id,))
    db.execute("UPDATE tanks SET owner_user_id=NULL WHERE owner_user_id=?", (user_id,))
    db.execute("UPDATE additives SET owner_user_id=NULL WHERE owner_user_id=?", (user_id,))
    db.execute("UPDATE test_kits SET owner_user_id=NULL WHERE owner_user_id=?", (user_id,))
    db.execute("DELETE FROM audit_logs WHERE actor_user_id=?", (user_id,))
    db.execute("DELETE FROM users WHERE id=?", (user_id,))
    log_audit(db, current_user, "user-delete", {"user_id": user_id})
    db.commit()
    db.close()
    return redirect("/admin/users?success=User deleted")

@app.post("/admin/users/{user_id}/tanks")
async def admin_user_tanks(request: Request, user_id: int):
    form = await request.form()
    tank_ids = [int(tid) for tid in form.getlist("tank_ids") if str(tid).isdigit()]
    clear_tanks = str(form.get("clear_tanks") or "").lower() in {"1", "true", "on", "yes"}
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    if not tank_ids and not clear_tanks:
        db.close()
        return redirect("/admin/users?error=Select+at+least+one+tank+or+use+Remove+all+tank+access")
    try:
        db.execute("DELETE FROM user_tanks WHERE user_id=?", (user_id,))
        if not clear_tanks:
            for tank_id in tank_ids:
                execute_with_retry(
                    db,
                    "INSERT INTO user_tanks (user_id, tank_id) VALUES (?, ?) ON CONFLICT (user_id, tank_id) DO NOTHING",
                    (user_id, tank_id),
                )
        assigned_rows = q(db, "SELECT tank_id FROM user_tanks WHERE user_id=?", (user_id,))
        assigned_ids = {int(row_get(row, "tank_id")) for row in assigned_rows if row_get(row, "tank_id") is not None}
        requested_ids = set(tank_ids)
        missing_ids = sorted(requested_ids - assigned_ids)
        if missing_ids:
            db.rollback()
            db.close()
            missing_param = ",".join(str(tid) for tid in missing_ids)
            return redirect(f"/admin/users?error=Unable+to+assign+tanks:+{urllib.parse.quote(missing_param)}")
        db.commit()
    except IntegrityError:
        db.rollback()
        db.close()
        return redirect("/admin/users?error=Unable+to+assign+tanks+to+this+user")
    try:
        log_audit(db, current_user, "user-tanks-update", {"user_id": user_id, "tanks": tank_ids})
    except Exception:
        pass
    db.close()
    return redirect("/admin/users?success=Tank+access+updated")

@app.post("/admin/tanks/{tank_id}/assign")
async def assign_tank(request: Request, tank_id: int):
    form = await request.form()
    user_id = to_float(form.get("user_id"))
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    if user_id is None:
        db.execute("UPDATE tanks SET owner_user_id=NULL WHERE id=?", (tank_id,))
    else:
        db.execute("UPDATE tanks SET owner_user_id=? WHERE id=?", (int(user_id), tank_id))
        db.execute(
            "INSERT INTO user_tanks (user_id, tank_id) VALUES (?, ?) ON CONFLICT (user_id, tank_id) DO NOTHING",
            (int(user_id), tank_id),
        )
    log_audit(db, current_user, "tank-owner-update", {"tank_id": tank_id, "owner_id": user_id})
    db.commit()
    db.close()
    return redirect("/admin/users")

@app.get("/admin/audit", response_class=HTMLResponse)
def admin_audit(request: Request):
    db = get_db()
    current_user = get_current_user(db, request)
    require_admin(current_user)
    action = (request.query_params.get("action") or "").strip()
    user_id = (request.query_params.get("user_id") or "").strip()
    date_from = (request.query_params.get("from") or "").strip()
    date_to = (request.query_params.get("to") or "").strip()
    search = (request.query_params.get("q") or "").strip()
    where = []
    params: List[Any] = []
    if action:
        # Escape special LIKE characters to prevent pattern injection
        escaped_action = action.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        where.append("a.action LIKE ? ESCAPE '\\'")
        params.append(f"%{escaped_action}%")
    if user_id:
        where.append("a.actor_user_id=?")
        params.append(int(user_id))
    if date_from:
        where.append("a.created_at >= ?")
        params.append(date_from)
    if date_to:
        where.append("a.created_at <= ?")
        params.append(date_to)
    if search:
        # Escape special LIKE characters to prevent pattern injection
        escaped_search = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        where.append("(a.details LIKE ? ESCAPE '\\' OR u.email LIKE ? ESCAPE '\\')")
        params.extend([f"%{escaped_search}%", f"%{escaped_search}%"])
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    logs = q(
        db,
        f"""SELECT a.*, u.email AS actor_email
            FROM audit_logs a
            JOIN users u ON u.id = a.actor_user_id
            {where_sql}
            ORDER BY a.created_at DESC
            LIMIT 500""",
        tuple(params),
    )
    users = q(db, "SELECT id, email FROM users ORDER BY email")
    db.close()
    return templates.TemplateResponse(
        "admin_audit.html",
        {
            "request": request,
            "logs": logs,
            "users": users,
            "filters": {
                "action": action,
                "user_id": user_id,
                "from": date_from,
                "to": date_to,
                "q": search,
            },
        },
    )

@app.get("/admin/download-template")
def download_template(request: Request):
    pd = require_pandas()
    db = get_db()
    require_admin(get_current_user(db, request))
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
    pd = require_pandas()
    db = get_db()
    require_admin(get_current_user(db, request))
    tanks = get_visible_tanks(db, request)
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

@app.get("/admin/backup-download")
def backup_download(request: Request):
    db = get_db()
    require_admin(get_current_user(db, request))
    db.close()
    if engine.dialect.name != "sqlite":
        return render_import_manager(
            request,
            error="SQLite backup downloads are disabled for Postgres deployments. Use Neon backups instead.",
        )
    if r2_enabled():
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        key = f"backups/reef_backup_{timestamp}.sqlite"
        try:
            upload_r2_file(DB_PATH, key, "application/x-sqlite3")
            cleanup_r2_backups()
            url = presign_r2_download(key)
            return RedirectResponse(url)
        except Exception as exc:
            return render_import_manager(
                request,
                error=f"R2 backup upload failed: {exc}",
            )
    return FileResponse(DB_PATH, filename="reef_backup.sqlite")

@app.post("/admin/backup-restore")
async def backup_restore(request: Request, file: UploadFile = File(...)):
    db = get_db()
    require_admin(get_current_user(db, request))
    db.close()
    if engine.dialect.name != "sqlite":
        return render_import_manager(
            request,
            error="SQLite backup restores are disabled for Postgres deployments. Use Neon backups instead.",
        )
    if not file.filename or not file.filename.endswith((".db", ".sqlite")):
        return render_import_manager(
            request,
            error="Invalid backup file. Please upload a .db or .sqlite file.",
        )
    backup_path = f"{DB_PATH}.bak-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    temp_path = f"{DB_PATH}.restore"
    try:
        with open(temp_path, "wb") as out:
            shutil.copyfileobj(file.file, out)
        if os.path.exists(DB_PATH):
            shutil.copy2(DB_PATH, backup_path)
        os.replace(temp_path, DB_PATH)
    except Exception as exc:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return render_import_manager(
            request,
            error=f"Restore failed: {exc}",
        )
    return render_import_manager(
        request,
        success="Backup restored. Please refresh the app to load the new data.",
    )

@app.get("/api/tanks")
def api_tanks(request: Request):
    db = get_db()
    tanks = get_visible_tanks(db, request)
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
def api_samples(request: Request, tank_id: int, limit: int = 50):
    db = get_db()
    user = get_current_user(db, request)
    tank = get_tank_for_user(db, user, tank_id)
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
@limiter.limit("10/minute")
async def upload_excel(request: Request, file: UploadFile = File(...)):
    pd = get_pandas()
    if pd is None:
        return render_import_manager(
            request,
            error="Excel import requires pandas. Install pandas and restart the app.",
        )
    if not file.filename.endswith(('.xlsx', '.xls')):
        return render_import_manager(
            request,
            error="Invalid format. Please upload an Excel file.",
        )
    
    db = get_db()
    require_admin(get_current_user(db, request))
    stats = {"tanks": 0, "samples": 0}
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))
        pdefs = list_parameters(db)
        current_user = get_current_user(db, request)

        for _, row in df.iterrows():
            t_name = str(row.get("Tank Name", "Unknown")).strip()
            if not t_name or t_name == "nan": continue

            # Resolve Tank
            tank = one(db, "SELECT id FROM tanks WHERE name=?", (t_name,))
            if not tank:
                vol = to_float(row.get("Volume (L)", 0))
                tid = execute_insert_returning_id(
                    db,
                    "INSERT INTO tanks (name, volume_l, owner_user_id) VALUES (?,?,?)",
                    (t_name, vol, current_user["id"] if current_user else None),
                )
                if tid is None:
                    continue
                if current_user:
                    execute_with_retry(
                        db,
                        "INSERT INTO user_tanks (user_id, tank_id) VALUES (?, ?) ON CONFLICT (user_id, tank_id) DO NOTHING",
                        (current_user["id"], tid),
                    )
                execute_with_retry(
                    db,
                    "INSERT INTO tank_profiles (tank_id, volume_l, net_percent) VALUES (?,?,100) ON CONFLICT (tank_id) DO NOTHING",
                    (tid, vol),
                )
                stats["tanks"] += 1
            else:
                tid = tank["id"]
            
            # Resolve Date
            dt_raw = row.get("Date (YYYY-MM-DD)")
            dt_str = str(dt_raw.date()) if hasattr(dt_raw, 'date') else str(dt_raw)
            notes = str(row.get("Notes", "")) if row.get("Notes") and str(row.get("Notes")) != "nan" else ""
            
            sid = execute_insert_returning_id(
                db,
                "INSERT INTO samples (tank_id, taken_at, notes) VALUES (?,?,?)",
                (tid, dt_str, notes),
            )
            if sid is None:
                continue
            stats["samples"] += 1
            
            # Resolve readings for all parameters
            for p in pdefs:
                val = to_float(row.get(p["name"]))
                if isinstance(val, float) and math.isnan(val):
                    continue
                if val is not None:
                    insert_sample_reading(db, sid, p["name"], val)
        
        db.commit()
        return render_import_manager(
            request,
            success=f"Imported {stats['samples']} samples across {stats['tanks']} new tanks.",
        )
    except Exception as e:
        return render_import_manager(request, error=str(e))
    finally:
        db.close()

# --- Legacy Import (kept for compatibility) ---
@app.get("/admin/import-excel", response_class=HTMLResponse)
def import_excel_page(request: Request):
    db = get_db()
    require_admin(get_current_user(db, request))
    db.close()
    return templates.TemplateResponse("simple_message.html", {"request": request, "title": "Import Tank Parameters Sheet", "message": "This will import tank volumes and historical readings from the bundled Tank Parameters Sheet.xlsx.", "actions": [{"label": "Run import", "method": "post", "href": "/admin/import-excel"}]})

@app.post("/admin/import-excel")
async def import_excel_run(request: Request):
    db = get_db()
    require_admin(get_current_user(db, request))
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
