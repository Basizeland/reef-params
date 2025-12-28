from fastapi import APIRouter, Depends, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session, joinedload
from typing import Optional, List
from datetime import datetime
import os
import re

from database import get_db
from models import Tank, TankProfile, Sample, SampleValue, ParameterDef, Target, DoseLog, Additive, TestKit

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Helpers
def fmt2(v):
    if v is None: return ""
    try:
        if isinstance(v, bool): return "1" if v else "0"
        if isinstance(v, int): return str(v)
        fv = float(v)
        return f"{fv:.2f}".rstrip("0").rstrip(".") if "." in f"{fv:.2f}" else f"{fv:.2f}"
    except: return str(v)

def dtfmt(v):
    if not v: return ""
    try:
        if isinstance(v, str): dt = datetime.fromisoformat(v)
        else: dt = v
        return dt.strftime("%H:%M - %d/%m/%Y")
    except: return str(v)

def slug_key(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", (name or "").strip().lower()).strip("_")
    return s or "x"

templates.env.filters["fmt2"] = fmt2
templates.env.filters["dtfmt"] = dtfmt
templates.env.globals["format_value"] = lambda p, v: fmt2(v)

router = APIRouter()

@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db)):
    tanks = db.query(Tank).all()
    tank_cards = []
    for t in tanks:
        latest = db.query(Sample).filter(Sample.tank_id == t.id).order_by(Sample.taken_at.desc()).first()
        readings = []
        if latest:
            vals = db.query(SampleValue).options(joinedload(SampleValue.parameter_def))\
                     .filter(SampleValue.sample_id == latest.id).all()
            vals.sort(key=lambda x: x.parameter_def.sort_order)
            for v in vals:
                readings.append({"name": v.parameter_def.name, "value": v.value, "unit": v.parameter_def.unit})
        vol = t.profile.volume_l if t.profile else t.volume_l
        tank_cards.append({"tank": t, "latest": latest, "readings": readings, "volume": vol})
    return templates.TemplateResponse("dashboard.html", {"request": request, "tank_cards": tank_cards, "extra_css": ["/static/dashboard.css"]})

@router.get("/tanks/new", response_class=HTMLResponse)
def tank_new_form(request: Request):
    return HTMLResponse('<html><body><h2>Add Tank</h2><form method="post" action="/tanks/new"><input name="name" placeholder="Name" required /><input name="volume_l" placeholder="Volume L" /><button type="submit">Create</button></form></body></html>')

@router.post("/tanks/new")
def tank_new(name: str = Form(...), volume_l: Optional[float] = Form(None), db: Session = Depends(get_db)):
    t = Tank(name=name, volume_l=volume_l)
    db.add(t)
    db.commit()
    db.refresh(t)
    db.add(TankProfile(tank_id=t.id, volume_l=volume_l, net_percent=100))
    db.commit()
    return RedirectResponse(f"/tanks/{t.id}", status_code=303)

@router.get("/tanks/{tank_id}", response_class=HTMLResponse)
def tank_detail(request: Request, tank_id: int, parameter: Optional[str] = None, db: Session = Depends(get_db)):
    tank = db.query(Tank).filter(Tank.id == tank_id).first()
    if not tank: raise HTTPException(404, "Tank not found")
    samples = db.query(Sample).filter(Sample.tank_id == tank_id).order_by(Sample.taken_at.desc()).limit(50).all()
    all_params = db.query(ParameterDef).filter(ParameterDef.active == 1).order_by(ParameterDef.sort_order).all()
    latest_vals = {}
    if samples:
        for lv in db.query(SampleValue).filter(SampleValue.sample_id == samples[0].id).all():
            latest_vals[lv.parameter_id] = lv.value
    targets = db.query(Target).filter(Target.tank_id == tank_id).all()
    target_map = {t.parameter: t for t in targets}
    
    selected_param = None
    if parameter: selected_param = db.query(ParameterDef).filter(ParameterDef.id == parameter).first()
    if not selected_param and all_params: selected_param = all_params[0]
    
    series = []
    if selected_param:
        history = db.query(Sample.taken_at, SampleValue.value).join(SampleValue).filter(Sample.tank_id == tank_id, SampleValue.parameter_id == selected_param.id).order_by(Sample.taken_at.asc()).all()
        series = [{"x": d, "y": v} for d, v in history]
        
    return templates.TemplateResponse("tank_detail.html", {
        "request": request, "tank": tank, "params": all_params, "recent_samples": samples,
        "latest_vals": latest_vals, "target_map": target_map, "series": series, "selected_param": selected_param
    })

@router.post("/tanks/{tank_id}/delete")
def tank_delete(tank_id: int, db: Session = Depends(get_db)):
    db.query(Tank).filter(Tank.id == tank_id).delete()
    db.commit()
    return RedirectResponse("/", status_code=303)

# --- Profile ---
@router.get("/tanks/{tank_id}/profile", response_class=HTMLResponse)
def tank_profile(request: Request, tank_id: int, db: Session = Depends(get_db)):
    tank = db.query(Tank).filter(Tank.id == tank_id).first()
    if not tank.profile:
        db.add(TankProfile(tank_id=tank.id, volume_l=tank.volume_l, net_percent=100))
        db.commit()
    return templates.TemplateResponse("tank_profile.html", {"request": request, "tank": tank})

@router.post("/tanks/{tank_id}/profile")
def tank_profile_save(tank_id: int, volume_l: Optional[float] = Form(None), net_percent: float = Form(100), db: Session = Depends(get_db)):
    prof = db.query(TankProfile).filter(TankProfile.tank_id == tank_id).first()
    if prof:
        prof.volume_l = volume_l
        prof.net_percent = net_percent
        # Update legacy field
        tank = db.query(Tank).filter(Tank.id == tank_id).first()
        tank.volume_l = volume_l
        db.commit()
    return RedirectResponse(f"/tanks/{tank_id}", status_code=303)

# --- Samples ---
@router.get("/tanks/{tank_id}/add", response_class=HTMLResponse)
def add_sample_form(request: Request, tank_id: int, db: Session = Depends(get_db)):
    tank = db.query(Tank).filter(Tank.id == tank_id).first()
    params = db.query(ParameterDef).filter(ParameterDef.active == 1).order_by(ParameterDef.sort_order).all()
    return templates.TemplateResponse("add_sample.html", {"request": request, "tank": tank, "parameters": params, "kits_by_param": {}, "kits": []})

@router.post("/tanks/{tank_id}/add")
async def add_sample(request: Request, tank_id: int, db: Session = Depends(get_db)):
    form = await request.form()
    taken_at = (form.get("taken_at") or "").strip() or datetime.utcnow().isoformat()
    s = Sample(tank_id=tank_id, taken_at=taken_at, notes=form.get("notes"))
    db.add(s)
    db.commit()
    db.refresh(s)
    for p in db.query(ParameterDef).filter(ParameterDef.active == 1).all():
        v = form.get(f"value_{p.id}")
        if v and v.strip():
            db.add(SampleValue(sample_id=s.id, parameter_id=p.id, value=float(v)))
    db.commit()
    return RedirectResponse(f"/tanks/{tank_id}", status_code=303)

@router.get("/tanks/{tank_id}/samples/{sample_id}", response_class=HTMLResponse)
def sample_detail(request: Request, tank_id: int, sample_id: int, db: Session = Depends(get_db)):
    sample = db.query(Sample).filter(Sample.id == sample_id).first()
    readings = []
    for sv in sample.values:
        readings.append({"name": sv.parameter_def.name, "value": sv.value, "unit": sv.parameter_def.unit})
    return templates.TemplateResponse("sample_detail.html", {"request": request, "tank": sample.tank, "sample": sample, "readings": readings})

@router.post("/samples/{sample_id}/delete")
def sample_delete(sample_id: int, db: Session = Depends(get_db)):
    s = db.query(Sample).filter(Sample.id == sample_id).first()
    tid = s.tank_id
    db.delete(s)
    db.commit()
    return RedirectResponse(f"/tanks/{tid}", status_code=303)

# --- Targets ---
@router.get("/tanks/{tank_id}/targets", response_class=HTMLResponse)
def edit_targets(request: Request, tank_id: int, db: Session = Depends(get_db)):
    tank = db.query(Tank).filter(Tank.id == tank_id).first()
    params = db.query(ParameterDef).order_by(ParameterDef.sort_order).all()
    targets = {t.parameter: t for t in db.query(Target).filter(Target.tank_id == tank_id).all()}
    
    rows = []
    for p in params:
        t = targets.get(p.name)
        rows.append({
            "parameter": p,
            "key": slug_key(p.name),
            "target": t.target_low if t else None, # Simplified for UI
            "target_low": t.target_low if t else None,
            "target_high": t.target_high if t else None,
            "alert_low": t.alert_low if t else None,
            "alert_high": t.alert_high if t else None,
            "enabled": t.enabled if t else 1
        })
    return templates.TemplateResponse("edit_targets.html", {"request": request, "tank": tank, "rows": rows})

@router.post("/tanks/{tank_id}/targets")
async def save_targets(request: Request, tank_id: int, db: Session = Depends(get_db)):
    form = await request.form()
    params = db.query(ParameterDef).all()
    for p in params:
        key = slug_key(p.name)
        enabled = 1 if form.get(f"enabled_{key}") else 0
        
        t = db.query(Target).filter(Target.tank_id==tank_id, Target.parameter==p.name).first()
        if not t:
            t = Target(tank_id=tank_id, parameter=p.name)
            db.add(t)
        
        t.enabled = enabled
        t.unit = p.unit
        
        # Helper to safely float
        def getf(k): 
            v = form.get(k)
            return float(v) if v and v.strip() else None

        target_val = getf(f"target_{key}")
        t.target_low = getf(f"target_low_{key}") or target_val
        t.target_high = getf(f"target_high_{key}") or target_val
        t.alert_low = getf(f"alert_low_{key}")
        t.alert_high = getf(f"alert_high_{key}")

    db.commit()
    return RedirectResponse(f"/tanks/{tank_id}/targets", status_code=303)

# --- Dosing Log ---
@router.get("/tanks/{tank_id}/dosing-log", response_class=HTMLResponse)
def dosing_log(request: Request, tank_id: int, db: Session = Depends(get_db)):
    tank = db.query(Tank).filter(Tank.id == tank_id).first()
    logs = db.query(DoseLog).filter(DoseLog.tank_id == tank_id).order_by(DoseLog.logged_at.desc()).all()
    adds = db.query(Additive).filter(Additive.active == 1).all()
    # Normalize for template
    norm_logs = []
    for l in logs:
        norm_logs.append({
            "id": l.id, "logged_at_dt": datetime.fromisoformat(l.logged_at),
            "additive_name": l.additive.name if l.additive else "Unknown",
            "amount_ml": l.amount_ml, "reason": l.reason
        })
    return templates.TemplateResponse("dosing_log.html", {"request": request, "tank": tank, "logs": norm_logs, "additives": adds})

@router.post("/tanks/{tank_id}/dosing-log")
def dosing_log_add(tank_id: int, additive_id: int = Form(...), amount_ml: float = Form(...), reason: str = Form(None), db: Session = Depends(get_db)):
    db.add(DoseLog(tank_id=tank_id, additive_id=additive_id, amount_ml=amount_ml, reason=reason, logged_at=datetime.utcnow().isoformat()))
    db.commit()
    return RedirectResponse(f"/tanks/{tank_id}/dosing-log", status_code=303)

@router.post("/dose-logs/{log_id}/delete")
def dosing_log_delete(log_id: int, tank_id: int = Form(...), db: Session = Depends(get_db)):
    db.query(DoseLog).filter(DoseLog.id == log_id).delete()
    db.commit()
    return RedirectResponse(f"/tanks/{tank_id}/dosing-log", status_code=303)
