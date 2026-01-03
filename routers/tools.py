from fastapi import APIRouter, Depends, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from datetime import date, timedelta, datetime
import math, os
from database import get_db
import main as app_main
from models import Tank, Additive, TankProfile, ParameterDef, Sample, SampleValue, Target, DosePlanCheck, DoseLog

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
router = APIRouter()

# Re-register filters for tools
templates.env.filters["fmt2"] = lambda v: f"{float(v):.2f}".rstrip("0").rstrip(".") if v is not None and "." in f"{float(v):.2f}" else (str(v) if v is not None else "")

@router.get("/tools/calculators", response_class=HTMLResponse)
def calculators(request: Request, db: Session = Depends(get_db)):
    db_main = app_main.get_db()
    try:
        user = request.state.user if hasattr(request, "state") else None
        visible_ids = app_main.get_visible_tank_ids(db_main, user)
    finally:
        db_main.close()
    tanks = db.query(Tank).filter(Tank.id.in_(visible_ids)).all() if visible_ids else []
    return templates.TemplateResponse("calculators.html", {
        "request": request, "tanks": tanks, "additives": db.query(Additive).filter(Additive.active==1).all(),
        "profiles": {p.tank_id: p for p in db.query(TankProfile).filter(TankProfile.tank_id.in_(visible_ids)).all()}
    })

@router.post("/tools/calculators", response_class=HTMLResponse)
def calculators_post(request: Request, tank_id: int = Form(...), additive_id: int = Form(...), desired_change: float = Form(...), db: Session = Depends(get_db)):
    db_main = app_main.get_db()
    try:
        user = request.state.user if hasattr(request, "state") else None
        if not app_main.get_tank_for_user(db_main, user, tank_id):
            raise HTTPException(status_code=404, detail="Tank not found")
    finally:
        db_main.close()
    tank = db.query(Tank).filter(Tank.id==tank_id).first()
    additive = db.query(Additive).filter(Additive.id==additive_id).first()
    prof = tank.profile
    
    dose_ml, days, daily_ml = None, 1, None
    if prof and additive.strength:
        vol = prof.volume_l * (prof.net_percent / 100.0)
        dose_ml = (desired_change / additive.strength) * (vol / 100.0)
        
        # Max Daily Logic
        pdef = db.query(ParameterDef).filter(ParameterDef.name == additive.parameter).first()
        limit = pdef.max_daily_change if pdef else None
        if limit and limit > 0 and desired_change > limit:
            days = int(math.ceil(desired_change / limit))
            daily_ml = dose_ml / days

    return templates.TemplateResponse("calculators.html", {
        "request": request, "tanks": db.query(Tank).all(), "additives": db.query(Additive).all(),
        "profiles": {p.tank_id: p for p in db.query(TankProfile).all()},
        "result": {"dose_ml": dose_ml, "days": days, "daily_ml": daily_ml, "tank": tank, "additive": additive, "desired_change": desired_change},
        "selected": {"tank_id": tank_id, "additive_id": additive_id}
    })

@router.get("/tools/dose-plan", response_class=HTMLResponse)
def dose_plan(request: Request, db: Session = Depends(get_db)):
    today = date.today()
    plans = []
    
    # Preload checks
    checks = (
        db.query(DosePlanCheck)
        .filter(
            DosePlanCheck.planned_date >= (today - timedelta(days=60)).isoformat(),
            DosePlanCheck.planned_date <= (today + timedelta(days=60)).isoformat(),
        )
        .all()
    )
    check_map = {(c.tank_id, c.parameter, c.additive_id, c.planned_date): c.checked for c in checks}
    
    db_main = app_main.get_db()
    try:
        user = request.state.user if hasattr(request, "state") else None
        visible_ids = app_main.get_visible_tank_ids(db_main, user)
    finally:
        db_main.close()
    for t in db.query(Tank).filter(Tank.id.in_(visible_ids)).all():
        if not t.profile: continue
        eff_vol = t.profile.volume_l * (t.profile.net_percent / 100.0)
        
        latest = db.query(Sample).filter(Sample.tank_id == t.id).order_by(Sample.taken_at.desc()).first()
        if not latest: continue
        plan_start_date = latest.taken_at.date()
        
        l_vals = {v.parameter_def.name: v.value for v in latest.values}
        targets = db.query(Target).filter(Target.tank_id == t.id).all()
        
        tank_rows = []
        total_ml_tank = 0
        
        for tr in targets:
            if not tr.enabled or not tr.parameter in l_vals: continue
            
            # Determine target
            tv = tr.target_low
            if tr.target_low and tr.target_high: tv = (tr.target_low + tr.target_high) / 2.0
            
            current = l_vals[tr.parameter]
            delta = tv - current
            if delta <= 0: continue
            
            # Find additive
            adds = db.query(Additive).filter(Additive.parameter == tr.parameter, Additive.active == 1).all()
            if not adds: continue
            
            pdef = db.query(ParameterDef).filter(ParameterDef.name == tr.parameter).first()
            max_daily = pdef.max_daily_change if pdef else None
            
            days = 1
            if max_daily and max_daily > 0 and delta > max_daily:
                days = int(math.ceil(delta / max_daily))
                
            add_rows = []
            for a in adds:
                if not a.strength: continue
                total_ml = (delta / a.strength) * (eff_vol / 100.0)
                per_day = total_ml / days
                
                # Schedule
                sched = []
                for i in range(days):
                    d = (plan_start_date + timedelta(days=i)).isoformat()
                    sched.append({
                        "date": d, "ml": per_day,
                        "checked": check_map.get((t.id, tr.parameter, a.id, d), 0),
                        "key": f"{t.id}|{tr.parameter}|{a.id}|{d}",
                        "tank_id": t.id, "additive_id": a.id, "parameter": tr.parameter
                    })
                
                add_rows.append({"additive_name": a.name, "total_ml": total_ml, "per_day_ml": per_day, "schedule": sched})
                total_ml_tank += total_ml

            tank_rows.append({
                "parameter": tr.parameter, "latest": current, "target": tv, "change": delta,
                "days": days, "additives": add_rows, "unit": pdef.unit if pdef else ""
            })
            
        plans.append({"tank": t, "rows": tank_rows, "total_ml": total_ml_tank})

    return templates.TemplateResponse("dose_plan.html", {"request": request, "plans": plans})

@router.post("/tools/dose-plan/check")
async def dose_plan_check(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    # Simple composite key handling
    key = form.get("key") or ""
    parts = key.split("|")
    if len(parts) != 4: return {"ok": False}
    
    tid, param, aid, d = int(parts[0]), parts[1], int(parts[2]), parts[3]
    checked = 1 if form.get("checked") == "true" else 0
    
    # Upsert check
    chk = db.query(DosePlanCheck).filter_by(tank_id=tid, parameter=param, additive_id=aid, planned_date=d).first()
    if not chk:
        chk = DosePlanCheck(tank_id=tid, parameter=param, additive_id=aid, planned_date=d)
        db.add(chk)
    chk.checked = checked
    
    # Log logic
    try:
        planned_dt = datetime.fromisoformat(d)
    except ValueError:
        planned_dt = datetime.utcnow()
    logged_at = datetime.combine(planned_dt.date(), datetime.utcnow().time()).isoformat()
    reason = f"Dose plan: {param} ({d})"
    if checked:
        existing = db.query(DoseLog).filter(
            DoseLog.tank_id == tid,
            DoseLog.additive_id == aid,
            DoseLog.logged_at.like(f"{d}%"),
            DoseLog.reason == reason,
        ).first()
        if not existing:
            db.add(DoseLog(tank_id=tid, additive_id=aid, amount_ml=float(form.get("amount_ml") or 0), reason=reason, logged_at=logged_at))
    else:
        db.query(DoseLog).filter(
            DoseLog.tank_id == tid,
            DoseLog.additive_id == aid,
            DoseLog.logged_at.like(f"{d}%"),
            DoseLog.reason == reason,
        ).delete()
        
    db.commit()
    return {"ok": True}
