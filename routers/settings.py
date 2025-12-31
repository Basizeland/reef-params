from fastapi import APIRouter, Depends, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from typing import Optional
import os
from database import get_db
import main as app_main
from models import ParameterDef, TestKit, Additive, Preset, PresetItem

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
router = APIRouter()

# --- Parameters ---
@router.get("/settings/parameters", response_class=HTMLResponse)
def parameters_list(request: Request, db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    return templates.TemplateResponse("parameters.html", {"request": request, "parameters": db.query(ParameterDef).order_by(ParameterDef.sort_order).all()})

@router.get("/settings/parameters/new", response_class=HTMLResponse)
def parameter_new(request: Request):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    return templates.TemplateResponse("parameter_edit.html", {"request": request, "param": None})

@router.get("/settings/parameters/{param_id}/edit", response_class=HTMLResponse)
def parameter_edit(request: Request, param_id: int, db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    return templates.TemplateResponse("parameter_edit.html", {"request": request, "param": db.query(ParameterDef).filter(ParameterDef.id==param_id).first()})

@router.post("/settings/parameters/save")
def parameter_save(request: Request, param_id: Optional[str] = Form(None), name: str = Form(...), unit: Optional[str] = Form(None), active: Optional[str] = Form(None), max_daily_change: Optional[float] = Form(None), db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    is_active = 1 if active else 0
    if param_id and param_id.isdigit():
        p = db.query(ParameterDef).filter(ParameterDef.id == int(param_id)).first()
        p.name = name; p.unit = unit; p.active = is_active; p.max_daily_change = max_daily_change
    else:
        db.add(ParameterDef(name=name, unit=unit, active=is_active, max_daily_change=max_daily_change))
    db.commit()
    return RedirectResponse("/settings/parameters", status_code=303)

@router.post("/settings/parameters/{param_id}/delete")
def parameter_delete(request: Request, param_id: int, db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    db.query(ParameterDef).filter(ParameterDef.id==param_id).delete()
    db.commit()
    return RedirectResponse("/settings/parameters", status_code=303)

# --- Test Kits ---
@router.get("/settings/test-kits", response_class=HTMLResponse)
def test_kits(request: Request, db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    return templates.TemplateResponse("test_kits.html", {"request": request, "kits": db.query(TestKit).all()})

@router.get("/settings/test-kits/new", response_class=HTMLResponse)
def test_kit_new(request: Request, db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    return templates.TemplateResponse("test_kit_edit.html", {"request": request, "kit": None, "parameters": db.query(ParameterDef).all()})

@router.get("/settings/test-kits/{kit_id}/edit", response_class=HTMLResponse)
def test_kit_edit(request: Request, kit_id: int, db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    return templates.TemplateResponse("test_kit_edit.html", {"request": request, "kit": db.query(TestKit).filter(TestKit.id==kit_id).first(), "parameters": db.query(ParameterDef).all()})

@router.post("/settings/test-kits/save")
def test_kit_save(request: Request, kit_id: Optional[str] = Form(None), name: str = Form(...), parameter: str = Form(...), unit: str = Form(None), active: str=Form(None), db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    act = 1 if active else 0
    if kit_id and kit_id.isdigit():
        k = db.query(TestKit).filter(TestKit.id==int(kit_id)).first()
        k.name=name; k.parameter=parameter; k.unit=unit; k.active=act
    else:
        db.add(TestKit(name=name, parameter=parameter, unit=unit, active=act))
    db.commit()
    return RedirectResponse("/settings/test-kits", status_code=303)

@router.post("/settings/test-kits/{kit_id}/delete")
def test_kit_delete(request: Request, kit_id: int, db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    db.query(TestKit).filter(TestKit.id==kit_id).delete()
    db.commit()
    return RedirectResponse("/settings/test-kits", status_code=303)

# --- Additives ---
@router.get("/additives", response_class=HTMLResponse)
def additives_list(request: Request, db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    adds = db.query(Additive).all()
    return templates.TemplateResponse("additives.html", {"request": request, "additives": adds, "rows": adds})

@router.get("/additives/new", response_class=HTMLResponse)
def additive_new(request: Request, db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    return templates.TemplateResponse("additive_edit.html", {"request": request, "additive": None, "parameters": db.query(ParameterDef).all()})

@router.get("/additives/{id}/edit", response_class=HTMLResponse)
def additive_edit(request: Request, id: int, db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    return templates.TemplateResponse("additive_edit.html", {"request": request, "additive": db.query(Additive).filter(Additive.id==id).first(), "parameters": db.query(ParameterDef).all()})

@router.post("/additives/save")
def additive_save(request: Request, additive_id: Optional[str] = Form(None), name: str = Form(...), parameter: str = Form(...), strength: float = Form(...), unit: str = Form(...), db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    if additive_id and additive_id.isdigit():
        a = db.query(Additive).filter(Additive.id==int(additive_id)).first()
        a.name=name; a.parameter=parameter; a.strength=strength; a.unit=unit
    else:
        db.add(Additive(name=name, parameter=parameter, strength=strength, unit=unit))
    db.commit()
    return RedirectResponse("/additives", status_code=303)

@router.post("/additives/{id}/delete")
def additive_delete(request: Request, id: int, db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    db.query(Additive).filter(Additive.id==id).delete()
    db.commit()
    return RedirectResponse("/additives", status_code=303)

# --- Presets ---
@router.get("/settings/presets", response_class=HTMLResponse)
def presets(request: Request, db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    return templates.TemplateResponse("presets.html", {"request": request, "presets": db.query(Preset).all()})

@router.post("/settings/presets/create")
def preset_create(request: Request, name: str = Form(...), db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    db.add(Preset(name=name))
    db.commit()
    return templates.TemplateResponse("presets.html", {"request": request, "presets": db.query(Preset).all(), "created": True})

@router.get("/settings/presets/{id}", response_class=HTMLResponse)
def preset_detail(request: Request, id: int, db: Session = Depends(get_db)):
    app_main.require_admin(request.state.user if hasattr(request, "state") else None)
    return templates.TemplateResponse("preset_detail.html", {"request": request, "preset": db.query(Preset).filter(Preset.id==id).first(), "items": db.query(PresetItem).filter(PresetItem.preset_id==id).all(), "parameters": db.query(ParameterDef).all()})
