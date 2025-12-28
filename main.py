import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from database import engine, Base
from routers import tanks, settings, tools, admin

# Initialize DB
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Reef Tank Parameters")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(BASE_DIR, "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Include All Routers
app.include_router(tanks.router)
app.include_router(settings.router)
app.include_router(tools.router)
app.include_router(admin.router)

@app.get("/add", response_class=RedirectResponse)
def add_shortcut():
    return RedirectResponse("/", status_code=303)
