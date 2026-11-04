"""Dashboard application factory and resource configuration."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..app import Aioq
from .api import create_api_router
from .monitoring import create_monitoring_router
from .pages import create_pages_router

_TEMPLATES_DIR = Path(__file__).parent / "templates"
_STATIC_DIR = Path(__file__).parent / "static"


def create_dashboard(app: Aioq) -> FastAPI:
    """Create and return a FastAPI app serving the aioq dashboard."""
    if not app.dashboard_enabled:
        raise RuntimeError(
            "Dashboard is disabled for this Aioq instance (dashboard_enabled=False)."
        )

    dashboard = FastAPI(title="aioq dashboard")
    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

    if _STATIC_DIR.exists():
        dashboard.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    dashboard.include_router(create_pages_router(app, templates))
    dashboard.include_router(create_api_router(app))
    dashboard.include_router(create_monitoring_router(app))
    return dashboard
