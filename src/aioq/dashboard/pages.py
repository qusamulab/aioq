"""Server-rendered dashboard pages."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..app import Aioq
from ..models import JobStatus
from .helpers import get_job_or_404, load_snapshot, parse_status


def create_pages_router(app: Aioq, templates: Jinja2Templates) -> APIRouter:
    router = APIRouter()

    @router.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        snapshot = await load_snapshot(app.broker)
        return templates.TemplateResponse(request, "index.html", snapshot)

    @router.get("/jobs", response_class=HTMLResponse)
    async def jobs_page(
        request: Request,
        queue: str | None = None,
        status: str | None = None,
        page: int = 1,
        per_page: int = 20,
    ):
        broker = app.broker
        status_enum = parse_status(status)
        jobs = await broker.list_jobs(
            queue=queue,
            status=status_enum,
            limit=per_page,
            offset=(page - 1) * per_page,
        )
        stats = await broker.queue_stats()
        return templates.TemplateResponse(
            request,
            "jobs.html",
            {
                "jobs": jobs,
                "stats": stats,
                "current_queue": queue,
                "current_status": status,
                "page": page,
                "per_page": per_page,
                "statuses": [s.value for s in JobStatus],
            },
        )

    @router.get("/jobs/{job_id}", response_class=HTMLResponse)
    async def job_detail(request: Request, job_id: str):
        job = await get_job_or_404(app.broker, job_id)
        return templates.TemplateResponse(request, "job_detail.html", {"job": job})

    return router
