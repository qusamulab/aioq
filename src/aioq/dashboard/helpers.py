"""Broker reads and validation shared by dashboard routes."""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from ..backends.base import BaseBroker
from ..models import Job, JobStatus


def parse_status(status: str | None) -> JobStatus | None:
    """Validate a ``?status=`` query value, returning 400 for unknown values."""
    if not status:
        return None
    try:
        return JobStatus(status)
    except ValueError as exc:
        valid = ", ".join(s.value for s in JobStatus)
        raise HTTPException(
            status_code=400, detail=f"Unknown status {status!r}. Expected one of: {valid}"
        ) from exc


async def get_job_or_404(broker: BaseBroker, job_id: str) -> Job:
    job = await broker.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


async def load_snapshot(broker: BaseBroker) -> dict[str, Any]:
    """Read the queue and worker snapshot used by pages, the API and SSE."""
    stats = await broker.queue_stats()
    workers = await broker.list_workers()
    return {"stats": stats, "workers": workers}
