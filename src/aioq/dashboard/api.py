"""JSON API for job queries and lifecycle actions."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..app import Aioq
from .helpers import get_job_or_404, load_snapshot, parse_status


def create_api_router(app: Aioq) -> APIRouter:
    router = APIRouter(prefix="/api")

    @router.get("/stats")
    async def api_stats():
        return await load_snapshot(app.broker)

    @router.get("/jobs")
    async def api_jobs(
        queue: str | None = None,
        status: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ):
        jobs = await app.broker.list_jobs(
            queue=queue, status=parse_status(status), limit=limit, offset=offset
        )
        return [j.model_dump_json_safe() for j in jobs]

    @router.get("/dead")
    async def api_dead_jobs(queue: str | None = None):
        jobs = await app.broker.list_dead_jobs(queue=queue)
        return [j.model_dump_json_safe() for j in jobs]

    @router.get("/jobs/{job_id}")
    async def api_job(job_id: str):
        job = await get_job_or_404(app.broker, job_id)
        return job.model_dump_json_safe()

    @router.post("/jobs/{job_id}/cancel")
    async def api_cancel_job(job_id: str):
        cancelled = await app.broker.cancel_job(job_id)
        if not cancelled:
            raise HTTPException(
                status_code=409,
                detail="Job cannot be cancelled (not in pending/retrying state or not found)",
            )
        return {"cancelled": True, "job_id": job_id}

    @router.post("/jobs/{job_id}/retry")
    async def api_retry_job(job_id: str):
        retried = await app.broker.retry_job(job_id)
        if not retried:
            raise HTTPException(
                status_code=409,
                detail="Job cannot be retried (not in failed/cancelled state or not found)",
            )
        return {"retried": True, "job_id": job_id}

    @router.post("/jobs/{job_id}/replay")
    async def api_replay_job(job_id: str):
        replayed = await app.broker.replay_dead_job(job_id)
        if not replayed:
            raise HTTPException(
                status_code=409,
                detail="Job cannot be replayed (not in dead state or not found)",
            )
        return {"replayed": True, "job_id": job_id}

    return router
