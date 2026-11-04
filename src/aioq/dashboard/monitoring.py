"""Real-time stats, health checks and optional Prometheus metrics."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response, StreamingResponse

from ..app import Aioq
from .helpers import load_snapshot

try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    from ..metrics import AioqCollector, make_registry

    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

#: How often the SSE stream pushes fresh stats.
_SSE_INTERVAL = 2.0


def create_monitoring_router(app: Aioq) -> APIRouter:
    router = APIRouter()

    if _PROMETHEUS_AVAILABLE:
        collector = AioqCollector()
        registry = make_registry(collector)

    @router.get("/sse/stats")
    async def sse_stats(request: Request) -> StreamingResponse:
        async def event_generator() -> AsyncGenerator[str, None]:
            while True:
                if await request.is_disconnected():
                    break
                payload = json.dumps(await load_snapshot(app.broker))
                yield f"data: {payload}\n\n"
                await asyncio.sleep(_SSE_INTERVAL)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @router.get("/healthz")
    async def healthz():
        """Liveness probe: confirms the dashboard can reach the broker."""
        try:
            await app.broker.queue_stats()
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Broker unreachable: {exc}") from exc
        return {"status": "ok"}

    @router.get("/metrics")
    async def metrics_endpoint():
        if not _PROMETHEUS_AVAILABLE:
            raise HTTPException(status_code=501, detail="prometheus-client not installed")

        await collector.update(app.broker)
        output = generate_latest(registry)
        return Response(content=output, media_type=CONTENT_TYPE_LATEST)

    return router
