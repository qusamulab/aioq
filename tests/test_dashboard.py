"""Dashboard tests against a real (in-process) broker.

test_metrics.py covers the /metrics endpoint with a mocked broker; these render
the actual pages and exercise the JSON API so template regressions show up.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import Request

from aioq import Aioq
from aioq.backends.memory import MemoryBroker
from aioq.dashboard.app import create_dashboard
from aioq.models import Job, JobStatus


@pytest.fixture
async def broker():
    async with MemoryBroker() as b:
        yield b


@pytest.fixture
def app(broker):
    return Aioq(broker=broker)


@pytest.fixture
def client(app):
    dashboard = create_dashboard(app)
    transport = httpx.ASGITransport(app=dashboard)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


async def seed(broker, **kwargs) -> Job:
    kwargs.setdefault("task_name", "tasks.add")
    kwargs.setdefault("queue", "default")
    status = kwargs.pop("status", None)
    job = Job(**kwargs)
    await broker.enqueue(job)
    if status is not None:
        job.status = status
        await broker.update_job(job)
    return job


# ----------------------------------------------------------------------
# Pages
# ----------------------------------------------------------------------


async def test_index_renders(broker, client):
    await seed(broker)
    await broker.register_worker("w1", ["default"])

    response = await client.get("/")

    assert response.status_code == 200
    assert "default" in response.text


async def test_jobs_page_renders(broker, client):
    job = await seed(broker)
    response = await client.get("/jobs")

    assert response.status_code == 200
    assert job.id[:8] in response.text


async def test_jobs_page_filters_by_status(broker, client):
    done = await seed(broker, status=JobStatus.completed)
    pending = await seed(broker)

    response = await client.get("/jobs", params={"status": "completed"})

    assert done.id[:8] in response.text
    assert pending.id[:8] not in response.text


async def test_jobs_page_rejects_unknown_status(client):
    response = await client.get("/jobs", params={"status": "banana"})
    assert response.status_code == 400
    assert "Unknown status" in response.json()["detail"]


async def test_job_detail_renders(broker, client):
    job = await seed(broker, priority=10, timeout=30, max_retries=2, retry_backoff=True)

    response = await client.get(f"/jobs/{job.id}")

    assert response.status_code == 200
    assert job.id in response.text
    assert "30" in response.text  # the timeout row


async def test_job_detail_shows_duration(broker, client):
    from datetime import UTC, datetime, timedelta

    job = await seed(broker)
    job.status = JobStatus.completed
    job.started_at = datetime.now(UTC)
    job.completed_at = job.started_at + timedelta(milliseconds=1500)
    await broker.update_job(job)

    response = await client.get(f"/jobs/{job.id}")

    assert "1.500s" in response.text


async def test_job_detail_404s_for_unknown_job(client):
    response = await client.get("/jobs/nope")
    assert response.status_code == 404


# ----------------------------------------------------------------------
# JSON API
# ----------------------------------------------------------------------


async def test_api_stats(broker, client):
    await seed(broker)
    await broker.register_worker("w1", ["default"])

    body = (await client.get("/api/stats")).json()

    assert body["stats"] == {"default": {"pending": 1}}
    assert body["workers"][0]["worker_id"] == "w1"


async def test_sse_stream_matches_api_stats_and_stops_on_disconnect(
    app, broker, client, monkeypatch
):
    await seed(broker)
    await broker.register_worker("w1", ["default"])
    expected = (await client.get("/api/stats")).json()
    dashboard = create_dashboard(app)
    endpoint = next(route.endpoint for route in dashboard.routes if route.path == "/sse/stats")
    request = AsyncMock(spec=Request)
    request.is_disconnected.side_effect = [False, True]
    monkeypatch.setattr("aioq.dashboard.monitoring._SSE_INTERVAL", 0)

    response = await endpoint(request)
    stream = response.body_iterator
    event = await anext(stream)

    assert event.startswith("data: ") and event.endswith("\n\n")
    assert json.loads(event.removeprefix("data: ")) == expected
    assert response.media_type == "text/event-stream"
    assert response.headers["Cache-Control"] == "no-cache"
    assert response.headers["X-Accel-Buffering"] == "no"
    with pytest.raises(StopAsyncIteration):
        await anext(stream)


async def test_api_jobs(broker, client):
    job = await seed(broker, kwargs={"a": 1})

    body = (await client.get("/api/jobs")).json()

    assert [j["id"] for j in body] == [job.id]
    assert body[0]["kwargs"] == {"a": 1}
    assert isinstance(body[0]["enqueued_at"], str)


async def test_api_jobs_pagination(broker, client):
    for _ in range(5):
        await seed(broker)

    body = (await client.get("/api/jobs", params={"limit": 2})).json()

    assert len(body) == 2


async def test_api_job_detail(broker, client):
    job = await seed(broker)
    assert (await client.get(f"/api/jobs/{job.id}")).json()["id"] == job.id


async def test_api_job_404(client):
    assert (await client.get("/api/jobs/nope")).status_code == 404


async def test_api_dead_jobs(broker, client):
    dead = await seed(broker, dead_letter_queue="dlq")
    dead.status = JobStatus.dead
    dead.queue = "dlq"
    await broker.update_job(dead)
    await seed(broker)

    body = (await client.get("/api/dead")).json()

    assert [j["id"] for j in body] == [dead.id]


async def test_healthz_ok(client):
    response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_healthz_reports_a_broken_broker(app, client, monkeypatch):
    async def boom():
        raise ConnectionError("redis is down")

    monkeypatch.setattr(app.broker, "queue_stats", boom)

    response = await client.get("/healthz")

    assert response.status_code == 503
    assert "redis is down" in response.json()["detail"]


# ----------------------------------------------------------------------
# Actions
# ----------------------------------------------------------------------


async def test_cancel_endpoint(broker, client):
    job = await seed(broker)

    response = await client.post(f"/api/jobs/{job.id}/cancel")

    assert response.status_code == 200
    assert (await broker.get_job(job.id)).status == JobStatus.cancelled


async def test_cancel_endpoint_conflicts_on_running_job(broker, client):
    job = await seed(broker, status=JobStatus.running)
    assert (await client.post(f"/api/jobs/{job.id}/cancel")).status_code == 409


async def test_retry_endpoint(broker, client):
    job = await seed(broker, status=JobStatus.failed)

    response = await client.post(f"/api/jobs/{job.id}/retry")

    assert response.status_code == 200
    assert (await broker.get_job(job.id)).status == JobStatus.pending


async def test_retry_endpoint_conflicts_on_pending_job(broker, client):
    job = await seed(broker)
    assert (await client.post(f"/api/jobs/{job.id}/retry")).status_code == 409


async def test_replay_endpoint(broker, client):
    job = await seed(broker, dead_letter_queue="dlq", retries=3, status=JobStatus.dead)

    response = await client.post(f"/api/jobs/{job.id}/replay")

    assert response.status_code == 200
    refreshed = await broker.get_job(job.id)
    assert refreshed.status == JobStatus.pending
    assert refreshed.retries == 0


async def test_replay_endpoint_conflicts_on_live_job(broker, client):
    job = await seed(broker)
    assert (await client.post(f"/api/jobs/{job.id}/replay")).status_code == 409


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------


@pytest.mark.parametrize("path", ["/jobs", "/api/jobs", "/api/stats", "/metrics"])
async def test_dashboards_keep_their_brokers_and_metrics_isolated(path):
    async with MemoryBroker() as first, MemoryBroker() as second:
        await seed(first, queue="first-queue")
        await seed(second, queue="second-queue")
        first_transport = httpx.ASGITransport(app=create_dashboard(Aioq(broker=first)))
        second_transport = httpx.ASGITransport(app=create_dashboard(Aioq(broker=second)))
        async with (
            httpx.AsyncClient(transport=first_transport, base_url="http://first") as first_client,
            httpx.AsyncClient(
                transport=second_transport, base_url="http://second"
            ) as second_client,
        ):
            for selected_client, expected, unexpected in (
                (first_client, "first-queue", "second-queue"),
                (second_client, "second-queue", "first-queue"),
                (first_client, "first-queue", "second-queue"),
            ):
                response = await selected_client.get(path)
                assert response.status_code == 200
                assert expected in response.text
                assert unexpected not in response.text


async def test_dashboard_without_prometheus_keeps_serving_other_routes(app, monkeypatch):
    monkeypatch.setattr("aioq.dashboard.monitoring._PROMETHEUS_AVAILABLE", False)
    transport = httpx.ASGITransport(app=create_dashboard(app))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/metrics")
        assert response.status_code == 501
        assert response.json()["detail"] == "prometheus-client not installed"
        assert (await client.get("/api/stats")).status_code == 200
        assert (await client.get("/")).status_code == 200


def test_create_dashboard_refuses_when_disabled():
    disabled = Aioq(broker=MemoryBroker(), dashboard_enabled=False)
    with pytest.raises(RuntimeError, match="Dashboard is disabled"):
        create_dashboard(disabled)
