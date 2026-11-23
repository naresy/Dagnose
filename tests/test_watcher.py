"""Polling the Airflow API for failed tasks (no plugin needed)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest

from dagnose import worker
from dagnose.collector import AirflowAPIError, AirflowClient
from dagnose.queue import JobQueue, write_spool_job
from dagnose.watcher import FLOOR_KEY, WATERMARK_KEY, poll_failures

NOW = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)


def ti(dag: str, run: str, task: str, try_number: int = 1, map_index: int = -1) -> dict[str, Any]:
    return {
        "dag_id": dag,
        "dag_run_id": run,
        "task_id": task,
        "try_number": try_number,
        "map_index": map_index,
        "state": "failed",
        "end_date": "2026-11-23T11:59:00Z",
    }


class FakeAirflow:
    """Answers the failed-task list query from a fixed list, with real paging."""

    def __init__(self, tis: list[dict[str, Any]], api: str = "v2") -> None:
        self.tis = tis
        self.api = api
        self.bodies: list[dict[str, Any]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/version"):
            ok = request.url.path.startswith(f"/api/{self.api}/")
            return httpx.Response(
                200 if ok else 404, json={"version": "3.3.2" if self.api == "v2" else "2.11.1"}
            )
        if request.url.path == "/auth/token":
            return httpx.Response(201, json={"access_token": "t"})
        assert request.url.path == f"/api/{self.api}/dags/~/dagRuns/~/taskInstances/list"
        body = json.loads(request.content)
        self.bodies.append(body)
        start, size = body["page_offset"], body["page_limit"]
        return httpx.Response(200, json={"task_instances": self.tis[start : start + size]})


def client(fake: FakeAirflow, make_settings: Any) -> AirflowClient:
    return AirflowClient(make_settings(), transport=httpx.MockTransport(fake))


@pytest.mark.parametrize("api", ["v1", "v2"])
def test_first_poll_looks_back_and_queues_failures(api: str, make_settings: Any) -> None:
    settings = make_settings(poll_lookback_minutes=60)
    fake = FakeAirflow([ti("a", "r1", "t"), ti("b", "r1", "t", map_index=3)], api=api)
    queue = JobQueue(settings.queue_path)
    with client(fake, make_settings) as c:
        assert poll_failures(c, queue, settings, now=NOW) == 2
    [body] = fake.bodies
    assert body["state"] == ["failed"]
    assert datetime.fromisoformat(body["end_date_gte"]) == NOW - timedelta(minutes=60)
    assert ("order_by" in body) is (api == "v2")  # v1 cannot sort this endpoint
    assert "dag_ids" not in body
    assert queue.get_state(WATERMARK_KEY) == NOW.isoformat()
    jobs = [queue.claim(), queue.claim()]
    assert {(j.dag_id, j.map_index) for j in jobs if j} == {("a", -1), ("b", 3)}


def test_next_poll_starts_at_the_watermark_minus_overlap(make_settings: Any) -> None:
    settings = make_settings(poll_overlap_s=600)
    queue = JobQueue(settings.queue_path)
    queue.set_state(WATERMARK_KEY, NOW.isoformat())
    queue.set_state(FLOOR_KEY, (NOW - timedelta(hours=5)).isoformat())
    fake = FakeAirflow([])
    with client(fake, make_settings) as c:
        poll_failures(c, queue, settings, now=NOW + timedelta(minutes=5))
    assert datetime.fromisoformat(fake.bodies[0]["end_date_gte"]) == NOW - timedelta(minutes=10)


def test_seen_again_is_not_queued_twice(make_settings: Any) -> None:
    """Overlapping polls, and the plugin plus polling, both report the same failure."""
    settings = make_settings()
    queue = JobQueue(settings.queue_path)
    write_spool_job(
        settings.spool_dir,
        {"dag_id": "a", "run_id": "r1", "task_id": "t", "try_number": 1, "map_index": -1},
    )
    assert queue.ingest_spool(settings.spool_dir) == 1
    fake = FakeAirflow([ti("a", "r1", "t"), ti("a", "r1", "t", try_number=2)])
    with client(fake, make_settings) as c:
        assert poll_failures(c, queue, settings, now=NOW) == 1  # only try 2 is new
        assert poll_failures(c, queue, settings, now=NOW + timedelta(seconds=30)) == 0
    assert queue.counts() == {"pending": 2}


def test_pages_through_many_failures(make_settings: Any) -> None:
    settings = make_settings()
    fake = FakeAirflow([ti("a", f"r{i}", "t") for i in range(250)])
    with client(fake, make_settings) as c:
        assert poll_failures(c, JobQueue(settings.queue_path), settings, now=NOW) == 250
    assert [b["page_offset"] for b in fake.bodies] == [0, 100, 200]


def test_only_selected_dags(make_settings: Any) -> None:
    settings = make_settings(watch_dag_ids=["sales_daily"])
    fake = FakeAirflow([])
    with client(fake, make_settings) as c:
        poll_failures(c, JobQueue(settings.queue_path), settings, now=NOW)
    assert fake.bodies[0]["dag_ids"] == ["sales_daily"]


def test_airflow_down_keeps_the_watermark(make_settings: Any) -> None:
    settings = make_settings()
    queue = JobQueue(settings.queue_path)
    queue.set_state(WATERMARK_KEY, NOW.isoformat())
    down = AirflowClient(
        make_settings(airflow_api="v2", airflow_token="t"),
        transport=httpx.MockTransport(lambda r: httpx.Response(503)),
    )
    with down, pytest.raises(AirflowAPIError):
        poll_failures(down, queue, settings, now=NOW + timedelta(hours=1))
    assert queue.get_state(WATERMARK_KEY) == NOW.isoformat()  # nothing skipped


@pytest.mark.parametrize(
    ("watch", "spool_used", "api_polled"),
    [("plugin", True, False), ("api", False, True), ("both", True, True)],
)
def test_worker_watch_modes(
    watch: str,
    spool_used: bool,
    api_polled: bool,
    make_settings: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = make_settings(watch=watch)
    write_spool_job(
        settings.spool_dir,
        {"dag_id": "a", "run_id": "r1", "task_id": "t", "try_number": 1, "map_index": -1},
    )
    polled: list[bool] = []
    monkeypatch.setattr(worker, "poll_failures", lambda *a, **k: polled.append(True) or 0)
    monkeypatch.setattr(worker, "process", lambda *a, **k: settings.home / "x.json")
    processed = worker.run(settings, once=True)
    assert processed == (1 if spool_used else 0)
    assert bool(polled) is api_polled


def test_overlap_never_reaches_behind_the_first_window(make_settings: Any) -> None:
    """Regression (found by running the worker container): with a 1-minute lookback and a
    10-minute overlap, the second poll went 10 minutes back and queued older failures."""
    settings = make_settings(poll_lookback_minutes=1, poll_overlap_s=600)
    queue = JobQueue(settings.queue_path)
    fake = FakeAirflow([])
    with client(fake, make_settings) as c:
        poll_failures(c, queue, settings, now=NOW)
        poll_failures(c, queue, settings, now=NOW + timedelta(seconds=5))
        poll_failures(c, queue, settings, now=NOW + timedelta(minutes=30))
    starts = [datetime.fromisoformat(b["end_date_gte"]) for b in fake.bodies]
    assert starts[0] == NOW - timedelta(minutes=1)
    assert starts[1] == NOW - timedelta(minutes=1)  # was NOW - 10 min: the bug
    assert starts[2] == NOW - timedelta(minutes=1)  # watermark - overlap is still behind the floor

    with client(fake, make_settings) as c:
        poll_failures(c, queue, settings, now=NOW + timedelta(minutes=45))
    # Once the window has moved past the floor, the normal overlap applies again.
    assert datetime.fromisoformat(fake.bodies[3]["end_date_gte"]) == NOW + timedelta(
        minutes=30
    ) - timedelta(minutes=10)
