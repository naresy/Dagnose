"""Record real Airflow REST API traffic for the broken DAGs, for offline tests.

For each failed broken_* task, this runs the collector against a live local Airflow,
saves every HTTP exchange to tests/fixtures/api/<api>/<dag_id>.json, and saves the
resulting FailureContext to tests/fixtures/contexts/<api>/<dag_id>.json.

    cd dev && docker compose up -d      # then trigger the broken DAGs once
    AIRFLOW_DOCTOR_AIRFLOW_USERNAME=airflow AIRFLOW_DOCTOR_AIRFLOW_PASSWORD=airflow \
        uv run python dev/record_fixtures.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx

from airflow_doctor.collector import AirflowClient, collect
from airflow_doctor.config import Settings
from airflow_doctor.models import FailureJob

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures"


class RecordingTransport(httpx.BaseTransport):
    def __init__(self) -> None:
        self.inner = httpx.HTTPTransport()
        self.exchanges: list[dict[str, Any]] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        response = self.inner.handle_request(request)
        response.read()
        body = response.text
        if request.url.path == "/auth/token":
            body = json.dumps({"access_token": "test-token", "token_type": "bearer"})
        self.exchanges.append(
            {
                "method": request.method,
                "path": request.url.raw_path.decode().split("?")[0],
                "query": request.url.query.decode(),
                "status": response.status_code,
                "content_type": response.headers.get("content-type", ""),
                "body": body,
            }
        )
        return response


def failed_broken_tasks(client: AirflowClient) -> list[FailureJob]:
    """Latest failed try of each task in each broken_* DAG."""
    seen: dict[tuple[str, str], FailureJob] = {}
    dags = client._get("/dags", params={"dag_id_pattern": "broken_", "limit": 100})["dags"]
    for dag in dags:
        body = {"dag_ids": [dag["dag_id"]], "state": ["failed"], "page_limit": 100}
        resp = client._request("POST", "/dags/~/dagRuns/~/taskInstances/list", json=body)
        for ti in sorted(resp.json()["task_instances"], key=lambda t: t.get("start_date") or ""):
            seen[(ti["dag_id"], ti["task_id"])] = FailureJob(
                dag_id=ti["dag_id"],
                run_id=ti["dag_run_id"],
                task_id=ti["task_id"],
                try_number=ti["try_number"],
                map_index=ti.get("map_index", -1),
            )
    return list(seen.values())


def main() -> None:
    settings = Settings(git_repo_path=ROOT, git_dags_subdir="dev/dags")
    with AirflowClient(settings) as lister:
        jobs = failed_broken_tasks(lister)
        api = lister.api
    if not jobs:
        raise SystemExit("No failed broken_* tasks found. Trigger the broken DAGs first.")
    for job in jobs:
        transport = RecordingTransport()
        with AirflowClient(settings, transport=transport) as client:
            ctx = collect(job, client, settings)
        api_dir = FIXTURES / "api" / api
        ctx_dir = FIXTURES / "contexts" / api
        api_dir.mkdir(parents=True, exist_ok=True)
        ctx_dir.mkdir(parents=True, exist_ok=True)
        (api_dir / f"{job.dag_id}.json").write_text(
            json.dumps(
                {"job": job.model_dump(mode="json"), "exchanges": transport.exchanges}, indent=1
            )
        )
        (ctx_dir / f"{job.dag_id}.json").write_text(ctx.model_dump_json(indent=2))
        exc = ctx.exception
        print(f"{job.dag_id}: {exc.type if exc else None}, {len(transport.exchanges)} requests")


if __name__ == "__main__":
    main()
