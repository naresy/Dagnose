"""End-to-end check against a real, running Airflow (2.x or 3.x).

Triggers every broken DAG, waits for the tasks to fail, lets the listener spool the
failures, runs the worker, and checks each collected FailureContext against
dev/dags/broken/expected.json. Exits non-zero on any mismatch. Used by CI; works
locally the same way:

    cd dev && docker compose up -d                 && uv run python dev/e2e.py --airflow 3
    cd dev/airflow2 && docker compose up -d        && uv run python dev/e2e.py --airflow 2
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import httpx

from airflow_doctor import worker
from airflow_doctor.config import Settings
from airflow_doctor.models import FailureContext

ROOT = Path(__file__).resolve().parent.parent
EXPECTED: dict[str, dict[str, Any]] = {
    k: v
    for k, v in json.loads((ROOT / "dev/dags/broken/expected.json").read_text()).items()
    if not k.startswith("_")
}
TARGETS = {
    "3": {"url": "http://localhost:8080", "api": "v2", "home": ROOT / "dev/doctor-data"},
    "2": {"url": "http://localhost:8081", "api": "v1", "home": ROOT / "dev/airflow2/doctor-data"},
}
USER = PASSWORD = "airflow"


def log(msg: str) -> None:
    print(f"[e2e] {msg}", flush=True)


def wait_for(what: str, check: Any, timeout_s: float, every_s: float = 3.0) -> Any:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            last_error = exc
        time.sleep(every_s)
    raise SystemExit(f"[e2e] timed out after {timeout_s:.0f}s waiting for {what} ({last_error})")


class Airflow:
    """Minimal write-capable client for the test (Doctor's own client is read-only)."""

    def __init__(self, url: str, api: str) -> None:
        self.api = api
        self.http = httpx.Client(base_url=url, timeout=30)
        self.headers: dict[str, str] = {}

    def login(self) -> None:
        if self.api == "v2":
            resp = self.http.post("/auth/token", json={"username": USER, "password": PASSWORD})
            resp.raise_for_status()
            self.headers = {"Authorization": f"Bearer {resp.json()['access_token']}"}
        else:
            self.http.auth = httpx.BasicAuth(USER, PASSWORD)

    def call(self, method: str, path: str, **kwargs: Any) -> Any:
        resp = self.http.request(method, f"/api/{self.api}{path}", headers=self.headers, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def healthy(self) -> bool:
        path = "/api/v2/monitor/health" if self.api == "v2" else "/health"
        health = self.http.get(path).json()
        return all(health[c]["status"] == "healthy" for c in ("metadatabase", "scheduler"))

    def broken_dags_ready(self) -> bool:
        dags = self.call("GET", "/dags", params={"dag_id_pattern": "broken_", "limit": 100})
        found = {d["dag_id"] for d in dags["dags"] if not d.get("is_paused")}
        return set(EXPECTED) <= found

    def trigger(self, dag_id: str) -> str:
        body: dict[str, Any] = {"logical_date": None} if self.api == "v2" else {}
        run = self.call("POST", f"/dags/{dag_id}/dagRuns", json=body)
        return str(run["dag_run_id"])

    def run_state(self, dag_id: str, run_id: str) -> str:
        from urllib.parse import quote

        run = self.call("GET", f"/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}")
        return str(run["state"])


def check_context(ctx: FailureContext, expected: dict[str, Any], api: str, major: str) -> list[str]:
    problems: list[str] = []

    def expect(ok: bool, what: str) -> None:
        if not ok:
            problems.append(what)

    exc = ctx.exception
    user_frames = [f for f in exc.frames if f.in_user_code] if exc else []
    expect(ctx.collection_errors == [], f"collection errors: {ctx.collection_errors}")
    expect(ctx.api_version == api, f"api_version {ctx.api_version} != {api}")
    expect(
        bool(ctx.airflow_version and ctx.airflow_version.startswith(f"{major}.")),
        f"airflow_version {ctx.airflow_version}",
    )
    expect(
        exc is not None and exc.type == expected["exception"],
        f"exception {exc.type if exc else None} != {expected['exception']}",
    )
    last = user_frames[-1] if user_frames else None
    expect(
        last is not None
        and last.filename.endswith(expected["file"])
        and last.lineno == expected["line"],
        f"failing line {last.filename if last else None}:{last.lineno if last else None}"
        f" != {expected['file']}:{expected['line']}",
    )
    expect(bool(ctx.log_tail), "empty log tail")
    expect(bool(ctx.dag_source), "no DAG source")
    expect(
        bool(ctx.run_history) and ctx.run_history[0].is_current,
        "run history: current run not first",
    )
    expect(bool(ctx.recent_commits), "no git commits for the DAG file")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--airflow", choices=sorted(TARGETS), required=True)
    parser.add_argument("--timeout", type=float, default=600, help="seconds to wait for Airflow")
    args = parser.parse_args()
    target = TARGETS[args.airflow]
    api = str(target["api"])

    airflow = Airflow(str(target["url"]), api)
    wait_for("Airflow to be healthy", airflow.healthy, args.timeout)
    airflow.login()
    wait_for("broken DAGs to be parsed and unpaused", airflow.broken_dags_ready, args.timeout)
    log(f"Airflow {args.airflow} is up at {target['url']}")

    runs = {dag_id: airflow.trigger(dag_id) for dag_id in EXPECTED}
    log(f"triggered {len(runs)} broken DAGs")
    for dag_id, run_id in runs.items():
        state = wait_for(
            f"{dag_id} to fail",
            lambda d=dag_id, r=run_id: (
                (s := airflow.run_state(d, r)) in ("failed", "success") and s
            ),
            300,
        )
        if state != "failed":
            raise SystemExit(f"[e2e] {dag_id} ended as {state}, expected failed")
    log("all runs failed as expected")

    settings = Settings(
        airflow_url=str(target["url"]),
        airflow_username=USER,
        airflow_password=PASSWORD,
        home=Path(target["home"]),
        git_repo_path=ROOT,
        git_dags_subdir="dev/dags",
    )

    def collected() -> dict[str, FailureContext] | None:
        worker.run(settings, once=True)
        found: dict[str, FailureContext] = {}
        for path in settings.contexts_dir.glob("*.json"):
            ctx = FailureContext.model_validate_json(path.read_text())
            if runs.get(ctx.job.dag_id) == ctx.job.run_id:
                found[ctx.job.dag_id] = ctx
        return found if len(found) == len(runs) else None

    contexts: dict[str, FailureContext] = wait_for(
        "listener -> spool -> worker to collect every failure", collected, 120
    )

    failures = 0
    for dag_id, expected in EXPECTED.items():
        problems = check_context(contexts[dag_id], expected, api, args.airflow)
        exc = contexts[dag_id].exception
        status = "ok  " if not problems else "FAIL"
        log(f"{status} {dag_id:27} {exc.type if exc else None}")
        for problem in problems:
            log(f"       - {problem}")
        failures += bool(problems)
    if failures:
        log(f"{failures} of {len(EXPECTED)} failed")
        return 1
    log(f"all {len(EXPECTED)} failures collected correctly on Airflow {args.airflow}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
