"""Build a FailureContext for one failed task try."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, TypeVar

from airflow_doctor.collector.airflow_api import AirflowClient
from airflow_doctor.collector.git import recent_commits
from airflow_doctor.collector.logs import log_unavailable_reason, parse_log
from airflow_doctor.config import Settings
from airflow_doctor.models import FailureContext, FailureJob, RunSummary

T = TypeVar("T")


def collect(job: FailureJob, client: AirflowClient, settings: Settings) -> FailureContext:
    """Collect everything about the failure. Never raises for a missing piece: each
    problem is recorded in `collection_errors` and the rest is still collected."""
    errors: list[str] = []

    def attempt(what: str, fn: Callable[[], T], default: T) -> T:
        try:
            return fn()
        except Exception as exc:
            errors.append(f"{what}: {type(exc).__name__}: {exc}")
            return default

    # The API must answer; nothing else works without it.
    client.api  # noqa: B018 - detects v1/v2, raises AirflowAPIError if unreachable
    ctx = FailureContext(
        job=job,
        airflow_version=attempt("airflow version", client.airflow_version, None),
        api_version=client.api,
        collected_at=datetime.now(timezone.utc),
    )

    ti: dict[str, Any] = attempt("task instance", lambda: client.task_instance(job), {})
    if ti:
        ctx.task_state = ti.get("state")
        ctx.operator = ti.get("operator")
        ctx.hostname = ti.get("hostname")
        ctx.duration_s = ti.get("duration")
        ctx.task_params = ti.get("rendered_fields") or {}
    if job.try_number is None and ti.get("try_number"):
        job = job.model_copy(update={"try_number": ti["try_number"]})
        ctx.job = job

    run: dict[str, Any] = attempt("dag run", lambda: client.dag_run(job.dag_id, job.run_id), {})
    ctx.run_conf = run.get("conf") or {}

    if job.try_number is None:
        errors.append("task log: unknown try number")
    else:
        try_number = job.try_number
        raw = attempt("task log", lambda: client.task_log(job, try_number), None)
        if raw is not None:
            parsed = parse_log(raw)
            unavailable = log_unavailable_reason(raw)
            if unavailable and not parsed.exceptions:
                errors.append(f"task log: Airflow could not serve it: {unavailable}")
            ctx.exceptions = parsed.exceptions
            ctx.log_tail = parsed.tail(settings.log_tail_lines)
            ctx.log_total_lines = len(parsed.lines)

    dag: dict[str, Any] = attempt("dag details", lambda: client.dag(job.dag_id), {})
    ctx.dag_file = _relative_dag_file(dag, settings.airflow_dags_folder)
    ctx.dag_source = attempt("dag source", lambda: client.dag_source(job.dag_id), None)

    history: list[dict[str, Any]] = attempt(
        "run history",
        lambda: client.task_history(job.dag_id, job.task_id, settings.run_history_limit),
        [],
    )
    ctx.run_history = [_run_summary(t, job) for t in history]

    repo = settings.git_repo_path
    if repo and ctx.dag_file:
        repo_relpath = str(PurePosixPath(settings.git_dags_subdir or ".") / ctx.dag_file)
        ctx.recent_commits = attempt(
            "git history",
            lambda: recent_commits(
                repo,
                repo_relpath,
                settings.git_commit_limit,
                settings.git_diff_max_chars,
            ),
            [],
        )

    ctx.collection_errors = errors
    return ctx


def _relative_dag_file(dag: dict[str, Any], dags_folder: str) -> str | None:
    if dag.get("relative_fileloc"):
        return str(dag["relative_fileloc"])
    fileloc = dag.get("fileloc")
    if not fileloc:
        return None
    path, base = PurePosixPath(str(fileloc)), PurePosixPath(dags_folder)
    return str(path.relative_to(base)) if path.is_relative_to(base) else path.name


def _run_summary(ti: dict[str, Any], job: FailureJob) -> RunSummary:
    return RunSummary(
        run_id=ti.get("dag_run_id", "?"),
        state=ti.get("state"),
        try_number=ti.get("try_number"),
        start_date=ti.get("start_date"),
        end_date=ti.get("end_date"),
        duration_s=ti.get("duration"),
        is_current=ti.get("dag_run_id") == job.run_id,
    )
