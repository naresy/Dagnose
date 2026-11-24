"""The notification card: what one failure looks like to a person, independent of channel.

Built only from the RedactedContext and DiagnosisRun, and every string goes through
the job's RedactionSession once more before it can leave (defense in depth: Slack and
email are outside the customer's network). Raw logs are never included: a card has a
few short quotes and a link back to Airflow.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import quote, urlencode

from dagnose.models import DiagnosisRun, RedactedContext
from dagnose.redactor import RedactionSession, Redactor

MAX_QUOTE = 200
MAX_EVIDENCE = 4
CATEGORY_LABEL = {
    "code_bug": "Code bug",
    "upstream_data": "Upstream data",
    "dependency": "Dependency",
    "config": "Configuration",
    "permissions": "Permissions",
    "timeout": "Timeout",
    "infra": "Infrastructure",
    "unknown": "Unknown cause",
}


@dataclass
class EvidenceRow:
    verified: bool
    where: str  # e.g. "log:48", "dag_source:27", "exception"
    quote: str


@dataclass
class Card:
    dag_id: str
    task_id: str
    title: str  # "demo_orders_tax.compute_tax failed"
    headline: str  # "Code bug · confidence 0.92" or "Not diagnosed"
    summary: str
    root_cause: str | None = None
    evidence: list[EvidenceRow] = field(default_factory=list)
    fix: str | None = None
    has_patch: bool = False
    facts: list[tuple[str, str]] = field(default_factory=list)
    airflow_url: str | None = None
    note: str | None = None  # e.g. why there is no diagnosis, or that confidence was capped
    diagnosed: bool = False


def _short(text: str, limit: int) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def airflow_link(ctx: RedactedContext, ui_url: str) -> str:
    """Deep link to the failed task try in the Airflow UI (2.x grid / 3.x run view)."""
    job = ctx.job
    base = ui_url.rstrip("/")
    dag, run, task = (quote(v, safe="") for v in (job.dag_id, job.run_id, job.task_id))
    if ctx.api_version == "v1":
        params: dict[str, str | int] = {
            "dag_run_id": job.run_id,
            "task_id": job.task_id,
            "tab": "logs",
        }
        if job.map_index >= 0:
            params["map_index"] = job.map_index
        return f"{base}/dags/{dag}/grid?{urlencode(params)}"
    path = f"{base}/dags/{dag}/runs/{run}/tasks/{task}"
    if job.map_index >= 0:
        path += f"/mapped/{job.map_index}"
    return path + (f"?try_number={job.try_number}" if job.try_number else "")


def build_card(
    ctx: RedactedContext,
    run: DiagnosisRun | None,
    *,
    ui_url: str | None,
    reason: str | None = None,
    session: RedactionSession | None = None,
    occurrence: int = 1,
    first_seen: float | None = None,
) -> Card:
    session = session or Redactor().session()

    def safe(text: str | None, limit: int) -> str | None:
        if text is None:
            return None
        return _short(session.redact_text(text, "notification"), limit)

    job = ctx.job
    exc = ctx.exception
    log_problem = next(
        (e.removeprefix("task log: ") for e in ctx.collection_errors if e.startswith("task log:")),
        None,
    )
    if exc:
        summary = safe(f"{exc.type}: {exc.message}", 500)
    elif log_problem:
        summary = safe(f"The task failed, but its log could not be read: {log_problem}", 500)
    elif last_error := next(
        (ln.message for ln in reversed(ctx.log_tail) if ln.level in ("error", "critical")), None
    ):
        # No exception, e.g. a task killed by a signal (out of memory): Airflow's own last
        # error line says the most ("Process terminated by signal. Likely out of memory").
        summary = safe(f"The task failed without an exception. Last error: {last_error}", 500)
    else:
        summary = "The task failed; no exception was found in the log."
    card = Card(
        dag_id=job.dag_id,
        task_id=job.task_id,
        title=_short(f"{job.dag_id}.{job.task_id} failed", 140),
        headline="Not diagnosed",
        summary=summary or "",
        airflow_url=airflow_link(ctx, ui_url) if ui_url else None,
    )
    card.facts = [
        ("Run", job.run_id),
        ("Try", str(job.try_number)),
        ("Airflow", ctx.airflow_version or "?"),
    ]
    if job.map_index >= 0:
        card.facts.insert(2, ("Map index", str(job.map_index)))
    if ctx.duration_s is not None:
        card.facts.append(("Duration", f"{ctx.duration_s:.1f}s"))

    if run is None:
        card.note = safe(f"No diagnosis: {reason}" if reason else None, 300)
        if exc:
            frame = next((f for f in reversed(exc.frames) if f.in_user_code), None)
            if frame:
                where = f"{frame.filename.rsplit('/', 1)[-1]}:{frame.lineno}"
                card.evidence.append(EvidenceRow(True, where, f"in {frame.name}"))
        return _with_repeats(card, None, occurrence, first_seen)

    d = run.diagnosis
    card.diagnosed = True
    card.headline = f"{CATEGORY_LABEL.get(d.category, d.category)} · confidence {d.confidence:.2f}"
    card.summary = safe(d.summary, 500) or ""
    card.root_cause = safe(d.root_cause, 1200)
    card.fix = safe(d.suggested_fix, 1200)
    card.has_patch = bool(d.patch)
    for ev in d.evidence[:MAX_EVIDENCE]:
        where = f"{ev.source}:{ev.line}" if ev.line is not None else ev.source
        card.evidence.append(EvidenceRow(ev.verified, where, safe(ev.quote, MAX_QUOTE) or ""))
    if run.confidence_capped:
        card.note = (
            f"Confidence lowered from {run.model_confidence:.2f}: only "
            f"{run.evidence_verified} of {run.evidence_total} evidence quotes "
            "were found in the logs."
        )
    return _with_repeats(card, run, occurrence, first_seen)


def _with_repeats(
    card: Card, run: DiagnosisRun | None, occurrence: int, first_seen: float | None
) -> Card:
    """A repeat of a known failure says so, instead of looking like a new incident."""
    if occurrence <= 1:
        return card
    card.title = _short(f"{card.dag_id}.{card.task_id} still failing ({occurrence}×)", 140)
    since = (
        datetime.fromtimestamp(first_seen, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        if first_seen
        else "earlier"
    )
    parts = [f"Same failure {occurrence}× since {since}."]
    if run is not None and run.reused_from:
        parts.append("Diagnosis reused from the first time: no new model call.")
    if card.note:
        parts.append(card.note)
    card.note = " ".join(parts)
    return card
