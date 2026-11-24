"""`dagnose` command line."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Optional

import typer

from dagnose import __version__

app = typer.Typer(help="Dagnose: diagnose failed Airflow tasks.", no_args_is_help=True)


@app.callback()
def main(verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False) -> None:
    """Dagnose CLI."""
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@app.command()
def version() -> None:
    """Print the installed version."""
    typer.echo(__version__)


@app.command("collect")
def collect_cmd(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: Annotated[
        Optional[int], typer.Option("--try", help="Default: the latest try.")  # noqa: UP045
    ] = None,
    map_index: Annotated[int, typer.Option(help="For mapped tasks.")] = -1,
    out: Annotated[
        Optional[Path], typer.Option(help="Write JSON here instead of stdout.")  # noqa: UP045
    ] = None,
    raw: Annotated[
        bool, typer.Option("--raw", help="Skip redaction. Output may contain secrets.")
    ] = False,
) -> None:
    """Collect the failure context of one task try and print it as JSON (redacted)."""
    from pydantic import ValidationError

    from dagnose.collector import AirflowAPIError, AirflowClient, collect
    from dagnose.config import Settings
    from dagnose.models import FailureJob

    settings = Settings()
    try:
        job = FailureJob(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            try_number=try_number,
            map_index=map_index,
        )
    except ValidationError as exc:
        for err in exc.errors():
            typer.echo(f"error: {'.'.join(map(str, err['loc']))}: {err['msg']}", err=True)
        raise typer.Exit(2) from exc
    try:
        with AirflowClient(settings) as client:
            ctx = collect(job, client, settings)
    except AirflowAPIError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(1) from exc
    if not raw:
        from dagnose.redactor import Redactor

        redacted = Redactor.from_settings(settings).redact(ctx)
        _echo_summary(redacted.summary())
        ctx = redacted
    data = ctx.model_dump_json(indent=2)
    if out:
        out.write_text(data)
        typer.echo(f"wrote {out}", err=True)
    else:
        typer.echo(data)
    for problem in ctx.collection_errors:
        typer.echo(f"warning: {problem}", err=True)


def _echo_summary(summary: dict[str, int]) -> None:
    if summary:
        typer.echo("redacted: " + ", ".join(f"{k} x{n}" for k, n in summary.items()), err=True)
    else:
        typer.echo("redacted: nothing found", err=True)


@app.command()
def redact(
    path: Annotated[Path, typer.Argument(help="A FailureContext JSON file.", exists=True)],
) -> None:
    """Redact a saved FailureContext JSON and print the result (summary on stderr)."""
    from dagnose.config import Settings
    from dagnose.models import FailureContext
    from dagnose.redactor import Redactor

    ctx = FailureContext.model_validate_json(path.read_text())
    redacted = Redactor.from_settings(Settings()).redact(ctx)
    typer.echo(redacted.model_dump_json(indent=2))
    _echo_summary(redacted.summary())


@app.command("diagnose")
def diagnose_cmd(
    path: Annotated[Path, typer.Argument(help="A FailureContext JSON file.", exists=True)],
    tools: Annotated[
        bool,
        typer.Option(help="Let the model read more from Airflow (needs DAGNOSE_AIRFLOW_*)."),
    ] = True,
    out: Annotated[
        Optional[Path], typer.Option(help="Also write the DiagnosisRun JSON here.")  # noqa: UP045
    ] = None,
) -> None:
    """Diagnose a saved failure context with the configured LLM (calls the model API)."""
    from dagnose.agent import (
        DiagnosisError,
        ProviderError,
        ToolBox,
        diagnose,
        make_provider,
    )
    from dagnose.collector import AirflowClient
    from dagnose.config import Settings
    from dagnose.models import FailureContext
    from dagnose.redactor import Redactor

    settings = Settings()
    ctx = FailureContext.model_validate_json(path.read_text())
    session = Redactor.from_settings(settings).session()
    redacted = session.redact(ctx)  # idempotent: a saved context is already redacted
    client = AirflowClient(settings) if tools else None
    try:
        run = diagnose(
            redacted,
            make_provider(settings),
            ToolBox(redacted, client, session),
            max_turns=settings.llm_max_turns,
        )
    except (ProviderError, DiagnosisError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(1) from exc
    finally:
        if client is not None:
            client.close()
    if out:
        out.write_text(run.model_dump_json(indent=2))
    d = run.diagnosis
    typer.echo(f"{d.category} (confidence {d.confidence:.2f}): {d.summary}")
    typer.echo(f"root cause: {d.root_cause}")
    for ev in d.evidence:
        mark = "ok" if ev.verified else "??"
        where = f"{ev.source}:{ev.line}" if ev.line is not None else ev.source
        typer.echo(f"  [{mark}] {where}: {ev.quote}")
    if d.suggested_fix:
        typer.echo(f"fix: {d.suggested_fix}")
    if d.patch:
        typer.echo(d.patch)
    u = run.usage
    typer.echo(
        f"{run.served_model or run.model}, {run.turns} turn(s), "
        f"{len(run.tool_calls)} tool call(s), "
        f"{u.input_tokens}+{u.cache_read_input_tokens} cached in / {u.output_tokens} out tokens, "
        f"evidence {run.evidence_verified}/{run.evidence_total} verified",
        err=True,
    )


@app.command("notify")
def notify_cmd(
    path: Annotated[Path, typer.Argument(help="A saved context JSON file.", exists=True)],
    diagnosis: Annotated[
        Optional[Path],  # noqa: UP045
        typer.Option(help="DiagnosisRun JSON. Default: same file name under diagnoses/."),
    ] = None,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Print the Slack and email messages, send nothing.")
    ] = False,
) -> None:
    """Send (or preview) the notification card for one saved failure."""
    import json

    from dagnose.config import Settings
    from dagnose.models import DiagnosisRun, FailureContext
    from dagnose.notifier import build_card, notify
    from dagnose.notifier.mail import email_subject, email_text
    from dagnose.notifier.slack import slack_payload
    from dagnose.redactor import Redactor

    settings = Settings()
    session = Redactor.from_settings(settings).session()
    ctx = session.redact(FailureContext.model_validate_json(path.read_text()))
    diag_path = diagnosis or path.parent.parent / "diagnoses" / path.name
    run = DiagnosisRun.model_validate_json(diag_path.read_text()) if diag_path.exists() else None
    reason = None if run else f"no diagnosis file ({diag_path})"
    if dry_run:
        ui = settings.airflow_ui_url or settings.airflow_url
        card = build_card(ctx, run, ui_url=ui, reason=reason, session=session)
        typer.echo("--- Slack payload")
        typer.echo(json.dumps(slack_payload(card), indent=2, ensure_ascii=False))
        typer.echo(f"--- Email: {email_subject(card)}")
        typer.echo(email_text(card))
        return
    report = notify(ctx, run, settings, session=session, reason=reason, name=path.name)
    if not report.sent and not report.failed:
        typer.echo("no channel configured (DAGNOSE_SLACK_WEBHOOK_URL / SMTP_*)", err=True)
        raise typer.Exit(2)
    for channel in report.sent:
        typer.echo(f"sent: {channel}")
    for channel, error in report.failed.items():
        typer.echo(f"failed: {channel}: {error} (saved to outbox)", err=True)
    if report.failed:
        raise typer.Exit(1)


@app.command()
def outbox(
    resend: Annotated[bool, typer.Option("--resend", help="Try to send them again.")] = False,
) -> None:
    """Notifications that could not be delivered."""
    import json

    from dagnose.config import Settings
    from dagnose.notifier import resend_outbox

    settings = Settings()
    if resend:
        report = resend_outbox(settings)
        typer.echo(f"resent {len(report.sent)}, still failing {len(report.failed)}")
        for name, error in report.failed.items():
            typer.echo(f"  {name}: {error}", err=True)
        raise typer.Exit(1 if report.failed else 0)
    items = sorted(settings.outbox_dir.glob("*.json")) if settings.outbox_dir.is_dir() else []
    typer.echo(f"{settings.outbox_dir}: {len(items)} waiting")
    for item in items:
        typer.echo(f"  {item.name}: {json.loads(item.read_text()).get('error', '')}")


@app.command()
def check(
    send_test: Annotated[
        bool, typer.Option("--send-test", help="Also post a test message to Slack / email.")
    ] = False,
) -> None:
    """Check the setup: Airflow API and login, how failures are found, storage, git,
    LLM key (free check), Slack, email and redaction."""
    from pydantic import ValidationError

    from dagnose.check import run_checks
    from dagnose.config import Settings

    try:
        settings = Settings()
    except ValidationError as exc:
        for err in exc.errors():
            name = "DAGNOSE_" + str(err["loc"][0]).upper()
            typer.echo(f"✗ Settings: {name}: {err['msg']}", err=True)
        raise typer.Exit(1) from exc

    icons = {"ok": "✓", "warn": "!", "fail": "✗", "skip": "–"}
    failed = warned = 0
    typer.echo("Dagnose check")
    for r in run_checks(settings, send_test=send_test):
        typer.echo(f"  {icons[r.status]} {r.name:17} {r.detail}")
        if r.fix and r.status in ("fail", "warn"):
            typer.echo(f"      → {r.fix}")
        failed += r.status == "fail"
        warned += r.status == "warn"
    if failed:
        typer.echo(f"\n{failed} problem(s) to fix.")
        raise typer.Exit(1)
    typer.echo(
        "\nAll good" + (f" ({warned} warning(s))" if warned else "") + ". Start the worker: "
        "dagnose worker"
    )


@app.command()
def worker(
    once: Annotated[bool, typer.Option(help="Exit when the queue is empty.")] = False,
) -> None:
    """Process failure jobs from the queue (run this as a separate, long-lived process)."""
    import os

    from dagnose import worker as worker_mod
    from dagnose.config import Settings

    settings = Settings()
    port = settings.health_port or (int(os.environ["PORT"]) if os.environ.get("PORT") else None)
    if port:
        from dagnose.health import start_health_server

        start_health_server(port, settings.queue_path)
        typer.echo(f"health: http://0.0.0.0:{port}/healthz", err=True)
    typer.echo(
        f"watching: {settings.watch} (poll every {settings.poll_interval_s:.0f}s)\n"
        f"queue: {settings.queue_path}\ncontexts: {settings.contexts_dir}",
        err=True,
    )
    n = worker_mod.run(settings, once=once)
    typer.echo(f"processed {n} job(s)", err=True)


@app.command()
def queue() -> None:
    """Show job counts by status."""
    from dagnose.config import Settings
    from dagnose.queue import JobQueue

    settings = Settings()
    q = JobQueue(settings.queue_path)
    q.ingest_spool(settings.spool_dir)
    counts = q.counts()
    typer.echo(f"{settings.queue_path}")
    for status in ("pending", "running", "done", "error"):
        typer.echo(f"  {status:8} {counts.get(status, 0)}")


if __name__ == "__main__":
    app()
