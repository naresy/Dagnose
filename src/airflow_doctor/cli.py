"""`airflow-doctor` command line."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Optional

import typer

from airflow_doctor import __version__

app = typer.Typer(help="Airflow Doctor: diagnose failed Airflow tasks.", no_args_is_help=True)


@app.callback()
def main(verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False) -> None:
    """Airflow Doctor CLI."""
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
) -> None:
    """Collect the failure context of one task try and print it as JSON."""
    from pydantic import ValidationError

    from airflow_doctor.collector import AirflowAPIError, AirflowClient, collect
    from airflow_doctor.config import Settings
    from airflow_doctor.models import FailureJob

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
    data = ctx.model_dump_json(indent=2)
    if out:
        out.write_text(data)
        typer.echo(f"wrote {out}", err=True)
    else:
        typer.echo(data)
    for problem in ctx.collection_errors:
        typer.echo(f"warning: {problem}", err=True)


@app.command()
def worker(
    once: Annotated[bool, typer.Option(help="Exit when the queue is empty.")] = False,
) -> None:
    """Process failure jobs from the queue (run this as a separate, long-lived process)."""
    from airflow_doctor import worker as worker_mod
    from airflow_doctor.config import Settings

    settings = Settings()
    typer.echo(f"queue: {settings.queue_path}\ncontexts: {settings.contexts_dir}", err=True)
    n = worker_mod.run(settings, once=once)
    typer.echo(f"processed {n} job(s)", err=True)


@app.command()
def queue() -> None:
    """Show job counts by status."""
    from airflow_doctor.config import Settings
    from airflow_doctor.queue import JobQueue

    settings = Settings()
    q = JobQueue(settings.queue_path)
    q.ingest_spool(settings.spool_dir)
    counts = q.counts()
    typer.echo(f"{settings.queue_path}")
    for status in ("pending", "running", "done", "error"):
        typer.echo(f"  {status:8} {counts.get(status, 0)}")


if __name__ == "__main__":
    app()
