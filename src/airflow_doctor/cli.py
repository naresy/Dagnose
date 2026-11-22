"""`airflow-doctor` command line."""

import typer

from airflow_doctor import __version__

app = typer.Typer(help="Airflow Doctor: diagnose failed Airflow tasks.", no_args_is_help=True)


@app.callback()
def main() -> None:
    """Airflow Doctor CLI."""


@app.command()
def version() -> None:
    """Print the installed version."""
    typer.echo(__version__)


if __name__ == "__main__":
    app()
