from typer.testing import CliRunner

from airflow_doctor import __version__
from airflow_doctor.cli import app


def test_version() -> None:
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__
