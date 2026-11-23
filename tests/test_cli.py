from typer.testing import CliRunner

from airflow_doctor import __version__
from airflow_doctor.cli import app


def test_version() -> None:
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_settings_read_dotenv(tmp_path: object, monkeypatch: object) -> None:
    import pytest

    from airflow_doctor.config import Settings

    mp: pytest.MonkeyPatch = monkeypatch  # type: ignore[assignment]
    mp.chdir(tmp_path)  # type: ignore[arg-type]
    mp.delenv("AIRFLOW_DOCTOR_LLM_MODEL", raising=False)
    (tmp_path / ".env").write_text(  # type: ignore[operator]
        "AIRFLOW_DOCTOR_LLM_API_KEY=sk-test\nAIRFLOW_DOCTOR_LLM_MODEL=claude-opus-5-5\nOTHER=x\n"
    )
    s = Settings()
    assert s.llm_api_key is not None and s.llm_api_key.get_secret_value() == "sk-test"
    mp.setenv("AIRFLOW_DOCTOR_LLM_MODEL", "anthropic/claude-sonnet-5-5")
    assert Settings().llm_model == "anthropic/claude-sonnet-5-5"  # env beats the file
