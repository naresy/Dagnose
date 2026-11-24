from typer.testing import CliRunner

from dagnose import __version__
from dagnose.cli import app


def test_version() -> None:
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_settings_read_dotenv(tmp_path: object, monkeypatch: object) -> None:
    import pytest

    from dagnose.config import Settings

    mp: pytest.MonkeyPatch = monkeypatch  # type: ignore[assignment]
    mp.chdir(tmp_path)  # type: ignore[arg-type]
    mp.delenv("DAGNOSE_LLM_MODEL", raising=False)
    (tmp_path / ".env").write_text(  # type: ignore[operator]
        "DAGNOSE_LLM_API_KEY=sk-test\nDAGNOSE_LLM_MODEL=claude-opus-5-5\nOTHER=x\n"
    )
    s = Settings()
    assert s.llm_api_key is not None and s.llm_api_key.get_secret_value() == "sk-test"
    mp.setenv("DAGNOSE_LLM_MODEL", "anthropic/claude-sonnet-5-5")
    assert Settings().llm_model == "anthropic/claude-sonnet-5-5"  # env beats the file


def test_verbose_does_not_log_request_urls(caplog: object) -> None:
    """Webhook URLs are secrets; httpx would log them at INFO with every request."""
    import logging

    import httpx
    import pytest

    cap: pytest.LogCaptureFixture = caplog  # type: ignore[assignment]
    assert CliRunner().invoke(app, ["-v", "version"]).exit_code == 0
    url = "https://hooks.slack.com/services/T0/B0/secret-part"
    transport = httpx.MockTransport(lambda request: httpx.Response(200))
    with cap.at_level(logging.INFO), httpx.Client(transport=transport) as http:
        http.post(url, json={})
    assert "secret-part" not in cap.text


def test_queue_lists_recent_jobs(tmp_path: object, monkeypatch: object) -> None:
    import pytest

    from dagnose.config import Settings
    from dagnose.queue import JobQueue

    mp: pytest.MonkeyPatch = monkeypatch  # type: ignore[assignment]
    mp.chdir(tmp_path)  # type: ignore[arg-type]
    mp.setenv("DAGNOSE_HOME", str(tmp_path))
    q = JobQueue(Settings().queue_path)
    q.enqueue("scenario_mapped", "manual__1", "load_region", 1, map_index=1)
    q.enqueue("demo_orders_tax", "manual__2", "compute_tax", 3)
    out = CliRunner().invoke(app, ["queue"]).stdout
    assert "pending  2" in out
    assert "scenario_mapped.load_region  manual__1" in out
    assert "demo_orders_tax.compute_tax" in out
    only = CliRunner().invoke(app, ["queue", "--dag", "demo_orders_tax"]).stdout
    assert "scenario_mapped" not in only and "demo_orders_tax" in only
    assert "task / run" not in CliRunner().invoke(app, ["queue", "-n", "0"]).stdout
