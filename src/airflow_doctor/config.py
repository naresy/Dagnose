"""Settings, read from AIRFLOW_DOCTOR_* environment variables."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from airflow_doctor.queue import default_home


class Settings(BaseSettings):
    # Also reads AIRFLOW_DOCTOR_* from a .env file in the current directory (git-ignored).
    # Real environment variables win over the file.
    model_config = SettingsConfigDict(
        env_prefix="AIRFLOW_DOCTOR_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # Airflow REST API
    airflow_url: str = "http://localhost:8080"
    airflow_username: str | None = None
    airflow_password: SecretStr | None = None
    airflow_token: SecretStr | None = None  # use instead of username/password
    airflow_api: Literal["auto", "v1", "v2"] = "auto"  # auto: v2 (Airflow 3), else v1 (2.x)
    airflow_verify_ssl: bool = True
    airflow_dags_folder: str = "/opt/airflow/dags"  # to make 2.x file paths relative
    request_timeout_s: float = 30.0

    # Collection limits
    log_tail_lines: int = 500
    run_history_limit: int = 10

    # Git history of the DAG repo (optional)
    git_repo_path: Path | None = None
    git_dags_subdir: str = ""  # where the DAGs folder lives inside the repo, e.g. "dags"
    git_commit_limit: int = 5
    git_diff_max_chars: int = 4000

    # Redaction (always on; these only tune it). Lists are JSON in env vars, e.g.
    # AIRFLOW_DOCTOR_REDACT_EXTRA_PATTERNS='["INTERNAL-[0-9]{6}"]'
    redact_extra_patterns: list[str] = Field(default_factory=list)  # also redact these
    redact_allow_patterns: list[str] = Field(default_factory=list)  # never redact these
    redact_people: bool = True  # git author names

    # Diagnosis (LLM). Without credentials the worker still collects and redacts.
    diagnose: bool = True
    llm_provider: Literal["anthropic"] = "anthropic"
    llm_model: str = "claude-opus-5-5"  # or "anthropic/claude-opus-5-5"
    llm_api_key: SecretStr | None = None  # else ANTHROPIC_API_KEY / `ant auth login`
    llm_effort: Literal["low", "medium", "high", "xhigh", "max"] = "high"
    llm_max_turns: int = 6
    llm_fallbacks: bool = True  # server-side fallback model if Claude declines

    # Notifications. Cards carry only redacted text and short quotes, never raw logs.
    airflow_ui_url: str | None = None  # links in cards; default: airflow_url
    slack_webhook_url: SecretStr | None = None
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_security: Literal["starttls", "ssl", "none"] = "starttls"
    smtp_username: str | None = None
    smtp_password: SecretStr | None = None
    email_from: str | None = None
    email_to: list[str] = Field(default_factory=list)  # JSON list in the env var

    home: Path = Field(default_factory=default_home)

    @property
    def spool_dir(self) -> Path:
        return self.home / "spool"

    @property
    def queue_path(self) -> Path:
        return self.home / "queue.db"

    @property
    def contexts_dir(self) -> Path:
        return self.home / "contexts"

    @property
    def diagnoses_dir(self) -> Path:
        return self.home / "diagnoses"

    @property
    def outbox_dir(self) -> Path:
        """Notifications that could not be delivered, kept for `airflow-doctor outbox`."""
        return self.home / "outbox"

    @property
    def email_enabled(self) -> bool:
        return bool(self.smtp_host and self.email_from and self.email_to)
