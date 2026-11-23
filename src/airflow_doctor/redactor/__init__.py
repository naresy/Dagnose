"""Scrub secrets, connection strings, emails and tokens before anything reaches an LLM."""

from airflow_doctor.redactor.redact import Redactor, redact

__all__ = ["Redactor", "redact"]
