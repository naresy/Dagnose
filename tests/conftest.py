from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

FIXTURES = Path(__file__).parent / "fixtures"


class ReplayTransport(httpx.BaseTransport):
    """Serves HTTP exchanges recorded by dev/record_fixtures.py, matched on
    method + path + query. Unknown requests fail loudly."""

    def __init__(self, exchanges: list[dict[str, Any]]) -> None:
        self.exchanges = exchanges
        self.requests: list[httpx.Request] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.raw_path.decode().split("?")[0]
        query = request.url.query.decode()
        for ex in self.exchanges:
            if (ex["method"], ex["path"], ex["query"]) == (request.method, path, query):
                return httpx.Response(
                    ex["status"],
                    content=ex["body"].encode(),
                    headers={"content-type": ex["content_type"]},
                )
        raise AssertionError(f"no recorded response for {request.method} {path}?{query}")


def load_recording(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text())
    return data


def recordings(api: str) -> list[Path]:
    return sorted((FIXTURES / "api" / api).glob("*.json"))


@pytest.fixture
def make_settings(tmp_path: Path) -> Any:
    from airflow_doctor.config import Settings

    def factory(**overrides: Any) -> Settings:
        values: dict[str, Any] = {
            "airflow_url": "http://airflow.test",
            "airflow_username": "airflow",
            "airflow_password": "airflow",
            "home": tmp_path / "doctor-home",
            # Never call a real model from tests (a dev machine may have a key set).
            "diagnose": False,
        }
        values.update(overrides)
        # Ignore a developer's .env (it may hold a real Slack webhook or API key).
        return Settings(_env_file=None, **values)

    return factory
