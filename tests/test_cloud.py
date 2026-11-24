"""Managed Airflow logins (AWS MWAA, Google Cloud Composer, Azure Entra ID), offline.

The MWAA tests run the real collector over a fake MWAA that answers InvokeRestApi
from the recorded Airflow 2 traffic, and check request shapes against boto3's own
service model (botocore Stubber), so a wrong parameter name fails here, not in AWS.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import unquote

import boto3
import httpx
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from dagnose.collector import AirflowAPIError, AirflowClient, collect
from dagnose.collector.cloud import (
    AzureAuth,
    CloudSetupError,
    GoogleAuth,
    MwaaTransport,
    connect,
)
from dagnose.models import FailureJob
from tests.conftest import FIXTURES, load_recording

# ---- AWS MWAA -------------------------------------------------------------------------


class FakeMwaa:
    """Answers InvokeRestApi the way MWAA does: JSON bodies, paths without /api/v1."""

    def __init__(self, exchanges: list[dict[str, Any]], airflow_version: str = "2.10.3") -> None:
        self.exchanges = exchanges
        self.airflow_version = airflow_version
        self.calls: list[dict[str, Any]] = []

    def get_environment(self, Name: str) -> dict[str, Any]:  # noqa: N803 - boto3 names
        return {"Environment": {"Name": Name, "AirflowVersion": self.airflow_version}}

    def invoke_rest_api(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        for ex in self.exchanges:
            # An Airflow 2 environment only has REST API v1.
            if ex["method"] != kwargs["Method"] or not ex["path"].startswith("/api/v1/"):
                continue
            path = "/" + ex["path"].split("/", 3)[3]
            query = (
                dict(p.split("=", 1) for p in ex["query"].split("&") if p) if ex["query"] else {}
            )
            query = {k: unquote(v) for k, v in query.items()}
            if path == kwargs["Path"] and query == kwargs.get("QueryParameters", {}):
                body = ex["body"]
                if "json" not in ex["content_type"]:  # MWAA wraps text answers in JSON
                    return {"RestApiStatusCode": ex["status"], "RestApiResponse": {"content": body}}
                return {"RestApiStatusCode": ex["status"], "RestApiResponse": json.loads(body)}
        raise AssertionError(f"unexpected InvokeRestApi call: {kwargs}")


def test_mwaa_collects_a_real_airflow2_failure(make_settings: Any) -> None:
    """The full collector over MWAA gives the same context as over the plain REST API."""
    rec = load_recording(FIXTURES / "api" / "v1" / "demo_orders_tax.json")
    fake = FakeMwaa(rec["exchanges"])
    settings = make_settings(airflow_auth="mwaa", mwaa_environment="prod-airflow")
    with AirflowClient(settings, transport=MwaaTransport("prod-airflow", client=fake)) as client:
        ctx = collect(FailureJob(**rec["job"]), client, settings)

    assert ctx.collection_errors == []
    assert ctx.api_version == "v1" and ctx.airflow_version and ctx.airflow_version.startswith("2.")
    assert ctx.exception is not None and ctx.exception.type == "KeyError"
    assert ctx.dag_source and "TAX_RATES" in ctx.dag_source  # text endpoint, JSON-wrapped
    assert ctx.log_total_lines > 10  # same
    saved = json.loads((FIXTURES / "contexts" / "v1" / "demo_orders_tax.json").read_text())
    got = json.loads(ctx.model_dump_json())
    for volatile in ("collected_at", "recent_commits"):
        saved.pop(volatile)
        got.pop(volatile)
    assert got == saved
    # Every call names the environment and uses a path without the /api/v1 prefix.
    assert all(
        c["Name"] == "prod-airflow" and not c["Path"].startswith("/api/") for c in fake.calls
    )


def test_mwaa_request_shapes_match_boto3() -> None:
    """Stubber validates our parameters against the real MWAA service model."""
    client = boto3.client(
        "mwaa", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="y"
    )
    stub = Stubber(client)
    stub.add_response(
        "get_environment",
        {"Environment": {"Name": "env", "AirflowVersion": "2.10.3"}},
        {"Name": "env"},
    )
    stub.add_response(
        "invoke_rest_api",
        {"RestApiStatusCode": 200, "RestApiResponse": {"task_instances": []}},
        {
            "Name": "env",
            "Path": "/dags/~/dagRuns/~/taskInstances/list",
            "Method": "POST",
            "Body": {
                "state": ["failed"],
                "end_date_gte": "2026-11-23T10:00:00+00:00",
                "page_limit": 100,
                "page_offset": 0,
            },
        },
    )
    stub.add_response(
        "invoke_rest_api",
        {"RestApiStatusCode": 200, "RestApiResponse": {"dags": []}},
        {"Name": "env", "Path": "/dags", "Method": "GET", "QueryParameters": {"limit": "1"}},
    )
    with stub:
        transport = MwaaTransport("env", client=client)
        http = httpx.Client(base_url="http://mwaa.invalid", transport=transport)
        r1 = http.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            json={
                "state": ["failed"],
                "end_date_gte": "2026-11-23T10:00:00+00:00",
                "page_limit": 100,
                "page_offset": 0,
            },
        )
        r2 = http.get("/api/v1/dags", params={"limit": 1})
        assert (r1.status_code, r1.json()) == (200, {"task_instances": []})
        assert r2.json() == {"dags": []}
        assert http.get("/api/v2/version").status_code == 404  # Airflow 2 env: v2 not served
    stub.assert_no_pending_responses()


@pytest.mark.parametrize(
    ("version", "served", "not_served"),
    [
        ("2.10.3", "/api/v1/version", "/api/v2/version"),
        ("3.0.6", "/api/v2/version", "/api/v1/version"),
    ],
)
def test_mwaa_serves_only_the_environments_api(version: str, served: str, not_served: str) -> None:
    class Env:
        def get_environment(self, Name: str) -> dict[str, Any]:  # noqa: N803
            return {"Environment": {"AirflowVersion": version}}

        def invoke_rest_api(self, **kw: Any) -> dict[str, Any]:
            return {"RestApiStatusCode": 200, "RestApiResponse": {"version": version}}

    http = httpx.Client(base_url="http://mwaa.invalid", transport=MwaaTransport("e", client=Env()))
    assert http.get(served).json() == {"version": version}
    assert http.get(not_served).status_code == 404


@pytest.mark.parametrize(
    ("error", "status"),
    [
        ({"Error": {"Code": "AccessDeniedException", "Message": "no airflow:InvokeRestApi"}}, 403),
        ({"Error": {"Code": "ResourceNotFoundException", "Message": "no such env"}}, 404),
        (
            {
                "Error": {"Code": "RestApiClientException", "Message": "x"},
                "RestApiStatusCode": 404,
                "RestApiResponse": {"title": "DAG not found"},
            },
            404,
        ),
        ({"Error": {"Code": "InternalServerException", "Message": "boom"}}, 502),
    ],
)
def test_mwaa_errors_become_http_statuses(
    error: dict[str, Any], status: int, make_settings: Any
) -> None:
    class Failing:
        def get_environment(self, Name: str) -> dict[str, Any]:  # noqa: N803
            return {"Environment": {"AirflowVersion": "2.10.3"}}

        def invoke_rest_api(self, **kw: Any) -> dict[str, Any]:
            raise ClientError(error, "InvokeRestApi")  # type: ignore[arg-type]

    settings = make_settings(airflow_auth="mwaa", mwaa_environment="e", airflow_api="v1")
    with (
        AirflowClient(settings, transport=MwaaTransport("e", client=Failing())) as client,
        pytest.raises(AirflowAPIError, match=f"HTTP {status}"),
    ):
        client.dag("x")


# ---- Google Cloud Composer ------------------------------------------------------------


class FakeGoogleCredentials:
    def __init__(self) -> None:
        self.refreshes = 0
        self.token: str | None = None
        self.expiry: datetime | None = None

    def refresh(self, request: Any) -> None:
        self.refreshes += 1
        self.token = f"g-token-{self.refreshes}"
        self.expiry = datetime.now(timezone.utc) + timedelta(hours=1)


def test_google_bearer_and_refresh_on_401() -> None:
    creds = FakeGoogleCredentials()
    seen: list[str] = []

    def composer(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["authorization"])
        return httpx.Response(401 if len(seen) == 1 else 200, json={"version": "2.10.5"})

    http = httpx.Client(transport=httpx.MockTransport(composer), auth=GoogleAuth(creds))
    assert http.get("https://composer.example/api/v1/version").status_code == 200
    assert seen == ["Bearer g-token-1", "Bearer g-token-2"]  # refreshed after the 401
    http.get("https://composer.example/api/v1/version")
    assert creds.refreshes == 2  # still valid: no extra refresh


def test_google_login_skips_password_flow(
    make_settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    import dagnose.collector.cloud as cloud

    monkeypatch.setattr(cloud, "GoogleAuth", lambda: GoogleAuth(FakeGoogleCredentials()))
    paths: list[str] = []

    def composer(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        assert request.headers["authorization"].startswith("Bearer g-token")
        if request.url.path == "/api/v2/version":
            return httpx.Response(200, json={"version": "3.0.6"})
        return httpx.Response(200, json={"dags": []})

    settings = make_settings(
        airflow_auth="google", airflow_url="https://composer.example", airflow_username=None
    )
    base, _transport, auth = connect(settings)
    assert base == "https://composer.example" and isinstance(auth, GoogleAuth)
    client = AirflowClient(settings, transport=httpx.MockTransport(composer))
    client._cloud_auth = auth  # what connect() wires up when no test transport is given
    assert client.api == "v2"  # regression: version detection must carry the token too
    assert client.airflow_version() == "3.0.6"
    client._get("/dags")
    assert "/auth/token" not in paths  # no username/password login


# ---- Azure (Entra ID) -----------------------------------------------------------------


class FakeAzureCredential:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_token(self, scope: str) -> Any:
        from azure.core.credentials import AccessToken

        self.calls.append(scope)
        return AccessToken(
            f"az-token-{len(self.calls)}", int(datetime.now(timezone.utc).timestamp()) + 3600
        )


def test_azure_token_for_the_configured_scope() -> None:
    cred = FakeAzureCredential()
    seen: list[str] = []
    http = httpx.Client(
        transport=httpx.MockTransport(
            lambda r: seen.append(r.headers["authorization"]) or httpx.Response(200, json={})
        ),
        auth=AzureAuth("api://airflow-app/.default", cred),
    )
    http.get("https://fabric.example/api/v2/version")
    http.get("https://fabric.example/api/v2/version")
    assert cred.calls == ["api://airflow-app/.default"]  # cached until close to expiry
    assert seen == ["Bearer az-token-1", "Bearer az-token-1"]


# ---- setup errors -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"airflow_auth": "mwaa"}, "MWAA_ENVIRONMENT"),
        ({"airflow_auth": "azure"}, "AZURE_SCOPE"),
    ],
)
def test_missing_cloud_settings(
    overrides: dict[str, Any], message: str, make_settings: Any
) -> None:
    with pytest.raises(CloudSetupError, match=message):
        connect(make_settings(**overrides))
    with pytest.raises(AirflowAPIError, match=message):  # the client turns it into an API error
        AirflowClient(make_settings(**overrides))


def test_missing_extra_says_what_to_install(
    make_settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    import builtins

    real_import = builtins.__import__

    def no_boto3(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "boto3":
            raise ImportError("No module named 'boto3'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_boto3)
    with pytest.raises(CloudSetupError, match=r"pip install 'dagnose\[aws\]'"):
        connect(make_settings(airflow_auth="mwaa", mwaa_environment="e"))


def test_mwaa_with_known_api_version_skips_get_environment() -> None:
    class NoGetEnvironment:
        def get_environment(self, Name: str) -> dict[str, Any]:  # noqa: N803
            raise AssertionError("airflow:GetEnvironment should not be needed")

        def invoke_rest_api(self, **kw: Any) -> dict[str, Any]:
            return {"RestApiStatusCode": 200, "RestApiResponse": {"version": "2.10.3"}}

    http = httpx.Client(
        base_url="http://mwaa.invalid",
        transport=MwaaTransport("e", client=NoGetEnvironment(), api="v1"),
    )
    assert http.get("/api/v1/version").json() == {"version": "2.10.3"}
