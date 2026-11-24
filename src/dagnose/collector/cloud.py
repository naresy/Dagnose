"""Logging into managed Airflow on AWS, Google Cloud and Azure.

Each cloud plugs in below AirflowClient, so collection, polling, `dagnose check` and the
agent's tools work unchanged:

- AWS MWAA: a transport that turns every Airflow REST API request into an
  `InvokeRestApi` call, signed with the AWS credentials boto3 finds (IAM role, profile,
  environment). The IAM principal needs `airflow:InvokeRestApi` on a Viewer RBAC role.
- Google Cloud Composer: Google credentials (service account / workload identity) as a
  bearer token, refreshed before it expires.
- Azure: a Microsoft Entra ID token for the Airflow API's scope, refreshed the same way.

The cloud SDKs are optional extras, imported only when that login is configured.
"""

from __future__ import annotations

import json
import re
import threading
import time
from collections.abc import Generator
from typing import Any
from urllib.parse import parse_qsl

import httpx

MWAA_BASE_URL = "http://mwaa.invalid"  # not contacted: the MWAA transport handles requests
_API_PATH = re.compile(r"^/api/(v1|v2)(/.*)$")


class CloudSetupError(RuntimeError):
    """A cloud login is configured but cannot be used (missing extra, missing setting)."""


def _need(module: str, extra: str) -> Any:
    try:
        return __import__(module, fromlist=["_"])
    except ImportError as exc:
        raise CloudSetupError(
            f"{module} is not installed: pip install 'dagnose[{extra}]' (the Docker image has it)"
        ) from exc


# ---- AWS MWAA -------------------------------------------------------------------------


class MwaaTransport(httpx.BaseTransport):
    """Send Airflow REST API calls through MWAA's InvokeRestApi."""

    def __init__(
        self,
        environment: str,
        region: str | None = None,
        client: Any = None,
        api: str | None = None,
    ) -> None:
        if client is None:
            boto3 = _need("boto3", "aws")
            client = boto3.client("mwaa", region_name=region)
        self.client = client
        self.environment = environment
        # Known API version (DAGNOSE_AIRFLOW_API): then airflow:GetEnvironment is not needed.
        self._api: str | None = api

    def api_version(self) -> str:
        """v1 for Airflow 2.x environments, v2 for Airflow 3.x."""
        if self._api is None:
            env = self.client.get_environment(Name=self.environment)["Environment"]
            major = str(env.get("AirflowVersion", "2")).split(".")[0]
            self._api = "v2" if major == "3" else "v1"
        return self._api

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        raw_path = request.url.raw_path.decode().split("?")[0]
        match = _API_PATH.match(raw_path)
        # Only answer for this environment's API version, so version detection works.
        if not match or match.group(1) != self.api_version():
            return httpx.Response(404, json={"detail": "not served by MWAA InvokeRestApi"})
        kwargs: dict[str, Any] = {
            "Name": self.environment,
            "Path": match.group(2),
            "Method": request.method,
        }
        query = dict(parse_qsl(request.url.query.decode(), keep_blank_values=True))
        if query:
            kwargs["QueryParameters"] = query
        if request.content:
            kwargs["Body"] = json.loads(request.content)
        try:
            result = self.client.invoke_rest_api(**kwargs)
        except Exception as exc:  # botocore ClientError and friends
            return _mwaa_error(exc)
        return httpx.Response(
            int(result.get("RestApiStatusCode", 200)), json=result.get("RestApiResponse", {})
        )


def _mwaa_error(exc: Exception) -> httpx.Response:
    response = getattr(exc, "response", None) or {}
    code = response.get("Error", {}).get("Code", type(exc).__name__)
    # Errors from Airflow itself carry its status code and body.
    if "RestApiStatusCode" in response:
        return httpx.Response(
            int(response["RestApiStatusCode"]), json=response.get("RestApiResponse", {})
        )
    status = {
        "AccessDeniedException": 403,
        "ResourceNotFoundException": 404,
        "ValidationException": 400,
    }.get(code, 502)
    message = response.get("Error", {}).get("Message", str(exc))
    return httpx.Response(status, json={"detail": f"MWAA {code}: {message}"})


# ---- bearer tokens (Google, Azure) -----------------------------------------------------


class _RefreshingBearer(httpx.Auth):
    """Adds a bearer token, refreshing it shortly before it expires or after a 401."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at = 0.0

    def _fetch(self) -> tuple[str, float]:  # (token, unix expiry)
        raise NotImplementedError

    def _get(self, force: bool = False) -> str:
        with self._lock:
            if force or self._token is None or time.time() > self._expires_at - 120:
                self._token, self._expires_at = self._fetch()
            return self._token

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers["Authorization"] = f"Bearer {self._get()}"
        response = yield request
        if response.status_code == 401:
            request.headers["Authorization"] = f"Bearer {self._get(force=True)}"
            yield request


class GoogleAuth(_RefreshingBearer):
    """Cloud Composer: Google default credentials with the cloud-platform scope."""

    SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)

    def __init__(self, credentials: Any = None) -> None:
        super().__init__()
        if credentials is None:
            google_auth = _need("google.auth", "gcp")
            credentials, _project = google_auth.default(scopes=list(self.SCOPES))
        self.credentials = credentials

    def _fetch(self) -> tuple[str, float]:
        requests_transport = _need("google.auth.transport.requests", "gcp")
        self.credentials.refresh(requests_transport.Request())
        expiry = getattr(self.credentials, "expiry", None)
        expires_at = expiry.timestamp() if expiry else time.time() + 3000
        return str(self.credentials.token), expires_at


class AzureAuth(_RefreshingBearer):
    """Microsoft Entra ID token for the Airflow API's scope (DefaultAzureCredential:
    managed identity, workload identity, environment, or `az login`)."""

    def __init__(self, scope: str, credential: Any = None) -> None:
        super().__init__()
        if credential is None:
            identity = _need("azure.identity", "azure")
            credential = identity.DefaultAzureCredential()
        self.credential = credential
        self.scope = scope

    def _fetch(self) -> tuple[str, float]:
        token = self.credential.get_token(self.scope)
        return str(token.token), float(token.expires_on)


# ---- wiring ----------------------------------------------------------------------------


def connect(settings: Any) -> tuple[str, httpx.BaseTransport | None, httpx.Auth | None]:
    """(base_url, transport, auth) for the configured Airflow login."""
    mode = settings.airflow_auth
    if mode == "mwaa":
        if not settings.mwaa_environment:
            raise CloudSetupError("DAGNOSE_AIRFLOW_AUTH=mwaa needs DAGNOSE_MWAA_ENVIRONMENT")
        api = None if settings.airflow_api == "auto" else settings.airflow_api
        transport = MwaaTransport(settings.mwaa_environment, settings.aws_region, api=api)
        return MWAA_BASE_URL, transport, None
    if mode == "google":
        return settings.airflow_url, None, GoogleAuth()
    if mode == "azure":
        if not settings.azure_scope:
            raise CloudSetupError("DAGNOSE_AIRFLOW_AUTH=azure needs DAGNOSE_AZURE_SCOPE")
        return settings.airflow_url, None, AzureAuth(settings.azure_scope)
    return settings.airflow_url, None, None
