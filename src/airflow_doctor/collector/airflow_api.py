"""Read-only Airflow REST API client: /api/v2 (Airflow 3.x) and /api/v1 (Airflow 2.x).

Methods return plain dicts in the shape of the v2 API, so the rest of the collector does
not care which Airflow it is talking to. Only GET requests read data; the one POST
(task instance list) is a read-only query endpoint.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import quote

import httpx

from airflow_doctor.config import Settings
from airflow_doctor.models import ApiVersion, FailureJob


class AirflowAPIError(RuntimeError):
    pass


def _seg(value: str) -> str:
    """Encode one path segment (run ids contain ':' and '+')."""
    return quote(value, safe="")


class AirflowClient:
    def __init__(self, settings: Settings, transport: httpx.BaseTransport | None = None) -> None:
        self.settings = settings
        self._http = httpx.Client(
            base_url=settings.airflow_url.rstrip("/"),
            timeout=settings.request_timeout_s,
            verify=settings.airflow_verify_ssl,
            transport=transport,
            headers={"Accept": "application/json"},
        )
        self._api: ApiVersion | None = (
            None if settings.airflow_api == "auto" else settings.airflow_api
        )
        self._version: str | None = None
        self._token: str | None = (
            settings.airflow_token.get_secret_value() if settings.airflow_token else None
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> AirflowClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---- version / auth -------------------------------------------------------------

    @property
    def api(self) -> ApiVersion:
        if self._api is None:
            self._detect()
        assert self._api is not None
        return self._api

    def airflow_version(self) -> str:
        if self._version is None:
            self._version = str(self._get("/version").get("version"))
        return self._version

    def _detect(self) -> None:
        # /version needs no auth on either API version.
        for api in ("v2", "v1"):
            resp = self._http.get(f"/api/{api}/version")
            if resp.status_code == 200:
                self._api = api
                self._version = str(resp.json().get("version"))
                return
        raise AirflowAPIError(
            f"No Airflow REST API found at {self.settings.airflow_url} (tried /api/v2, /api/v1)"
        )

    def _auth(self) -> httpx.Auth | None:
        s = self.settings
        if self._token is None and self.api == "v1" and s.airflow_username:
            password = s.airflow_password.get_secret_value() if s.airflow_password else ""
            return httpx.BasicAuth(s.airflow_username, password)
        return None

    def _auth_headers(self, refresh: bool = False) -> dict[str, str]:
        s = self.settings
        if self.api == "v2" and s.airflow_username and (self._token is None or refresh):
            password = s.airflow_password.get_secret_value() if s.airflow_password else ""
            resp = self._http.post(
                "/auth/token", json={"username": s.airflow_username, "password": password}
            )
            if resp.status_code not in (200, 201):
                raise AirflowAPIError(f"Login to Airflow failed: HTTP {resp.status_code}")
            self._token = str(resp.json()["access_token"])
        return {"Authorization": f"Bearer {self._token}"} if self._token else {}

    def _request(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        url = f"/api/{self.api}{path}"
        extra = headers or {}  # e.g. Accept: text/plain for v1 logs and DAG source
        resp = self._http.request(
            method, url, headers={**extra, **self._auth_headers()}, auth=self._auth(), **kwargs
        )
        if resp.status_code == 401 and self.settings.airflow_username and self.api == "v2":
            # Token expired: log in again once.
            resp = self._http.request(
                method, url, headers={**extra, **self._auth_headers(refresh=True)}, **kwargs
            )
        if resp.status_code >= 400:
            raise AirflowAPIError(f"{method} {url} -> HTTP {resp.status_code}: {resp.text[:300]}")
        return resp

    def _get(self, path: str, **kwargs: Any) -> dict[str, Any]:
        data = self._request("GET", path, **kwargs).json()
        if not isinstance(data, dict):
            raise AirflowAPIError(f"Unexpected response for {path}: {type(data).__name__}")
        return data

    # ---- data -----------------------------------------------------------------------

    def _ti_base(self, job: FailureJob) -> str:
        return (
            f"/dags/{_seg(job.dag_id)}/dagRuns/{_seg(job.run_id)}/taskInstances/{_seg(job.task_id)}"
        )

    def _ti_path(self, job: FailureJob) -> str:
        path = self._ti_base(job)
        return f"{path}/{job.map_index}" if job.map_index >= 0 else path

    def task_instance(self, job: FailureJob) -> dict[str, Any]:
        return self._get(self._ti_path(job))

    def dag_run(self, dag_id: str, run_id: str) -> dict[str, Any]:
        return self._get(f"/dags/{_seg(dag_id)}/dagRuns/{_seg(run_id)}")

    def task_log(self, job: FailureJob, try_number: int) -> list[dict[str, Any]] | str:
        """v2: list of structured log events. v1: the log as plain text."""
        base = (
            f"/dags/{_seg(job.dag_id)}/dagRuns/{_seg(job.run_id)}/taskInstances/{_seg(job.task_id)}"
        )
        params: dict[str, Any] = {"full_content": "true"}
        if job.map_index >= 0:
            params["map_index"] = job.map_index
        path = f"{base}/logs/{try_number}"
        if self.api == "v1":
            return self._request("GET", path, params=params, headers={"Accept": "text/plain"}).text
        content = self._get(path, params=params).get("content", [])
        if isinstance(content, str):  # some log handlers return one text blob
            return content
        return [e if isinstance(e, dict) else {"event": str(e)} for e in content]

    def dag(self, dag_id: str) -> dict[str, Any]:
        """DAG details. Always has `fileloc`; v2 also has `relative_fileloc`."""
        path = f"/dags/{_seg(dag_id)}"
        return self._get(f"{path}/details" if self.api == "v2" else path)

    def dag_source(self, dag_id: str) -> str:
        if self.api == "v2":
            return str(self._get(f"/dagSources/{_seg(dag_id)}").get("content", ""))
        token = self.dag(dag_id).get("file_token")
        if not token:
            raise AirflowAPIError(f"DAG {dag_id} has no file_token")
        resp = self._request("GET", f"/dagSources/{token}", headers={"Accept": "text/plain"})
        return resp.text

    def failed_task_instances(
        self,
        since: datetime,
        dag_ids: list[str] | None = None,
        *,
        page_size: int = 100,
        max_pages: int = 50,
    ) -> list[dict[str, Any]]:
        """Task instances in state "failed" that ended at or after `since`, all DAGs
        (or only `dag_ids`). Same query endpoint on v1 and v2, paged."""
        body: dict[str, Any] = {
            "state": ["failed"],
            "end_date_gte": since.isoformat(),
            "page_limit": page_size,
        }
        if dag_ids:
            body["dag_ids"] = dag_ids
        if self.api == "v2":
            body["order_by"] = "end_date"
        found: list[dict[str, Any]] = []
        for page in range(max_pages):
            body["page_offset"] = page * page_size
            data = self._request("POST", "/dags/~/dagRuns/~/taskInstances/list", json=body).json()
            batch: list[dict[str, Any]] = data.get("task_instances", [])
            found.extend(batch)
            if len(batch) < page_size:
                break
        return found

    def run_task_instances(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
        """All task instances of one DAG run (same path on v1 and v2)."""
        data = self._get(
            f"/dags/{_seg(dag_id)}/dagRuns/{_seg(run_id)}/taskInstances", params={"limit": 100}
        )
        tis: list[dict[str, Any]] = data.get("task_instances", [])
        return tis

    def task_history(self, dag_id: str, task_id: str, limit: int) -> list[dict[str, Any]]:
        """Most recent task instances of this task across DAG runs, newest first."""
        body: dict[str, Any] = {"dag_ids": [dag_id], "task_ids": [task_id]}
        if self.api == "v2":
            body.update(order_by="-start_date", page_limit=limit)
        else:
            # v1 cannot sort this endpoint, so the first page may hold the oldest runs.
            # Fetch a wide page and keep the newest.
            body["page_limit"] = max(limit, 100)
        data = self._request("POST", "/dags/~/dagRuns/~/taskInstances/list", json=body).json()
        tis: list[dict[str, Any]] = data.get("task_instances", [])
        # v1 has no order_by on this endpoint; sort here for both to be safe.
        tis.sort(key=lambda t: t.get("start_date") or "", reverse=True)
        return tis[:limit]
