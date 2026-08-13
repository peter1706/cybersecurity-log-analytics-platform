"""Minimal Airflow 3 REST client for the dashboard (stdlib urllib only)."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


class AirflowAuthError(Exception):
    """Raised when JWT token acquisition fails."""


class AirflowApiError(Exception):
    """Raised when an authenticated Airflow API call fails."""


@dataclass(frozen=True, slots=True)
class AirflowConfig:
    """Connection settings for the Airflow public API."""

    base_url: str
    username: str
    password: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))


def _request(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise AirflowApiError(f"{method} {url} -> HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise AirflowApiError(f"{method} {url} failed: {exc.reason}") from exc
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise AirflowApiError(f"Expected JSON object from {url}, got {type(parsed).__name__}")
    return parsed


@dataclass(frozen=True, slots=True)
class AirflowClient:
    """Thin wrapper around Airflow 3 JWT + DAG-run endpoints."""

    config: AirflowConfig

    def fetch_token(self) -> str:
        """Obtain a JWT access token via Simple Auth Manager."""
        try:
            payload = _request(
                "POST",
                f"{self.config.base_url}/auth/token",
                body={"username": self.config.username, "password": self.config.password},
            )
        except AirflowApiError as exc:
            raise AirflowAuthError(str(exc)) from exc
        token = payload.get("access_token")
        if not token or not isinstance(token, str):
            raise AirflowAuthError("Airflow /auth/token response missing access_token")
        return token

    def trigger_dag_run(
        self,
        dag_id: str,
        *,
        conf: dict[str, Any],
        dag_run_id: str | None = None,
        token: str | None = None,
    ) -> dict[str, Any]:
        """Trigger a DAG run; ``conf`` carries ``day`` / ``window_days``."""
        access = token or self.fetch_token()
        body: dict[str, Any] = {
            # Required by Airflow 3 TriggerDAGRunPostBody; null = no logical date.
            "logical_date": None,
            "conf": conf,
        }
        if dag_run_id is not None:
            body["dag_run_id"] = dag_run_id
        return _request(
            "POST",
            f"{self.config.base_url}/api/v2/dags/{dag_id}/dagRuns",
            headers={"Authorization": f"Bearer {access}"},
            body=body,
        )

    def get_dag_run(
        self,
        dag_id: str,
        dag_run_id: str,
        *,
        token: str | None = None,
    ) -> dict[str, Any]:
        """Fetch one DAG run by id."""
        access = token or self.fetch_token()
        return _request(
            "GET",
            f"{self.config.base_url}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}",
            headers={"Authorization": f"Bearer {access}"},
        )
