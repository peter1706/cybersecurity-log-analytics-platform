"""Load pipeline settings from YAML and required environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "pipeline_config.yaml"


@dataclass(frozen=True)
class PipelineSettings:
    """Resolved Docker wiring and task-container environment."""

    img_simulator: str
    img_spark: str
    img_delivery: str
    img_ml_mock: str
    network_name: str
    host_project_dir: str
    task_environment: dict[str, str]


def require_env(key: str) -> str:
    """Return a non-empty environment variable or raise KeyError."""
    value = os.environ.get(key)
    if value is None or value == "":
        raise KeyError(
            f"Required environment variable {key!r} is missing or empty. "
            "Set it in .env (see .env.example) so docker-compose can inject it."
        )
    return value


def missing_env_keys(keys: list[str]) -> list[str]:
    """Return keys that are unset or empty in the environment."""
    return [key for key in keys if not os.environ.get(key)]


def load_pipeline_settings(config_path: Path | str | None = None) -> PipelineSettings:
    """Load YAML key names and resolve each from the process environment."""
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"Invalid pipeline config in {path}: expected a mapping")

    docker = data.get("docker") or {}
    task_keys = list(data.get("task_environment") or [])
    required = list(docker.values()) + task_keys
    missing = missing_env_keys(required)
    if missing:
        raise KeyError(
            "Required environment variables are missing or empty: "
            + ", ".join(missing)
            + ". Set them in .env (see .env.example)."
        )

    return PipelineSettings(
        img_simulator=require_env(docker["img_simulator"]),
        img_spark=require_env(docker["img_spark"]),
        img_delivery=require_env(docker["img_delivery"]),
        img_ml_mock=require_env(docker["img_ml_mock"]),
        network_name=require_env(docker["network_name"]),
        host_project_dir=require_env(docker["host_project_dir"]),
        task_environment={key: require_env(key) for key in task_keys},
    )
