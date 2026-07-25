"""Unit tests for Airflow DAG pipeline settings (YAML + required env)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pipeline_settings import load_pipeline_settings, missing_env_keys, require_env

FIXTURE_YAML = """\
docker:
  img_simulator: IMG_SIMULATOR
  img_spark: IMG_SPARK
  img_delivery: IMG_DELIVERY
  img_ml_mock: IMG_ML_MOCK
  network_name: NETWORK_NAME
  host_project_dir: HOST_PROJECT_DIR
task_environment:
  - MINIO_ENDPOINT
  - MINIO_ROOT_USER
  - MINIO_ROOT_PASSWORD
  - LANDING_BUCKET
  - BRONZE_BUCKET
  - SILVER_BUCKET
  - GOLD_BUCKET
"""


@pytest.fixture
def config_path(tmp_path: Path) -> Path:
    path = tmp_path / "pipeline_config.yaml"
    path.write_text(FIXTURE_YAML)
    return path


class TestRequireEnv:
    def test_returns_value_when_set(self, monkeypatch):
        monkeypatch.setenv("IMG_SPARK", "clap-spark:dev")
        assert require_env("IMG_SPARK") == "clap-spark:dev"

    def test_raises_when_unset(self, monkeypatch):
        monkeypatch.delenv("IMG_SPARK", raising=False)
        with pytest.raises(KeyError, match="IMG_SPARK"):
            require_env("IMG_SPARK")

    def test_raises_when_empty(self, monkeypatch):
        monkeypatch.setenv("IMG_SPARK", "")
        with pytest.raises(KeyError, match="IMG_SPARK"):
            require_env("IMG_SPARK")


class TestMissingEnvKeys:
    def test_lists_only_missing(self, monkeypatch):
        monkeypatch.setenv("A", "1")
        monkeypatch.delenv("B", raising=False)
        monkeypatch.setenv("C", "")
        assert missing_env_keys(["A", "B", "C"]) == ["B", "C"]


class TestLoadPipelineSettings:
    def test_resolves_all_keys_from_env(self, config_path, monkeypatch):
        env = {
            "IMG_SIMULATOR": "sim:dev",
            "IMG_SPARK": "spark:dev",
            "IMG_DELIVERY": "delivery:dev",
            "IMG_ML_MOCK": "ml-mock:dev",
            "NETWORK_NAME": "platform-net",
            "HOST_PROJECT_DIR": "/repo",
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ROOT_USER": "user",
            "MINIO_ROOT_PASSWORD": "secret",
            "LANDING_BUCKET": "landing",
            "BRONZE_BUCKET": "bronze",
            "SILVER_BUCKET": "silver",
            "GOLD_BUCKET": "gold",
        }
        for key, value in env.items():
            monkeypatch.setenv(key, value)

        settings = load_pipeline_settings(config_path)

        assert settings.img_simulator == "sim:dev"
        assert settings.img_spark == "spark:dev"
        assert settings.img_delivery == "delivery:dev"
        assert settings.img_ml_mock == "ml-mock:dev"
        assert settings.network_name == "platform-net"
        assert settings.host_project_dir == "/repo"
        assert settings.task_environment == {
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ROOT_USER": "user",
            "MINIO_ROOT_PASSWORD": "secret",
            "LANDING_BUCKET": "landing",
            "BRONZE_BUCKET": "bronze",
            "SILVER_BUCKET": "silver",
            "GOLD_BUCKET": "gold",
        }

    def test_fails_when_any_env_missing(self, config_path, monkeypatch):
        monkeypatch.setenv("IMG_SIMULATOR", "sim:dev")
        monkeypatch.setenv("IMG_SPARK", "spark:dev")
        monkeypatch.setenv("NETWORK_NAME", "platform-net")
        monkeypatch.setenv("HOST_PROJECT_DIR", "/repo")
        # Intentionally omit MinIO / bucket vars
        for key in (
            "MINIO_ENDPOINT",
            "MINIO_ROOT_USER",
            "MINIO_ROOT_PASSWORD",
            "LANDING_BUCKET",
            "BRONZE_BUCKET",
            "SILVER_BUCKET",
            "GOLD_BUCKET",
        ):
            monkeypatch.delenv(key, raising=False)

        with pytest.raises(KeyError, match="MINIO_ENDPOINT"):
            load_pipeline_settings(config_path)

    def test_shipped_yaml_lists_expected_keys(self):
        """The real YAML next to the DAG must declare the keys we forward."""
        shipped = Path(__file__).resolve().parents[2] / "airflow" / "dags" / "pipeline_config.yaml"
        data = yaml.safe_load(shipped.read_text())
        assert set(data["docker"].values()) == {
            "IMG_SIMULATOR",
            "IMG_SPARK",
            "IMG_DELIVERY",
            "IMG_ML_MOCK",
            "NETWORK_NAME",
            "HOST_PROJECT_DIR",
        }
        assert data["task_environment"] == [
            "MINIO_ENDPOINT",
            "MINIO_ROOT_USER",
            "MINIO_ROOT_PASSWORD",
            "LANDING_BUCKET",
            "BRONZE_BUCKET",
            "SILVER_BUCKET",
            "GOLD_BUCKET",
            "DELIVERED_BUCKET",
            "ROLLING_WINDOW_DAYS",
            "SCHEMA_VERSION",
            "DELIVERY_ENCRYPTION_KEY",
        ]
