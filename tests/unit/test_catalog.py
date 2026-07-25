"""Unit tests for the governance-catalog config, SQL builders, and executor.

No live Postgres required: the ``build_*`` functions are pure, and the executor
is exercised against a fake connection/cursor. Migrations are validated by
inspecting the SQL text (idempotency markers), not by applying them.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from catalog import (
    CatalogClient,
    CatalogConfig,
    Checksum,
    DeliveryManifest,
    JobRun,
    Lineage,
    SchemaRegistration,
    read_secret,
)
from catalog import client as client_module

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "catalog" / "migrations"


# --- Config -----------------------------------------------------------------


class TestCatalogConfig:
    def test_from_env_reads_all_fields(self):
        env = {
            "CATALOG_DB_HOST": "db",
            "CATALOG_DB_PORT": "6543",
            "CATALOG_DB_NAME": "catalog",
            "CATALOG_DB_USER": "cat",
            "CATALOG_DB_PASSWORD": "secret",
        }
        cfg = CatalogConfig.from_env(env)
        assert cfg.host == "db"
        assert cfg.port == 6543
        assert cfg.dbname == "catalog"
        assert cfg.user == "cat"
        assert cfg.password == "secret"

    def test_host_and_port_have_defaults(self):
        cfg = CatalogConfig.from_env(
            {
                "CATALOG_DB_NAME": "catalog",
                "CATALOG_DB_USER": "cat",
                "CATALOG_DB_PASSWORD": "secret",
            }
        )
        assert cfg.host == "postgres-catalog"
        assert cfg.port == 5432

    @pytest.mark.parametrize(
        "missing", ["CATALOG_DB_NAME", "CATALOG_DB_USER", "CATALOG_DB_PASSWORD"]
    )
    def test_missing_required_field_raises(self, missing):
        env = {
            "CATALOG_DB_NAME": "catalog",
            "CATALOG_DB_USER": "cat",
            "CATALOG_DB_PASSWORD": "secret",
        }
        del env[missing]
        with pytest.raises(KeyError):
            CatalogConfig.from_env(env)

    def test_dsn_contains_all_connection_fields(self):
        cfg = CatalogConfig(host="db", port=5432, dbname="catalog", user="cat", password="secret")
        dsn = cfg.dsn()
        for token in ("host=db", "port=5432", "dbname=catalog", "user=cat", "password=secret"):
            assert token in dsn

    def test_from_env_reads_password_from_secret_file(self, tmp_path, monkeypatch):
        (tmp_path / "postgres_catalog_password").write_text("file-secret\n")
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
        # The env fallback is present but the mounted file must win.
        cfg = CatalogConfig.from_env(
            {
                "CATALOG_DB_NAME": "catalog",
                "CATALOG_DB_USER": "cat",
                "CATALOG_DB_PASSWORD": "env-secret",
                "SECRETS_DIR": str(tmp_path),
            }
        )
        assert cfg.password == "file-secret"


# --- Secrets ----------------------------------------------------------------


class TestReadSecret:
    def test_file_wins_over_env(self, tmp_path, monkeypatch):
        (tmp_path / "minio_root_password").write_text("from-file\n")
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "from-env")
        assert read_secret("minio_root_password") == "from-file"

    def test_env_fallback_when_no_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "from-env")
        assert read_secret("minio_root_password") == "from-env"

    def test_explicit_env_name(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
        monkeypatch.setenv("CATALOG_DB_PASSWORD", "pw")
        assert read_secret("postgres_catalog_password", env="CATALOG_DB_PASSWORD") == "pw"

    def test_blank_file_falls_back_to_env(self, tmp_path, monkeypatch):
        (tmp_path / "minio_root_password").write_text("   \n")
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "from-env")
        assert read_secret("minio_root_password") == "from-env"

    def test_default_used_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
        monkeypatch.delenv("SOME_TOKEN", raising=False)
        assert read_secret("some_token", default="fallback") == "fallback"

    def test_missing_raises_keyerror(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
        monkeypatch.delenv("MISSING_SECRET", raising=False)
        with pytest.raises(KeyError):
            read_secret("missing_secret")

    def test_env_mapping_overrides_source(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
        monkeypatch.delenv("MINIO_ROOT_PASSWORD", raising=False)
        assert read_secret("minio_root_password", env_mapping={"MINIO_ROOT_PASSWORD": "m"}) == "m"


# --- SQL builders -----------------------------------------------------------


class TestSqlBuilders:
    def test_insert_lineage(self):
        sql, params = client_module.build_insert_lineage(
            Lineage(
                day=6,
                to_layer="bronze",
                record_count=100,
                schema_version="v1",
                source="auth",
                from_layer="landing",
            )
        )
        assert "INSERT INTO lineage" in sql
        assert params == ("auth", 6, "landing", "bronze", 100, "v1", None)

    def test_upsert_checksum_is_idempotent(self):
        sql, params = client_module.build_upsert_checksum(
            Checksum(layer="silver", day=3, checksum="abc", source="dns", record_count=5)
        )
        # Upsert on the COALESCE partition key -> re-runs overwrite, not duplicate.
        assert "ON CONFLICT" in sql
        assert "COALESCE(source, '')" in sql
        assert "COALESCE(window_days, -1)" in sql
        assert params == ("silver", "dns", 3, None, "sha256", "abc", 5)

    def test_select_checksum_matches_on_coalesced_key(self):
        sql, params = client_module.build_select_checksum("gold", 6, window_days=7)
        assert "SELECT checksum" in sql
        assert params == ("gold", None, 6, 7)

    def test_upsert_schema_serializes_columns_as_json(self):
        sql, params = client_module.build_upsert_schema(
            SchemaRegistration(
                source="auth", layer="silver", schema_version="v1", columns=["a", "b"]
            )
        )
        assert "%s::jsonb" in sql
        assert params[:3] == ("auth", "silver", "v1")
        assert params[3] == '["a", "b"]'

    def test_upsert_delivery_manifest(self):
        sql, params = client_module.build_upsert_delivery_manifest(
            DeliveryManifest(
                dataset="computer_features",
                dataset_version="computer_features-w7-d06",
                schema_version="v1",
                window_days=7,
                anchor_day=6,
                record_count=42,
                checksum_sha256="deadbeef",
                encryption_scheme="fernet",
                encryption_key_id="abc123",
                data_object="k",
            )
        )
        assert "ON CONFLICT (window_days, anchor_day)" in sql
        assert params[3] == 7 and params[4] == 6
        assert params[6] == "deadbeef"

    def test_insert_job_run_upserts_on_try(self):
        sql, _ = client_module.build_insert_job_run(
            JobRun(dag_id="d", task_id="t", run_id="r", job="deliver", status="success")
        )
        assert "INSERT INTO job_runs" in sql
        assert "ON CONFLICT (dag_id, task_id, run_id, try_number)" in sql


# --- Executor against a fake connection -------------------------------------


class _FakeCursor:
    def __init__(self, fetch_result=None):
        self.executed: list[tuple] = []
        self._fetch = fetch_result

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._fetch


class _FakeConnection:
    def __init__(self, fetch_result=None):
        self.cursor_obj = _FakeCursor(fetch_result)
        self.commits = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


class TestExecutor:
    def test_record_lineage_executes_and_commits(self):
        conn = _FakeConnection()
        client = CatalogClient(conn)
        client.record_lineage(
            Lineage(day=0, to_layer="landing", record_count=1, schema_version="v1", source="auth")
        )
        assert len(conn.cursor_obj.executed) == 1
        assert "INSERT INTO lineage" in conn.cursor_obj.executed[0][0]
        assert conn.commits == 1

    def test_get_checksum_returns_value_or_none(self):
        found = CatalogClient(_FakeConnection(fetch_result=("abc123", 10)))
        assert found.get_checksum("bronze", 0, source="auth") == "abc123"

        missing = CatalogClient(_FakeConnection(fetch_result=None))
        assert missing.get_checksum("bronze", 0, source="auth") is None

    def test_context_manager_closes_connection(self):
        conn = _FakeConnection()
        with CatalogClient(conn) as client:
            client.record_checksum(Checksum(layer="gold", day=6, checksum="x", window_days=7))
        assert conn.closed is True


# --- Migrations -------------------------------------------------------------


class TestMigrations:
    def test_migration_files_exist(self):
        assert (MIGRATIONS_DIR / "0001_init.sql").is_file()

    def test_every_table_is_created_idempotently(self):
        sql = (MIGRATIONS_DIR / "0001_init.sql").read_text()
        expected = {
            "job_runs",
            "lineage",
            "checksums",
            "schema_registry",
            "delivery_manifests",
        }
        for table in expected:
            assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
