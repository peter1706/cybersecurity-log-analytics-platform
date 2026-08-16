# Cybersecurity Log Analytics Platform

[![CI](https://github.com/peter1706/cybersecurity-log-analytics-platform/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/peter1706/cybersecurity-log-analytics-platform/actions/workflows/ci.yml)
[![Publish images](https://github.com/peter1706/cybersecurity-log-analytics-platform/actions/workflows/publish-images.yml/badge.svg?branch=main)](https://github.com/peter1706/cybersecurity-log-analytics-platform/actions/workflows/publish-images.yml)
[![codecov](https://codecov.io/gh/peter1706/cybersecurity-log-analytics-platform/branch/main/graph/badge.svg)](https://codecov.io/gh/peter1706/cybersecurity-log-analytics-platform)

This batch-processing platform pre-processes and aggregates cybersecurity events for an anomaly detection model that detects unusual computer behavior in a fictional enterprise network. It is demonstrated on a 14-day subset of the Los Alamos National Laboratory (LANL) dataset, covering authentication, process, network-flow, and DNS events with a total of about 20 million records. The processing pipeline is meant to be executed daily based on a rolling window of 7 days looking into the past processing about 8-10 million records per processing. Results can be accessed via a dashboard which reports warnings for affected computers.

To get the platform running quickly, jump to [Running the platform](#running-the-platform). To see what the running dashboard looks like, see the [Showcase](#showcase).

## Table of contents

- [Showcase](#showcase)
- [Architecture](#architecture)
- [Running the platform](#running-the-platform)
  - [Prerequisites](#prerequisites)
  - [Quick start](#quick-start)
  - [Service endpoints](#service-endpoints)
  - [Run the pipeline](#run-the-pipeline)
  - [Stop and clean up](#stop-and-clean-up)
- [Configuration](#configuration)
- [Repository layout](#repository-layout)
- [Development and testing](#development-and-testing)
  - [Data volume processed by the e2e tests](#data-volume-processed-by-the-e2e-tests)
- [Published images](#published-images)
- [Documentation](#documentation)
- [Dataset and citation](#dataset-and-citation)

## Showcase

![ML consumer dashboard](cybersecurity-dashboard.gif)

The dashboard in this illustration shows the running platform filled with a 14-day subset of the LANL dataset. Given a 7-day rolling window for processing, it shows delivered data for all anchor days which can be selected.

Each *Deliveries*-entry relates to a single day processing of the pipeline. The daily pipeline runs end-to-end including the following six stages: **landing → bronze → silver → gold → deliver → consume**, which are described in the [Architecture](#architecture) section. Under *Delivery Information* you can find detailed information about the aggregated records to see how the pipeline has processed the raw records for the given time window. Under *Schema check* you can see which data schema has been expected by the dashboard and whether this has been correctly delivered by the pipeline.

The center of this dashboard shows several computer risk statistics for the processed delivery as well as a list of affected computers scored by their risk level. Clicking on an entry in this watchlist displays a diagram on the right which outlines the reasons for why this computer has been identified as a potential risk.

## Architecture

![Cybersecurity Log Analytics Platform architecture](docs/architecture/Cybersecurity_Log_Analytics_Platform_Architecture.drawio.png)

**Tech stack overview:** Apache Airflow (orchestration), Apache Spark + Delta Lake
(Medallion layer for bronze/silver/gold), MinIO (object storage), PostgreSQL (Airflow
metadata + governance catalog), Streamlit (ML-mock dashboard), Python (general development of services) Docker Compose.

The platform follows a lakehouse architecture built from microservices. A Data Producer Service (Python), container `lanl-simulator`, replays the LANL subset as daily raw batches into the landing bucket of a MinIO object store (`minio`). A Data Processing Service (Apache Spark), container `spark-processor`, refines these batches through three Medallion layers, each a Delta Lake table: schema-validated ingestion into Bronze, cleaning and transformation into Silver, and aggregation over a configurable and default 7-day rolling window into Gold. A Data Delivery Service (Python), container `delivery`, encrypts each Gold partition using Parquet modular encryption and writes it to a delivered bucket, from which a Mock Anomaly Detection Service, container `ml-dashboard`, consumes the features and simulates retraining and displays the results in a dashboard.

An Airflow Orchestration Service (`airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`) with its own PostgreSQL metadata database (`airflow-postgres`) schedules all jobs, and every stage records lineage, schema versions, checksums, and delivery manifests in a PostgreSQL Governance Store (`postgres-catalog`). All components run as Docker containers in isolated networks as shown below. GitHub versions the source code and GitHub Actions provides the test and build pipeline.

The daily Airflow `daily_pipeline` DAG of the Airflow Orchestration Services runs the six stages shown in the diagram
above — **landing → bronze → silver → gold → deliver → consume** — as sequential
tasks (`simulate` writes landing; then `land_to_bronze`, `bronze_to_silver`,
`silver_to_gold`, `deliver`, and `ml_consume`).

**Docker networks and services.** The Compose stack isolates components across three networks:

| Network | Purpose | Typical members |
|---------|---------|-----------------|
| `pipeline-net` | Medallion data path and governance | `minio`, `postgres-catalog`, `lanl-simulator`, `spark-processor`, `delivery`, Airflow services (for catalog/MinIO access) |
| `airflow-net` | Orchestration control plane | `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-postgres` |
| `ml-net` | Consumer isolation | `minio` (delivered bucket), `ml-dashboard` |

## Running the platform

### Prerequisites

Install these before continuing:

| Requirement | Why | How (short) |
|-------------|-----|-------------|
| Docker (with Compose) | Runs all platform services | Install [Docker Desktop](https://docs.docker.com/get-docker/) (macOS/Windows) or Docker Engine + Compose plugin (Linux). Confirm with `docker compose version`. |
| `make` | Runs the project commands below | Usually preinstalled on macOS. On Linux: `sudo apt install make` (or equivalent). On Windows: use WSL or Git Bash. |

**Hardware:** prefer ≥8 GB RAM / ≥4 CPU cores for a full 14-day run. On smaller machines, lower `SPARK_DRIVER_MEMORY` in `.env` after the copy step below or use the smaller 7-day run.

### Quick start

From the repository root:

1. `cp .env.example .env`
2. `make secrets` — creates `./secrets`
3. `make build` — builds the Docker images (first time can take several minutes)
4. `make up` — starts MinIO, Postgres, Airflow, and the dashboard

Then open the URLs in [Service endpoints](#service-endpoints). To load data, continue with [Run the pipeline](#run-the-pipeline).

### Service endpoints

Once the complete stack is set up and healthy you can access the following services with the default credentials set in your environment variables:

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | `AIRFLOW_ADMIN_USER` in `.env` / `airflow_admin_password` in `./secrets` |
| MinIO console | http://localhost:9001 | `minio_root_user` and `minio_root_password` in `./secrets` |
| ML-mock consumer dashboard | http://localhost:8501 | `ml_dashboard_username` and `ml_dashboard_password` in `./secrets` |

### Run the pipeline

Seed a 7-day or up to full 14-day backfill to simulate the fill-in and daily
executed pipeline. Below runs the `daily_pipeline` DAG for a single day
end-to-end (simulate → bronze → silver → gold → deliver → consume) for every day
for the default rolling window of 7 days (the default rolling window for
processing can be changed by setting `ROLLING_WINDOW_DAYS` in the `.env` file):

```bash
make backfill START=0 END=6         # smaller 7-day subset - start anchor day 0 and end anchor day 6
make backfill START=0 END=13        # complete 14-day subset - start anchor day 0 and end anchor day 13
```

To check that the platform holds up a volume of multiple million data
records, run the full 14-day backfill. It processes the entire ~19.8M-row /
14-day subset (seeding the full 7-day rolling window for every one of the 14
anchor days), delivers + consumes each day, and asserts the multi-day
Gold/delivered output in MinIO. Daily pipelines for this configuration process about 10 million data records per anchor day. This 14-day backfill takes about **~12 minutes** with the image build included on a 12-core / 36 GB RAM machine.

### Stop and clean up

```bash
make down    # stop the stack (volumes are kept)
make clean   # remove local caches and generated data layers (bronze/silver/gold/…)
```

## Configuration

Non-sensitive settings live in `.env` (copy from `.env.example`). Credentials are
file-based Docker secrets under `./secrets`, created by `make secrets`
(`scripts/init_secrets.sh`).

Key variables:

| Variable | Role |
|----------|------|
| `ROLLING_WINDOW_DAYS` | Silver → Gold window length (default `7`) |
| `SPARK_DRIVER_MEMORY` | Spark driver heap (default `3g`; lower on small machines) |
| `AIRFLOW_ADMIN_USER` | Airflow UI username (password is the `airflow_admin_password` secret) |
| `IMG_*` | Image tags for airflow / simulator / spark / delivery / ml-mock (local `:dev` by default) |

See `.env.example` for the full list, including MinIO buckets, Spark shuffle
tuning, and how to point `IMG_*` at published GHCR tags.

## Repository layout

```text
airflow/                 Airflow DAGs and config
catalog/                 Governance catalog (schema, lineage, manifests)
data/subset/             Pre-filtered LANL demonstration subset
docs/                    Use case, dataset, requirements, architecture
scripts/                 Secrets init, pipeline, and backfill helpers
services/
  lanl-simulator/        Replays daily LANL batches into landing
  spark-processor/       Bronze → Silver → Gold processing
  delivery/              Encrypts Gold and writes delivered output
  ml-mock/               Mock consumer + Streamlit dashboard
tests/                   Unit and e2e tests
```

## Development and testing

Requires Python 3 (for `make install`, lint, and local tests). From the repository root:

```bash
make install     # one-time: install dev/test tooling
make lint        # ruff lint checks
make fmt         # auto-format with ruff
make test-unit   # fast pytest suite with coverage, no Docker required
make e2e         # runs `make pipeline` then verifies Gold/delivered output in MinIO
make e2e-full    # backfills days START..END (default 0..6), then verifies multi-day Gold output
make e2e-full-14 # backfills the full 14-day demonstration subset (days 0..13)
```

`make e2e` and `make e2e-full` build the images and bring up the full Docker Compose
stack themselves, so a plain `make test-unit` is enough for quick iteration.

Unit coverage (Codecov / `make test-unit`) measures library and transform code. Streamlit
UI, service CLIs (`dashboard.py`, `consume.py`, `deliver.py`, `run_job.py`), and
`scripts/generate_sample_data.py` are omitted; those paths are exercised by the e2e
pipeline instead.

### Data volume processed by the e2e tests

The e2e tests run against the pre-filtered LANL subset checked into `data/subset/`
(auth, proc, flows, dns, redteam), which covers 14 simulated days over 250
background hosts plus the redteam-compromised hosts, ~19.8M rows / ~101 MB gzipped
in total:

- `make e2e` (single day, day 0): ~1.5M rows across all sources (~7 MB gzipped).
- `make e2e-full` (7-day backfill, days 0-6): ~9.3M rows across all sources
  (~45 MB gzipped), which seeds a full rolling window for the Silver → Gold step.
- `make e2e-full-14` (full 14-day backfill, days 0-13): the entire ~19.8M-row
  demonstration subset (~101 MB gzipped).

## Published images

`.github/workflows/publish-images.yml` builds every custom image (airflow,
lanl-simulator, spark-processor, delivery, ml-mock) from the same Dockerfiles
used locally and publishes them to GitHub Container Registry (GHCR) on every push to `main` (tag
`sha-<short-git-sha>`) and on release tags (tag `vX.Y[.Z][-pre]`). `make build`
never needs registry access — see `.env.example` for how to point the `IMG_*`
variables at a published tag instead of building locally.

## Documentation

| Document | Description |
|----------|-------------|
| [docs/use_case/USE_CASE_DESCRIPTION.md](docs/use_case/USE_CASE_DESCRIPTION.md) | Business problem and high-level use case description |
| [docs/dataset/DATASET_DESCRIPTION.md](docs/dataset/DATASET_DESCRIPTION.md) | Description of the LANL dataset and the used demonstration subset |
| [docs/requirements/REQUIREMENTS_SYSTEM_OWNER.md](docs/requirements/REQUIREMENTS_SYSTEM_OWNER.md) | System-owner non-functional requirements and constraints that have been set |
| [docs/requirements/REQUIREMENTS_DATA_SCIENCE_TEAM.md](docs/requirements/REQUIREMENTS_DATA_SCIENCE_TEAM.md) | Data-science-team requirements for the anomaly scoring model and processed features |

## Dataset and citation

The platform is demonstrated on a subset of the
[LANL Comprehensive, Multi-Source Cyber-Security Events](https://csr.lanl.gov/data/cyber1/)
dataset. Details of the full dataset and the checked-in demonstration subset are
in [docs/dataset/DATASET_DESCRIPTION.md](docs/dataset/DATASET_DESCRIPTION.md).

> A. D. Kent, “Comprehensive, Multi-Source Cyber-Security Events,”
> Los Alamos National Laboratory, http://dx.doi.org/10.17021/1179829, 2015.
