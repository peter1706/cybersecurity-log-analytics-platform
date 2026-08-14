# Cybersecurity Log Analytics Platform
This repository includes a batch-processing platform which should ingest, store and process cybersecurity events in order to be used for a downstream machine learning application. The system makes uses of the LANL Cyber Security Dataset, which includes authentication activities, user login, computer accesses, and network-related events.

## Architecture

![Cybersecurity Log Analytics Platform architecture](docs/architecture/Cybersecurity_Log_Analytics_Platform_Architecture.drawio.png)

See [PLATFORM_ABSTRACT.md](PLATFORM_ABSTRACT.md) for a one-paragraph summary,
[docs/use_case/USE_CASE_DESCRIPTION.md](docs/use_case/USE_CASE_DESCRIPTION.md) for
the business problem, [docs/dataset/DATASET_DESCRIPTION.md](docs/dataset/DATASET_DESCRIPTION.md)
for the LANL dataset and demonstration subset, and
[docs/requirements/](docs/requirements/) for the system-owner and data-science-team
requirements the platform is built against.

## Running the platform

Prerequisites: Docker + Docker Compose, Python 3, and `make`. The full 14-day
volume run below is comfortable on ≥8 GB RAM / ≥4 cores (`SPARK_DRIVER_MEMORY`
and friends in `.env.example` can be lowered for smaller machines).

```bash
cp .env.example .env       # non-sensitive config; edit if needed
make secrets                # generate ./secrets
make build                  # build airflow, simulator, spark-processor, delivery, ml-mock Docker images
make up                     # start MinIO, Postgres (catalog + Airflow), Airflow Docker services
```

Once the stack is healthy:

- Airflow UI — http://localhost:8080 (user/password from `AIRFLOW_ADMIN_USER` / the `airflow_admin_password` secret)
- MinIO console — http://localhost:9001
- ML-mock consumer dashboard — http://localhost:8501

Run the `daily_pipeline` DAG for a single day end-to-end (simulate → bronze → silver → gold → deliver → consume):

```bash
make pipeline                       # day 0
make backfill START=0 END=6         # seed a full rolling window across several days
```

Stop the stack (volumes are kept) with `make down`.

## Testing

```bash
make install     # one-time: install dev/test tooling
make test-unit   # fast pytest suite, no Docker required
make e2e         # runs `make pipeline` then verifies Gold/delivered output in MinIO
make e2e-full    # backfills days START..END (default 0..6), then verifies multi-day Gold output
make e2e-full-14 # backfills the full 14-day demonstration subset (days 0..13)
```

`make e2e` and `make e2e-full` build the images and bring up the full Docker Compose
stack themselves, so a plain `make test-unit` is enough for quick iteration.

**To confirm the platform holds up at realistic volume**, run the full 14-day
backfill end to end — one command, no setup beyond `.env`:

```bash
cp .env.example .env    # if not already done
make e2e-full-14        # build, start the stack, backfill 14 days, verify Gold/delivered
```

This builds every image, starts the stack, replays and processes the entire
~19.8M-row / 14-day subset (seeding the full 7-day rolling window for every one
of the 14 anchor days), delivers + consumes each day, and asserts the multi-day
Gold/delivered output in MinIO — end to end in **~12 minutes** (image build
included) on a 12-core / 36 GB machine. `make down` afterwards stops the stack
(volumes are kept, so a re-run is fast and safe — every stage is idempotent).

### Data volume processed by the e2e tests

The e2e tests run against the pre-filtered LANL subset checked into `data/subset/`
(auth, proc, flows, dns, redteam), which covers 14 simulated days over 250
background hosts plus the redteam-compromised hosts, ~19.8M rows / ~101 MB gzipped
in total:

- `make e2e` (single day, day 0): ~1.5M rows across all sources (~7 MB gzipped).
- `make e2e-full` (7-day backfill, days 0-6): ~9.3M rows across all sources
  (~45 MB gzipped), which seeds a full rolling window for the Silver → Gold step.
- `make e2e-full-14` (full 14-day backfill, days 0-13): the entire ~19.8M-row
  demonstration subset (~101 MB gzipped). A manual/local reproducibility check,
  not run in CI — the `e2e` job (day 0) is CI's e2e gate, on every PR into `main`.

## Published images

`.github/workflows/publish-images.yml` builds every custom image (airflow,
lanl-simulator, spark-processor, delivery, ml-mock) from the same Dockerfiles
used locally and publishes them to GHCR on every push to `main` (tag
`sha-<short-git-sha>`) and on release tags (tag `vX.Y[.Z][-pre]`). `make build`
never needs registry access — see `.env.example` for how to point the `IMG_*`
variables at a published tag instead of building locally.
