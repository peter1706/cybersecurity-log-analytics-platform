# Built from the repo root (see docker-compose build.context) so the shared
# `catalog/` governance client can be layered into the image.
FROM python:3.11-slim

RUN useradd --create-home --uid 1000 --shell /usr/sbin/nologin app
WORKDIR /app

COPY services/delivery/requirements.txt .
COPY catalog/requirements.txt catalog-requirements.txt
RUN pip install --no-cache-dir -r requirements.txt -r catalog-requirements.txt

COPY services/delivery/bundle.py services/delivery/deliver.py ./
# Shared governance-catalog client (delivery-manifest writes).
COPY catalog ./catalog
RUN chown -R app:app /app

USER app

# Launched per anchor day by the Airflow DAG: `--day <n> [--window-days <n>]`.
ENTRYPOINT ["python", "/app/deliver.py"]
