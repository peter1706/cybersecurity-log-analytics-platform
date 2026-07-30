# Built from the repo root (see docker-compose build.context) so the shared
# `catalog/` package can be layered in for read_secret() credential loading.
FROM python:3.11-slim

RUN useradd --create-home --uid 1000 --shell /usr/sbin/nologin app
WORKDIR /app

COPY services/ml-mock/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/ml-mock/feature_contract.py services/ml-mock/consume.py services/ml-mock/dashboard.py ./
# Shared package: ml-mock only uses catalog.read_secret to load its credentials
# from mounted secrets -- it never talks to the governance catalog DB, so the
# Postgres driver is intentionally not installed here.
COPY catalog ./catalog
RUN chown -R app:app /app

USER app

EXPOSE 8501

# Default run mode is the one-shot consumer (the `ml_consume` DAG task):
#   docker run <img> --day <n>
# The always-on dashboard service overrides the entrypoint in docker-compose:
#   streamlit run /app/dashboard.py
ENTRYPOINT ["python", "/app/consume.py"]
