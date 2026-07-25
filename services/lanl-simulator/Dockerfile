# Built from the repo root (see docker-compose build.context) so the shared
# `catalog/` governance client can be layered into the image.
FROM python:3.11-slim

WORKDIR /app

COPY services/lanl-simulator/requirements.txt .
COPY catalog/requirements.txt catalog-requirements.txt
RUN pip install --no-cache-dir -r requirements.txt -r catalog-requirements.txt

COPY services/lanl-simulator/simulate.py .
# Shared governance-catalog client (landing lineage writes).
COPY catalog ./catalog

# Source subset is bind-mounted at /data/subset by the launching orchestrator.
ENTRYPOINT ["python", "/app/simulate.py"]
