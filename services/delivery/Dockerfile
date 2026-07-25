FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bundle.py deliver.py ./

# Launched per anchor day by the Airflow DAG: `--day <n> [--window-days <n>]`.
ENTRYPOINT ["python", "/app/deliver.py"]
