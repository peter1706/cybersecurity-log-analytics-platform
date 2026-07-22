FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulate.py .

# Source subset is bind-mounted at /data/subset by the launching orchestrator.
ENTRYPOINT ["python", "/app/simulate.py"]
