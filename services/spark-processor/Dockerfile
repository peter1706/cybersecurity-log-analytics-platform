FROM python:3.11-slim

# Java runtime for Spark.
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY jobs ./jobs
COPY run_job.py .

# Pre-download the Delta + hadoop-aws jars into the Ivy cache so runtime starts
# offline. build_spark() reads the MinIO creds up front, so dummy values are
# supplied here (they only populate s3a config strings; nothing connects at
# session start). Best-effort: if the build has no network, the first job run
# fetches the jars instead.
RUN MINIO_ROOT_USER=warmup MINIO_ROOT_PASSWORD=warmup \
    python -c "from jobs.common import build_spark; build_spark('warmup').stop()" || true

ENTRYPOINT ["python", "/app/run_job.py"]
