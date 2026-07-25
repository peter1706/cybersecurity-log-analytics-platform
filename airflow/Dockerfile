# Built from the repo root (see docker-compose build.context) so the shared
# `catalog/` governance client can be layered in for the DAG's job-run callbacks.
FROM apache/airflow:slim-3.3.0-python3.11

COPY airflow/requirements.txt /requirements.txt
# The slim base ships no DB driver; the [postgres] extra pulls in the Postgres
# provider + psycopg2-binary so the metadata DB (postgresql+psycopg2) works.
# The official constraints file pins every package to the versions blessed for
# this Airflow release (and prevents pip from upgrading/downgrading core).
RUN pip install --no-cache-dir \
    "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

# Shared governance-catalog client, placed on PYTHONPATH (see docker-compose) so
# the DAG's job-run callbacks can `from catalog import ...`. Kept out of the
# bind-mounted dags/ dir so the volume mount does not shadow it.
COPY catalog /opt/airflow/governance/catalog

# Create the simple-auth dir in the image (owned by the airflow user) so the
# named volume mounted here inherits airflow ownership on first init and is
# writable by the non-root runtime user -- otherwise Docker creates it root:root.
RUN mkdir -p /opt/airflow/auth
