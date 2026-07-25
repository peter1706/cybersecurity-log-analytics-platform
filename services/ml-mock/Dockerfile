FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY feature_contract.py consume.py dashboard.py ./

EXPOSE 8501

# Default run mode is the one-shot consumer (the `ml_consume` DAG task):
#   docker run <img> --day <n>
# The always-on dashboard service overrides the entrypoint in docker-compose:
#   streamlit run /app/dashboard.py
ENTRYPOINT ["python", "/app/consume.py"]
