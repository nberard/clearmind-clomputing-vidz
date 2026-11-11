FROM apache/airflow:2.10.5-python3.12

# dbt gets its own virtualenv so its dependencies never clash with Airflow's
USER root
RUN python -m venv /opt/dbt-venv && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-bigquery~=1.9"
USER airflow
RUN pip install --no-cache-dir google-cloud-bigquery requests

ENV DBT_BIN=/opt/dbt-venv/bin/dbt \
    VIBEGEN_ROOT=/opt/vibegen \
    BQ_AUTH_METHOD=service-account \
    GOOGLE_APPLICATION_CREDENTIALS=/opt/vibegen/secrets/gcp-key.json
