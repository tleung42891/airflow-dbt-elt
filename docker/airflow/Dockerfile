FROM apache/airflow:2.8.4-python3.11

USER root
# git is required for `dbt deps` (Elementary and other git/Hub packages)
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Cosmos 1.14+ requires Airflow >=2.9; pin 1.13.x for apache/airflow:2.8.4.
RUN pip install --no-cache-dir astronomer-cosmos==1.13.1

# Align dbt-core / dbt-postgres pins with docker/dbt/Dockerfile when bumping.
RUN PIP_USER=false python -m venv /opt/airflow/dbt_venv \
    && PIP_USER=false /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
    && PIP_USER=false /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.11.10 \
        dbt-postgres==1.10.0
