# Extends the project Airflow image (Cosmos + dbt venv already installed).
# `airflow_base` is supplied by compose as the airflow-webserver image
# (see dag-tests.build.additional_contexts in docker-compose.yaml).
FROM airflow_base

USER airflow
RUN pip install --no-cache-dir \
      'pytest>=7.4' \
      'pytest-mock>=3.12'
