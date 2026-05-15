FROM python:3.13-slim
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir 'elementary-data[postgres]' \
    && pip install --no-cache-dir \
        dbt-core==1.11.10 \
        dbt-postgres==1.10.0 \
        pre-commit==4.6.0 \
        dbt-coverage==0.4.2

WORKDIR /usr/app/dbt
