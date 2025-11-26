FROM python:3.11-slim
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
RUN pip install dbt-core==1.7.0 dbt-postgres==1.7.0
WORKDIR /usr/app/dbt