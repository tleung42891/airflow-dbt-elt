FROM python:3.11-slim
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
RUN pip install dbt-core==1.7.0 dbt-postgres==1.7.0 "pre-commit>=3.5.0" dbt-coverage
RUN pip install 'elementary-data[postgres]'
RUN pip install 'protobuf==4.25.3'

WORKDIR /usr/app/dbt