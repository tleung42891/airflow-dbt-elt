FROM python:3.11-slim
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
RUN pip install protobuf==4.25.3 dbt-core==1.7.0 dbt-postgres==1.7.0
RUN pip install 'elementary-data[postgres]'
WORKDIR /usr/app/dbt