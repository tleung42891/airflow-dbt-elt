# Airflow GitHub Project

This project sets up an Airflow-based ETL pipeline for extracting GitHub data, loading it into PostgreSQL, and transforming it with dbt.

## Architecture

The project consists of:
- **Airflow**: Orchestrates data pipelines using CeleryExecutor
- **PostgreSQL (Airflow DB)**: Stores Airflow metadata
- **PostgreSQL (pg-warehouse)**: Data warehouse for GitHub data
- **dbt**: Transforms raw data into analytics-ready models
- **Metabase**: Business intelligence and visualization tool (optional)
- **Redis**: Message broker for Celery

## Prerequisites

- Docker and Docker Compose installed
- Git
- Access to GitHub API (token required)

## Setup Instructions

### 1. Start Core Services

Start the Airflow stack with docker-compose:

```bash
docker-compose up -d
```

This will start:
- PostgreSQL (for Airflow metadata)
- Redis (for Celery)
- Airflow webserver (port 8080)
- Airflow scheduler
- Airflow worker
- dbt CLI container

### 2. Set Up Data Warehouse (pg-warehouse)

The data warehouse PostgreSQL instance needs to be run as a standalone container to match the network configuration:

```bash
docker run --name pg-warehouse \
  --network airflow-github-project_default \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=mysecretpassword \
  -e POSTGRES_DB=postgres \
  -p 5432:5432 \
  -d postgres:latest
```

**Note**: Make sure the password matches your `dbt_project/profiles.yml` configuration. If your profiles.yml uses a different password (e.g., `mysecretpassword`), update the `POSTGRES_PASSWORD` environment variable accordingly.

### 3. Set Up Metabase (Optional)

Metabase can be used for data visualization and SQL reader:

```bash
# Pull the Metabase image
docker pull metabase/metabase:latest

# Run Metabase container
docker run -d -p 3000:3000 --name metabase metabase/metabase
```

Access Metabase at `http://localhost:3000` after it starts.
