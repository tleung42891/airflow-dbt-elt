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

### 4. Rebuild dbt Container (After Changes)

If you modify the dbt Dockerfile or need to update dependencies:

```bash
docker-compose build dbt
docker-compose up -d
```

## Configuration

### dbt Configuration

The dbt project is configured in `dbt_project/profiles.yml`. Ensure the connection details match your pg-warehouse container:

```yaml
postgres: 
  target: dev
  outputs:
    dev:
      type: postgres
      host: pg-warehouse
      user: postgres
      password: mysecretpassword  # Update to match your container
      port: 5432
      dbname: postgres
      schema: public
```
### Airflow Connections

Configure the following connections in Airflow UI (Admin → Connections):

1. **PostgreSQL Connection** (`postgres_default`):
   - Connection Type: Postgres
   - Host: `postgres` (for Airflow metadata)
   - Schema: `airflow`
   - Login: `airflow`
   - Password: `airflow`
   - Port: `5432`

2. **GitHub API Connection** (`github_api_conn`):
   - Connection Type: HTTP
   - Host: `https://api.github.com`
   - Extra: `{"token": "your_github_token"}`

## Accessing Services

- **Airflow Web UI**: http://localhost:8080
  - Username: `airflow`
  - Password: `airflow` (default)
- **Metabase**: http://localhost:3000
- **PostgreSQL (pg-warehouse)**: `localhost:5432`