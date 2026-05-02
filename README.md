# Airflow GitHub Project

This project sets up an Airflow-based ETL pipeline for extracting GitHub data, loading it into PostgreSQL, and transforming it with dbt.

## Architecture

- **Airflow** (CeleryExecutor) — orchestration
- **PostgreSQL** — Airflow metadata DB + **pg-warehouse** (data warehouse)
- **Redis** — Celery broker
- **dbt** — transformations (runs in container)
- **Elementary** — data observability (dbt package)
- **Metabase** (optional) — BI / viz

## Project Structure

```
.
├── dags/                    # Airflow DAG definitions
│   ├── github_to_postgres_and_dbt.py
│   ├── github_contributions_to_postgres_and_dbt.py
│   └── utils/               # Utility functions
├── tests/                   # pytest (DAGs & dags/utils)
├── scripts/                 # dbt lineage scripts
├── docker/                  # Custom Compose images
│   ├── dbt/Dockerfile       # dbt_cli image
│   └── dag-tests/Dockerfile # pytest image (Airflow base)
├── dbt_project/             # dbt project files
│   ├── models/              # dbt models
│   └── profiles.yml         # dbt connection configuration
├── config/                  # Configuration files
├── logs/                    # Airflow logs
└── docker-compose.yaml
```

## Prerequisites

- Docker and Docker Compose installed
- Git
- Access to GitHub API (token required)

## Setup Instructions

### 1. Start Core Services

```bash
docker compose up -d
```

This will start:
- PostgreSQL (Airflow metadata)
- Redis (Celery broker)
- Airflow webserver (port 8080)
- Airflow scheduler
- Airflow worker
- dbt CLI container

### 2. Set Up Data Warehouse (pg-warehouse)

The data warehouse PostgreSQL instance runs as a standalone container on the Compose network:

```bash
docker run --name pg-warehouse \
  --network airflow-github-project_default \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=mysecretpassword \
  -e POSTGRES_DB=postgres \
  -p 5432:5432 \
  -d postgres:latest
```

**Note**: Ensure the password matches `dbt_project/profiles.yml`.

### 3. Set Up Metabase (Optional)

```bash
docker pull metabase/metabase:latest
docker run -d -p 3000:3000 --name metabase metabase/metabase
```

Access Metabase at `http://localhost:3000`.

### 4. Rebuild dbt Container (After Any Changes)

If you modify `docker/dbt/Dockerfile` or need to update dependencies:

```bash
docker compose build dbt
docker compose up -d dbt
```

### 5. Set Up Elementary (Data Observability)

1. **Install dbt packages**:
   ```bash
   docker exec -it dbt_cli dbt deps --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
   ```

2. **Deploy Elementary models**:
   ```bash
   docker exec -it dbt_cli dbt run --select elementary --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
   ```

3. **Run Elementary tests**:
   ```bash
   docker exec -it dbt_cli dbt test --select elementary --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
   ```

4. **Generate Elementary CLI profile** (optional):
   ```bash
   docker exec -it dbt_cli dbt run-operation elementary.generate_elementary_cli_profile --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
   ```

5. **Generate observability report**:
   ```bash
   docker exec -it dbt_cli edr report --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
   ```

## Configuration

### dbt

The dbt project is configured in `dbt_project/profiles.yml`:

```yaml
postgres:
  target: dev
  outputs:
    dev:
      type: postgres
      host: pg-warehouse
      user: postgres
      password: mysecretpassword
      port: 5432
      dbname: postgres
      schema: public
```

### Airflow Connections

Configure in Airflow UI (Admin → Connections):

1. **PostgreSQL** (`postgres_default`):
   - Connection Type: Postgres
   - Host: `pg-warehouse`
   - Schema: `public`
   - Login: `postgres`
   - Password: `mysecretpassword`
   - Port: `5432`

2. **GitHub API** (`github_api_conn`):
   - Connection Type: HTTP
   - Host: `https://api.github.com`
   - Extra: `{"token": "your_github_token"}`

## Accessing Services

- **Airflow Web UI**: http://localhost:8080 (default: `airflow` / `airflow`)
- **Metabase**: http://localhost:3000
- **PostgreSQL (pg-warehouse)**: `localhost:5432`

## Common Operations

```bash
# Start all services
docker compose up -d

# Stop all services
docker compose down

# View logs
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver

# Rebuild dbt container
docker compose build dbt
docker compose up -d dbt

# Run dbt manually
docker exec -it dbt_cli dbt run --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
```

## Tests

Tests cover DAG loading (`DagBag`), `dags/utils/`, and related helpers. They run in a separate Compose service (`dag-tests`) built from `docker/dag-tests/Dockerfile` (extends `apache/airflow:2.8.4`).

1. **Build the test image** (after editing `docker/dag-tests/Dockerfile`):

   ```bash
   docker compose build dag-tests
   ```

2. **Run the full suite**:

   ```bash
   docker compose --profile tests run --rm -T -w /repo --entrypoint python3 dag-tests -m pytest tests/ -q
   ```

3. **Pre-commit**: the `pytest (dag-tests)` hook runs the same command when files under `dags/` or `tests/` change.

Configuration: `pytest.ini` (`pythonpath = dags`, `addopts = -rN`), shared Airflow env in `tests/conftest.py`.

## Cleanup

```bash
docker compose down
docker rm -f pg-warehouse metabase
docker volume rm airflow-github-project_postgres-db-volume  # WARNING: deletes all data
```

## Troubleshooting

### dbt Cannot Connect to pg-warehouse

1. Verify the container is running:
   ```bash
   docker ps | grep pg-warehouse
   ```

2. Check network connectivity:
   ```bash
   docker exec -it dbt_cli ping pg-warehouse
   ```

3. Verify credentials match between `profiles.yml` and the container environment variables.

### Container Network Issues

Ensure all containers are on the same Docker network:

```bash
docker network ls
docker network inspect airflow-github-project_default
```

## Development

### Adding New DAGs

1. Create a new Python file in `dags/`
2. Define your DAG using Airflow decorators
3. The DAG will be automatically discovered by Airflow

### Modifying dbt Models

1. Edit SQL files in `dbt_project/models/`
2. Test locally: `docker exec -it dbt_cli dbt run --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt`
3. Models will be run automatically by Airflow DAGs
