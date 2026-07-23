# Airflow GitHub Project

This project sets up an Airflow-based ETL pipeline for extracting GitHub data, loading it into PostgreSQL, and transforming it with dbt via [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/).

## Architecture

- **Airflow** (CeleryExecutor) — orchestration; custom image with Cosmos + an isolated dbt venv
- **PostgreSQL** — Airflow metadata DB + **pg-warehouse** (data warehouse)
- **Redis** — Celery broker
- **dbt** (Cosmos `ExecutionMode.LOCAL`) — transforms run on the Airflow worker against the mounted `dbt_project/`
- **Elementary** — optional observability post-step on Cosmos DAGs (also usable via legacy `dbt_cli`)
- **Metabase** (optional) — BI / viz
- **`dbt_cli` container** (optional) — slim image for pre-commit, manual `docker exec`, and the legacy `run_dbt` DAG

### DAG Orchestration

Ingestion loads raw tables into Postgres, then triggers the shared Cosmos DAG with a runtime `select` in `conf`. The UI always shows the **full** model graph; tasks outside the selector are skipped.

```text
github_to_postgres_and_dbt ──► run_dbt_cosmos   conf.select=tag:pulls+
github_contributions_…     ──► run_dbt_cosmos   conf.select=tag:contributions+
                               run_dbt_cosmos   (empty select = full rebuild)
                               run_dbt          (legacy docker-exec; manual only)
```

- **`github_to_postgres_and_dbt`** (`@daily`) — closed PRs → `raw_github_pulls` → triggers `run_dbt_cosmos` with `select=tag:pulls+`, plus `elementary` / `drop_stale_relations`.
- **`github_contributions_to_postgres_and_dbt`** (`@daily`) — contribution counts → `raw_github_contributions` → same DAG with `select=tag:contributions+`.
- **`run_dbt_cosmos`** (`schedule=None`, `max_active_runs=1`, `max_active_tasks=4`) — one Airflow task per model (full `path:models` graph), then **`TestBehavior.AFTER_ALL`**. `resolve_selection` + per-task skips honor `conf.select` (`tag:<name>` / `tag:<name>+`). Optional Elementary and `drop_stale_relations` post-steps.
- **`run_dbt`** — legacy path: one Bash task `docker exec`s into `dbt_cli`.

Staging models are tagged in `dbt_project.yml` (`pulls` / `contributions`); marts are included via dbt’s `+` graph operator on those tags.

## Project Structure

```
.
├── dags/
│   ├── github_to_postgres_and_dbt.py
│   ├── github_contributions_to_postgres_and_dbt.py
│   ├── run_dbt_cosmos.py    # Cosmos: full graph + runtime tag skip
│   ├── run_dbt.py           # Legacy docker-exec dbt (manual)
│   ├── table_configs/       # Raw table DDL / upsert YAML
│   └── utils/
├── dbt_project/             # dbt project (mounted into Airflow + dbt_cli)
├── docker/
│   ├── airflow/Dockerfile   # Runtime: Airflow + Cosmos + dbt venv
│   ├── dag-tests/Dockerfile # Extends Airflow image + pytest
│   └── dbt/Dockerfile       # Slim dbt_cli (legacy / pre-commit)
├── scripts/                 # pre-commit helpers (modified run/test, coverage)
├── tests/                   # pytest (DagBag + utils)
└── docker-compose.yaml
```

## Prerequisites

- Docker and Docker Compose installed
- Git
- Access to GitHub API (token required)

## Setup Instructions

### 1. Start Core Services

Build the custom Airflow image (Cosmos + isolated dbt venv), then start the stack:

```bash
docker compose build
docker compose up -d
```

This will start:
- PostgreSQL (Airflow metadata)
- Redis (Celery broker)
- Airflow webserver (port 8080)
- Airflow scheduler
- Airflow worker
- dbt CLI container

`airflow-init` migrates the Airflow DB and runs `dbt deps` into the mounted `dbt_project/` (Cosmos LOCAL; per-task `install_deps` is off).

#### After changing `dbt_project/packages.yml`

No image rebuild — packages land on the host mount. Re-run init:

```bash
docker compose up airflow-init
```

That refreshes `dbt_project/dbt_packages/` (and `package-lock.yml`). Restart the scheduler if Cosmos DAGs were already failing to parse:

```bash
docker compose restart airflow-scheduler airflow-worker
```

#### After changing `docker/airflow/Dockerfile`

```bash
docker compose build airflow-webserver airflow-scheduler airflow-worker
docker compose up -d
```

### 2. Set Up Data Warehouse (pg-warehouse)

The data warehouse PostgreSQL instance runs as a standalone container on the Compose network:

```bash
docker run --name pg-warehouse \
  --network airflow-dbt-elt_default \
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

1. **Install dbt packages** (prefer init — same mount Cosmos uses):
   ```bash
   docker compose up airflow-init
   # or legacy: docker exec -it dbt_cli dbt deps --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
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

### Airflow (Cosmos)

Airflow services use the custom image from `docker/airflow/Dockerfile` (not the stock `apache/airflow` tag alone). Notable pins:

| Item | Version / constraint |
|------|----------------------|
| Base image | `apache/airflow:2.8.4-python3.11` |
| `astronomer-cosmos` | `1.13.1` (last line supporting Airflow 2.8; 1.14+ needs ≥2.9) |
| Isolated dbt venv | `/opt/airflow/dbt_venv` with `dbt-core==1.11.10`, `dbt-postgres==1.10.0` |

The dbt project is mounted into Airflow at `/opt/airflow/dbt_project` for Cosmos `ExecutionMode.LOCAL`. Profiles for Cosmos tasks come from the Airflow connection `postgres_default` (ProfileMapping), not from `profiles.yml`. The separate `dbt_cli` container remains the path used by `run_dbt`, pre-commit, and manual `docker exec`.

### dbt

The `dbt` Compose service (`container_name: dbt_cli`) is built from `docker/dbt/Dockerfile`. Current image requirements:

| Item | Version / constraint |
|------|----------------------|
| Base image | `python:3.13-slim` |
| OS packages | `git` |
| `elementary-data` | `[postgres]` extra (PyPI; version follows image build) |
| `dbt-core` | `1.11.10` |
| `dbt-postgres` | `1.10.0` (latest Postgres adapter on PyPI; compatible with dbt-core 1.11) |
| `pre-commit` | `4.6.0` |
| `dbt-coverage` | `0.4.2` |

After changing the Dockerfile, rebuild with `docker compose build dbt` (see setup step 4).

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
# Build custom images, then start all services
docker compose build
docker compose up -d

# Stop all services
docker compose down

# View logs
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver

# After editing dbt_project/packages.yml (no image rebuild)
docker compose up airflow-init
docker compose restart airflow-scheduler airflow-worker

# Rebuild Airflow image (Cosmos / dbt venv / Dockerfile changes)
docker compose build airflow-webserver
docker compose up -d

# Rebuild dbt container (legacy CLI / pre-commit image)
docker compose build dbt
docker compose up -d dbt

# Run dbt manually (legacy dbt_cli)
docker exec -it dbt_cli dbt run --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
```

## Tests

Tests cover DAG loading (`DagBag`), `dags/utils/`, and related helpers. They run in a Compose service (`dag-tests`) that extends the Airflow image and adds pytest (`docker/dag-tests/Dockerfile`).

1. **Build images** (Airflow first, then dag-tests via compose `additional_contexts`):

   ```bash
   docker compose build
   docker compose --profile tests build dag-tests
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
docker network inspect airflow-dbt-elt_default
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

**Tag-based selection**: the staging layer is tagged in `dbt_project.yml` (`stg_github_pulls` → `pulls`, `stg_github_contributions` → `contributions`). Downstream marts are selected via dbt's graph operator (`tag:pulls+`, `tag:contributions+`), so a new mart is picked up automatically as long as it `ref()`s its tagged staging model. To scope a run/test to one domain:

```bash
docker exec -it dbt_cli dbt build --select tag:pulls+ --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
```
