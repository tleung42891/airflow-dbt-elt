#!/usr/bin/env bash
# setup-elementary.sh — deploy Elementary models + run its tests via dbt_cli.
#
# Requires: core stack up (dbt packages installed by airflow-init), pg-warehouse running.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

log "Deploying Elementary models (dbt packages were installed by airflow-init)..."
docker exec dbt_cli dbt run --select elementary --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt

log "Running Elementary tests..."
docker exec dbt_cli dbt test --select elementary --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt \
  || warn "Elementary tests reported failures (non-fatal for setup)."
