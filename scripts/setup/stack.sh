#!/usr/bin/env bash
# setup-stack.sh — build images and start the core Compose stack.
#
# Env: SKIP_BUILD=true to reuse existing images,
#      WITH_FLOWER=true to also start the Flower service (compose profile).
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

if [[ "$SKIP_BUILD" == true ]]; then
  log "Skipping image build (SKIP_BUILD)."
else
  log "Building images (Airflow + Cosmos + dbt venv, dbt_cli)..."
  docker compose build
fi

COMPOSE_UP_ARGS=(up -d)
if [[ "$WITH_FLOWER" == true ]]; then
  COMPOSE_UP_ARGS=(--profile flower up -d)
fi
log "Starting core services (airflow-init migrates the DB and runs dbt deps)..."
docker compose "${COMPOSE_UP_ARGS[@]}"
