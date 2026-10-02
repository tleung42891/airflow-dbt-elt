#!/usr/bin/env bash
# setup-teardown.sh — stop and remove the compose stack, pg-warehouse, and Metabase.
# Keeps volumes; delete manually per README Cleanup.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

log "Tearing down compose stack (project: $COMPOSE_PROJECT_NAME)..."
docker compose --profile flower --profile tests down --remove-orphans || true

for c in "$WAREHOUSE_CONTAINER" metabase; do
  if container_exists "$c"; then
    log "Removing container: $c"
    docker rm -f "$c" >/dev/null
  fi
done

log "Done. Volumes were kept. To wipe Airflow metadata:"
log "  docker volume rm ${COMPOSE_PROJECT_NAME}_postgres-db-volume"
