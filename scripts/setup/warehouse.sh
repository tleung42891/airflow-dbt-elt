#!/usr/bin/env bash
# setup-warehouse.sh — start pg-warehouse on the compose network and wait for it.
#
# Env: WAREHOUSE_* to override container name / credentials (defaults in lib.sh).
# Requires the compose network to exist (run setup-stack.sh first).
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

docker network inspect "$NETWORK_NAME" >/dev/null 2>&1 \
  || die "Compose network $NETWORK_NAME not found — did 'docker compose up' succeed?"

if container_running "$WAREHOUSE_CONTAINER"; then
  log "$WAREHOUSE_CONTAINER already running."
elif container_exists "$WAREHOUSE_CONTAINER"; then
  log "Starting existing $WAREHOUSE_CONTAINER container..."
  docker start "$WAREHOUSE_CONTAINER" >/dev/null
else
  log "Creating $WAREHOUSE_CONTAINER on network $NETWORK_NAME (host port ${WAREHOUSE_PORT})..."
  docker run --name "$WAREHOUSE_CONTAINER" \
    --network "$NETWORK_NAME" \
    -e POSTGRES_USER="$WAREHOUSE_USER" \
    -e POSTGRES_PASSWORD="$WAREHOUSE_PASSWORD" \
    -e POSTGRES_DB="$WAREHOUSE_DB" \
    -p "${WAREHOUSE_PORT}:5432" \
    --restart unless-stopped \
    -d postgres:latest >/dev/null
fi

# Make sure it's attached to the compose network (covers pre-existing containers).
if ! docker inspect -f '{{range $k,$_ := .NetworkSettings.Networks}}{{$k}} {{end}}' "$WAREHOUSE_CONTAINER" | grep -qw "$NETWORK_NAME"; then
  log "Connecting $WAREHOUSE_CONTAINER to $NETWORK_NAME..."
  docker network connect "$NETWORK_NAME" "$WAREHOUSE_CONTAINER"
fi

log "Waiting for $WAREHOUSE_CONTAINER to accept connections..."
for i in $(seq 1 30); do
  docker exec "$WAREHOUSE_CONTAINER" pg_isready -U "$WAREHOUSE_USER" >/dev/null 2>&1 && break
  [[ $i -eq 30 ]] && die "$WAREHOUSE_CONTAINER never became ready."
  sleep 2
done
log "$WAREHOUSE_CONTAINER is ready."
