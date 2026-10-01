#!/usr/bin/env bash
# setup-metabase.sh — start Metabase on port 3000, attached to the compose network.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

if container_running metabase; then
  log "Metabase already running."
elif container_exists metabase; then
  docker start metabase >/dev/null
  log "Started existing Metabase container."
else
  log "Starting Metabase on port 3000 (attached to $NETWORK_NAME so it can reach $WAREHOUSE_CONTAINER)..."
  docker run -d -p 3000:3000 --name metabase --network "$NETWORK_NAME" \
    --restart unless-stopped metabase/metabase:latest >/dev/null
fi
