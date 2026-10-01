#!/usr/bin/env bash
# setup-dag-tests.sh — build the dag-tests image (compose profile `tests`).
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

log "Building dag-tests image..."
docker compose --profile tests build dag-tests
log "Run the suite with:"
log "  docker compose --profile tests run --rm -T -w /repo --entrypoint python3 dag-tests -m pytest tests/ -q"
