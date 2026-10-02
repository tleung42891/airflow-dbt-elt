#!/usr/bin/env bash
# lib.sh — shared helpers + config defaults for the scripts/setup/ modules.
#
# Sourced by every scripts/setup/*.sh module and by setup.sh.
# All config comes from environment variables with defaults below, so each
# module can also be run standalone, e.g.:
#   ./scripts/setup/warehouse.sh
#   GITHUB_TOKEN=ghp_xxx ./scripts/setup/airflow.sh

# --- Paths / compose project -------------------------------------------------
SETUP_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SETUP_LIB_DIR/../.." && pwd)"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-$(basename "$PROJECT_DIR" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9_-')}"
NETWORK_NAME="${COMPOSE_PROJECT_NAME}_default"
export COMPOSE_PROJECT_NAME

cd "$PROJECT_DIR"

# --- Warehouse defaults (match dbt_project/profiles.yml) ---------------------
WAREHOUSE_CONTAINER="${WAREHOUSE_CONTAINER:-pg-warehouse}"
WAREHOUSE_USER="${WAREHOUSE_USER:-postgres}"
WAREHOUSE_PASSWORD="${WAREHOUSE_PASSWORD:-mysecretpassword}"
WAREHOUSE_DB="${WAREHOUSE_DB:-postgres}"
WAREHOUSE_PORT="${WAREHOUSE_PORT:-5432}"

# --- Feature toggles / bootstrap values (setup.sh exports these) --------------
INSTALL_DOCKER="${INSTALL_DOCKER:-false}"
SKIP_BUILD="${SKIP_BUILD:-false}"
FORCE_PROFILES="${FORCE_PROFILES:-false}"
WITH_FLOWER="${WITH_FLOWER:-false}"
UNPAUSE="${UNPAUSE:-false}"
GITHUB_TOKEN="${GITHUB_TOKEN:-}"
GITHUB_REPOS="${GITHUB_REPOS:-}"
GITHUB_USERNAMES="${GITHUB_USERNAMES:-}"

# --- Logging ------------------------------------------------------------------
log()  { printf '\033[1;34m[setup]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[setup]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[setup]\033[0m %s\n' "$*" >&2; exit 1; }

# --- Shared helpers -------------------------------------------------------------
airflow_cli() {
  docker compose exec -T airflow-webserver airflow "$@"
}

container_running() {
  docker ps --format '{{.Names}}' | grep -qx "$1"
}

container_exists() {
  docker ps -a --format '{{.Names}}' | grep -qx "$1"
}
