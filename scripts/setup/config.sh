#!/usr/bin/env bash
# setup-config.sh — generate .env (AIRFLOW_UID) and dbt_project/profiles.yml.
#
# Env: FORCE_PROFILES=true to overwrite an existing profiles.yml,
#      WAREHOUSE_* to override connection values (defaults in lib.sh).
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# --- .env: AIRFLOW_UID ---------------------------------------------------------
if [[ ! -f .env ]] || ! grep -q '^AIRFLOW_UID=' .env; then
  log "Writing AIRFLOW_UID=$(id -u) to .env"
  echo "AIRFLOW_UID=$(id -u)" >> .env
fi

# --- dbt_project/profiles.yml ----------------------------------------------------
PROFILES_FILE="dbt_project/profiles.yml"
if [[ -f "$PROFILES_FILE" && "$FORCE_PROFILES" != true ]]; then
  log "$PROFILES_FILE already exists — leaving it alone (use --force-profiles to regenerate)."
  if ! grep -q "password: ${WAREHOUSE_PASSWORD}" "$PROFILES_FILE"; then
    warn "profiles.yml password does not match WAREHOUSE_PASSWORD. dbt may fail to connect."
  fi
  exit 0
fi

log "Generating $PROFILES_FILE"
cat > "$PROFILES_FILE" <<EOF
postgres:
  target: dev
  outputs:
    dev:
      type: postgres
      host: ${WAREHOUSE_CONTAINER}
      user: ${WAREHOUSE_USER}
      password: ${WAREHOUSE_PASSWORD}
      port: ${WAREHOUSE_PORT}
      dbname: ${WAREHOUSE_DB}
      schema: public

elementary:
  outputs:
    default:
      type: postgres
      host: ${WAREHOUSE_CONTAINER}
      port: ${WAREHOUSE_PORT}
      user: ${WAREHOUSE_USER}
      password: ${WAREHOUSE_PASSWORD}
      dbname: ${WAREHOUSE_DB}
      schema: public_elementary
      threads: 4
EOF
