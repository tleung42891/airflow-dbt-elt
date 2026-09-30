#!/usr/bin/env bash
# setup-airflow.sh — wait for the webserver, then register connections/variables.
#
# Env: GITHUB_TOKEN      -> github_api_conn connection (skipped if empty)
#      GITHUB_REPOS      -> github_repos variable, JSON list (skipped if empty)
#      GITHUB_USERNAMES  -> github_usernames variable, JSON list (skipped if empty)
#      UNPAUSE=true      -> unpause the two ingestion DAGs
#      WAREHOUSE_*       -> values for the postgres_default connection
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

log "Waiting for the Airflow webserver (this includes airflow-init on first run — can take a few minutes)..."
for i in $(seq 1 90); do
  if curl -fsS http://localhost:8080/health >/dev/null 2>&1; then
    break
  fi
  [[ $i -eq 90 ]] && die "Airflow webserver not healthy after ~7.5 minutes. Check: docker compose logs airflow-init airflow-webserver"
  sleep 5
done
log "Airflow webserver is up."

log "Registering Airflow connection: postgres_default -> $WAREHOUSE_CONTAINER"
airflow_cli connections delete postgres_default >/dev/null 2>&1 || true
airflow_cli connections add postgres_default \
  --conn-type postgres \
  --conn-host "$WAREHOUSE_CONTAINER" \
  --conn-schema public \
  --conn-login "$WAREHOUSE_USER" \
  --conn-password "$WAREHOUSE_PASSWORD" \
  --conn-port "$WAREHOUSE_PORT" >/dev/null

if [[ -n "$GITHUB_TOKEN" ]]; then
  log "Registering Airflow connection: github_api_conn"
  airflow_cli connections delete github_api_conn >/dev/null 2>&1 || true
  airflow_cli connections add github_api_conn \
    --conn-type http \
    --conn-host "https://api.github.com" \
    --conn-extra "{\"token\": \"${GITHUB_TOKEN}\"}" >/dev/null
else
  warn "No GitHub token given (--github-token / GITHUB_TOKEN) — skipping github_api_conn."
  warn "The ingestion DAGs will fail at extract time until it is set."
fi

if [[ -n "$GITHUB_REPOS" ]]; then
  log "Setting Airflow variable: github_repos"
  airflow_cli variables set github_repos "$GITHUB_REPOS" >/dev/null
else
  warn "Variable github_repos not set — github_to_postgres_and_dbt will have no extract tasks."
fi

if [[ -n "$GITHUB_USERNAMES" ]]; then
  log "Setting Airflow variable: github_usernames"
  airflow_cli variables set github_usernames "$GITHUB_USERNAMES" >/dev/null
else
  warn "Variable github_usernames not set — github_contributions_to_postgres_and_dbt will have no extract tasks."
fi

if [[ "$UNPAUSE" == true ]]; then
  log "Unpausing ingestion DAGs..."
  airflow_cli dags unpause github_to_postgres_and_dbt >/dev/null 2>&1 || warn "Could not unpause github_to_postgres_and_dbt (not parsed yet?)"
  airflow_cli dags unpause github_contributions_to_postgres_and_dbt >/dev/null 2>&1 || warn "Could not unpause github_contributions_to_postgres_and_dbt (not parsed yet?)"
fi
