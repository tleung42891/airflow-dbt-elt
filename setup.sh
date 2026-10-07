#!/usr/bin/env bash
#
# setup.sh — friction-less, out-of-the-box setup for the Airflow + dbt + Cosmos stack.
#
# Thin orchestrator: parses flags, exports config, and stacks the modules in
# scripts/setup/. Each module is also runnable standalone, e.g.:
#   ./scripts/setup/warehouse.sh
#   GITHUB_TOKEN=ghp_xxx ./scripts/setup/airflow.sh
#
# Module order:
#   docker.sh     Docker + Compose prerequisites (optional install)
#   config.sh     .env (AIRFLOW_UID) + dbt_project/profiles.yml
#   stack.sh      docker compose build + up (core stack, optional Flower)
#   warehouse.sh  pg-warehouse on the compose network
#   airflow.sh    connections (postgres_default, github_api_conn) + variables
#   metabase.sh   [--with-metabase]
#   elementary.sh [--with-elementary]
#   dag-tests.sh  [--with-tests]
#   teardown.sh   [--teardown]
#
# Run ./setup.sh --help for all flags.

set -euo pipefail

SETUP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/scripts/setup"
source "$SETUP_DIR/lib.sh"

WITH_METABASE=false
WITH_ELEMENTARY=false
WITH_TESTS=false
TEARDOWN=false

usage() {
  cat <<'EOF'
Usage: ./setup.sh [options]

Core options:
  --install-docker          Install Docker Desktop via Homebrew if missing (macOS)
  --skip-build              Skip `docker compose build` (reuse existing images)
  --warehouse-password PW   pg-warehouse password (default: mysecretpassword;
                            also written into profiles.yml + postgres_default)
  --force-profiles          Overwrite dbt_project/profiles.yml even if it exists

Airflow bootstrap (skips the manual UI steps in the README):
  --github-token TOKEN      GitHub token for the github_api_conn connection
                            (or set env GITHUB_TOKEN)
  --github-repos JSON       Airflow variable `github_repos`,
                            e.g. '["your-user/your-repo"]'
  --github-usernames JSON   Airflow variable `github_usernames`,
                            e.g. '["your-user","a-teammate"]'
  --unpause                 Unpause the two ingestion DAGs after setup

Optional components:
  --with-metabase           Start Metabase on port 3000
  --with-elementary         Deploy Elementary models + tests into the warehouse
  --with-flower             Start Flower (Celery UI) on port 5555
  --with-tests              Build the dag-tests image (compose profile `tests`)
  --all                     Shorthand for all four --with-* flags

Lifecycle:
  --teardown                Stop and remove the stack, pg-warehouse, and Metabase
                            (keeps volumes; delete manually per README Cleanup)
  -h, --help                Show this help

Modules live in scripts/setup/ and can be run individually.

Examples:
  ./setup.sh --github-token ghp_xxx --github-repos '["me/repo"]' \
             --github-usernames '["me"]' --unpause
  ./setup.sh --all --with-elementary
  ./setup.sh --teardown
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-docker)      INSTALL_DOCKER=true ;;
    --skip-build)          SKIP_BUILD=true ;;
    --warehouse-password)  WAREHOUSE_PASSWORD="${2:?missing value for --warehouse-password}"; shift ;;
    --force-profiles)      FORCE_PROFILES=true ;;
    --github-token)        GITHUB_TOKEN="${2:?missing value for --github-token}"; shift ;;
    --github-repos)        GITHUB_REPOS="${2:?missing value for --github-repos}"; shift ;;
    --github-usernames)    GITHUB_USERNAMES="${2:?missing value for --github-usernames}"; shift ;;
    --unpause)             UNPAUSE=true ;;
    --with-metabase)       WITH_METABASE=true ;;
    --with-elementary)     WITH_ELEMENTARY=true ;;
    --with-flower)         WITH_FLOWER=true ;;
    --with-tests)          WITH_TESTS=true ;;
    --all)                 WITH_METABASE=true; WITH_ELEMENTARY=true; WITH_FLOWER=true; WITH_TESTS=true ;;
    --teardown)            TEARDOWN=true ;;
    -h|--help)             usage; exit 0 ;;
    *)                     die "Unknown option: $1 (see --help)" ;;
  esac
  shift
done

# Export config consumed by the modules (see scripts/setup/lib.sh for defaults).
export INSTALL_DOCKER SKIP_BUILD FORCE_PROFILES WITH_FLOWER UNPAUSE
export WAREHOUSE_CONTAINER WAREHOUSE_USER WAREHOUSE_PASSWORD WAREHOUSE_DB WAREHOUSE_PORT
export GITHUB_TOKEN GITHUB_REPOS GITHUB_USERNAMES

if [[ "$TEARDOWN" == true ]]; then
  exec "$SETUP_DIR/teardown.sh"
fi

# --- Stack the modules ---------------------------------------------------------
"$SETUP_DIR/docker.sh"
"$SETUP_DIR/config.sh"
"$SETUP_DIR/stack.sh"
"$SETUP_DIR/warehouse.sh"
"$SETUP_DIR/airflow.sh"

[[ "$WITH_METABASE" == true ]]   && "$SETUP_DIR/metabase.sh"
[[ "$WITH_ELEMENTARY" == true ]] && "$SETUP_DIR/elementary.sh"
[[ "$WITH_TESTS" == true ]]      && "$SETUP_DIR/dag-tests.sh"

# --- Summary --------------------------------------------------------------------
echo
log "Setup complete."
echo "  Airflow UI:      http://localhost:8080  (airflow / airflow)"
echo "  pg-warehouse:    localhost:${WAREHOUSE_PORT}  (${WAREHOUSE_USER} / ${WAREHOUSE_PASSWORD})"
[[ "$WITH_METABASE" == true ]] && echo "  Metabase:        http://localhost:3000"
[[ "$WITH_FLOWER" == true ]]   && echo "  Flower:          http://localhost:5555"
if [[ -z "$GITHUB_TOKEN" || -z "$GITHUB_REPOS" || -z "$GITHUB_USERNAMES" ]]; then
  echo
  warn "Remaining manual steps (or re-run with the matching flags):"
  [[ -z "$GITHUB_TOKEN" ]]     && warn "  - github_api_conn connection:  --github-token ghp_xxx"
  [[ -z "$GITHUB_REPOS" ]]     && warn "  - github_repos variable:       --github-repos '[\"owner/repo\"]'"
  [[ -z "$GITHUB_USERNAMES" ]] && warn "  - github_usernames variable:   --github-usernames '[\"user\"]'"
fi
exit 0
