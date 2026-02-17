#!/usr/bin/env bash
# Run only models that are modified (staged) and their downstream. From repo root.
set -e
PROJECT_DIR=/usr/app/dbt
PROFILES_DIR=/usr/app/dbt

CHANGED_SQL=$(git diff --cached --name-only | grep -E '^dbt_project/models/.*\.sql$' || true)

if [ -z "$CHANGED_SQL" ]; then
  dbt run --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR"
else
  SELECTOR=$(echo "$CHANGED_SQL" | sed 's|^dbt_project/||' | while read -r p; do echo "path:${p}+"; done | tr '\n' ' ')
  dbt run --select $SELECTOR --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR"
fi
