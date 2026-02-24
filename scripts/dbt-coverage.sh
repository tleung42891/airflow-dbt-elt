#!/usr/bin/env bash
# Run dbt-coverage for modified models and full lineage; require at least one test per model.
# Run from container with -w /repo. Uses staged .sql to determine modified.
# Usage: docker exec -w /repo dbt_cli bash /repo/scripts/dbt-coverage.sh
set -e
PROJECT_DIR=/usr/app/dbt
PROFILES_DIR=/usr/app/dbt
COVERAGE_DIR=/repo/coverage
REPORT="$COVERAGE_DIR/models_without_tests.txt"

export PATH="/usr/local/bin:$PATH"
mkdir -p "$COVERAGE_DIR"
trap 'rm -rf "$COVERAGE_DIR"' EXIT

# Staged dbt model files and schema files (test definitions)
CHANGED_SQL=$(git diff --cached --name-only | grep -E '^dbt_project/models/.*\.sql$' || true)
CHANGED_YAML=$(git diff --cached --name-only | grep -E '^dbt_project/models/.*\.(yml|yaml)$' || true)
if [ -n "$CHANGED_SQL" ]; then
  SELECTOR=$(echo "$CHANGED_SQL" | sed 's|^dbt_project/||' | while read -r p; do echo "+path:${p}+"; done | tr '\n' ' ')
  LINEAGE_MODELS=$(cd "$PROJECT_DIR" && dbt ls --select $SELECTOR --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" 2>/dev/null | sed -n 's/^model\.[^.]*\.//p' | sort -u || true)
else
  LINEAGE_MODELS=""
fi

# Generate artifacts and test coverage (quiet; only script summary is printed)
cd "$PROJECT_DIR"
dbt docs generate --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" >/dev/null 2>&1
dbt-coverage compute test --run-artifacts-dir "$PROJECT_DIR/target" --cov-report "$COVERAGE_DIR/test.json" > "$COVERAGE_DIR/test_report.txt" 2>&1

# Models with zero tests (exclude elementary)
ALL_ZERO=$(grep '(0 tests)' "$COVERAGE_DIR/test_report.txt" | grep -vi 'elementary' | awk '{print $1}' | sed 's/^[^.]*\.//' | sort -u || true)

# Restrict to lineage when we have modified .sql; otherwise check all when schema changed
if [ -n "$CHANGED_SQL" ] && [ -n "$LINEAGE_MODELS" ]; then
  comm -12 <(echo "$LINEAGE_MODELS") <(echo "$ALL_ZERO") > "$REPORT" || true
else
  [ -n "$ALL_ZERO" ] && echo "$ALL_ZERO" > "$REPORT" || : > "$REPORT"
fi

# Output
if [ ! -s "$REPORT" ]; then
  echo "All models have at least one test."
else
  sed 's/^/  /' "$REPORT"
  echo "Add at least one test for each model above."
  # Fail when staged .sql (lineage) or staged schema .yml changed
  if { [ -n "$CHANGED_SQL" ] && [ -n "$LINEAGE_MODELS" ]; } || [ -n "$CHANGED_YAML" ]; then
    exit 1
  fi
fi
