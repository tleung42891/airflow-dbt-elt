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

# Staged dbt model files and their full lineage
CHANGED_SQL=$(git diff --cached --name-only | grep -E '^dbt_project/models/.*\.sql$' || true)
if [ -n "$CHANGED_SQL" ]; then
  SELECTOR=$(echo "$CHANGED_SQL" | sed 's|^dbt_project/||' | while read -r p; do echo "+path:${p}+"; done | tr '\n' ' ')
  LINEAGE_MODELS=$(cd "$PROJECT_DIR" && dbt ls --select $SELECTOR --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" 2>/dev/null | sed -n 's/^model\.[^.]*\.//p' | sort -u || true)
else
  LINEAGE_MODELS=""
fi

# Generate artifacts and test coverage
cd "$PROJECT_DIR"
dbt docs generate --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR"
dbt-coverage compute test --run-artifacts-dir "$PROJECT_DIR/target" --cov-report "$COVERAGE_DIR/test.json" > "$COVERAGE_DIR/test_report.txt" 2>&1

# Models with zero tests (exclude elementary)
ALL_ZERO=$(grep '(0 tests)' "$COVERAGE_DIR/test_report.txt" | grep -vi 'elementary' | awk '{print $1}' | sed 's/^[^.]*\.//' | sort -u || true)

# Restrict to lineage when we have modified models
if [ -n "$CHANGED_SQL" ] && [ -n "$LINEAGE_MODELS" ]; then
  echo "Modified models (full lineage) checked for at least one test."
  comm -12 <(echo "$LINEAGE_MODELS") <(echo "$ALL_ZERO") > "$REPORT" || true
else
  echo "No staged dbt model files; reporting all models without tests (non-elementary)."
  [ -n "$ALL_ZERO" ] && echo "$ALL_ZERO" > "$REPORT" || : > "$REPORT"
fi

# Output
echo "Test coverage: $COVERAGE_DIR/test.json, $COVERAGE_DIR/test_report.txt"
if [ ! -s "$REPORT" ]; then
  echo "All models have at least one test."
else
  echo "Models without at least one test:"
  sed 's/^/  /' "$REPORT"
  if [ -n "$CHANGED_SQL" ] && [ -n "$LINEAGE_MODELS" ]; then
    echo "Some models have no tests; failing."
    exit 1
  fi
fi
