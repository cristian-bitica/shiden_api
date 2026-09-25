#!/usr/bin/env bash
# Runs the acceptance pipeline for every feature file under
# tests/acceptance/features/: gherkin-parser -> ir-dry-checker ->
# entrypoint generator -> generated pytest tests.
#
# Run from the project root (or any subdirectory of the worktree):
#   tests/acceptance/run_acceptance.sh
#
# Each feature's generator output maps to a step-handler module of the
# same stem under tests/acceptance/steps/ (e.g. daily_job_step_isolation
# -> tests.acceptance.steps.daily_job_step_isolation_steps). Add a new
# feature by adding both files.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

tmp_dir="./tmp/acceptance"
mkdir -p "$tmp_dir"

features_dir="tests/acceptance/features"
failures=0

for feature in "$features_dir"/*.feature; do
  stem="$(basename "$feature" .feature)"
  steps_module="tests.acceptance.steps.${stem}_steps"
  ir_json="${tmp_dir}/${stem}.json"
  dry_json="${tmp_dir}/${stem}.dry.json"
  generated_py="${tmp_dir}/generated_${stem}.py"

  echo "== ${stem} =="

  echo "-- gherkin-parser"
  gherkin-parser "$feature" "$ir_json"

  echo "-- ir-dry-checker"
  ir-dry-checker "$ir_json" "$dry_json"
  if python3 -c "import json,sys; sys.exit(1 if json.load(open('$dry_json'))['summary']['findings'] else 0)"; then
    :
  else
    echo "ir-dry-checker found duplication in ${feature}:"
    cat "$dry_json"
    failures=1
  fi

  echo "-- generate"
  uv run --active python -m tests.acceptance.runtime.generate \
    "$ir_json" "$steps_module" "$generated_py"

  echo "-- pytest"
  # `python -m pytest` (not the `pytest` script) so the repo root -- where
  # the `tests` package the generated file imports from lives -- is on
  # sys.path even though the generated file itself is outside tests/.
  if ! uv run --active python -m pytest "$generated_py" -q; then
    failures=1
  fi
done

exit "$failures"
