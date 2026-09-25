#!/usr/bin/env zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ "${1:-}" == --test-* ]]; then
  REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
  exec bb "$REPO_ROOT/test/swarmforge/pack_web_test.bb" "$@"
else
  exec bb "$SCRIPT_DIR/pack_web.bb" "$@"
fi
