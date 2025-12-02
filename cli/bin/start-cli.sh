#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLI_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TARGET_DIR="$CLI_DIR/target"

JAR="$(ls -1 "$TARGET_DIR"/*.jar 2>/dev/null | grep -v '^original-' | head -n 1 || true)"
if [[ -z "${JAR:-}" ]]; then
  echo "Error: CLI jar not found under $TARGET_DIR" >&2
  echo "Build it with: mvn -q -pl cli -am package -DskipTests" >&2
  exit 1
fi

echo "Running: java $JAVA_OPTS -jar \"$JAR\" $*"
exec java $JAVA_OPTS -jar "$JAR" "$@"