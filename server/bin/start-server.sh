#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TARGET_DIR="$SERVER_DIR/target"

JAR="$(ls -1 "$TARGET_DIR"/*-jar-with-dependencies.jar 2>/dev/null | head -n 1 || true)"
if [[ -z "${JAR:-}" ]]; then
  echo "Error: server fat-jar not found under $TARGET_DIR" >&2
  echo "Build it with: mvn -q -pl server -am package -DskipTests" >&2
  exit 1
fi

PORT="${PORT:-${JIMSQL_PORT:-8821}}"
HOST="${HOST:-${JIMSQL_HOST:-0.0.0.0}}"
DATADIR="${DATADIR:-${JIMSQL_DATADIR:-$SERVER_DIR/src/main/resources/datadir}}"
PROTOCOL="${PROTOCOL:-${JIMSQL_PROTOCOL:-}}"
JAVA_OPTS="${JAVA_OPTS:-}"

if [[ -n "${JIMSQL_WIRELOG:-}" ]]; then
  JAVA_OPTS="$JAVA_OPTS -Djimsql.wirelog=$JIMSQL_WIRELOG"
fi

ARGS=("$PORT" "$HOST" "$DATADIR")
if [[ -n "$PROTOCOL" ]]; then
  ARGS+=("$PROTOCOL")
fi

echo "Running: java $JAVA_OPTS -jar \"$JAR\" ${ARGS[*]}"
exec java $JAVA_OPTS -jar "$JAR" "${ARGS[@]}"