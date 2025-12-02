#!/usr/bin/env bash
set -euo pipefail
CONFIG=${1:-site/tailwind.config.js}
IN=${2:-site/styles/input.css}
OUT=${3:-site/assets/site.css}
if ! command -v npx >/dev/null 2>&1; then echo 'npx not found. Install Node.js from https://nodejs.org/' >&2; exit 1; fi
npx tailwindcss -c "$CONFIG" -i "$IN" -o "$OUT" --minify
echo "Built CSS -> $OUT"
