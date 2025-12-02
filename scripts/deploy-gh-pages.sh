#!/usr/bin/env bash
set -euo pipefail
BRANCH=${1:-gh-pages}
ORIGIN=${2:-origin}
SITEDIR=${3:-site}
if ! command -v git >/dev/null 2>&1; then echo "git not found" >&2; exit 1; fi
ROOT=$(git rev-parse --show-toplevel)
WORK="$ROOT/.gh-pages"

git fetch "$ORIGIN"
if [ -d "$WORK" ]; then git worktree remove "$WORK" -f; fi
git worktree add -B "$BRANCH" "$WORK" HEAD
rsync -a --delete "$SITEDIR/" "$WORK/"
: > "$WORK/.nojekyll"
cd "$WORK"
  git add -A
  git commit -m "Deploy site $(date -Is)" || true
  git push "$ORIGIN" "$BRANCH"
echo "Deployed to $BRANCH"
