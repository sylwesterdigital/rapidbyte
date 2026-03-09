#!/usr/bin/env bash

set -euo pipefail

git_common_dir="$(git rev-parse --path-format=absolute --git-common-dir)"
repo_root="$(dirname "$git_common_dir")"
compose_project="$(basename "$repo_root")"

cd "$repo_root"
docker compose -p "$compose_project" up -d --wait

# Keep the repo-supported local benchmark credentials aligned with the
# committed docker-compose/environment-profile defaults. Existing containers can
# preserve an older postgres role password across config changes.
docker exec "${compose_project}-postgres-1" \
  psql -U postgres -d rapidbyte_test -c "ALTER USER postgres WITH PASSWORD 'postgres';" \
  >/dev/null
