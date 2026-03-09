#!/usr/bin/env bash

set -euo pipefail

git_common_dir="$(git rev-parse --path-format=absolute --git-common-dir)"
repo_root="$(dirname "$git_common_dir")"
compose_project="$(basename "$repo_root")"

cd "$repo_root"
docker compose -p "$compose_project" up -d --wait
