#!/usr/bin/env bash
set -euo pipefail

repo_root="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd
)"
build_script="$repo_root/scripts/build-plugin.sh"
plugin_path="plugins/sources/postgres"
target_dir="$(mktemp -d "${TMPDIR:-/tmp}/rapidbyte-plugin-build.XXXXXX")"

cleanup() {
  rm -rf "$target_dir"
}
trap cleanup EXIT

host_build_dir="$target_dir/release/build"
wasm_build_dir="$target_dir/wasm32-wasip2/release/build"
host_marker="$host_build_dir/stale-marker"
wasm_marker="$wasm_build_dir/stale-marker"

cd "$repo_root"

CARGO_TARGET_DIR="$target_dir" "$build_script" "$plugin_path" release

mkdir -p "$host_build_dir" "$wasm_build_dir"
touch "$host_marker" "$wasm_marker"

RUSTC_BOOTSTRAP=1 CARGO_TARGET_DIR="$target_dir" \
  "$build_script" "$plugin_path" release

test ! -e "$host_marker"
test ! -e "$wasm_marker"

touch "$host_marker" "$wasm_marker"

RUSTC_BOOTSTRAP=1 CARGO_TARGET_DIR="$target_dir" \
  "$build_script" "$plugin_path" release

test -e "$host_marker"
test -e "$wasm_marker"

unset RUSTC_BOOTSTRAP || true
CARGO_TARGET_DIR="$target_dir" "$build_script" "$plugin_path" release

test ! -e "$host_marker"
test ! -e "$wasm_marker"
