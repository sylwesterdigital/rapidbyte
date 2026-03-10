#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
hook_path="$repo_root/.githooks/pre-push"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  if [[ "$haystack" != *"$needle"* ]]; then
    fail "expected output to contain '$needle', got: $haystack"
  fi
}

make_repo() {
  local temp_dir
  temp_dir="$(mktemp -d)"
  mkdir -p "$temp_dir/.githooks" "$temp_dir/src"
  cp "$hook_path" "$temp_dir/.githooks/pre-push"
  chmod +x "$temp_dir/.githooks/pre-push"
  cat >"$temp_dir/Cargo.toml" <<'EOF'
[package]
name = "hook-test"
version = "0.1.0"
edition = "2021"
EOF
  cat >"$temp_dir/src/main.rs" <<'EOF'
fn main() {
    println!("hi");
}
EOF
  (
    cd "$temp_dir"
    git init -q
    git config user.name test
    git config user.email test@example.com
    git add Cargo.toml src/main.rs
    git commit -q -m "initial"
  )
  echo "$temp_dir"
}

test_formats_and_aborts() {
  local temp_dir output status staged
  temp_dir="$(make_repo)"
  cat >"$temp_dir/src/main.rs" <<'EOF'
fn main(){println!("hi");}
EOF
  (
    cd "$temp_dir"
    git add src/main.rs
    git commit -q -m "unformatted"
    set +e
    output="$("$temp_dir/.githooks/pre-push" 2>&1)"
    status=$?
    set -e
    printf '%s' "$status" >"$temp_dir/.status"
    printf '%s' "$output" >"$temp_dir/.output"
  )
  status="$(cat "$temp_dir/.status")"
  output="$(cat "$temp_dir/.output")"
  [ "$status" -ne 0 ] || fail "expected pre-push to abort after formatting"
  assert_contains "$output" "pre-push: applied formatting updates."
  staged="$(cd "$temp_dir" && git diff --cached --name-only)"
  assert_contains "$staged" "src/main.rs"
}

test_clean_repo_passes() {
  local temp_dir output status
  temp_dir="$(make_repo)"
  (
    cd "$temp_dir"
    set +e
    output="$("$temp_dir/.githooks/pre-push" 2>&1)"
    status=$?
    set -e
    printf '%s' "$status" >"$temp_dir/.status"
    printf '%s' "$output" >"$temp_dir/.output"
  )
  status="$(cat "$temp_dir/.status")"
  output="$(cat "$temp_dir/.output")"
  [ "$status" -eq 0 ] || fail "expected clean repo to pass, got output: $output"
}

test_formats_and_aborts
test_clean_repo_passes

echo "pre-push hook tests passed"
