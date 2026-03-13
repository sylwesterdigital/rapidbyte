#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: scripts/build-plugin.sh <plugin-path> <release|debug>
EOF
}

resolve_path() {
  local path="$1"
  if [[ "$path" = /* ]]; then
    printf '%s\n' "$path"
  else
    printf '%s\n' "$repo_root/$path"
  fi
}

resolve_command_path() {
  local cmd="$1"
  if [[ "$cmd" == */* ]]; then
    printf '%s\n' "$cmd"
  elif command -v "$cmd" >/dev/null 2>&1; then
    command -v "$cmd"
  else
    printf '%s\n' "$cmd"
  fi
}

fingerprint_contents() {
  local rustc_cmd rustc_path rustc_version
  rustc_cmd="${RUSTC:-rustc}"
  rustc_path="$(resolve_command_path "$rustc_cmd")"
  rustc_version="$("$rustc_cmd" --version --verbose 2>/dev/null || "$rustc_cmd" --version 2>/dev/null || true)"

  cat <<EOF
mode=$mode
target=wasm32-wasip2
plugin=$plugin_root
rustc=$rustc_path
$rustc_version
rustc_wrapper=${RUSTC_WRAPPER:-}
rustup_toolchain=${RUSTUP_TOOLCHAIN:-}
rustc_bootstrap=${RUSTC_BOOTSTRAP:-}
rustflags=${RUSTFLAGS:-}
cargo_encoded_rustflags=${CARGO_ENCODED_RUSTFLAGS:-}
EOF
}

if [[ $# -ne 2 ]]; then
  usage
  exit 2
fi

repo_root="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd
)"
plugin_root="$(resolve_path "$1")"
mode="$2"

if [[ ! -d "$plugin_root" ]] || [[ ! -f "$plugin_root/Cargo.toml" ]]; then
  echo "rapidbyte: plugin path is invalid: $1" >&2
  exit 2
fi

case "$mode" in
  release)
    cargo_args=(--release)
    ;;
  debug)
    cargo_args=()
    ;;
  *)
    usage
    exit 2
    ;;
esac

if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
  if [[ "${CARGO_TARGET_DIR}" = /* ]]; then
    target_dir="${CARGO_TARGET_DIR}"
  else
    target_dir="$plugin_root/${CARGO_TARGET_DIR}"
  fi
else
  target_dir="$plugin_root/target"
fi

mkdir -p "$target_dir"

host_profile_dir="$target_dir/$mode"
wasm_profile_dir="$target_dir/wasm32-wasip2/$mode"
host_build_dir="$host_profile_dir/build"
wasm_build_dir="$wasm_profile_dir/build"
host_fingerprint_dir="$host_profile_dir/.fingerprint"
wasm_fingerprint_dir="$wasm_profile_dir/.fingerprint"
state_file="$target_dir/.rapidbyte-build-env"
fingerprint="$(fingerprint_contents)"

needs_cleanup=0
cleanup_reason=""

if [[ -f "$state_file" ]]; then
  stored_fingerprint="$(<"$state_file")"
  if [[ "$stored_fingerprint" != "$fingerprint" ]]; then
    needs_cleanup=1
    cleanup_reason="build environment changed"
  fi
elif [[ -d "$host_build_dir" ]] || [[ -d "$wasm_build_dir" ]] || [[ -d "$host_fingerprint_dir" ]] || [[ -d "$wasm_fingerprint_dir" ]]; then
  needs_cleanup=1
  cleanup_reason="existing plugin target state predates fingerprinting"
fi

if [[ "$needs_cleanup" -eq 1 ]]; then
  echo "rapidbyte: clearing stale plugin build state for $plugin_root ($cleanup_reason)" >&2
  rm -rf -- \
    "$host_build_dir" \
    "$wasm_build_dir" \
    "$host_fingerprint_dir" \
    "$wasm_fingerprint_dir"
fi

(
  cd "$plugin_root"
  cargo build "${cargo_args[@]}"
)

printf '%s\n' "$fingerprint" >"$state_file"
