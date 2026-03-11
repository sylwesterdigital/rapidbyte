#!/usr/bin/env sh
set -eu

root="${1:-.}"
plugins_dir="${root}/plugins"

if [ ! -d "${plugins_dir}" ]; then
  echo "plugins directory not found: ${plugins_dir}" >&2
  exit 1
fi

matches="$(rg -n --glob '*.rs' --glob 'Cargo.toml' 'UNIMPLEMENTED' "${plugins_dir}" || true)"

if [ -n "${matches}" ]; then
  echo "Committed plugin scaffold stubs detected under plugins/:" >&2
  printf '%s\n' "${matches}" >&2
  exit 1
fi
