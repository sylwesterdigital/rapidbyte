# Design: Connector → Plugin Rename

**Date:** 2026-03-07
**Status:** Approved
**Approach:** Full atomic rename, no backward compatibility

## Motivation

Connectors and transforms are really plugins providing different functionality based on their type. The codebase already hints at this (`~/.rapidbyte/plugins/` directory, `RAPIDBYTE_PLUGIN_DIR` intent). This refactor aligns terminology across the entire codebase to use "plugin" consistently.

## Decisions

1. **Flat plugin model** — `PluginKind { Source, Destination, Transform }` (no nested hierarchy)
2. **Directory structure by kind** — `plugins/sources/postgres/`, `plugins/destinations/postgres/`, `plugins/transforms/sql/`
3. **YAML simplification** — `use: postgres` instead of `use: source-postgres` (kind inferred from YAML section)
4. **WIT package bump** — `rapidbyte:plugin@5.0.0` (breaking change, clean break)
5. **Protocol version** — `V4` → `V5`
6. **No backward compatibility** — clean cut

## Type Renames

| Current | New |
|---------|-----|
| `ConnectorRole` | `PluginKind` |
| `ConnectorManifest` | `PluginManifest` |
| `ConnectorInfo` | `PluginInfo` |
| `ConnectorError` (SDK) | `PluginError` |
| `ConnectorEntry` (CLI) | `PluginEntry` |
| `ResolvedConnectors` | `ResolvedPlugins` |

## Function Renames

| Current | New |
|---------|-----|
| `parse_connector_ref()` | `parse_plugin_ref()` |
| `connector_search_dirs()` | `plugin_search_dirs()` |
| `resolve_connector_path()` | `resolve_plugin_path()` |
| `load_connector_manifest()` | `load_plugin_manifest()` |
| `resolve_connectors()` | `resolve_plugins()` |
| `validate_connector()` | `validate_plugin()` |

## File Renames

| Current | New |
|---------|-----|
| `crates/rapidbyte-runtime/src/connector.rs` | `crates/rapidbyte-runtime/src/plugin.rs` |
| `crates/rapidbyte-sdk/src/connector.rs` | `crates/rapidbyte-sdk/src/plugin.rs` |
| `crates/rapidbyte-sdk/macros/src/connector.rs` | `crates/rapidbyte-sdk/macros/src/plugin.rs` |
| `crates/rapidbyte-cli/src/commands/connectors.rs` | `crates/rapidbyte-cli/src/commands/plugins.rs` |
| `wit/rapidbyte-connector.wit` | `wit/rapidbyte-plugin.wit` |
| `docs/CONNECTOR_DEV.md` | `docs/PLUGIN_DEV.md` |

## Directory Structure

### Repository layout

```
plugins/
  sources/
    postgres/          (was connectors/source-postgres)
  destinations/
    postgres/          (was connectors/dest-postgres)
  transforms/
    sql/               (was connectors/transform-sql)
    validate/          (was connectors/transform-validate)
```

Cargo package names stay as-is (`source-postgres`, `dest-postgres`, etc.).

### Build output

```
target/plugins/
  sources/
    postgres.wasm      (cargo produces source_postgres.wasm, renamed at copy)
  destinations/
    postgres.wasm
  transforms/
    sql.wasm
    validate.wasm
```

### User plugin directory

```
~/.rapidbyte/plugins/
  sources/
    postgres.wasm
  destinations/
    postgres.wasm
  transforms/
    sql.wasm
```

## Resolution Logic

`resolve_plugin_path(name: &str, kind: PluginKind)`:
1. Map kind → subdirectory: `Source` → `sources/`, `Destination` → `destinations/`, `Transform` → `transforms/`
2. Convert hyphens to underscores in name, append `.wasm`
3. Search `$RAPIDBYTE_PLUGIN_DIR/<kind_dir>/<name>.wasm` then `~/.rapidbyte/plugins/<kind_dir>/<name>.wasm`

## Pipeline YAML

```yaml
# Before
source:
  use: source-postgres
destination:
  use: dest-postgres
transforms:
  - use: transform-sql

# After
source:
  use: postgres
destination:
  use: postgres
transforms:
  - use: sql
```

## WIT Interface

- Package: `rapidbyte:plugin@5.0.0`
- `connector-role` → `plugin-kind`
- `connector-error` → `plugin-error`
- `connector-instance` (state-scope) → `plugin-instance`
- `run-summary.role` → `run-summary.kind`
- Interface names (`source`, `destination`, `transform`, `host`) unchanged
- World names (`rapidbyte-source`, etc.) unchanged

## SDK Macro

- `#[connector(source)]` → `#[plugin(source)]`
- Generated bindings path: `rapidbyte::connector::*` → `rapidbyte::plugin::*`
- `static CONNECTOR` → `static PLUGIN`

## Environment Variables

- `RAPIDBYTE_CONNECTOR_DIR` → `RAPIDBYTE_PLUGIN_DIR`

## CLI

- `rapidbyte connectors` → `rapidbyte plugins`
- Scaffold default output: `plugins/<kind>s/<name>`

## WASM Custom Section

`rapidbyte_manifest_v1` stays — it's a section format version, not tied to protocol version.

## Documentation Updates

- `docs/CONNECTOR_DEV.md` → `docs/PLUGIN_DEV.md`
- `README.md`: "Connectors" → "Plugins", CLI examples
- `CLAUDE.md`: project structure, commands, architecture notes
- `docs/PROTOCOL.md`: protocol version, type names

## Test Updates

- `tests/e2e/src/harness/connectors.rs` → `plugins.rs`
- All fixture YAMLs: `use: postgres` instead of `use: source-postgres`
- Bench fixtures: same YAML changes
- Env var in test harness: `RAPIDBYTE_PLUGIN_DIR`
