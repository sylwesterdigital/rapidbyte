
![No Docker](./images/what.png)

# Running locally without Docker

> Work in progress.
>
> This project can be run without Docker if there are already local PostgreSQL instances available. The Docker-based `dev-up` flow is mainly a convenience wrapper around:
>
> 1. starting PostgreSQL
> 2. building the host binary and plugins
> 3. seeding the database
> 4. running a pipeline

## What `just dev-up` normally does

The `dev-up` recipe is equivalent to:

```sh
docker compose up -d --wait
just build-all
just seed 1000000
````

If PostgreSQL is already running locally, the Docker step can be skipped and the rest can be run manually.

## Requirements

* Rust toolchain with `cargo`
* A working shell environment (`zsh` is fine)
* Local PostgreSQL instance(s)
* Any additional toolchain required by `scripts/build-plugin.sh`

## Important note about PostgreSQL

The default `run` recipe sets:

```sh
TEST_SOURCE_PG_HOST=localhost TEST_SOURCE_PG_PORT=5433
TEST_DEST_PG_HOST=localhost TEST_DEST_PG_PORT=5433
```

That means the default local workflow expects PostgreSQL to already be reachable on `localhost`, typically on port `5433`.

Depending on the fixture or environment configuration, source and destination may either:

* use the same PostgreSQL instance, or
* be overridden elsewhere by the pipeline config or environment

If local clusters use different ports, export the correct values before running.

## Build without `just`

### Build the host binary

Release mode:

```sh
cargo build --release
```

Debug mode:

```sh
cargo build
```

### Build plugins

Release mode:

```sh
./scripts/build-plugin.sh plugins/sources/postgres release
./scripts/build-plugin.sh plugins/destinations/postgres release
./scripts/build-plugin.sh plugins/transforms/sql release
./scripts/build-plugin.sh plugins/transforms/validate release
```

Debug mode:

```sh
./scripts/build-plugin.sh plugins/sources/postgres debug
./scripts/build-plugin.sh plugins/destinations/postgres debug
./scripts/build-plugin.sh plugins/transforms/sql debug
./scripts/build-plugin.sh plugins/transforms/validate debug
```

### Copy plugin artifacts into `target/plugins`

Release mode:

```sh
mkdir -p target/plugins/sources target/plugins/destinations target/plugins/transforms

cp plugins/sources/postgres/target/wasm32-wasip2/release/source_postgres.wasm \
  target/plugins/sources/postgres.wasm

cp plugins/destinations/postgres/target/wasm32-wasip2/release/dest_postgres.wasm \
  target/plugins/destinations/postgres.wasm

cp plugins/transforms/sql/target/wasm32-wasip2/release/transform_sql.wasm \
  target/plugins/transforms/sql.wasm

cp plugins/transforms/validate/target/wasm32-wasip2/release/transform_validate.wasm \
  target/plugins/transforms/validate.wasm
```

Debug mode:

```sh
mkdir -p target/plugins/sources target/plugins/destinations target/plugins/transforms

cp plugins/sources/postgres/target/wasm32-wasip2/debug/source_postgres.wasm \
  target/plugins/sources/postgres.wasm

cp plugins/destinations/postgres/target/wasm32-wasip2/debug/dest_postgres.wasm \
  target/plugins/destinations/postgres.wasm

cp plugins/transforms/sql/target/wasm32-wasip2/debug/transform_sql.wasm \
  target/plugins/transforms/sql.wasm

cp plugins/transforms/validate/target/wasm32-wasip2/debug/transform_validate.wasm \
  target/plugins/transforms/validate.wasm
```

### Strip WASM artifacts in release mode

Only needed for release builds:

```sh
./scripts/strip-wasm.sh target/plugins/sources/postgres.wasm target/plugins/sources/postgres.wasm
./scripts/strip-wasm.sh target/plugins/destinations/postgres.wasm target/plugins/destinations/postgres.wasm
./scripts/strip-wasm.sh target/plugins/transforms/sql.wasm target/plugins/transforms/sql.wasm
./scripts/strip-wasm.sh target/plugins/transforms/validate.wasm target/plugins/transforms/validate.wasm
```

## Seed local PostgreSQL

The standard seed step is:

```sh
./scripts/seed-dev.sh 1000000
```

This assumes the script is compatible with the local PostgreSQL setup.

If the script expects Docker-specific ports, credentials, or database names, either:

* update the environment so the script points at the local clusters, or
* seed the database manually

## Run locally without Docker

### Run interactive dev mode

Release:

```sh
RAPIDBYTE_PLUGIN_DIR=target/plugins \
./target/release/rapidbyte dev
```

Debug:

```sh
RAPIDBYTE_PLUGIN_DIR=target/plugins \
./target/debug/rapidbyte dev
```

### Run a pipeline

Example using the existing fixture:

Release:

```sh
RAPIDBYTE_PLUGIN_DIR=target/plugins \
TEST_SOURCE_PG_HOST=localhost \
TEST_SOURCE_PG_PORT=5433 \
TEST_DEST_PG_HOST=localhost \
TEST_DEST_PG_PORT=5433 \
./target/release/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg.yaml
```

Debug:

```sh
RAPIDBYTE_PLUGIN_DIR=target/plugins \
TEST_SOURCE_PG_HOST=localhost \
TEST_SOURCE_PG_PORT=5433 \
TEST_DEST_PG_HOST=localhost \
TEST_DEST_PG_PORT=5433 \
./target/debug/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg.yaml
```

To pass additional flags:

```sh
RAPIDBYTE_PLUGIN_DIR=target/plugins \
TEST_SOURCE_PG_HOST=localhost \
TEST_SOURCE_PG_PORT=5433 \
TEST_DEST_PG_HOST=localhost \
TEST_DEST_PG_PORT=5433 \
./target/release/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg.yaml -vv
```

## Example: using two existing local PostgreSQL clusters

If source and destination run on different local ports, set them explicitly:

```sh
RAPIDBYTE_PLUGIN_DIR=target/plugins \
TEST_SOURCE_PG_HOST=localhost \
TEST_SOURCE_PG_PORT=5432 \
TEST_DEST_PG_HOST=localhost \
TEST_DEST_PG_PORT=5434 \
./target/release/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg.yaml
```

## Equivalent “no Docker” workflow

This is the rough manual replacement for `just dev-up` when PostgreSQL is already running locally:

```sh
cargo build --release

./scripts/build-plugin.sh plugins/sources/postgres release
./scripts/build-plugin.sh plugins/destinations/postgres release
./scripts/build-plugin.sh plugins/transforms/sql release
./scripts/build-plugin.sh plugins/transforms/validate release

mkdir -p target/plugins/sources target/plugins/destinations target/plugins/transforms

cp plugins/sources/postgres/target/wasm32-wasip2/release/source_postgres.wasm \
  target/plugins/sources/postgres.wasm

cp plugins/destinations/postgres/target/wasm32-wasip2/release/dest_postgres.wasm \
  target/plugins/destinations/postgres.wasm

cp plugins/transforms/sql/target/wasm32-wasip2/release/transform_sql.wasm \
  target/plugins/transforms/sql.wasm

cp plugins/transforms/validate/target/wasm32-wasip2/release/transform_validate.wasm \
  target/plugins/transforms/validate.wasm

./scripts/strip-wasm.sh target/plugins/sources/postgres.wasm target/plugins/sources/postgres.wasm
./scripts/strip-wasm.sh target/plugins/destinations/postgres.wasm target/plugins/destinations/postgres.wasm
./scripts/strip-wasm.sh target/plugins/transforms/sql.wasm target/plugins/transforms/sql.wasm
./scripts/strip-wasm.sh target/plugins/transforms/validate.wasm target/plugins/transforms/validate.wasm

./scripts/seed-dev.sh 1000000

RAPIDBYTE_PLUGIN_DIR=target/plugins \
TEST_SOURCE_PG_HOST=localhost \
TEST_SOURCE_PG_PORT=5433 \
TEST_DEST_PG_HOST=localhost \
TEST_DEST_PG_PORT=5433 \
./target/release/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg.yaml
```

## Recommended next improvements for this README

This section should eventually be updated with:

* the exact database names, users, and passwords expected by `seed-dev.sh`
* whether source and destination are meant to be separate databases or just separate roles/schemas
* required PostgreSQL extensions
* a documented example for running against two separate local clusters
* a small wrapper script such as `scripts/dev-up-local.sh` to replace the long manual command sequence

## Troubleshooting

### `seed-dev.sh` fails

Check whether the script assumes:

* Docker container hostnames
* fixed ports such as `5433`
* specific usernames or passwords
* specific databases already created

### Pipeline cannot connect to PostgreSQL

Verify that:

* PostgreSQL is running
* the chosen ports are correct
* environment variables match the local setup
* the pipeline fixture is using the same connection settings expected by the environment

### Plugin files are missing

Verify that:

* all `build-plugin.sh` commands succeeded
* the `.wasm` files were copied into `target/plugins`
* `RAPIDBYTE_PLUGIN_DIR=target/plugins` is set when running the binary
