````md
![No Docker](./images/what.png)

# Running Rapidbyte locally without Docker

This guide is the full local setup for running Rapidbyte **without Docker** on a machine that already has PostgreSQL running.

It includes:

- what Rapidbyte is
- what the normal Docker flow does
- how to replace it locally
- what files to create
- what commands to run
- what errors are likely
- how to fix them

---

## What Rapidbyte is

Rapidbyte moves data from one place to another.

A simple example:

- read rows from one Postgres database
- optionally transform or validate them
- write them into another Postgres database

A one-off script can also move data once. Rapidbyte starts to matter when the move needs to be repeatable, testable, configurable, and reusable.

---

## What the normal dev flow does

The normal Docker-oriented dev flow is:

```sh
docker compose up -d --wait
just build-all
just seed 1000000
````

That means:

1. start Postgres with Docker
2. build the host binary
3. build the plugins
4. seed test databases
5. run a pipeline

If PostgreSQL is already running locally, Docker is not required, but the rest still needs to happen.

---

## What was discovered during the no-Docker setup

The Docker-oriented setup assumes things that may not be true on a local machine:

* PostgreSQL is on `localhost:5433`
* there is a role named `postgres`
* that role uses password `postgres`
* the test databases are named `source_test` and `dest_test`

That is why the default setup failed on a local machine where:

* Postgres was running on `localhost:5433`
* the actual role was `smielniczuk`
* there was no role named `postgres`

That caused this kind of error:

```text
FATAL: role "postgres" does not exist
```

So the no-Docker setup needs:

* a separate seed script
* a separate pipeline YAML
* environment variables in a `.env` file
* dedicated database names that do not collide with existing data

---

## Safety first

Do **not** point this at databases you already care about.

The seed step drops and recreates test tables and schema inside the target databases.

Safe approach:

* create dedicated databases only for Rapidbyte local testing
* use names like:

  * `rapidbyte_source_test`
  * `rapidbyte_dest_test`

That way the script does not touch unrelated databases in the same Postgres cluster.

---

## Requirements

You need:

* PostgreSQL already running locally
* a Postgres role you can log in with
* Rust and Cargo
* shell access
* the repo checked out locally

Example local Postgres start command that was used:

```sh
/opt/homebrew/opt/postgresql@14/bin/pg_ctl -D /opt/homebrew/var/postgresql@14 -o "-p 5433" -l /opt/homebrew/var/postgresql@14/server_5433.log start
```

---

## Step 1: install Rust and Cargo if needed

Check whether Cargo exists:

```sh
cargo --version
rustc --version
```

If `cargo` is missing, install Rust:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

After installation, reload the current shell:

```sh
. "$HOME/.cargo/env"
```

Verify again:

```sh
cargo --version
rustc --version
```

### Important note

Only run the shell setup line for the shell currently being used.

For `zsh`, use:

```sh
. "$HOME/.cargo/env"
```

Do **not** run the `fish`, `tcsh`, `pwsh`, `xonsh`, or other shell examples unless actually using those shells.

If Cargo still is not available in new terminals, add this once:

```sh
echo '. "$HOME/.cargo/env"' >> ~/.zshrc
source ~/.zshrc
```

---

## Step 2: find the local PostgreSQL role

The default Docker scripts assume the role is `postgres`, but the local machine may use a different role.

Check roles with:

```sh
psql -h localhost -p 5433 -d postgres -c '\du'
```

If that database is not available, use:

```sh
psql -h localhost -p 5433 -d template1 -c '\du'
```

Example result:

```text
Role name    | Attributes
-------------+------------------------------------------------------------
appuser      |
drupal       |
smielniczuk  | Superuser, Create role, Create DB, Replication, Bypass RLS
studio333    |
```

In that case the local role to use is:

```text
smielniczuk
```

---

## Step 3: create `.env`

Create a `.env` file in the repo root.

Example:

```dotenv
TEST_SOURCE_PG_HOST=localhost
TEST_SOURCE_PG_PORT=5433
TEST_DEST_PG_HOST=localhost
TEST_DEST_PG_PORT=5433

PGUSER=smielniczuk
SOURCE_DB=rapidbyte_source_test
DEST_DB=rapidbyte_dest_test
DEST_SCHEMA=raw

RAPIDBYTE_PLUGIN_DIR=target/plugins
```

### Why these values

* `TEST_SOURCE_PG_HOST` / `TEST_SOURCE_PG_PORT` tell Rapidbyte where the source Postgres is
* `TEST_DEST_PG_HOST` / `TEST_DEST_PG_PORT` tell Rapidbyte where the destination Postgres is
* `PGUSER` is the local Postgres role
* `SOURCE_DB` and `DEST_DB` are dedicated safe test databases
* `DEST_SCHEMA` is the target schema in the destination
* `RAPIDBYTE_PLUGIN_DIR` tells Rapidbyte where the built `.wasm` plugins are

### If there is no password

Do not try to force an empty password through `.env` if the YAML validator does not like it.

For no-password local setups, it is cleaner to put:

```yaml
password: ""
```

directly in the no-Docker pipeline YAML.

### Load `.env` into the current shell

```sh
set -a
source .env
set +a
```

Check:

```sh
echo $PGUSER
echo $SOURCE_DB
echo $DEST_DB
echo $RAPIDBYTE_PLUGIN_DIR
```

---

## Step 4: build the host binary

From the repo root:

```sh
cargo build --release
```

If build succeeds, continue.

If not, fix the build error before moving on.

---

## Step 5: build the plugins

Run:

```sh
./scripts/build-plugin.sh plugins/sources/postgres release
./scripts/build-plugin.sh plugins/destinations/postgres release
./scripts/build-plugin.sh plugins/transforms/sql release
./scripts/build-plugin.sh plugins/transforms/validate release
```

### If there are warnings

Warnings like this are not fatal:

```text
warning: unused import: `ArrayRef`
Finished `release` profile [optimized] target(s)
```

That means the plugin built successfully.

Only stop if the command ends with an actual error.

---

## Step 6: copy plugins into `target/plugins`

Create the output folders:

```sh
mkdir -p target/plugins/sources target/plugins/destinations target/plugins/transforms
```

Copy the built `.wasm` files:

```sh
cp plugins/sources/postgres/target/wasm32-wasip2/release/source_postgres.wasm target/plugins/sources/postgres.wasm
cp plugins/destinations/postgres/target/wasm32-wasip2/release/dest_postgres.wasm target/plugins/destinations/postgres.wasm
cp plugins/transforms/sql/target/wasm32-wasip2/release/transform_sql.wasm target/plugins/transforms/sql.wasm
cp plugins/transforms/validate/target/wasm32-wasip2/release/transform_validate.wasm target/plugins/transforms/validate.wasm
```

Strip the WASM artifacts:

```sh
./scripts/strip-wasm.sh target/plugins/sources/postgres.wasm target/plugins/sources/postgres.wasm
./scripts/strip-wasm.sh target/plugins/destinations/postgres.wasm target/plugins/destinations/postgres.wasm
./scripts/strip-wasm.sh target/plugins/transforms/sql.wasm target/plugins/transforms/sql.wasm
./scripts/strip-wasm.sh target/plugins/transforms/validate.wasm target/plugins/transforms/validate.wasm
```

---

## Step 7: use a dedicated no-Docker seed script

The original `scripts/seed-dev.sh` is Docker-specific.

It hardcodes:

```sh
PG_USER="postgres"
PG_PASS="postgres"
```

and uses Docker-shaped database assumptions.

Do **not** use it unchanged on a local setup that has a different role.

Create and use a separate script such as:

```text
scripts/seed-nodocker-dev.sh
```

This script should:

* use env vars instead of hardcoded role/password
* use safe DB names like `rapidbyte_source_test` and `rapidbyte_dest_test`
* create databases if missing
* create `pgcrypto` if needed
* seed the source tables
* clean the destination schema

### Make it executable

```sh
chmod +x ./scripts/seed-nodocker-dev.sh
```

### Run it

For a small safe test:

```sh
./scripts/seed-nodocker-dev.sh
```

That uses the default `100` rows.

For a larger test:

```sh
./scripts/seed-nodocker-dev.sh 1000000
```

### What success looks like

Something like:

```text
Seeding Postgres at localhost:5433 as smielniczuk (100 rows per table)...
Source DB: rapidbyte_source_test
Dest DB:   rapidbyte_dest_test
Schema:    raw
NOTICE:  table "users" does not exist, skipping
NOTICE:  table "orders" does not exist, skipping
NOTICE:  schema "raw" does not exist, skipping
Done.
```

Those `NOTICE` lines are normal if the test tables/schema did not exist yet.

---

## Step 8: use a dedicated no-Docker pipeline YAML

Do **not** overwrite the original Docker-oriented pipeline file.

Keep the original:

```text
tests/fixtures/pipelines/simple_pg_to_pg.yaml
```

Create a dedicated no-Docker version:

```text
tests/fixtures/pipelines/simple_pg_to_pg_nodocker.yaml
```

The original file contains Docker-shaped hardcoded values like:

```yaml
user: postgres
password: postgres
database: source_test
database: dest_test
```

That is why the local run failed.

### The no-Docker YAML should look like this

```yaml
version: "1.0"
pipeline: test_pg_to_pg_nodocker

source:
  use: postgres
  config:
    host: ${TEST_SOURCE_PG_HOST}
    port: ${TEST_SOURCE_PG_PORT}
    user: ${PGUSER}
    password: ""
    database: ${SOURCE_DB}
  streams:
    - name: users
      sync_mode: full_refresh
    - name: orders
      sync_mode: full_refresh

destination:
  use: postgres
  config:
    host: ${TEST_DEST_PG_HOST}
    port: ${TEST_DEST_PG_PORT}
    user: ${PGUSER}
    password: ""
    database: ${DEST_DB}
    schema: ${DEST_SCHEMA}
  write_mode: append

state:
  backend: sqlite
```

### Why `password: ""`

Using:

```yaml
password: ${PGPASSWORD}
```

with an empty password caused this validation error:

```text
Configuration validation failed for plugin 'postgres':
  - null is not of type "string"
```

So for local no-password setups, use:

```yaml
password: ""
```

---

## Step 9: run the pipeline

First load `.env`:

```sh
set -a
source .env
set +a
```

Then run:

```sh
./target/release/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg_nodocker.yaml -vv
```

### What success looks like

Something like:

```text
✓ Pipeline 'test_pg_to_pg_nodocker' completed in 0.1s

  Records     200 read → 200 written
  Data        55.1 MiB read → 40.6 KiB written
  Throughput  1K rows/s | 376.4 MiB/s
  Streams     2 completed | parallelism: 8
```

That means the no-Docker path is working.

---

## Full command sequence

After the files are in place:

```sh
set -a
source .env
set +a

cargo build --release

./scripts/build-plugin.sh plugins/sources/postgres release
./scripts/build-plugin.sh plugins/destinations/postgres release
./scripts/build-plugin.sh plugins/transforms/sql release
./scripts/build-plugin.sh plugins/transforms/validate release

mkdir -p target/plugins/sources target/plugins/destinations target/plugins/transforms

cp plugins/sources/postgres/target/wasm32-wasip2/release/source_postgres.wasm target/plugins/sources/postgres.wasm
cp plugins/destinations/postgres/target/wasm32-wasip2/release/dest_postgres.wasm target/plugins/destinations/postgres.wasm
cp plugins/transforms/sql/target/wasm32-wasip2/release/transform_sql.wasm target/plugins/transforms/sql.wasm
cp plugins/transforms/validate/target/wasm32-wasip2/release/transform_validate.wasm target/plugins/transforms/validate.wasm

./scripts/strip-wasm.sh target/plugins/sources/postgres.wasm target/plugins/sources/postgres.wasm
./scripts/strip-wasm.sh target/plugins/destinations/postgres.wasm target/plugins/destinations/postgres.wasm
./scripts/strip-wasm.sh target/plugins/transforms/sql.wasm target/plugins/transforms/sql.wasm
./scripts/strip-wasm.sh target/plugins/transforms/validate.wasm target/plugins/transforms/validate.wasm

chmod +x ./scripts/seed-nodocker-dev.sh
./scripts/seed-nodocker-dev.sh 1000000

./target/release/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg_nodocker.yaml -vv
```

---

## Troubleshooting

## `cargo: command not found`

Install Rust:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Then reload the shell:

```sh
. "$HOME/.cargo/env"
```

Verify:

```sh
cargo --version
rustc --version
```

---

## `FATAL: role "postgres" does not exist`

Cause:

* the local Postgres role is not `postgres`
* the Docker-shaped script or YAML is still hardcoded to `postgres`

Fix:

* check local roles:

```sh
psql -h localhost -p 5433 -d postgres -c '\du'
```

or:

```sh
psql -h localhost -p 5433 -d template1 -c '\du'
```

* set `PGUSER` in `.env` to the real local role
* use the no-Docker seed script
* use the no-Docker pipeline YAML

---

## `permission denied` when running the seed script

Cause:

* the script is not executable

Fix:

```sh
chmod +x ./scripts/seed-nodocker-dev.sh
```

Then rerun it.

---

## `no such file or directory` for the seed script

Cause:

* wrong filename or wrong path

Fix:

* check the actual filename in `scripts/`
* run the exact filename

Example mistake:

```sh
./scripts/seed-nodocker.dev.sh
```

Example actual file:

```sh
./scripts/seed-nodocker-dev.sh
```

---

## `Configuration validation failed ... null is not of type "string"`

Cause:

* the pipeline YAML is using `${PGPASSWORD}` but the local password is empty

Fix:

* in the no-Docker pipeline YAML, use:

```yaml
password: ""
```

---

## Huge debug logs ending with `CONNECTION_FAILED`

Cause:

* Rapidbyte itself loaded fine
* the plugins loaded fine
* the pipeline config is wrong

Usually one of these is still wrong:

* `user`
* `password`
* `database`
* `schema`
* `host`
* `port`

This happened when the pipeline was still using the Docker-style values:

```yaml
user: postgres
password: postgres
database: source_test
database: dest_test
```

Fix:

* use `simple_pg_to_pg_nodocker.yaml`
* make sure `.env` matches the safe local test databases

---

## `gen_random_uuid()` fails

Cause:

* `pgcrypto` is not enabled in the seeded database

Fix:

* add this in the no-Docker seed script, or run manually:

```sh
psql -h localhost -p 5433 -U "$PGUSER" -d "$SOURCE_DB" -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto;'
psql -h localhost -p 5433 -U "$PGUSER" -d "$DEST_DB" -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto;'
```

---

## The seed script worked but the pipeline still fails

Cause:

* seed created one set of DB names
* pipeline YAML points to another set of DB names

Example:

* seed created `rapidbyte_source_test` and `rapidbyte_dest_test`
* pipeline still points to `source_test` and `dest_test`

Fix:

* check `.env`
* check `simple_pg_to_pg_nodocker.yaml`
* make sure they match exactly

---

## The plugin build printed warnings

Cause:

* warnings are not necessarily errors

If the output ends with something like:

```text
Finished `release` profile [optimized] target(s)
```

then the build succeeded.

---

## GitHub image does not show in README

If the README header image does not appear and GitHub shows a 404, the path is wrong.

Example:

```md
![No Docker](./images/what.png)
```

That only works if the image is actually at:

```text
images/what.png
```

relative to the README file.

Check:

```sh
find . -iname 'what.png'
```

Then fix the Markdown path so it matches the real location.

---

## Useful file locations

Docker-oriented pipeline:

```text
tests/fixtures/pipelines/simple_pg_to_pg.yaml
```

No-Docker pipeline:

```text
tests/fixtures/pipelines/simple_pg_to_pg_nodocker.yaml
```

Seed script:

```text
scripts/seed-nodocker-dev.sh
```

---

## Minimal version

If everything is already prepared:

```sh
set -a && source .env && set +a && ./scripts/seed-nodocker-dev.sh 1000000 && ./target/release/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg_nodocker.yaml -vv
```

---

## Final result

A working no-Docker setup means:

* PostgreSQL is running locally
* Rapidbyte builds locally
* plugins build locally
* a dedicated seed script seeds safe test databases
* a dedicated no-Docker pipeline reads from source and writes to destination
* the original Docker-oriented files stay untouched

```

This version is intentionally the “everything someone needs to proceed, including where it breaks and why” dump.
```
