➜  rapidbyte git:(main) ✗ psql -h localhost -p 5433 -d postgres -c '\du'
                              List of roles
  Role name  |                         Attributes                         
-------------+------------------------------------------------------------
 appuser     | 
 drupal      | 
 smielniczuk | Superuser, Create role, Create DB, Replication, Bypass RLS
 studio333   | 

➜  rapidbyte git:(main) ✗ psql -h localhost -p 5433 -d template1 -c '\du'
                              List of roles
  Role name  |                         Attributes                         
-------------+------------------------------------------------------------
 appuser     | 
 drupal      | 
 smielniczuk | Superuser, Create role, Create DB, Replication, Bypass RLS
 studio333   | 

➜  rapidbyte git:(main) ✗ 


Nice — your local Postgres role is `smielniczuk`, not `postgres`.

So the next fix is to make `seed-dev.sh` use your local role instead of the Docker default. Right now the script is hardcoded to `PG_USER="postgres"` and `PG_PASS="postgres"`, and it explicitly says it expects the Docker Postgres on `5433`. 

Change these two lines in `scripts/seed-dev.sh`:

```bash
PG_USER="postgres"
PG_PASS="postgres"
```

to:

```bash
PG_USER="${PGUSER:-smielniczuk}"
PG_PASS="${PGPASSWORD:-}"
```

Then run:

```bash
export PGUSER=smielniczuk
unset PGPASSWORD
./scripts/seed-dev.sh 1000000
```

Why this should work:

* your `smielniczuk` role is a superuser and can create DBs, so it can create `source_test` and `dest_test` as the script wants to do. 
* the script then seeds `source_test` and resets `dest_test.raw`.  

If the next error mentions `gen_random_uuid()`, run this once:

```bash
psql -h localhost -p 5433 -U smielniczuk -d source_test -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto;'
psql -h localhost -p 5433 -U smielniczuk -d dest_test -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto;'
```

Then retry:

```bash
./scripts/seed-dev.sh 1000000
```

After seed succeeds, the next command is:

```bash
export RAPIDBYTE_PLUGIN_DIR=target/plugins
./target/release/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg.yaml -vv
```

#IMPORTANT

Q: is it safe to run - I have a lot of things in my postgres?
A: Not fully safe on a Postgres instance that already has important stuff.

Why:

* it **creates databases** if missing: `source_test` and `dest_test`
* it **drops and recreates** tables in `source_test`:

  * `public.users`
  * `public.orders`
* it **drops and recreates** the destination schema:

  * `raw` in `dest_test`

So it should be safe **only if**:

* `source_test` and `dest_test` are throwaway dev databases, and
* you do not care about replacing `public.users`, `public.orders`, or `dest_test.raw`

What it does **not** do:

* it does not drop your whole cluster
* it does not wipe unrelated databases
* it does not touch other schemas/tables outside the target DBs it connects to

Best safe practice:

* use a **separate local dev cluster** or
* change the script to use uniquely named databases like:

  * `rapidbyte_source_test`
  * `rapidbyte_dest_test`

Safer move before running:

```bash
export SOURCE_DB=rapidbyte_source_test
export DEST_DB=rapidbyte_dest_test
export DEST_SCHEMA=raw
```

Then run the seed script against those dedicated DB names.

If you already have a `source_test` or `dest_test` database with real data, do **not** run it as-is.

# At this point you probably failed having a flood of DEBUG log
```
ne
2026-03-14T13:50:11.892495Z  INFO Starting per-stream pipeline execution pipeline="test_pg_to_pg" parallelism=8 num_streams=16 num_transforms=0
2026-03-14T13:50:11.892506Z  INFO Running destination DDL preflight before shard workers unique_streams=2 preflight_parallelism=2
⠹ Running — 0 B | 0 batches | 0 of 2 streams done                                                                                                       2026-03-14T13:50:11.892919Z  INFO Opening destination plugin for stream plugin="postgres" version="unknown" stream="orders"
2026-03-14T13:50:11.892928Z  INFO Opening destination plugin for stream plugin="postgres" version="unknown" stream="users"
2026-03-14T13:50:11.893130Z  INFO Starting destination write stream="orders"
2026-03-14T13:50:11.893130Z  INFO Starting destination write stream="users"
2026-03-14T13:50:11.896549Z  INFO [plugin] dest-postgres: close (no-op) pipeline="test_pg_to_pg" stream=orders
2026-03-14T13:50:11.896550Z  INFO [plugin] dest-postgres: close (no-op) pipeline="test_pg_to_pg" stream=users
2026-03-14T13:50:11.912755Z ERROR Max retries exhausted, failing pipeline attempt=4 max_retries=3 category=transient_network code=CONNECTION_FAILED safe_to_retry=true
...
```


➜  rapidbyte git:(main) ✗ pico .env
➜  rapidbyte git:(main) ✗ set -a
source .env
set +a
➜  rapidbyte git:(main) ✗ echo $PGUSER
echo $SOURCE_DB
echo $DEST_DB
smielniczuk
rapidbyte_source_test
rapidbyte_dest_test
➜  rapidbyte git:(main) ✗ 


# Solution

Yes — that’s the cleanest move.

Create a `.env` file in the repo root with your local settings:

```dotenv
TEST_SOURCE_PG_HOST=localhost
TEST_SOURCE_PG_PORT=5433
TEST_DEST_PG_HOST=localhost
TEST_DEST_PG_PORT=5433

PGUSER=smielniczuk
PGPASSWORD=

SOURCE_DB=rapidbyte_source_test
DEST_DB=rapidbyte_dest_test
DEST_SCHEMA=raw

RAPIDBYTE_PLUGIN_DIR=target/plugins
```

Then load it in `zsh` with:

```bash
set -a
source .env
set +a
```

Check:

```bash
echo $PGUSER
echo $SOURCE_DB
echo $DEST_DB
```

Then run:

```bash
./scripts/seed-nodocker-dev.sh 1000000
./target/release/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg.yaml -vv
```

Important bit: this only helps if your pipeline YAML uses environment variables like `${PGUSER}`, `${SOURCE_DB}`, `${DEST_DB}`, etc. If it still has hardcoded values, it will keep failing.

Paste the YAML and I’ll convert it to `.env`-driven form.

# possible changes to yaml config
➜  rapidbyte git:(main) ✗ sed -n '1,200p' ./tests/fixtures/pipelines/simple_pg_to_pg.yaml
version: "1.0"
pipeline: test_pg_to_pg

source:
  use: postgres
  config:
    host: ${TEST_SOURCE_PG_HOST}
    port: ${TEST_SOURCE_PG_PORT}
    user: postgres
    password: postgres
    database: source_test
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
    user: postgres
    password: postgres
    database: dest_test
    schema: raw
  write_mode: append

state:
  backend: sqlite
➜  rapidbyte git:(main) ✗ 

# let's make it as version for no docker custom setup
#

Yes — that’s better.

Keep the original fixture untouched and create a separate one, for example:

```bash
cp tests/fixtures/pipelines/simple_pg_to_pg.yaml \
   tests/fixtures/pipelines/simple_pg_to_pg_nodocker.yaml
```

Then edit `simple_pg_to_pg_nodocker.yaml` to use your env-based config:

```yaml
version: "1.0"
pipeline: test_pg_to_pg_nodocker

source:
  use: postgres
  config:
    host: ${TEST_SOURCE_PG_HOST}
    port: ${TEST_SOURCE_PG_PORT}
    user: ${PGUSER}
    password: ${PGPASSWORD}
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
    password: ${PGPASSWORD}
    database: ${DEST_DB}
    schema: ${DEST_SCHEMA}
  write_mode: append

state:
  backend: sqlite
```

Then run it with:

```bash
set -a
source .env
set +a

./target/release/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg_nodocker.yaml -vv
```

That gives you:

* original Docker-friendly fixture unchanged
* separate no-Docker fixture
* cleaner docs

A nice name is either `simple_pg_to_pg_nodocker.yaml` or `simple_pg_to_pg_local.yaml`. `local` reads a bit cleaner.








