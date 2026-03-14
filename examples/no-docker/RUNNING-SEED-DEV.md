
# Assuming you installed Rust and Cargo
```
➜  rapidbyte git:(main) export TEST_SOURCE_PG_HOST=localhost
export TEST_SOURCE_PG_PORT=5433
export TEST_DEST_PG_HOST=localhost
export TEST_DEST_PG_PORT=5433
➜  rapidbyte git:(main) cargo build --release
zsh: command not found: cargo
➜  rapidbyte git:(main) curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```


```
cargo build --release
    Updating crates.io index
  Downloaded alloc-no-stdlib v2.0.4
  Downloaded ambient-authority v0.0.2
...
```

# Assuming you compiled plugins 
```
./scripts/build-plugin.sh plugins/sources/postgres release
./scripts/build-plugin.sh plugins/destinations/postgres release
./scripts/build-plugin.sh plugins/transforms/sql release
./scripts/build-plugin.sh plugins/transforms/validate release
    Updating crates.io index
```

# Assuming 



➜  rapidbyte git:(main) ✗ RAPIDBYTE_PLUGIN_DIR=target/plugins
➜  rapidbyte git:(main) ✗ ./scripts/seed-dev.sh 1000000
Seeding Postgres at localhost:5433 (1000000 rows per table)...
psql: error: connection to server at "localhost" (::1), port 5433 failed: FATAL:  role "postgres" does not exist
psql: error: connection to server at "localhost" (::1), port 5433 failed: FATAL:  role "postgres" does not exist


The problem is in `seed-dev.sh`, not in your build.

That script is hardcoded to connect as:

* user: `postgres`
* password: `postgres`
* host/port from `TEST_SOURCE_PG_HOST` / `TEST_SOURCE_PG_PORT`

and it was written for the Docker dev database on port `5433`. 

Your error:

```text
FATAL: role "postgres" does not exist
```

means your local Postgres on `localhost:5433` is running, but it does **not** have a database role named `postgres`.

### What the script expects

`seed-dev.sh` does this:

```bash
PG_USER="postgres"
PG_PASS="postgres"
PSQL="psql -h $PG_HOST -p $PG_PORT -U $PG_USER -q"
```

Then it tries to create databases named `source_test` and `dest_test`, seed tables into `source_test`, and create schema `raw` in `dest_test`.   

### What to do next

You have two practical options.

#### Option 1: use your own Postgres user

Edit `scripts/seed-dev.sh` and replace:

```bash
PG_USER="postgres"
PG_PASS="postgres"
```

with your actual local Postgres username and password.

For example, if your macOS Postgres user is your local account and you use trust or peer-style auth, it may look like:

```bash
PG_USER="smielniczuk"
PG_PASS=""
```

Then the line:

```bash
export PGPASSWORD="$PG_PASS"
```

may need to be removed or left empty depending on your auth setup. The script currently always exports it. 

#### Option 2: create a `postgres` role in your local cluster

That would let the existing script work unchanged, but it’s usually less clean than just editing the script for your machine.

### Fastest way to confirm your local DB user

Run:

```bash
psql -h localhost -p 5433 -U smielniczuk -d postgres -c '\du'
```

If `postgres` database does not exist, try:

```bash
psql -h localhost -p 5433 -U smielniczuk -d template1 -c '\du'
```

That will show available roles.

### Likely next edit for your no-Docker setup

Change the top of `seed-dev.sh` from this:

```bash
PG_HOST="${TEST_SOURCE_PG_HOST:-localhost}"
PG_PORT="${TEST_SOURCE_PG_PORT:-5433}"
PG_USER="postgres"
PG_PASS="postgres"
```

to something like:

```bash
PG_HOST="${TEST_SOURCE_PG_HOST:-localhost}"
PG_PORT="${TEST_SOURCE_PG_PORT:-5433}"
PG_USER="${PGUSER:-smielniczuk}"
PG_PASS="${PGPASSWORD:-}"
```

That makes it work with your local environment instead of the Docker defaults. The current script is Docker-specific by its own comment: “Expects docker-compose Postgres running on port 5433.” 

### One more likely gotcha

This seed script uses `gen_random_uuid()`. That typically needs the right extension available in the database. The script does **not** create that extension first.  

So after fixing the user, if the next error mentions `gen_random_uuid`, run:

```bash
psql -h localhost -p 5433 -U your_user -d source_test -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto;'
```

The clean summary for your docs is:

**`seed-dev.sh` is currently written for the Docker dev database and assumes a `postgres/postgres` login on port `5433`; for local no-Docker use, it should be changed to use the local Postgres username/password via environment variables.** 

Paste the output of `\du` or tell me your local Postgres username, and I’ll give you the exact two-line edit.
