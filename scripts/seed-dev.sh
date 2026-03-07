#!/usr/bin/env bash
# Seed a local Postgres for manual CLI testing.
# Expects docker-compose Postgres running on port 5433.
#
# Usage: ./scripts/seed-dev.sh
#        ./scripts/seed-dev.sh --rows 1000   # more rows for testing throughput

set -euo pipefail

PG_HOST="${TEST_SOURCE_PG_HOST:-localhost}"
PG_PORT="${TEST_SOURCE_PG_PORT:-5433}"
PG_USER="postgres"
PG_PASS="postgres"
ROWS="${1:-100}"

export PGPASSWORD="$PG_PASS"
PSQL="psql -h $PG_HOST -p $PG_PORT -U $PG_USER -q"

echo "Seeding Postgres at $PG_HOST:$PG_PORT ($ROWS rows per table)..."

# Create source and destination databases (ignore if they already exist)
$PSQL -c "SELECT 1 FROM pg_database WHERE datname = 'source_test'" | grep -q 1 \
  || $PSQL -c "CREATE DATABASE source_test;"
$PSQL -c "SELECT 1 FROM pg_database WHERE datname = 'dest_test'" | grep -q 1 \
  || $PSQL -c "CREATE DATABASE dest_test;"

# Seed source tables (matches simple_pg_to_pg.yaml fixture)
$PSQL -d source_test <<SQL
DROP TABLE IF EXISTS public.users CASCADE;
CREATE TABLE public.users (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO public.users (name, email, created_at)
SELECT
    'user_' || i,
    'user_' || i || '@example.com',
    now() - make_interval(secs => (i * 37) % 86400)
FROM generate_series(1, $ROWS) AS s(i);

DROP TABLE IF EXISTS public.orders CASCADE;
CREATE TABLE public.orders (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL,
    amount      NUMERIC(10,2) NOT NULL,
    status      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO public.orders (user_id, amount, status, created_at)
SELECT
    (i % $ROWS) + 1,
    round((random() * 500 + 1)::numeric, 2),
    (ARRAY['completed','pending','cancelled','refunded'])[1 + (i % 4)],
    now() - make_interval(secs => (i * 23) % 86400)
FROM generate_series(1, $ROWS) AS s(i);
SQL

# Create destination schema
$PSQL -d dest_test -c "CREATE SCHEMA IF NOT EXISTS raw;"

echo "Done. Source: ${ROWS} users, ${ROWS} orders. Destination: dest_test.raw (empty)."
echo ""
echo "Run pipeline:"
echo "  export TEST_SOURCE_PG_HOST=$PG_HOST TEST_SOURCE_PG_PORT=$PG_PORT"
echo "  export TEST_DEST_PG_HOST=$PG_HOST TEST_DEST_PG_PORT=$PG_PORT"
echo "  export RAPIDBYTE_CONNECTOR_DIR=target/connectors"
echo "  ./target/debug/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg.yaml"
