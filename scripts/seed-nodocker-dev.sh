#!/usr/bin/env bash
# Seed a local Postgres for manual CLI testing.
#
# Usage:
#   ./scripts/seed-dev.sh [ROWS]
#   TEST_SOURCE_PG_HOST=localhost TEST_SOURCE_PG_PORT=5433 PGUSER=myuser ./scripts/seed-dev.sh 1000000

set -euo pipefail

PG_HOST="${TEST_SOURCE_PG_HOST:-localhost}"
PG_PORT="${TEST_SOURCE_PG_PORT:-5433}"
PG_USER="${PGUSER:-${USER:-postgres}}"
PG_PASS="${PGPASSWORD:-}"

SOURCE_DB="${SOURCE_DB:-source_test}"
DEST_DB="${DEST_DB:-dest_test}"
DEST_SCHEMA="${DEST_SCHEMA:-raw}"

ROWS="${1:-100}"

export PGPASSWORD="$PG_PASS"
PSQL=(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -q)

echo "Seeding Postgres at $PG_HOST:$PG_PORT as $PG_USER ($ROWS rows per table)..."
echo "Source DB: $SOURCE_DB"
echo "Dest DB:   $DEST_DB"
echo "Schema:    $DEST_SCHEMA"

# Create source and destination databases if missing
"${PSQL[@]}" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$SOURCE_DB'" | grep -q 1 \
  || "${PSQL[@]}" -d postgres -c "CREATE DATABASE \"$SOURCE_DB\";"

"${PSQL[@]}" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$DEST_DB'" | grep -q 1 \
  || "${PSQL[@]}" -d postgres -c "CREATE DATABASE \"$DEST_DB\";"

# Ensure pgcrypto exists for gen_random_uuid()
"${PSQL[@]}" -d "$SOURCE_DB" -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto;'
"${PSQL[@]}" -d "$DEST_DB"   -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto;'

# Seed source tables
"${PSQL[@]}" -d "$SOURCE_DB" <<SQL
DROP TABLE IF EXISTS public.users CASCADE;
CREATE TABLE public.users (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL DEFAULT gen_random_uuid(),
    event_type      TEXT NOT NULL,
    user_id         BIGINT NOT NULL,
    session_id      BIGINT,
    device_type     TEXT NOT NULL,
    ip_address      INET NOT NULL,
    country_code    TEXT NOT NULL,
    page_url        TEXT NOT NULL,
    referrer_url    TEXT,
    utm_source      TEXT,
    utm_medium      TEXT,
    duration_ms     INTEGER,
    is_converted    BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.users (
    event_id, event_type, user_id, session_id, device_type, ip_address,
    country_code, page_url, referrer_url, utm_source, utm_medium,
    duration_ms, is_converted, created_at
)
SELECT
    gen_random_uuid(),
    (ARRAY['click','purchase','view','signup','logout','scroll','hover','submit'])[1 + (i % 8)],
    (i % 100000) + 1,
    CASE WHEN i % 10 < 7 THEN (i * 31) % 500000 ELSE NULL END,
    (ARRAY['mobile','desktop','tablet'])[1 + (i % 3)],
    ('10.' || (i % 256) || '.' || (i / 256 % 256) || '.' || (i % 253 + 1))::inet,
    (ARRAY['US','GB','DE','FR','JP','CA','AU','BR','IN','MX',
           'KR','ES','IT','NL','SE','NO','DK','FI','PL','PT'])[1 + (i % 20)],
    '/section-' || (i % 12) || '/page-' || (i % 500),
    CASE WHEN i % 10 < 6 THEN 'https://ref-' || (i % 50) || '.example.com/campaign/' || (i % 200) ELSE NULL END,
    CASE WHEN i % 10 < 4 THEN (ARRAY['google','facebook','twitter','linkedin'])[1 + (i % 4)] ELSE NULL END,
    CASE WHEN i % 10 < 4 THEN (ARRAY['cpc','organic','social','email'])[1 + (i % 4)] ELSE NULL END,
    (i * 7) % 30000,
    (i % 5 = 0),
    NOW() - make_interval(secs => (i * 37) % 86400)
FROM generate_series(1, $ROWS) AS s(i);

DROP TABLE IF EXISTS public.orders CASCADE;
CREATE TABLE public.orders (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL DEFAULT gen_random_uuid(),
    event_type      TEXT NOT NULL,
    user_id         BIGINT NOT NULL,
    session_id      BIGINT,
    device_type     TEXT NOT NULL,
    ip_address      INET NOT NULL,
    country_code    TEXT NOT NULL,
    page_url        TEXT NOT NULL,
    referrer_url    TEXT,
    utm_source      TEXT,
    utm_medium      TEXT,
    duration_ms     INTEGER,
    is_converted    BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.orders (
    event_id, event_type, user_id, session_id, device_type, ip_address,
    country_code, page_url, referrer_url, utm_source, utm_medium,
    duration_ms, is_converted, created_at
)
SELECT
    gen_random_uuid(),
    (ARRAY['order','refund','cancel','ship','deliver','return','exchange','charge'])[1 + (i % 8)],
    (i % 100000) + 1,
    CASE WHEN i % 10 < 7 THEN (i * 31) % 500000 ELSE NULL END,
    (ARRAY['mobile','desktop','tablet'])[1 + (i % 3)],
    ('10.' || (i % 256) || '.' || (i / 256 % 256) || '.' || (i % 253 + 1))::inet,
    (ARRAY['US','GB','DE','FR','JP','CA','AU','BR','IN','MX',
           'KR','ES','IT','NL','SE','NO','DK','FI','PL','PT'])[1 + (i % 20)],
    '/checkout/' || (i % 12) || '/order-' || (i % 500),
    CASE WHEN i % 10 < 6 THEN 'https://ref-' || (i % 50) || '.example.com/campaign/' || (i % 200) ELSE NULL END,
    CASE WHEN i % 10 < 4 THEN (ARRAY['google','facebook','twitter','linkedin'])[1 + (i % 4)] ELSE NULL END,
    CASE WHEN i % 10 < 4 THEN (ARRAY['cpc','organic','social','email'])[1 + (i % 4)] ELSE NULL END,
    (i * 7) % 30000,
    (i % 5 = 0),
    NOW() - make_interval(secs => (i * 23) % 86400)
FROM generate_series(1, $ROWS) AS s(i);
SQL

# Clean destination schema
"${PSQL[@]}" -d "$DEST_DB" -c "DROP SCHEMA IF EXISTS \"$DEST_SCHEMA\" CASCADE; CREATE SCHEMA \"$DEST_SCHEMA\";"

echo "Done."
echo "Run pipeline:"
echo "  export TEST_SOURCE_PG_HOST=$PG_HOST TEST_SOURCE_PG_PORT=$PG_PORT"
echo "  export TEST_DEST_PG_HOST=$PG_HOST TEST_DEST_PG_PORT=$PG_PORT"
echo "  export RAPIDBYTE_PLUGIN_DIR=target/plugins"
echo "  ./target/release/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg.yaml"