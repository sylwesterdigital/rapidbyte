# Default build mode (release or debug)
MODE := "release"

default:
    @just --list

# ── Dev workflow ─────────────────────────────────────────────────────

# Start dev environment: docker + build + seed
dev-up rows="1000000":
    @echo "Starting dev environment..."
    docker compose up -d --wait
    just build-all
    just seed {{rows}}
    @echo ""
    @echo "Dev environment ready. Run a pipeline:"
    @echo "  just run tests/fixtures/pipelines/simple_pg_to_pg.yaml"
    @echo "  just run tests/fixtures/pipelines/simple_pg_to_pg.yaml -vv"

# Stop dev environment and clean state
dev-down:
    docker compose down -v

# Launch interactive dev shell (REPL)
dev *args="": build-all
    RAPIDBYTE_CONNECTOR_DIR=target/connectors \
    ./target/{{MODE}}/rapidbyte dev {{args}}

# Build + run a pipeline (pass flags like -v, -vv, --dry-run)
run pipeline *args="": build-all
    RAPIDBYTE_CONNECTOR_DIR=target/connectors \
    TEST_SOURCE_PG_HOST=localhost TEST_SOURCE_PG_PORT=5433 \
    TEST_DEST_PG_HOST=localhost TEST_DEST_PG_PORT=5433 \
    ./target/{{MODE}}/rapidbyte run {{pipeline}} {{args}}

# ── Build ────────────────────────────────────────────────────────────

# Build host + connectors (MODE=release by default, MODE=debug for debug)
build-all: _build-host _build-connectors

build:
    cargo build

build-no-sccache:
    NO_SCCACHE=1 cargo build

[private]
_build-host:
    cargo build {{ if MODE == "release" { "--release" } else { "" } }}

[private]
_build-connectors:
    cd connectors/source-postgres && cargo build {{ if MODE == "release" { "--release" } else { "" } }}
    cd connectors/dest-postgres && cargo build {{ if MODE == "release" { "--release" } else { "" } }}
    cd connectors/transform-sql && cargo build {{ if MODE == "release" { "--release" } else { "" } }}
    cd connectors/transform-validate && cargo build {{ if MODE == "release" { "--release" } else { "" } }}
    mkdir -p target/connectors
    cp connectors/source-postgres/target/wasm32-wasip2/{{MODE}}/source_postgres.wasm target/connectors/
    cp connectors/dest-postgres/target/wasm32-wasip2/{{MODE}}/dest_postgres.wasm target/connectors/
    cp connectors/transform-sql/target/wasm32-wasip2/{{MODE}}/transform_sql.wasm target/connectors/
    cp connectors/transform-validate/target/wasm32-wasip2/{{MODE}}/transform_validate.wasm target/connectors/
    {{ if MODE == "release" { "./scripts/strip-wasm.sh target/connectors/source_postgres.wasm target/connectors/source_postgres.wasm && ./scripts/strip-wasm.sh target/connectors/dest_postgres.wasm target/connectors/dest_postgres.wasm && ./scripts/strip-wasm.sh target/connectors/transform_sql.wasm target/connectors/transform_sql.wasm && ./scripts/strip-wasm.sh target/connectors/transform_validate.wasm target/connectors/transform_validate.wasm" } else { "true" } }}

release:
    cargo build --release

# ── Test & lint ──────────────────────────────────────────────────────

test:
    cargo test

check:
    cargo check --all-targets

check-no-sccache:
    NO_SCCACHE=1 cargo check --all-targets

fmt:
    cargo fmt --all

lint:
    cargo clippy --all-targets -- -D warnings

# ── E2E & bench ──────────────────────────────────────────────────────

e2e *args="":
    cargo test --manifest-path tests/e2e/Cargo.toml {{args}}

# Run benchmarks: bench [CONNECTOR] [ROWS] --profile PROFILE [--iters N] [--cpu-profile]
bench *args="":
    cargo run --manifest-path tests/bench/Cargo.toml -- run {{args}}

# Compare benchmarks between two git refs
bench-compare ref1 ref2 *args="":
    cargo run --manifest-path tests/bench/Cargo.toml -- compare {{ref1}} {{ref2}} {{args}}

# ── Utilities ────────────────────────────────────────────────────────

# Scaffold a new connector project
scaffold name output=("connectors/" + name):
    cargo run -- scaffold {{name}} --output {{output}}

# Seed local Postgres (default 1M rows)
seed rows="1000000":
    ./scripts/seed-dev.sh {{rows}}

docker-up:
    docker compose up -d --wait

docker-down:
    docker compose down -v

clean:
    cargo clean

sccache-stats:
    sccache --show-stats
