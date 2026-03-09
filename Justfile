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
    RAPIDBYTE_PLUGIN_DIR=target/plugins \
    ./target/{{MODE}}/rapidbyte dev {{args}}

# Build + run a pipeline (pass flags like -v, -vv, --dry-run)
run pipeline *args="": build-all
    RAPIDBYTE_PLUGIN_DIR=target/plugins \
    TEST_SOURCE_PG_HOST=localhost TEST_SOURCE_PG_PORT=5433 \
    TEST_DEST_PG_HOST=localhost TEST_DEST_PG_PORT=5433 \
    ./target/{{MODE}}/rapidbyte run {{pipeline}} {{args}}

# ── Build ────────────────────────────────────────────────────────────

# Build host + plugins (MODE=release by default, MODE=debug for debug)
build-all: _build-host _build-plugins

build:
    cargo build

build-no-sccache:
    NO_SCCACHE=1 cargo build

[private]
_build-host:
    cargo build {{ if MODE == "release" { "--release" } else { "" } }}

[private]
_build-plugins:
    cd plugins/sources/postgres && cargo build {{ if MODE == "release" { "--release" } else { "" } }}
    cd plugins/destinations/postgres && cargo build {{ if MODE == "release" { "--release" } else { "" } }}
    cd plugins/transforms/sql && cargo build {{ if MODE == "release" { "--release" } else { "" } }}
    cd plugins/transforms/validate && cargo build {{ if MODE == "release" { "--release" } else { "" } }}
    mkdir -p target/plugins/sources target/plugins/destinations target/plugins/transforms
    cp plugins/sources/postgres/target/wasm32-wasip2/{{MODE}}/source_postgres.wasm target/plugins/sources/postgres.wasm
    cp plugins/destinations/postgres/target/wasm32-wasip2/{{MODE}}/dest_postgres.wasm target/plugins/destinations/postgres.wasm
    cp plugins/transforms/sql/target/wasm32-wasip2/{{MODE}}/transform_sql.wasm target/plugins/transforms/sql.wasm
    cp plugins/transforms/validate/target/wasm32-wasip2/{{MODE}}/transform_validate.wasm target/plugins/transforms/validate.wasm
    {{ if MODE == "release" { "./scripts/strip-wasm.sh target/plugins/sources/postgres.wasm target/plugins/sources/postgres.wasm && ./scripts/strip-wasm.sh target/plugins/destinations/postgres.wasm target/plugins/destinations/postgres.wasm && ./scripts/strip-wasm.sh target/plugins/transforms/sql.wasm target/plugins/transforms/sql.wasm && ./scripts/strip-wasm.sh target/plugins/transforms/validate.wasm target/plugins/transforms/validate.wasm" } else { "true" } }}

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
    cargo clippy --workspace --all-targets -- -D warnings

ci:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace --all-targets
    cargo test --manifest-path tests/e2e/Cargo.toml --no-run

# ── E2E & bench ──────────────────────────────────────────────────────

e2e *args="":
    cargo test --manifest-path tests/e2e/Cargo.toml {{args}}

# Run the connector-agnostic benchmark runner
bench *args="":
    cargo run --manifest-path benchmarks/Cargo.toml -- run {{args}}

# Run a lab benchmark scenario against a named benchmark environment profile
bench-lab scenario env="local-dev-postgres" *args="":
    docker compose up -d --wait
    just build-all
    cargo run --manifest-path benchmarks/Cargo.toml -- run --suite lab --scenario {{scenario}} --env-profile {{env}} --output target/benchmarks/lab/{{scenario}}.jsonl {{args}}

# Compare two benchmark artifact sets
bench-compare baseline candidate *args="":
    python3 benchmarks/analysis/compare.py {{baseline}} {{candidate}} {{args}}

# Run the next-generation connector-agnostic benchmark runner
benchmarks *args="":
    cargo run --manifest-path benchmarks/Cargo.toml -- {{args}}

# Run the PR benchmark smoke suite and compare against the checked-in baseline artifact set
bench-pr:
    cargo run --manifest-path benchmarks/Cargo.toml -- run --suite pr --output target/benchmarks/pr/candidate.jsonl
    python3 benchmarks/analysis/compare.py benchmarks/baselines/main/pr.jsonl target/benchmarks/pr/candidate.jsonl --min-samples 1

# ── Utilities ────────────────────────────────────────────────────────

# Scaffold a new plugin project
scaffold name:
    cargo run -- scaffold {{name}}

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
