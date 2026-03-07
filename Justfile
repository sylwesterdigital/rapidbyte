default:
    @just --list

build:
    cargo build

build-no-sccache:
    NO_SCCACHE=1 cargo build

build-connectors:
    cd connectors/source-postgres && cargo build
    cd connectors/dest-postgres && cargo build
    cd connectors/transform-sql && cargo build
    cd connectors/transform-validate && cargo build
    mkdir -p target/connectors
    cp connectors/source-postgres/target/wasm32-wasip2/debug/source_postgres.wasm target/connectors/
    cp connectors/dest-postgres/target/wasm32-wasip2/debug/dest_postgres.wasm target/connectors/
    cp connectors/transform-sql/target/wasm32-wasip2/debug/transform_sql.wasm target/connectors/
    cp connectors/transform-validate/target/wasm32-wasip2/debug/transform_validate.wasm target/connectors/

release:
    cargo build --release

release-connectors:
    cd connectors/source-postgres && cargo build --release
    cd connectors/dest-postgres && cargo build --release
    cd connectors/transform-sql && cargo build --release
    cd connectors/transform-validate && cargo build --release
    mkdir -p target/connectors
    cp connectors/source-postgres/target/wasm32-wasip2/release/source_postgres.wasm target/connectors/
    cp connectors/dest-postgres/target/wasm32-wasip2/release/dest_postgres.wasm target/connectors/
    cp connectors/transform-sql/target/wasm32-wasip2/release/transform_sql.wasm target/connectors/
    cp connectors/transform-validate/target/wasm32-wasip2/release/transform_validate.wasm target/connectors/
    ./scripts/strip-wasm.sh target/connectors/source_postgres.wasm target/connectors/source_postgres.wasm
    ./scripts/strip-wasm.sh target/connectors/dest_postgres.wasm target/connectors/dest_postgres.wasm
    ./scripts/strip-wasm.sh target/connectors/transform_sql.wasm target/connectors/transform_sql.wasm
    ./scripts/strip-wasm.sh target/connectors/transform_validate.wasm target/connectors/transform_validate.wasm

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

e2e *args="":
    cargo test --manifest-path tests/e2e/Cargo.toml {{args}}

# Run benchmarks: bench [CONNECTOR] [ROWS] --profile PROFILE [--iters N] [--cpu-profile]
bench *args="":
    cargo run --manifest-path tests/bench/Cargo.toml -- run {{args}}

# Compare benchmarks between two git refs
bench-compare ref1 ref2 *args="":
    cargo run --manifest-path tests/bench/Cargo.toml -- compare {{ref1}} {{ref2}} {{args}}

# Scaffold a new connector project
scaffold name output=("connectors/" + name):
    cargo run -- scaffold {{name}} --output {{output}}

docker-up:
    docker compose up -d --wait

docker-down:
    docker compose down -v

# Seed local Postgres for manual testing (default 100 rows, or specify count)
seed rows="100":
    ./scripts/seed-dev.sh {{rows}}

clean:
    cargo clean

sccache-stats:
    sccache --show-stats
