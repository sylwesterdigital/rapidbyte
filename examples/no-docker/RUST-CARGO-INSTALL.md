
## Rust / Cargo setup on macOS (zsh)

If `cargo` is not available, install the Rust toolchain first.

### 1. Install Rust and Cargo

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
````

Choose the standard installation when prompted.

### 2. Reload the current zsh session

```sh
. "$HOME/.cargo/env"
```

For `zsh`, only run that one line.

### 3. Verify the installation

```sh
cargo --version
rustc --version
```

### 4. Export local PostgreSQL settings

```sh
export TEST_SOURCE_PG_HOST=localhost
export TEST_SOURCE_PG_PORT=5433
export TEST_DEST_PG_HOST=localhost
export TEST_DEST_PG_PORT=5433
```

Adjust the ports if local PostgreSQL uses different ones.

### What was done

* created and pushed `examples/no-docker/README.md`
* exported local PostgreSQL environment variables
* tried `cargo build --release`
* found that `cargo` was missing
* installed Rust and Cargo with `rustup`
* loaded Cargo into the current `zsh` session with `. "$HOME/.cargo/env"`
* verified that `cargo` and `rustc` now work


#If Rust and Cargo are installe, then

Run these next, from the repo root:

```bash
cargo build --release
```

Then build the plugins:

```bash
./scripts/build-plugin.sh plugins/sources/postgres release
./scripts/build-plugin.sh plugins/destinations/postgres release
./scripts/build-plugin.sh plugins/transforms/sql release
./scripts/build-plugin.sh plugins/transforms/validate release
```

Then place the plugin artifacts where the app expects them:

```bash
mkdir -p target/plugins/sources target/plugins/destinations target/plugins/transforms

cp plugins/sources/postgres/target/wasm32-wasip2/release/source_postgres.wasm target/plugins/sources/postgres.wasm
cp plugins/destinations/postgres/target/wasm32-wasip2/release/dest_postgres.wasm target/plugins/destinations/postgres.wasm
cp plugins/transforms/sql/target/wasm32-wasip2/release/transform_sql.wasm target/plugins/transforms/sql.wasm
cp plugins/transforms/validate/target/wasm32-wasip2/release/transform_validate.wasm target/plugins/transforms/validate.wasm
```

Then set:

```bash
export RAPIDBYTE_PLUGIN_DIR=target/plugins
```

Then seed your local Postgres:

```bash
./scripts/seed-dev.sh 1000000
```

Then run the example pipeline:

```bash
./target/release/rapidbyte run tests/fixtures/pipelines/simple_pg_to_pg.yaml -vv
```

If the first command fails, paste that error and I’ll give the exact next fix.
