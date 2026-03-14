
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


