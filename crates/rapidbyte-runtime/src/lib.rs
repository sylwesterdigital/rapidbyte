//! Wasmtime component runtime for Rapidbyte plugins.
//!
//! Manages the WASI component model runtime, host import implementations,
//! plugin resolution, and network/sandbox policies.
//!
//! # Crate structure
//!
//! | Module        | Responsibility |
//! |---------------|----------------|
//! | `engine`      | Wasmtime engine, AOT cache, component loading |
//! | `host_state`  | `ComponentHostState` builder + host import impls |
//! | `bindings`    | WIT-generated bindings + Host trait impls |
//! | `sandbox`     | WASI context builder, sandbox overrides |
//! | `acl`         | Network allow/deny ACL for plugin sockets |
//! | `socket`      | Host TCP socket helpers |
//! | `plugin`      | Plugin path resolution + manifest loading |
//! | `compression` | IPC channel compression (lz4/zstd) |
//! | `frame`       | Host-side frame table for V3 zero-copy batch transport |
//! | `error`       | Runtime error types |

#![warn(clippy::pedantic)]

pub mod acl;
pub mod bindings;
pub mod compression;
pub mod engine;
pub mod error;
pub mod frame;
pub mod host_state;
pub mod plugin;
pub mod sandbox;
pub mod socket;

// Top-level re-exports for convenience.
pub use bindings::{
    dest_bindings, dest_error_to_sdk, dest_validation_to_sdk, source_bindings, source_error_to_sdk,
    source_validation_to_sdk, transform_bindings, transform_error_to_sdk,
    transform_validation_to_sdk,
};
pub use compression::CompressionCodec;
pub use engine::{create_component_linker, HasStoreLimits, LoadedComponent, WasmRuntime};
pub use frame::FrameTable;
pub use host_state::{ComponentHostState, Frame, HostTimings};
pub use plugin::{
    extract_manifest_from_wasm, load_plugin_manifest, parse_plugin_ref, plugin_search_dirs,
    resolve_plugin_path,
};
pub use sandbox::{resolve_min_limit, SandboxOverrides};

/// Re-export Wasmtime types needed by downstream crates (e.g. for `add_to_linker` calls).
pub mod wasmtime_reexport {
    pub use wasmtime::component::HasSelf;
    pub use wasmtime::Error;
}
