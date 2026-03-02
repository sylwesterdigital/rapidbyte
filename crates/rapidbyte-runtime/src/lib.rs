//! Wasmtime component runtime for Rapidbyte connectors.
//!
//! Manages the WASI component model runtime, host import implementations,
//! connector resolution, and network/sandbox policies.
//!
//! # Crate structure
//!
//! | Module        | Responsibility |
//! |---------------|----------------|
//! | `engine`      | Wasmtime engine, AOT cache, component loading |
//! | `host_state`  | `ComponentHostState` builder + host import impls |
//! | `bindings`    | WIT-generated bindings + Host trait impls |
//! | `sandbox`     | WASI context builder, sandbox overrides |
//! | `acl`         | Network allow/deny ACL for connector sockets |
//! | `socket`      | Host TCP socket helpers |
//! | `connector`   | Connector path resolution + manifest loading |
//! | `compression` | IPC channel compression (lz4/zstd) |
//! | `frame`       | Host-side frame table for V3 zero-copy batch transport |
//! | `error`       | Runtime error types |

#![warn(clippy::pedantic)]

pub mod acl;
pub mod bindings;
pub mod compression;
pub mod connector;
pub mod engine;
pub mod error;
pub mod frame;
pub mod host_state;
pub mod sandbox;
pub mod socket;

// Top-level re-exports for convenience.
pub use bindings::{
    dest_bindings, dest_error_to_sdk, dest_validation_to_sdk, source_bindings, source_error_to_sdk,
    source_validation_to_sdk, transform_bindings, transform_error_to_sdk,
    transform_validation_to_sdk,
};
pub use compression::CompressionCodec;
pub use connector::{
    connector_search_dirs, load_connector_manifest, parse_connector_ref, resolve_connector_path,
};
pub use engine::{create_component_linker, HasStoreLimits, LoadedComponent, WasmRuntime};
pub use frame::FrameTable;
pub use host_state::{ComponentHostState, Frame, HostTimings};
pub use sandbox::{resolve_min_limit, SandboxOverrides};

/// Re-export Wasmtime types needed by downstream crates (e.g. for `add_to_linker` calls).
pub mod wasmtime_reexport {
    pub use wasmtime::component::HasSelf;
    pub use wasmtime::Error;
}
