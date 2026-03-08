//! Rapidbyte Connector SDK.
//!
//! Provides traits, protocol types, and host-import wrappers for building
//! WASI-based data pipeline connectors.

#[cfg(feature = "runtime")]
pub mod arrow;
#[cfg(feature = "build")]
pub mod build;
#[cfg(feature = "runtime")]
pub mod connector;
#[cfg(feature = "runtime")]
pub mod context;
#[cfg(feature = "runtime")]
pub mod frame_writer;
#[cfg(feature = "runtime")]
pub mod host_ffi;
#[cfg(feature = "runtime")]
pub mod host_tcp;
#[cfg(feature = "runtime")]
pub mod features;
#[cfg(feature = "runtime")]
pub mod prelude;
#[cfg(feature = "conformance")]
pub mod conformance;

// Type re-exports — always available (no feature gate)
pub use rapidbyte_types::arrow as arrow_types;
pub use rapidbyte_types::catalog;
pub use rapidbyte_types::checkpoint;
pub use rapidbyte_types::cursor;
pub use rapidbyte_types::envelope;
pub use rapidbyte_types::error;
pub use rapidbyte_types::manifest;
pub use rapidbyte_types::metric;
pub use rapidbyte_types::stream;
pub use rapidbyte_types::wire;

#[cfg(feature = "runtime")]
pub use wit_bindgen;

/// Trait for config types that provide a JSON Schema at compile time.
///
/// Derived via `#[derive(ConfigSchema)]`. Do not implement manually.
pub trait ConfigSchema {
    /// JSON Schema (Draft 7) as a compile-time string.
    const SCHEMA_JSON: &'static str;
}

/// Re-export the derive macro so users write `use rapidbyte_sdk::ConfigSchema`.
#[cfg(feature = "runtime")]
pub use rapidbyte_sdk_macros::ConfigSchema;

/// Re-export the `#[connector]` attribute macro.
#[cfg(feature = "runtime")]
pub use rapidbyte_sdk_macros::connector;
