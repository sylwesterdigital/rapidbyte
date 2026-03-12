//! Verify that resolve module items are publicly accessible.

use rapidbyte_engine::resolve::{create_state_backend, resolve_plugins, ResolvedPlugins};

#[test]
fn resolve_types_are_public() {
    // Compile-time check: these types are importable from outside the crate.
    let _: fn(&rapidbyte_engine::PipelineConfig) -> Result<ResolvedPlugins, _> = resolve_plugins;
    let _: fn(&rapidbyte_engine::PipelineConfig) -> anyhow::Result<_> = create_state_backend;
}
