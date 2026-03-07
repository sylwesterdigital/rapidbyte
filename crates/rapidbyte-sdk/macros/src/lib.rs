//! Proc macros for the Rapidbyte Plugin SDK.
//!
//! This crate is an internal implementation detail of `rapidbyte-sdk`.
//! Do not depend on it directly — use `rapidbyte_sdk::ConfigSchema` instead.

mod plugin;
mod schema;

use proc_macro::TokenStream;

/// Derive a JSON Schema (Draft 7) from a config struct at compile time.
///
/// Generates `impl ConfigSchema for T` with `const SCHEMA_JSON: &'static str`.
///
/// # Supported `#[schema(...)]` attributes
///
/// - `secret` — marks field as sensitive (`"x-secret": true`)
/// - `default = <value>` — sets JSON Schema default
/// - `advanced` — marks as advanced setting (`"x-advanced": true`)
/// - `example = <value>` — adds to `"examples"` array
/// - `env = "<VAR>"` — documents env var fallback (`"x-env-var"`)
/// - `values("<a>", "<b>")` — string enum constraint
#[proc_macro_derive(ConfigSchema, attributes(schema))]
pub fn derive_config_schema(input: TokenStream) -> TokenStream {
    schema::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Attribute macro that generates all component glue for a plugin struct.
///
/// Replaces `connector_main!`, `embed_manifest!`, and `embed_config_schema!`
/// with a single annotation:
///
/// ```ignore
/// #[plugin(source)]
/// pub struct MySource { ... }
/// ```
///
/// Accepted kinds: `source`, `destination`, `transform`.
///
/// The annotated struct must implement the corresponding trait
/// (`Source`, `Destination`, or `Transform` from `rapidbyte_sdk::plugin`).
#[proc_macro_attribute]
pub fn plugin(attr: TokenStream, item: TokenStream) -> TokenStream {
    let kind = syn::parse_macro_input!(attr as plugin::PluginKind);
    let input = syn::parse_macro_input!(item as syn::ItemStruct);
    match plugin::expand(kind, input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
