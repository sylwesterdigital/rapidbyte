//! `#[connector(source|destination|transform)]` attribute macro implementation.
//!
//! Generates all WIT bindings, component glue, manifest embedding, and config
//! schema embedding that previously required three separate declarative macros.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, ItemStruct, Result};

/// The connector role parsed from the attribute argument.
pub enum ConnectorRole {
    Source,
    Destination,
    Transform,
}

impl Parse for ConnectorRole {
    fn parse(input: ParseStream) -> Result<Self> {
        let ident: Ident = input.parse()?;
        match ident.to_string().as_str() {
            "source" => Ok(ConnectorRole::Source),
            "destination" => Ok(ConnectorRole::Destination),
            "transform" => Ok(ConnectorRole::Transform),
            other => Err(syn::Error::new(
                ident.span(),
                format!(
                    "expected `source`, `destination`, or `transform`, found `{}`",
                    other
                ),
            )),
        }
    }
}

/// Main expansion entry point.
pub fn expand(role: ConnectorRole, input: ItemStruct) -> Result<TokenStream> {
    let struct_name = &input.ident;

    let bindings_mod = quote! { __rb_bindings };
    let (world_name, trait_path) = match role {
        ConnectorRole::Source => (
            "rapidbyte-source",
            quote! { ::rapidbyte_sdk::connector::Source },
        ),
        ConnectorRole::Destination => (
            "rapidbyte-destination",
            quote! { ::rapidbyte_sdk::connector::Destination },
        ),
        ConnectorRole::Transform => (
            "rapidbyte-transform",
            quote! { ::rapidbyte_sdk::connector::Transform },
        ),
    };

    let wit_bindings = gen_wit_bindings(world_name);
    let common = gen_common(struct_name);
    let guest_impl = gen_guest_impl(&role, struct_name, &trait_path);
    let embeds = gen_embeds(struct_name, &trait_path);

    Ok(quote! {
        #input

        #wit_bindings
        #common
        #guest_impl

        #bindings_mod::export!(RapidbyteComponent with_types_in #bindings_mod);

        #embeds

        fn main() {}
    })
}

/// Generate the WIT bindings module.
fn gen_wit_bindings(world_name: &str) -> TokenStream {
    // We must produce a literal string for the world name inside the
    // wit_bindgen::generate! invocation.  Because the inner macro is a
    // declarative `macro_rules!` expansion the world name must be a string
    // literal, not an ident.
    let world_lit = syn::LitStr::new(world_name, proc_macro2::Span::call_site());
    quote! {
        mod __rb_bindings {
            ::rapidbyte_sdk::wit_bindgen::generate!({
                path: "../../wit",
                world: #world_lit,
            });
        }
    }
}

/// Generate statics, helpers, and conversion functions shared by all roles.
fn gen_common(struct_name: &Ident) -> TokenStream {
    quote! {
        use std::cell::RefCell;
        use std::sync::OnceLock;

        static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
        static CONFIG_JSON: OnceLock<String> = OnceLock::new();
        static CONTEXT: OnceLock<::rapidbyte_sdk::context::Context> = OnceLock::new();

        fn get_runtime() -> &'static tokio::runtime::Runtime {
            RUNTIME.get_or_init(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create guest tokio runtime")
            })
        }

        struct SyncRefCell(RefCell<Option<#struct_name>>);
        unsafe impl Sync for SyncRefCell {}

        static CONNECTOR: OnceLock<SyncRefCell> = OnceLock::new();

        fn get_state() -> &'static RefCell<Option<#struct_name>> {
            &CONNECTOR.get_or_init(|| SyncRefCell(RefCell::new(None))).0
        }

        fn to_component_error(
            error: ::rapidbyte_sdk::error::ConnectorError,
        ) -> __rb_bindings::rapidbyte::connector::types::ConnectorError {
            use __rb_bindings::rapidbyte::connector::types::{
                BackoffClass as CBackoffClass, CommitState as CCommitState,
                ConnectorError as CConnectorError, ErrorCategory as CErrorCategory,
                ErrorScope as CErrorScope,
            };
            use ::rapidbyte_sdk::error::{
                BackoffClass, CommitState, ErrorCategory, ErrorScope,
            };

            CConnectorError {
                category: match error.category {
                    ErrorCategory::Config => CErrorCategory::Config,
                    ErrorCategory::Auth => CErrorCategory::Auth,
                    ErrorCategory::Permission => CErrorCategory::Permission,
                    ErrorCategory::RateLimit => CErrorCategory::RateLimit,
                    ErrorCategory::TransientNetwork => CErrorCategory::TransientNetwork,
                    ErrorCategory::TransientDb => CErrorCategory::TransientDb,
                    ErrorCategory::Data => CErrorCategory::Data,
                    ErrorCategory::Schema => CErrorCategory::Schema,
                    ErrorCategory::Frame => CErrorCategory::Frame,
                    ErrorCategory::Internal | _ => CErrorCategory::Internal,
                },
                scope: match error.scope {
                    ErrorScope::Stream => CErrorScope::PerStream,
                    ErrorScope::Batch => CErrorScope::PerBatch,
                    ErrorScope::Record => CErrorScope::PerRecord,
                },
                code: error.code,
                message: error.message,
                retryable: error.retryable,
                retry_after_ms: error.retry_after_ms,
                backoff_class: match error.backoff_class {
                    BackoffClass::Fast => CBackoffClass::Fast,
                    BackoffClass::Normal => CBackoffClass::Normal,
                    BackoffClass::Slow => CBackoffClass::Slow,
                },
                safe_to_retry: error.safe_to_retry,
                commit_state: error.commit_state.map(|state| match state {
                    CommitState::BeforeCommit => CCommitState::BeforeCommit,
                    CommitState::AfterCommitUnknown => CCommitState::AfterCommitUnknown,
                    CommitState::AfterCommitConfirmed => CCommitState::AfterCommitConfirmed,
                }),
                details_json: error.details.map(|value| value.to_string()),
            }
        }

        fn parse_stream_context(
            ctx_json: String,
        ) -> Result<
            ::rapidbyte_sdk::stream::StreamContext,
            __rb_bindings::rapidbyte::connector::types::ConnectorError,
        > {
            serde_json::from_str(&ctx_json).map_err(|e| {
                to_component_error(::rapidbyte_sdk::error::ConnectorError::config(
                    "INVALID_STREAM_CTX",
                    format!("Invalid StreamContext JSON: {}", e),
                ))
            })
        }

        fn parse_config<T: serde::de::DeserializeOwned>(
            config_json: &str,
        ) -> Result<T, __rb_bindings::rapidbyte::connector::types::ConnectorError> {
            serde_json::from_str(config_json).map_err(|e| {
                to_component_error(::rapidbyte_sdk::error::ConnectorError::config(
                    "INVALID_CONFIG",
                    format!("Config parse error: {}", e),
                ))
            })
        }

        fn parse_saved_config<T: serde::de::DeserializeOwned>(
        ) -> Result<T, __rb_bindings::rapidbyte::connector::types::ConnectorError> {
            let json = CONFIG_JSON.get().expect("open must be called before validate");
            parse_config(json)
        }

        fn to_component_validation(
            result: ::rapidbyte_sdk::error::ValidationResult,
        ) -> __rb_bindings::rapidbyte::connector::types::ValidationReport {
            use __rb_bindings::rapidbyte::connector::types::{
                ValidationReport as CValidationReport, ValidationStatus as CValidationStatus,
            };
            use ::rapidbyte_sdk::error::ValidationStatus;

            CValidationReport {
                status: match result.status {
                    ValidationStatus::Success => CValidationStatus::Success,
                    ValidationStatus::Failed => CValidationStatus::Failed,
                    ValidationStatus::Warning => CValidationStatus::Warning,
                },
                message: result.message,
                warnings: Vec::new(),
            }
        }
    }
}

/// Generate the Guest impl with lifecycle + role-specific methods.
fn gen_guest_impl(
    role: &ConnectorRole,
    struct_name: &Ident,
    trait_path: &TokenStream,
) -> TokenStream {
    let lifecycle = gen_lifecycle_methods(struct_name, trait_path);

    let (guest_trait_path, role_methods) = match role {
        ConnectorRole::Source => (
            quote! { __rb_bindings::exports::rapidbyte::connector::source::Guest },
            gen_source_methods(struct_name, trait_path),
        ),
        ConnectorRole::Destination => (
            quote! { __rb_bindings::exports::rapidbyte::connector::destination::Guest },
            gen_dest_methods(struct_name, trait_path),
        ),
        ConnectorRole::Transform => (
            quote! { __rb_bindings::exports::rapidbyte::connector::transform::Guest },
            gen_transform_methods(struct_name, trait_path),
        ),
    };

    quote! {
        struct RapidbyteComponent;

        impl #guest_trait_path for RapidbyteComponent {
            #lifecycle
            #role_methods
        }
    }
}

/// Generate open, validate, close methods (shared by all roles).
fn gen_lifecycle_methods(struct_name: &Ident, trait_path: &TokenStream) -> TokenStream {
    quote! {
        fn open(
            config_json: String,
        ) -> Result<u64, __rb_bindings::rapidbyte::connector::types::ConnectorError> {
            let _ = CONFIG_JSON.set(config_json.clone());

            let config: <#struct_name as #trait_path>::Config =
                parse_config(&config_json)?;

            let rt = get_runtime();
            let (instance, _connector_info) = rt
                .block_on(<#struct_name as #trait_path>::init(config))
                .map_err(to_component_error)?;

            let _ = CONTEXT.set(::rapidbyte_sdk::context::Context::new(
                env!("CARGO_PKG_NAME"),
                "",
            ));
            *get_state().borrow_mut() = Some(instance);
            Ok(1)
        }

        fn validate(_session: u64) -> Result<
            __rb_bindings::rapidbyte::connector::types::ValidationReport,
            __rb_bindings::rapidbyte::connector::types::ConnectorError,
        > {
            let config: <#struct_name as #trait_path>::Config = parse_saved_config()?;
            let ctx = CONTEXT.get().expect("open must be called before validate");

            let rt = get_runtime();
            rt.block_on(<#struct_name as #trait_path>::validate(&config, ctx))
                .map(to_component_validation)
                .map_err(to_component_error)
        }

        fn close(_session: u64) -> Result<(), __rb_bindings::rapidbyte::connector::types::ConnectorError> {
            let rt = get_runtime();
            let ctx = CONTEXT.get().expect("open must be called before close");
            let state_cell = get_state();
            let mut state_ref = state_cell.borrow_mut();
            if let Some(conn) = state_ref.as_mut() {
                rt.block_on(<#struct_name as #trait_path>::close(conn, ctx))
                    .map_err(to_component_error)?;
            }
            *state_ref = None;
            Ok(())
        }
    }
}

/// Generate source-specific methods: discover, run.
fn gen_source_methods(struct_name: &Ident, trait_path: &TokenStream) -> TokenStream {
    quote! {
        fn discover(
            _session: u64,
        ) -> Result<String, __rb_bindings::rapidbyte::connector::types::ConnectorError> {
            let ctx = CONTEXT.get().expect("Connector not opened");
            let rt = get_runtime();
            let state_cell = get_state();
            let mut state_ref = state_cell.borrow_mut();
            let conn = state_ref.as_mut().expect("Connector not opened");

            let catalog = rt
                .block_on(<#struct_name as #trait_path>::discover(conn, ctx))
                .map_err(to_component_error)?;

            serde_json::to_string(&catalog).map_err(|e| {
                to_component_error(::rapidbyte_sdk::error::ConnectorError::internal(
                    "SERIALIZE_CATALOG",
                    e.to_string(),
                ))
            })
        }

        fn run(
            _session: u64,
            request: __rb_bindings::rapidbyte::connector::types::RunRequest,
        ) -> Result<
            __rb_bindings::rapidbyte::connector::types::RunSummary,
            __rb_bindings::rapidbyte::connector::types::ConnectorError,
        > {
            let stream = parse_stream_context(request.stream_context_json)?;
            let base_ctx = CONTEXT.get().expect("Connector not opened");
            let ctx = base_ctx.with_stream(&stream.stream_name);
            let rt = get_runtime();
            let state_cell = get_state();
            let mut state_ref = state_cell.borrow_mut();
            let conn = state_ref.as_mut().expect("Connector not opened");

            let summary = rt
                .block_on(<#struct_name as #trait_path>::read(conn, &ctx, stream))
                .map_err(to_component_error)?;

            Ok(__rb_bindings::rapidbyte::connector::types::RunSummary {
                role: __rb_bindings::rapidbyte::connector::types::ConnectorRole::Source,
                read: Some(__rb_bindings::rapidbyte::connector::types::ReadSummary {
                    records_read: summary.records_read,
                    bytes_read: summary.bytes_read,
                    batches_emitted: summary.batches_emitted,
                    checkpoint_count: summary.checkpoint_count,
                    records_skipped: summary.records_skipped,
                }),
                write: None,
                transform: None,
            })
        }
    }
}

/// Generate destination-specific methods: run.
fn gen_dest_methods(struct_name: &Ident, trait_path: &TokenStream) -> TokenStream {
    quote! {
        fn run(
            _session: u64,
            request: __rb_bindings::rapidbyte::connector::types::RunRequest,
        ) -> Result<
            __rb_bindings::rapidbyte::connector::types::RunSummary,
            __rb_bindings::rapidbyte::connector::types::ConnectorError,
        > {
            let stream = parse_stream_context(request.stream_context_json)?;
            let base_ctx = CONTEXT.get().expect("Connector not opened");
            let ctx = base_ctx.with_stream(&stream.stream_name);
            let rt = get_runtime();
            let state_cell = get_state();
            let mut state_ref = state_cell.borrow_mut();
            let conn = state_ref.as_mut().expect("Connector not opened");

            let summary = rt
                .block_on(<#struct_name as #trait_path>::write(conn, &ctx, stream))
                .map_err(to_component_error)?;

            Ok(__rb_bindings::rapidbyte::connector::types::RunSummary {
                role: __rb_bindings::rapidbyte::connector::types::ConnectorRole::Destination,
                read: None,
                write: Some(__rb_bindings::rapidbyte::connector::types::WriteSummary {
                    records_written: summary.records_written,
                    bytes_written: summary.bytes_written,
                    batches_written: summary.batches_written,
                    checkpoint_count: summary.checkpoint_count,
                    records_failed: summary.records_failed,
                }),
                transform: None,
            })
        }
    }
}

/// Generate transform-specific methods: run.
fn gen_transform_methods(struct_name: &Ident, trait_path: &TokenStream) -> TokenStream {
    quote! {
        fn run(
            _session: u64,
            request: __rb_bindings::rapidbyte::connector::types::RunRequest,
        ) -> Result<
            __rb_bindings::rapidbyte::connector::types::RunSummary,
            __rb_bindings::rapidbyte::connector::types::ConnectorError,
        > {
            let stream = parse_stream_context(request.stream_context_json)?;
            let base_ctx = CONTEXT.get().expect("Connector not opened");
            let ctx = base_ctx.with_stream(&stream.stream_name);
            let rt = get_runtime();
            let state_cell = get_state();
            let mut state_ref = state_cell.borrow_mut();
            let conn = state_ref.as_mut().expect("Connector not opened");

            let summary = rt
                .block_on(<#struct_name as #trait_path>::transform(conn, &ctx, stream))
                .map_err(to_component_error)?;

            Ok(__rb_bindings::rapidbyte::connector::types::RunSummary {
                role: __rb_bindings::rapidbyte::connector::types::ConnectorRole::Transform,
                read: None,
                write: None,
                transform: Some(__rb_bindings::rapidbyte::connector::types::TransformSummary {
                    records_in: summary.records_in,
                    records_out: summary.records_out,
                    bytes_in: summary.bytes_in,
                    bytes_out: summary.bytes_out,
                    batches_processed: summary.batches_processed,
                }),
            })
        }
    }
}

/// Generate manifest and config schema embeds.
fn gen_embeds(struct_name: &Ident, trait_path: &TokenStream) -> TokenStream {
    quote! {
        // Embed the manifest JSON as a `rapidbyte_manifest_v1` custom section.
        // Only on wasm32 — native builds don't support this link section format.
        #[cfg(target_arch = "wasm32")]
        include!(concat!(env!("OUT_DIR"), "/rapidbyte_manifest_embed.rs"));

        // Embed the config schema as a `rapidbyte_config_schema_v1` Wasm custom section.
        #[cfg(target_arch = "wasm32")]
        const __RB_SCHEMA_BYTES: &[u8] =
            <<#struct_name as #trait_path>::Config as ::rapidbyte_sdk::ConfigSchema>::SCHEMA_JSON
                .as_bytes();

        #[cfg(target_arch = "wasm32")]
        #[link_section = "rapidbyte_config_schema_v1"]
        #[used]
        static __RAPIDBYTE_CONFIG_SCHEMA: [u8; { __RB_SCHEMA_BYTES.len() }] = {
            let mut arr = [0u8; __RB_SCHEMA_BYTES.len()];
            let mut i = 0;
            while i < __RB_SCHEMA_BYTES.len() {
                arr[i] = __RB_SCHEMA_BYTES[i];
                i += 1;
            }
            arr
        };
    }
}
