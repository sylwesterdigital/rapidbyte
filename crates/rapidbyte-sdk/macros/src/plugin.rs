//! `#[plugin(source|destination|transform)]` attribute macro implementation.
//!
//! Generates all WIT bindings, component glue, manifest embedding, and config
//! schema embedding that previously required three separate declarative macros.

use std::path::PathBuf;

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, ItemStruct, Result};

struct ManifestFeatures {
    has_partitioned_read: bool,
    has_cdc: bool,
    has_bulk_load: bool,
}

fn read_manifest_features(kind: &PluginKind) -> Option<ManifestFeatures> {
    let out_dir = std::env::var("OUT_DIR").ok()?;
    let path = PathBuf::from(out_dir).join("rapidbyte_manifest.json");
    let json = std::fs::read_to_string(&path).ok()?;
    let manifest: serde_json::Value = serde_json::from_str(&json).ok()?;

    let features_key = match kind {
        PluginKind::Source => "source",
        PluginKind::Destination => "destination",
        PluginKind::Transform => {
            return Some(ManifestFeatures {
                has_partitioned_read: false,
                has_cdc: false,
                has_bulk_load: false,
            })
        }
    };

    let features: Vec<String> = manifest
        .get("roles")
        .and_then(|r| r.get(features_key))
        .and_then(|s| s.get("features"))
        .and_then(|f| serde_json::from_value(f.clone()).ok())
        .unwrap_or_default();

    Some(ManifestFeatures {
        has_partitioned_read: features.iter().any(|f| f == "partitioned_read"),
        has_cdc: features.iter().any(|f| f == "cdc"),
        has_bulk_load: features
            .iter()
            .any(|f| f == "bulk_load" || f == "bulk_load_copy"),
    })
}

fn gen_feature_assertions(
    kind: &PluginKind,
    struct_name: &Ident,
    features: &ManifestFeatures,
) -> TokenStream {
    let mut assertions = Vec::new();

    match kind {
        PluginKind::Source => {
            if features.has_partitioned_read {
                assertions.push(quote! {
                    const _: () = {
                        fn __assert_partitioned_source<T: ::rapidbyte_sdk::features::PartitionedSource>() {}
                        fn __check() { __assert_partitioned_source::<#struct_name>(); }
                    };
                });
            }
            if features.has_cdc {
                assertions.push(quote! {
                    const _: () = {
                        fn __assert_cdc_source<T: ::rapidbyte_sdk::features::CdcSource>() {}
                        fn __check() { __assert_cdc_source::<#struct_name>(); }
                    };
                });
            }
        }
        PluginKind::Destination => {
            if features.has_bulk_load {
                assertions.push(quote! {
                    const _: () = {
                        fn __assert_bulk_load_dest<T: ::rapidbyte_sdk::features::BulkLoadDestination>() {}
                        fn __check() { __assert_bulk_load_dest::<#struct_name>(); }
                    };
                });
            }
        }
        PluginKind::Transform => {}
    }

    quote! { #(#assertions)* }
}

/// The plugin kind parsed from the attribute argument.
pub enum PluginKind {
    Source,
    Destination,
    Transform,
}

impl Parse for PluginKind {
    fn parse(input: ParseStream) -> Result<Self> {
        let ident: Ident = input.parse()?;
        match ident.to_string().as_str() {
            "source" => Ok(PluginKind::Source),
            "destination" => Ok(PluginKind::Destination),
            "transform" => Ok(PluginKind::Transform),
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
pub fn expand(kind: PluginKind, input: ItemStruct) -> Result<TokenStream> {
    let struct_name = &input.ident;

    let bindings_mod = quote! { __rb_bindings };
    let (world_name, trait_path) = match kind {
        PluginKind::Source => (
            "rapidbyte-source",
            quote! { ::rapidbyte_sdk::plugin::Source },
        ),
        PluginKind::Destination => (
            "rapidbyte-destination",
            quote! { ::rapidbyte_sdk::plugin::Destination },
        ),
        PluginKind::Transform => (
            "rapidbyte-transform",
            quote! { ::rapidbyte_sdk::plugin::Transform },
        ),
    };

    let features = read_manifest_features(&kind);
    let wit_bindings = gen_wit_bindings(world_name);
    let common = gen_common(struct_name);
    let guest_impl = gen_guest_impl(&kind, struct_name, &trait_path, features.as_ref());
    let embeds = gen_embeds(struct_name, &trait_path);
    let feature_assertions = features
        .as_ref()
        .map(|f| gen_feature_assertions(&kind, struct_name, f))
        .unwrap_or_default();

    Ok(quote! {
        #input

        #wit_bindings
        #common
        #guest_impl

        #feature_assertions
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
                path: "../../../wit",
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

        static PLUGIN: OnceLock<SyncRefCell> = OnceLock::new();

        fn get_state() -> &'static RefCell<Option<#struct_name>> {
            &PLUGIN.get_or_init(|| SyncRefCell(RefCell::new(None))).0
        }

        fn to_component_error(
            error: ::rapidbyte_sdk::error::PluginError,
        ) -> __rb_bindings::rapidbyte::plugin::types::PluginError {
            use __rb_bindings::rapidbyte::plugin::types::{
                BackoffClass as CBackoffClass, CommitState as CCommitState,
                PluginError as CPluginError, ErrorCategory as CErrorCategory,
                ErrorScope as CErrorScope,
            };
            use ::rapidbyte_sdk::error::{
                BackoffClass, CommitState, ErrorCategory, ErrorScope,
            };

            CPluginError {
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
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            serde_json::from_str(&ctx_json).map_err(|e| {
                to_component_error(::rapidbyte_sdk::error::PluginError::config(
                    "INVALID_STREAM_CTX",
                    format!("Invalid StreamContext JSON: {}", e),
                ))
            })
        }

        fn parse_config<T: serde::de::DeserializeOwned>(
            config_json: &str,
        ) -> Result<T, __rb_bindings::rapidbyte::plugin::types::PluginError> {
            serde_json::from_str(config_json).map_err(|e| {
                to_component_error(::rapidbyte_sdk::error::PluginError::config(
                    "INVALID_CONFIG",
                    format!("Config parse error: {}", e),
                ))
            })
        }

        fn parse_saved_config<T: serde::de::DeserializeOwned>(
        ) -> Result<T, __rb_bindings::rapidbyte::plugin::types::PluginError> {
            let json = CONFIG_JSON.get().expect("open must be called before validate");
            parse_config(json)
        }

        fn to_component_validation(
            result: ::rapidbyte_sdk::error::ValidationResult,
        ) -> __rb_bindings::rapidbyte::plugin::types::ValidationReport {
            use __rb_bindings::rapidbyte::plugin::types::{
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
                warnings: result.warnings,
            }
        }
    }
}

/// Generate the Guest impl with lifecycle + role-specific methods.
fn gen_guest_impl(
    kind: &PluginKind,
    struct_name: &Ident,
    trait_path: &TokenStream,
    features: Option<&ManifestFeatures>,
) -> TokenStream {
    let lifecycle = gen_lifecycle_methods(struct_name, trait_path);

    let (guest_trait_path, role_methods) = match kind {
        PluginKind::Source => (
            quote! { __rb_bindings::exports::rapidbyte::plugin::source::Guest },
            gen_source_methods(struct_name, trait_path, features),
        ),
        PluginKind::Destination => (
            quote! { __rb_bindings::exports::rapidbyte::plugin::destination::Guest },
            gen_dest_methods(struct_name, trait_path, features),
        ),
        PluginKind::Transform => (
            quote! { __rb_bindings::exports::rapidbyte::plugin::transform::Guest },
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
        ) -> Result<u64, __rb_bindings::rapidbyte::plugin::types::PluginError> {
            let _ = CONFIG_JSON.set(config_json.clone());

            let config: <#struct_name as #trait_path>::Config =
                parse_config(&config_json)?;

            let rt = get_runtime();
            let (instance, _plugin_info) = rt
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
            __rb_bindings::rapidbyte::plugin::types::ValidationReport,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            let config: <#struct_name as #trait_path>::Config = parse_saved_config()?;
            let ctx = CONTEXT.get().expect("open must be called before validate");

            let rt = get_runtime();
            rt.block_on(<#struct_name as #trait_path>::validate(&config, ctx))
                .map(to_component_validation)
                .map_err(to_component_error)
        }

        fn close(_session: u64) -> Result<(), __rb_bindings::rapidbyte::plugin::types::PluginError> {
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

/// Generate the shared `run` method preamble: parse request, acquire state.
fn gen_run_preamble() -> TokenStream {
    quote! {
        let stream = parse_stream_context(request.stream_context_json)?;
        let base_ctx = CONTEXT.get().expect("Plugin not opened");
        let ctx = base_ctx.with_stream(&stream.stream_name);
        let rt = get_runtime();
        let state_cell = get_state();
        let mut state_ref = state_cell.borrow_mut();
        let conn = state_ref.as_mut().expect("Plugin not opened");
    }
}

/// Build the read dispatch body based on declared features.
///
/// Conditionally inserts `if` branches for PartitionedRead and Cdc,
/// falling back to `Source::read` in the else branch.
fn gen_read_dispatch(
    struct_name: &Ident,
    trait_path: &TokenStream,
    features: Option<&ManifestFeatures>,
) -> TokenStream {
    let has_partitioned = features.is_some_and(|f| f.has_partitioned_read);
    let has_cdc = features.is_some_and(|f| f.has_cdc);

    if !has_partitioned && !has_cdc {
        return quote! {
            rt.block_on(<#struct_name as #trait_path>::read(conn, &ctx, stream))
                .map_err(to_component_error)?
        };
    }

    let partition_branch = has_partitioned.then(|| quote! {
        if let Some(partition) = stream.partition_coordinates_typed() {
            return <#struct_name as ::rapidbyte_sdk::features::PartitionedSource>::read_partition(
                conn, &ctx, stream, partition
            ).await;
        }
    });

    let cdc_branch = has_cdc.then(|| {
        quote! {
            if stream.sync_mode == ::rapidbyte_sdk::wire::SyncMode::Cdc {
                let resume = stream.cdc_resume_token().unwrap_or(
                    ::rapidbyte_sdk::stream::CdcResumeToken {
                        value: None,
                        cursor_type: ::rapidbyte_sdk::cursor::CursorType::Utf8,
                    }
                );
                return <#struct_name as ::rapidbyte_sdk::features::CdcSource>::read_changes(
                    conn, &ctx, stream, resume
                ).await;
            }
        }
    });

    quote! {
        rt.block_on(async {
            #partition_branch
            #cdc_branch
            <#struct_name as #trait_path>::read(conn, &ctx, stream).await
        }).map_err(to_component_error)?
    }
}

/// Generate source-specific methods: discover, run.
fn gen_source_methods(
    struct_name: &Ident,
    trait_path: &TokenStream,
    features: Option<&ManifestFeatures>,
) -> TokenStream {
    let read_dispatch = gen_read_dispatch(struct_name, trait_path, features);
    let run_preamble = gen_run_preamble();

    quote! {
        fn discover(
            _session: u64,
        ) -> Result<String, __rb_bindings::rapidbyte::plugin::types::PluginError> {
            let ctx = CONTEXT.get().expect("Plugin not opened");
            let rt = get_runtime();
            let state_cell = get_state();
            let mut state_ref = state_cell.borrow_mut();
            let conn = state_ref.as_mut().expect("Plugin not opened");

            let catalog = rt
                .block_on(<#struct_name as #trait_path>::discover(conn, ctx))
                .map_err(to_component_error)?;

            serde_json::to_string(&catalog).map_err(|e| {
                to_component_error(::rapidbyte_sdk::error::PluginError::internal(
                    "SERIALIZE_CATALOG",
                    e.to_string(),
                ))
            })
        }

        fn run(
            _session: u64,
            request: __rb_bindings::rapidbyte::plugin::types::RunRequest,
        ) -> Result<
            __rb_bindings::rapidbyte::plugin::types::RunSummary,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            #run_preamble

            let summary = #read_dispatch;

            Ok(__rb_bindings::rapidbyte::plugin::types::RunSummary {
                kind: __rb_bindings::rapidbyte::plugin::types::PluginKind::Source,
                read: Some(__rb_bindings::rapidbyte::plugin::types::ReadSummary {
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
fn gen_dest_methods(
    struct_name: &Ident,
    trait_path: &TokenStream,
    features: Option<&ManifestFeatures>,
) -> TokenStream {
    let write_dispatch = match features {
        Some(f) if f.has_bulk_load => {
            quote! {
                rt.block_on(
                    <#struct_name as ::rapidbyte_sdk::features::BulkLoadDestination>::write_bulk(conn, &ctx, stream)
                ).map_err(to_component_error)?
            }
        }
        _ => {
            quote! {
                rt.block_on(<#struct_name as #trait_path>::write(conn, &ctx, stream))
                    .map_err(to_component_error)?
            }
        }
    };

    let run_preamble = gen_run_preamble();

    quote! {
        fn run(
            _session: u64,
            request: __rb_bindings::rapidbyte::plugin::types::RunRequest,
        ) -> Result<
            __rb_bindings::rapidbyte::plugin::types::RunSummary,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            #run_preamble

            let summary = #write_dispatch;

            Ok(__rb_bindings::rapidbyte::plugin::types::RunSummary {
                kind: __rb_bindings::rapidbyte::plugin::types::PluginKind::Destination,
                read: None,
                write: Some(__rb_bindings::rapidbyte::plugin::types::WriteSummary {
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
    let run_preamble = gen_run_preamble();

    quote! {
        fn run(
            _session: u64,
            request: __rb_bindings::rapidbyte::plugin::types::RunRequest,
        ) -> Result<
            __rb_bindings::rapidbyte::plugin::types::RunSummary,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            #run_preamble

            let summary = rt
                .block_on(<#struct_name as #trait_path>::transform(conn, &ctx, stream))
                .map_err(to_component_error)?;

            Ok(__rb_bindings::rapidbyte::plugin::types::RunSummary {
                kind: __rb_bindings::rapidbyte::plugin::types::PluginKind::Transform,
                read: None,
                write: None,
                transform: Some(__rb_bindings::rapidbyte::plugin::types::TransformSummary {
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
