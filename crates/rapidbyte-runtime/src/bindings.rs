//! WIT binding glue: generated Wasmtime component bindings, Host trait impls,
//! and error/validation type converters for all three plugin worlds.

#![allow(clippy::needless_pass_by_value)]

// --- Generated bindings from WIT ---

pub mod source_bindings {
    wasmtime::component::bindgen!({
        path: "../../wit",
        world: "rapidbyte-source",
    });
}

pub mod dest_bindings {
    wasmtime::component::bindgen!({
        path: "../../wit",
        world: "rapidbyte-destination",
    });
}

pub mod transform_bindings {
    wasmtime::component::bindgen!({
        path: "../../wit",
        world: "rapidbyte-transform",
    });
}

// --- Error + validation converters + Host trait impls ---

use rapidbyte_types::error::{
    BackoffClass, CommitState, ErrorCategory, ErrorScope, PluginError, ValidationResult,
    ValidationStatus,
};

use crate::host_state::ComponentHostState;
use crate::socket::{SocketReadResult, SocketWriteResult};

macro_rules! for_each_world {
    ($macro_name:ident) => {
        $macro_name!(
            source_bindings,
            to_source_error,
            source_error_to_sdk,
            source_validation_to_sdk
        );
        $macro_name!(
            dest_bindings,
            to_dest_error,
            dest_error_to_sdk,
            dest_validation_to_sdk
        );
        $macro_name!(
            transform_bindings,
            to_transform_error,
            transform_error_to_sdk,
            transform_validation_to_sdk
        );
    };
}

macro_rules! define_error_converters {
    ($to_world_fn:ident, $from_world_fn:ident, $module:ident) => {
        fn $to_world_fn(
            error: PluginError,
        ) -> $module::rapidbyte::plugin::types::PluginError {
            use $module::rapidbyte::plugin::types::{
                BackoffClass as CBackoffClass, CommitState as CCommitState,
                PluginError as CPluginError, ErrorCategory as CErrorCategory,
                ErrorScope as CErrorScope,
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
                code: error.code.to_string(),
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
                details_json: error.details.map(|v| v.to_string()),
            }
        }

        pub fn $from_world_fn(
            error: $module::rapidbyte::plugin::types::PluginError,
        ) -> PluginError {
            PluginError {
                category: match error.category {
                    $module::rapidbyte::plugin::types::ErrorCategory::Config => {
                        ErrorCategory::Config
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::Auth => {
                        ErrorCategory::Auth
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::Permission => {
                        ErrorCategory::Permission
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::RateLimit => {
                        ErrorCategory::RateLimit
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::TransientNetwork => {
                        ErrorCategory::TransientNetwork
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::TransientDb => {
                        ErrorCategory::TransientDb
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::Data => {
                        ErrorCategory::Data
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::Schema => {
                        ErrorCategory::Schema
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::Internal => {
                        ErrorCategory::Internal
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::Frame => {
                        ErrorCategory::Frame
                    }
                },
                scope: match error.scope {
                    $module::rapidbyte::plugin::types::ErrorScope::PerStream => {
                        ErrorScope::Stream
                    }
                    $module::rapidbyte::plugin::types::ErrorScope::PerBatch => ErrorScope::Batch,
                    $module::rapidbyte::plugin::types::ErrorScope::PerRecord => {
                        ErrorScope::Record
                    }
                },
                code: error.code.into(),
                message: error.message,
                retryable: error.retryable,
                retry_after_ms: error.retry_after_ms,
                backoff_class: match error.backoff_class {
                    $module::rapidbyte::plugin::types::BackoffClass::Fast => BackoffClass::Fast,
                    $module::rapidbyte::plugin::types::BackoffClass::Normal => {
                        BackoffClass::Normal
                    }
                    $module::rapidbyte::plugin::types::BackoffClass::Slow => BackoffClass::Slow,
                },
                safe_to_retry: error.safe_to_retry,
                commit_state: error.commit_state.map(|state| match state {
                    $module::rapidbyte::plugin::types::CommitState::BeforeCommit => {
                        CommitState::BeforeCommit
                    }
                    $module::rapidbyte::plugin::types::CommitState::AfterCommitUnknown => {
                        CommitState::AfterCommitUnknown
                    }
                    $module::rapidbyte::plugin::types::CommitState::AfterCommitConfirmed => {
                        CommitState::AfterCommitConfirmed
                    }
                }),
                details: error
                    .details_json
                    .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok()),
            }
        }
    };
}

macro_rules! impl_host_trait_for_world {
    ($module:ident, $to_world_error:ident) => {
        impl $module::rapidbyte::plugin::host::Host for ComponentHostState {
            fn emit_batch(
                &mut self,
                handle: u64,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                self.emit_batch_impl(handle).map_err($to_world_error)
            }

            fn next_batch(
                &mut self,
            ) -> std::result::Result<
                Option<u64>,
                $module::rapidbyte::plugin::types::PluginError,
            > {
                self.next_batch_impl().map_err($to_world_error)
            }

            fn log(&mut self, level: u32, msg: String) {
                self.log_impl(level, msg);
            }

            fn checkpoint(
                &mut self,
                kind: u32,
                payload_json: String,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                self.checkpoint_impl(kind, payload_json)
                    .map_err($to_world_error)
            }

            fn metric(
                &mut self,
                payload_json: String,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                self.metric_impl(payload_json).map_err($to_world_error)
            }

            fn emit_dlq_record(
                &mut self,
                stream_name: String,
                record_json: String,
                error_message: String,
                error_category: String,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                self.emit_dlq_record_impl(stream_name, record_json, error_message, error_category)
                    .map_err($to_world_error)
            }

            fn state_get(
                &mut self,
                scope: u32,
                key: String,
            ) -> std::result::Result<
                Option<String>,
                $module::rapidbyte::plugin::types::PluginError,
            > {
                self.state_get_impl(scope, key).map_err($to_world_error)
            }

            fn state_put(
                &mut self,
                scope: u32,
                key: String,
                val: String,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                self.state_put_impl(scope, key, val)
                    .map_err($to_world_error)
            }

            fn state_cas(
                &mut self,
                scope: u32,
                key: String,
                expected: Option<String>,
                new_val: String,
            ) -> std::result::Result<bool, $module::rapidbyte::plugin::types::PluginError>
            {
                self.state_cas_impl(scope, key, expected, new_val)
                    .map_err($to_world_error)
            }

            fn frame_new(
                &mut self,
                capacity: u64,
            ) -> std::result::Result<u64, $module::rapidbyte::plugin::types::PluginError>
            {
                Ok(self.frame_new_impl(capacity))
            }

            fn frame_write(
                &mut self,
                handle: u64,
                chunk: Vec<u8>,
            ) -> std::result::Result<u64, $module::rapidbyte::plugin::types::PluginError>
            {
                self.frame_write_impl(handle, chunk)
                    .map_err($to_world_error)
            }

            fn frame_seal(
                &mut self,
                handle: u64,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                self.frame_seal_impl(handle).map_err($to_world_error)
            }

            fn frame_len(
                &mut self,
                handle: u64,
            ) -> std::result::Result<u64, $module::rapidbyte::plugin::types::PluginError>
            {
                self.frame_len_impl(handle).map_err($to_world_error)
            }

            fn frame_read(
                &mut self,
                handle: u64,
                offset: u64,
                len: u64,
            ) -> std::result::Result<Vec<u8>, $module::rapidbyte::plugin::types::PluginError>
            {
                self.frame_read_impl(handle, offset, len)
                    .map_err($to_world_error)
            }

            fn frame_drop(&mut self, handle: u64) {
                self.frame_drop_impl(handle);
            }

            fn connect_tcp(
                &mut self,
                host: String,
                port: u16,
            ) -> std::result::Result<u64, $module::rapidbyte::plugin::types::PluginError>
            {
                self.connect_tcp_impl(host, port).map_err($to_world_error)
            }

            fn socket_read(
                &mut self,
                handle: u64,
                len: u64,
            ) -> std::result::Result<
                $module::rapidbyte::plugin::types::SocketReadResult,
                $module::rapidbyte::plugin::types::PluginError,
            > {
                self.socket_read_impl(handle, len)
                    .map(|result| match result {
                        SocketReadResult::Data(data) => {
                            $module::rapidbyte::plugin::types::SocketReadResult::Data(data)
                        }
                        SocketReadResult::Eof => {
                            $module::rapidbyte::plugin::types::SocketReadResult::Eof
                        }
                        SocketReadResult::WouldBlock => {
                            $module::rapidbyte::plugin::types::SocketReadResult::WouldBlock
                        }
                    })
                    .map_err($to_world_error)
            }

            fn socket_write(
                &mut self,
                handle: u64,
                data: Vec<u8>,
            ) -> std::result::Result<
                $module::rapidbyte::plugin::types::SocketWriteResult,
                $module::rapidbyte::plugin::types::PluginError,
            > {
                self.socket_write_impl(handle, data)
                    .map(|result| match result {
                        SocketWriteResult::Written(n) => {
                            $module::rapidbyte::plugin::types::SocketWriteResult::Written(n)
                        }
                        SocketWriteResult::WouldBlock => {
                            $module::rapidbyte::plugin::types::SocketWriteResult::WouldBlock
                        }
                    })
                    .map_err($to_world_error)
            }

            fn socket_close(&mut self, handle: u64) {
                self.socket_close_impl(handle);
            }
        }
    };
}

macro_rules! define_validation_to_sdk {
    ($fn_name:ident, $module:ident) => {
        pub fn $fn_name(
            value: $module::rapidbyte::plugin::types::ValidationReport,
        ) -> ValidationResult {
            ValidationResult {
                status: match value.status {
                    $module::rapidbyte::plugin::types::ValidationStatus::Success => {
                        ValidationStatus::Success
                    }
                    $module::rapidbyte::plugin::types::ValidationStatus::Failed => {
                        ValidationStatus::Failed
                    }
                    $module::rapidbyte::plugin::types::ValidationStatus::Warning => {
                        ValidationStatus::Warning
                    }
                },
                message: value.message,
            }
        }
    };
}

macro_rules! define_world_glue {
    ($module:ident, $to_world_error:ident, $from_world_error:ident, $validation_fn:ident) => {
        define_error_converters!($to_world_error, $from_world_error, $module);
        impl_host_trait_for_world!($module, $to_world_error);
        impl $module::rapidbyte::plugin::types::Host for ComponentHostState {}
        define_validation_to_sdk!($validation_fn, $module);
    };
}

for_each_world!(define_world_glue);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v5_world_bindings_exist() {
        let _ = std::any::TypeId::of::<source_bindings::RapidbyteSource>();
        let _ = std::any::TypeId::of::<dest_bindings::RapidbyteDestination>();
        let _ = std::any::TypeId::of::<transform_bindings::RapidbyteTransform>();

        let _: Option<source_bindings::rapidbyte::plugin::types::RunRequest> = None;
        let _: Option<source_bindings::rapidbyte::plugin::types::RunSummary> = None;
    }
}
