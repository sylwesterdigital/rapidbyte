//! Validation transform plugin for Rapidbyte.
//!
//! Applies rule-based data contract assertions (not-null, regex, range, unique)
//! to in-flight Arrow batches, filtering or failing rows that violate constraints.

mod config;
mod transform;
mod validate;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::plugin(transform)]
pub struct TransformValidate {
    config: config::CompiledConfig,
}

impl Transform for TransformValidate {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {
        let compiled = config.compile().map_err(|message| {
            PluginError::config("VALIDATE_CONFIG", format!("Invalid validation config: {message}"))
        })?;
        Ok((
            Self { config: compiled },
            PluginInfo {
                protocol_version: ProtocolVersion::V5,
                features: vec![],
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            },
        ))
    }

    async fn validate(
        config: &Self::Config,
        _ctx: &Context,
    ) -> Result<ValidationResult, PluginError> {
        match config.compile() {
            Ok(_) => Ok(ValidationResult {
                status: ValidationStatus::Success,
                message: "Validation transform config is valid".to_string(),
                warnings: Vec::new(),
            }),
            Err(message) => Ok(ValidationResult {
                status: ValidationStatus::Failed,
                message,
                warnings: Vec::new(),
            }),
        }
    }

    async fn transform(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, PluginError> {
        transform::run(ctx, &stream, &self.config).await
    }
}
