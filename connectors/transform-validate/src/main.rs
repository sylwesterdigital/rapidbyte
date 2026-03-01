//! Validation transform connector for Rapidbyte.
//!
//! Applies rule-based data contract assertions (not-null, regex, range, unique)
//! to in-flight Arrow batches, filtering or failing rows that violate constraints.

mod config;
mod transform;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::connector(transform)]
pub struct TransformValidate {
    config: config::CompiledConfig,
}

impl Transform for TransformValidate {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
        let compiled = config.compile().map_err(|message| {
            ConnectorError::config("VALIDATE_CONFIG", format!("Invalid validation config: {message}"))
        })?;
        Ok((
            Self { config: compiled },
            ConnectorInfo {
                protocol_version: ProtocolVersion::V4,
                features: vec![],
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            },
        ))
    }

    async fn validate(
        config: &Self::Config,
        _ctx: &Context,
    ) -> Result<ValidationResult, ConnectorError> {
        match config.compile() {
            Ok(_) => Ok(ValidationResult {
                status: ValidationStatus::Success,
                message: "Validation transform config is valid".to_string(),
            }),
            Err(message) => Ok(ValidationResult {
                status: ValidationStatus::Failed,
                message,
            }),
        }
    }

    async fn transform(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, ConnectorError> {
        transform::run(ctx, &stream, &self.config).await
    }
}
