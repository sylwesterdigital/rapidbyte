//! Destination connector for `PostgreSQL`.
//!
//! Receives Arrow IPC batches from the host and writes them to `PostgreSQL`
//! with transactional checkpoints and schema evolution handling.

mod client;
mod config;
mod copy;
mod ddl;
mod decode;
mod insert;
mod pg_error;
mod type_map;
mod watermark;
mod writer;

use rapidbyte_sdk::prelude::*;

use config::LoadMethod;

#[rapidbyte_sdk::connector(destination)]
pub struct DestPostgres {
    config: config::Config,
}

impl Destination for DestPostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
        let mut features = vec![Feature::ExactlyOnce];
        if config.load_method == LoadMethod::Copy {
            features.push(Feature::BulkLoadCopy);
        }
        Ok((
            Self { config },
            ConnectorInfo {
                protocol_version: ProtocolVersion::V4,
                features,
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            },
        ))
    }

    async fn validate(
        config: &Self::Config,
        _ctx: &Context,
    ) -> Result<ValidationResult, ConnectorError> {
        client::validate(config).await
    }

    async fn write(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, ConnectorError> {
        writer::write_stream(&self.config, ctx, &stream).await
    }

    async fn close(&mut self, ctx: &Context) -> Result<(), ConnectorError> {
        ctx.log(LogLevel::Info, "dest-postgres: close (no-op)");
        Ok(())
    }
}
