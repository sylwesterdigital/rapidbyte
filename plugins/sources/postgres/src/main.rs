//! Source plugin for `PostgreSQL`.
//!
//! Implements discovery and read paths (full-refresh, incremental cursor reads,
//! and CDC via `pgoutput` logical replication) and streams Arrow IPC batches to
//! the host.

mod cdc;
mod client;
mod config;
mod cursor;
mod discovery;
mod encode;
mod metrics;
mod query;
mod reader;
mod types;

use std::time::Instant;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::plugin(source)]
pub struct SourcePostgres {
    config: config::Config,
}

impl Source for SourcePostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {
        config.validate()?;
        Ok((
            Self { config },
            PluginInfo {
                protocol_version: ProtocolVersion::V5,
                features: vec![Feature::Cdc, Feature::PartitionedRead],
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            },
        ))
    }

    async fn discover(&mut self, ctx: &Context) -> Result<Catalog, PluginError> {
        let _ = ctx;
        let client = client::connect(&self.config)
            .await
            .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
        discovery::discover_catalog(&client)
            .await
            .map(|streams| Catalog { streams })
            .map_err(|e| PluginError::transient_db("DISCOVERY_FAILED", e))
    }

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, PluginError> {
        let _ = ctx;
        client::validate(config).await
    }

    async fn read(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<ReadSummary, PluginError> {
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
            .await
            .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();

        reader::read_stream(&client, ctx, &stream, connect_secs)
            .await
            .map_err(|e| PluginError::internal("READ_FAILED", e))
    }

    async fn close(&mut self, ctx: &Context) -> Result<(), PluginError> {
        ctx.log(LogLevel::Info, "source-postgres: close (no-op)");
        Ok(())
    }
}

impl PartitionedSource for SourcePostgres {
    async fn read_partition(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
        _partition: PartitionCoordinates,
    ) -> Result<ReadSummary, PluginError> {
        // Partition coordinates are already embedded in StreamContext;
        // reader::read_stream extracts them via stream.partition_coordinates().
        self.read(ctx, stream).await
    }
}

impl CdcSource for SourcePostgres {
    async fn read_changes(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
        _resume: CdcResumeToken,
    ) -> Result<ReadSummary, PluginError> {
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
            .await
            .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();

        cdc::read_cdc_changes(&client, ctx, &stream, &self.config, connect_secs)
            .await
            .map_err(|e| PluginError::internal("CDC_READ_FAILED", e))
    }
}
