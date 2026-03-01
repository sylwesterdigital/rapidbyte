//! Async-first connector traits and component export macros.

use serde::de::DeserializeOwned;

use crate::catalog::Catalog;
use crate::context::Context;
use crate::error::{ConnectorError, ValidationResult, ValidationStatus};
use crate::metric::{ReadSummary, TransformSummary, WriteSummary};
use crate::stream::StreamContext;
use crate::wire::ConnectorInfo;

/// Default validation response for connectors that do not implement validation.
pub fn default_validation<C>(
    _config: &C,
    _ctx: &Context,
) -> Result<ValidationResult, ConnectorError> {
    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message: "Validation not implemented".to_string(),
    })
}

/// Default close implementation.
pub async fn default_close(_ctx: &Context) -> Result<(), ConnectorError> {
    Ok(())
}

/// Source connector lifecycle.
#[allow(async_fn_in_trait)]
pub trait Source: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, ConnectorError> {
        default_validation(config, ctx)
    }

    async fn discover(&mut self, ctx: &Context) -> Result<Catalog, ConnectorError>;

    async fn read(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<ReadSummary, ConnectorError>;

    async fn close(&mut self, ctx: &Context) -> Result<(), ConnectorError> {
        default_close(ctx).await
    }
}

/// Destination connector lifecycle.
#[allow(async_fn_in_trait)]
pub trait Destination: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, ConnectorError> {
        default_validation(config, ctx)
    }

    async fn write(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, ConnectorError>;

    async fn close(&mut self, ctx: &Context) -> Result<(), ConnectorError> {
        default_close(ctx).await
    }
}

/// Transform connector lifecycle.
#[allow(async_fn_in_trait)]
pub trait Transform: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, ConnectorError> {
        default_validation(config, ctx)
    }

    async fn transform(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, ConnectorError>;

    async fn close(&mut self, ctx: &Context) -> Result<(), ConnectorError> {
        default_close(ctx).await
    }
}

#[cfg(test)]
#[allow(dead_code, unused_imports)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::context::Context;
    use crate::error::{ConnectorError, ValidationResult, ValidationStatus};
    use crate::metric::{ReadSummary, TransformSummary, WriteSummary};
    use crate::stream::StreamContext;
    use crate::wire::{ConnectorInfo, ProtocolVersion};
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct TestConfig {
        host: String,
    }

    struct TestSource {
        config: TestConfig,
    }

    impl Source for TestSource {
        type Config = TestConfig;

        async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
            Ok((
                Self { config },
                ConnectorInfo {
                    protocol_version: ProtocolVersion::V4,
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn discover(&mut self, _ctx: &Context) -> Result<Catalog, ConnectorError> {
            Ok(Catalog { streams: vec![] })
        }

        async fn read(
            &mut self,
            _ctx: &Context,
            _stream: StreamContext,
        ) -> Result<ReadSummary, ConnectorError> {
            Ok(ReadSummary {
                records_read: 0,
                bytes_read: 0,
                batches_emitted: 0,
                checkpoint_count: 0,
                records_skipped: 0,
                perf: None,
            })
        }
    }

    struct TestDest {
        config: TestConfig,
    }

    impl Destination for TestDest {
        type Config = TestConfig;

        async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
            Ok((
                Self { config },
                ConnectorInfo {
                    protocol_version: ProtocolVersion::V4,
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn write(
            &mut self,
            _ctx: &Context,
            _stream: StreamContext,
        ) -> Result<WriteSummary, ConnectorError> {
            Ok(WriteSummary {
                records_written: 0,
                bytes_written: 0,
                batches_written: 0,
                checkpoint_count: 0,
                records_failed: 0,
                perf: None,
            })
        }
    }

    struct TestTransform {
        config: TestConfig,
    }

    impl Transform for TestTransform {
        type Config = TestConfig;

        async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
            Ok((
                Self { config },
                ConnectorInfo {
                    protocol_version: ProtocolVersion::V4,
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn transform(
            &mut self,
            _ctx: &Context,
            _stream: StreamContext,
        ) -> Result<TransformSummary, ConnectorError> {
            Ok(TransformSummary {
                records_in: 0,
                records_out: 0,
                bytes_in: 0,
                bytes_out: 0,
                batches_processed: 0,
            })
        }
    }

    #[test]
    fn test_trait_shapes_compile() {
        fn assert_source<T: Source>() {}
        fn assert_dest<T: Destination>() {}
        fn assert_transform<T: Transform>() {}
        assert_source::<TestSource>();
        assert_dest::<TestDest>();
        assert_transform::<TestTransform>();
    }
}
