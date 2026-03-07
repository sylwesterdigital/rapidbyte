//! Async-first plugin traits and component export macros.

use serde::de::DeserializeOwned;

use crate::catalog::Catalog;
use crate::context::Context;
use crate::error::{PluginError, ValidationResult, ValidationStatus};
use crate::metric::{ReadSummary, TransformSummary, WriteSummary};
use crate::stream::StreamContext;
use crate::wire::PluginInfo;

/// Default validation response for plugins that do not implement validation.
pub fn default_validation<C>(
    _config: &C,
    _ctx: &Context,
) -> Result<ValidationResult, PluginError> {
    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message: "Validation not implemented".to_string(),
    })
}

/// Default close implementation.
pub async fn default_close(_ctx: &Context) -> Result<(), PluginError> {
    Ok(())
}

/// Source plugin lifecycle.
#[allow(async_fn_in_trait)]
pub trait Source: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError>;

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, PluginError> {
        default_validation(config, ctx)
    }

    async fn discover(&mut self, ctx: &Context) -> Result<Catalog, PluginError>;

    async fn read(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<ReadSummary, PluginError>;

    async fn close(&mut self, ctx: &Context) -> Result<(), PluginError> {
        default_close(ctx).await
    }
}

/// Destination plugin lifecycle.
#[allow(async_fn_in_trait)]
pub trait Destination: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError>;

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, PluginError> {
        default_validation(config, ctx)
    }

    async fn write(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, PluginError>;

    async fn close(&mut self, ctx: &Context) -> Result<(), PluginError> {
        default_close(ctx).await
    }
}

/// Transform plugin lifecycle.
#[allow(async_fn_in_trait)]
pub trait Transform: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError>;

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, PluginError> {
        default_validation(config, ctx)
    }

    async fn transform(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, PluginError>;

    async fn close(&mut self, ctx: &Context) -> Result<(), PluginError> {
        default_close(ctx).await
    }
}

#[cfg(test)]
#[allow(dead_code, unused_imports)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::context::Context;
    use crate::error::{PluginError, ValidationResult, ValidationStatus};
    use crate::metric::{ReadSummary, TransformSummary, WriteSummary};
    use crate::stream::StreamContext;
    use crate::wire::{PluginInfo, ProtocolVersion};
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

        async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {
            Ok((
                Self { config },
                PluginInfo {
                    protocol_version: ProtocolVersion::V5,
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn discover(&mut self, _ctx: &Context) -> Result<Catalog, PluginError> {
            Ok(Catalog { streams: vec![] })
        }

        async fn read(
            &mut self,
            _ctx: &Context,
            _stream: StreamContext,
        ) -> Result<ReadSummary, PluginError> {
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

        async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {
            Ok((
                Self { config },
                PluginInfo {
                    protocol_version: ProtocolVersion::V5,
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn write(
            &mut self,
            _ctx: &Context,
            _stream: StreamContext,
        ) -> Result<WriteSummary, PluginError> {
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

        async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {
            Ok((
                Self { config },
                PluginInfo {
                    protocol_version: ProtocolVersion::V5,
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn transform(
            &mut self,
            _ctx: &Context,
            _stream: StreamContext,
        ) -> Result<TransformSummary, PluginError> {
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
