//! `PostgreSQL` client connection and validation helpers for source-postgres.

use tokio_postgres::{Client, Config as PgConfig, NoTls};

use rapidbyte_sdk::prelude::*;

/// Connect to `PostgreSQL` using the provided config.
pub(crate) async fn connect(config: &crate::config::Config) -> Result<Client, String> {
    let mut pg = PgConfig::new();
    pg.host(&config.host);
    pg.port(config.port);
    pg.user(&config.user);
    if !config.password.is_empty() {
        pg.password(&config.password);
    }
    pg.dbname(&config.database);

    let stream = rapidbyte_sdk::host_tcp::HostTcpStream::connect(&config.host, config.port)
        .map_err(|e| format!("Connection failed: {e}"))?;
    let (client, connection) = pg
        .connect_raw(stream, NoTls)
        .await
        .map_err(|e| format!("Connection failed: {e}"))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            rapidbyte_sdk::host_ffi::log_error(&format!("PostgreSQL connection error: {e}"));
        }
    });

    Ok(client)
}

/// Validate `PostgreSQL` connectivity.
pub(crate) async fn validate(
    config: &crate::config::Config,
) -> Result<ValidationResult, PluginError> {
    let client = connect(config)
        .await
        .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
    client.query_one("SELECT 1", &[]).await.map_err(|e| {
        PluginError::transient_network(
            "CONNECTION_TEST_FAILED",
            format!("Connection test failed: {e}"),
        )
    })?;
    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message: format!(
            "Connected to {}:{}/{}",
            config.host, config.port, config.database
        ),
        warnings: Vec::new(),
    })
}
