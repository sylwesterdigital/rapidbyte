//! `PostgreSQL` client connection and validation helpers for dest-postgres.

use tokio_postgres::{Client, Config as PgConfig, NoTls};

use rapidbyte_sdk::prelude::*;

/// Connect to `PostgreSQL` using the provided config.
///
/// # Errors
///
/// Returns `Err` if the TCP connection or `PostgreSQL` authentication fails.
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
            rapidbyte_sdk::host_ffi::log(0, &format!("PostgreSQL connection error: {e}"));
        }
    });

    client
        .execute("SET synchronous_commit = OFF", &[])
        .await
        .map_err(|e| format!("Failed to set synchronous_commit: {e}"))?;

    Ok(client)
}

/// Validate `PostgreSQL` connectivity and target schema.
///
/// # Errors
///
/// Returns `Err` if the connection fails or the `SELECT 1` health check fails.
pub(crate) async fn validate(
    config: &crate::config::Config,
) -> Result<ValidationResult, ConnectorError> {
    let client = connect(config)
        .await
        .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;

    client.query_one("SELECT 1", &[]).await.map_err(|e| {
        ConnectorError::transient_network(
            "CONNECTION_TEST_FAILED",
            format!("Connection test failed: {e}"),
        )
    })?;

    let schema_check = client
        .query_one(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1",
            &[&config.schema],
        )
        .await;

    let message = match schema_check {
        Ok(_) => format!(
            "Connected to {}:{}/{} (schema: {})",
            config.host, config.port, config.database, config.schema
        ),
        Err(_) => format!(
            "Connected to {}:{}/{} (schema '{}' does not exist, will be created)",
            config.host, config.port, config.database, config.schema
        ),
    };

    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message,
    })
}
