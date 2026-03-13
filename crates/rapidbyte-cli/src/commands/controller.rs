//! Controller server subcommand.

use anyhow::Result;
use std::path::Path;
use std::time::Duration;

#[allow(clippy::too_many_arguments)]
pub async fn execute(
    listen: &str,
    metadata_database_url: Option<&str>,
    signing_key: Option<&str>,
    auth_token: Option<&str>,
    allow_unauthenticated: bool,
    allow_insecure_signing_key: bool,
    reconciliation_timeout: Option<Duration>,
    tls_cert: Option<&Path>,
    tls_key: Option<&Path>,
) -> Result<()> {
    let config = build_config(
        listen,
        metadata_database_url,
        signing_key,
        auth_token,
        allow_unauthenticated,
        allow_insecure_signing_key,
        reconciliation_timeout,
        tls_cert,
        tls_key,
    )?;
    rapidbyte_controller::run(config).await
}

#[allow(clippy::too_many_arguments)]
fn build_config(
    listen: &str,
    metadata_database_url: Option<&str>,
    signing_key: Option<&str>,
    auth_token: Option<&str>,
    allow_unauthenticated: bool,
    allow_insecure_signing_key: bool,
    reconciliation_timeout: Option<Duration>,
    tls_cert: Option<&Path>,
    tls_key: Option<&Path>,
) -> Result<rapidbyte_controller::ControllerConfig> {
    fn validate_auth_token(token: &str) -> Result<()> {
        if token.trim().is_empty() {
            anyhow::bail!("auth token must not be empty or whitespace");
        }
        Ok(())
    }

    let addr = listen
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid listen address: {e}"))?;
    let mut config = rapidbyte_controller::ControllerConfig {
        listen_addr: addr,
        ..Default::default()
    };
    if let Some(url) = metadata_database_url {
        if url.trim().is_empty() {
            anyhow::bail!("metadata database URL must not be empty or whitespace");
        }
        config.metadata_database_url = Some(url.to_string());
    }
    if let Some(key) = signing_key {
        config.signing_key = key.as_bytes().to_vec();
    }
    if let Some(token) = auth_token {
        validate_auth_token(token)?;
        config.auth_tokens = vec![token.to_string()];
    }
    config.allow_unauthenticated = allow_unauthenticated;
    config.allow_insecure_default_signing_key = allow_insecure_signing_key;
    if let Some(timeout) = reconciliation_timeout {
        config.reconciliation_timeout = timeout;
    }
    if config.auth_tokens.is_empty() && !config.allow_unauthenticated {
        anyhow::bail!(
            "controller requires --auth-token / RAPIDBYTE_AUTH_TOKEN or --allow-unauthenticated"
        );
    }
    if config.signing_key == rapidbyte_controller::ControllerConfig::default().signing_key
        && !config.allow_insecure_default_signing_key
    {
        anyhow::bail!(
            "controller requires --signing-key / RAPIDBYTE_SIGNING_KEY or --allow-insecure-signing-key"
        );
    }
    if config.metadata_database_url.is_none() {
        anyhow::bail!(
            "controller requires --metadata-database-url / RAPIDBYTE_CONTROLLER_METADATA_DATABASE_URL"
        );
    }
    match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => {
            config.tls = Some(rapidbyte_controller::ServerTlsConfig {
                cert_pem: std::fs::read(cert)?,
                key_pem: std::fs::read(key)?,
            });
        }
        (None, None) => {}
        _ => anyhow::bail!("controller TLS requires both --tls-cert and --tls-key"),
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn controller_execute_uses_auth_token() {
        let config = build_config(
            "[::]:9090",
            Some("postgresql://localhost/controller"),
            Some("signing"),
            Some("secret"),
            false,
            false,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(config.auth_tokens, vec!["secret".to_string()]);
        assert_eq!(config.signing_key, b"signing".to_vec());
    }

    #[test]
    fn controller_execute_requires_auth_or_explicit_override() {
        let err = build_config(
            "[::]:9090",
            Some("postgresql://localhost/controller"),
            Some("signing"),
            None,
            false,
            false,
            None,
            None,
            None,
        )
        .err()
        .unwrap();
        assert!(err.to_string().contains("controller requires --auth-token"));
    }

    #[test]
    fn controller_execute_allows_explicit_unauthenticated_mode() {
        let config = build_config(
            "[::]:9090",
            Some("postgresql://localhost/controller"),
            Some("signing"),
            None,
            true,
            false,
            None,
            None,
            None,
        )
        .unwrap();
        assert!(config.auth_tokens.is_empty());
        assert!(config.allow_unauthenticated);
    }

    #[test]
    fn controller_execute_rejects_empty_auth_token() {
        let err = build_config(
            "[::]:9090",
            Some("postgresql://localhost/controller"),
            Some("signing"),
            Some(""),
            false,
            false,
            None,
            None,
            None,
        )
        .err()
        .unwrap();
        assert!(err.to_string().contains("auth token must not be empty"));
    }

    #[test]
    fn controller_execute_rejects_whitespace_auth_token() {
        let err = build_config(
            "[::]:9090",
            Some("postgresql://localhost/controller"),
            Some("signing"),
            Some("   "),
            false,
            false,
            None,
            None,
            None,
        )
        .err()
        .unwrap();
        assert!(err.to_string().contains("auth token must not be empty"));
    }

    #[test]
    fn controller_execute_rejects_default_signing_key() {
        let err = build_config(
            "[::]:9090",
            Some("postgresql://localhost/controller"),
            None,
            Some("secret"),
            false,
            false,
            None,
            None,
            None,
        )
        .err()
        .unwrap();
        assert!(err
            .to_string()
            .contains("controller requires --signing-key"));
    }

    #[test]
    fn controller_execute_allows_insecure_signing_key() {
        let config = build_config(
            "[::]:9090",
            Some("postgresql://localhost/controller"),
            None,
            Some("secret"),
            false,
            true,
            None,
            None,
            None,
        )
        .unwrap();
        assert!(config.allow_insecure_default_signing_key);
    }

    #[test]
    fn controller_execute_requires_metadata_database_url() {
        let err = build_config(
            "[::]:9090",
            None,
            Some("signing"),
            Some("secret"),
            false,
            false,
            None,
            None,
            None,
        )
        .err()
        .unwrap();
        assert!(err
            .to_string()
            .contains("controller requires --metadata-database-url"));
    }

    #[test]
    fn controller_execute_wires_tls() {
        let dir = tempdir().unwrap();
        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("server.key");
        std::fs::write(&cert_path, b"cert-pem").unwrap();
        std::fs::write(&key_path, b"key-pem").unwrap();

        let config = build_config(
            "[::]:9090",
            Some("postgresql://localhost/controller"),
            Some("signing"),
            None,
            true,
            false,
            None,
            Some(cert_path.as_path()),
            Some(key_path.as_path()),
        )
        .unwrap();

        assert_eq!(config.tls.as_ref().unwrap().cert_pem, b"cert-pem");
        assert_eq!(config.tls.as_ref().unwrap().key_pem, b"key-pem");
    }

    #[test]
    fn controller_execute_rejects_empty_metadata_database_url() {
        let err = build_config(
            "[::]:9090",
            Some("   "),
            Some("signing"),
            Some("secret"),
            false,
            false,
            None,
            None,
            None,
        )
        .err()
        .unwrap();
        assert!(err
            .to_string()
            .contains("metadata database URL must not be empty"));
    }

    #[test]
    fn controller_execute_wires_reconciliation_timeout() {
        let config = build_config(
            "[::]:9090",
            Some("postgresql://localhost/controller"),
            Some("signing"),
            Some("secret"),
            false,
            false,
            Some(Duration::from_secs(42)),
            None,
            None,
        )
        .unwrap();
        assert_eq!(config.reconciliation_timeout, Duration::from_secs(42));
    }
}
