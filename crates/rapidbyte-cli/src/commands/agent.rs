//! Agent worker subcommand.

use anyhow::Result;
use std::path::Path;

#[allow(clippy::too_many_arguments)]
pub async fn execute(
    controller: &str,
    flight_listen: &str,
    flight_advertise: &str,
    max_tasks: u32,
    signing_key: Option<&str>,
    auth_token: Option<&str>,
    controller_ca_cert: Option<&Path>,
    controller_tls_domain: Option<&str>,
    flight_tls_cert: Option<&Path>,
    flight_tls_key: Option<&Path>,
) -> Result<()> {
    let config = build_config(
        controller,
        flight_listen,
        flight_advertise,
        max_tasks,
        signing_key,
        auth_token,
        controller_ca_cert,
        controller_tls_domain,
        flight_tls_cert,
        flight_tls_key,
    )?;
    rapidbyte_agent::run(config).await
}

#[allow(clippy::too_many_arguments)]
fn build_config(
    controller: &str,
    flight_listen: &str,
    flight_advertise: &str,
    max_tasks: u32,
    signing_key: Option<&str>,
    auth_token: Option<&str>,
    controller_ca_cert: Option<&Path>,
    controller_tls_domain: Option<&str>,
    flight_tls_cert: Option<&Path>,
    flight_tls_key: Option<&Path>,
) -> Result<rapidbyte_agent::AgentConfig> {
    let mut config = rapidbyte_agent::AgentConfig {
        controller_url: controller.into(),
        flight_listen: flight_listen.into(),
        flight_advertise: flight_advertise.into(),
        max_tasks,
        ..Default::default()
    };
    if let Some(key) = signing_key {
        config.signing_key = key.as_bytes().to_vec();
    }
    config.auth_token = auth_token.map(str::to_owned);
    if controller_ca_cert.is_some() || controller_tls_domain.is_some() {
        config.controller_tls = Some(rapidbyte_agent::ClientTlsConfig {
            ca_cert_pem: match controller_ca_cert {
                Some(ca_cert) => std::fs::read(ca_cert)?,
                None => Vec::new(),
            },
            domain_name: controller_tls_domain.map(str::to_owned),
        });
    }
    match (flight_tls_cert, flight_tls_key) {
        (Some(cert), Some(key)) => {
            config.flight_tls = Some(rapidbyte_agent::ServerTlsConfig {
                cert_pem: std::fs::read(cert)?,
                key_pem: std::fs::read(key)?,
            });
        }
        (None, None) => {}
        _ => anyhow::bail!("agent TLS requires both --flight-tls-cert and --flight-tls-key"),
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn agent_execute_wires_tls() {
        let dir = tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        let cert_path = dir.path().join("flight.crt");
        let key_path = dir.path().join("flight.key");
        std::fs::write(&ca_path, b"ca-pem").unwrap();
        std::fs::write(&cert_path, b"cert-pem").unwrap();
        std::fs::write(&key_path, b"key-pem").unwrap();

        let config = build_config(
            "https://controller.example:9090",
            "[::]:9091",
            "agent.example:9091",
            4,
            Some("signing"),
            Some("secret"),
            Some(ca_path.as_path()),
            Some("controller.example"),
            Some(cert_path.as_path()),
            Some(key_path.as_path()),
        )
        .unwrap();

        assert_eq!(
            config.controller_tls.as_ref().unwrap().ca_cert_pem,
            b"ca-pem"
        );
        assert_eq!(
            config
                .controller_tls
                .as_ref()
                .unwrap()
                .domain_name
                .as_deref(),
            Some("controller.example")
        );
        assert_eq!(config.flight_tls.as_ref().unwrap().cert_pem, b"cert-pem");
        assert_eq!(config.flight_tls.as_ref().unwrap().key_pem, b"key-pem");
        assert_eq!(config.auth_token.as_deref(), Some("secret"));
        assert_eq!(config.signing_key, b"signing".to_vec());
    }
}
