//! Shared transport helpers for distributed controller and Flight clients.

use std::path::PathBuf;

use anyhow::{Context, Result};
use rustls::crypto::ring::default_provider;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsClientConfig {
    pub ca_cert_path: Option<PathBuf>,
    pub domain_name: Option<String>,
}

fn install_rustls_provider() {
    let _ = default_provider().install_default();
}

impl TlsClientConfig {
    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.ca_cert_path.is_some() || self.domain_name.is_some()
    }
}

pub fn build_endpoint(url: &str, tls: Option<&TlsClientConfig>) -> Result<Endpoint> {
    let mut endpoint = Endpoint::from_shared(url.to_string())?;
    if url.starts_with("https://") || tls.is_some() {
        install_rustls_provider();
        let mut tls_config = ClientTlsConfig::new();
        if let Some(tls) = tls {
            if let Some(path) = &tls.ca_cert_path {
                let pem = std::fs::read(path).with_context(|| {
                    format!("Failed to read TLS CA certificate {}", path.display())
                })?;
                tls_config = tls_config.ca_certificate(Certificate::from_pem(pem));
            }
            if let Some(domain_name) = &tls.domain_name {
                tls_config = tls_config.domain_name(domain_name.clone());
            }
        }
        endpoint = endpoint.tls_config(tls_config)?;
    }
    Ok(endpoint)
}

pub async fn connect_channel(url: &str, tls: Option<&TlsClientConfig>) -> Result<Channel> {
    Ok(build_endpoint(url, tls)?.connect().await?)
}

pub fn request_with_bearer<T>(message: T, auth_token: Option<&str>) -> Result<tonic::Request<T>> {
    let mut request = tonic::Request::new(message);
    if let Some(token) = auth_token {
        let metadata = format!("Bearer {token}")
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid bearer token: {e}"))?;
        request.metadata_mut().insert("authorization", metadata);
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    #[test]
    fn request_with_bearer_adds_authorization_metadata() {
        let request = request_with_bearer("payload", Some("secret")).unwrap();
        assert_eq!(
            request.metadata().get("authorization"),
            Some(&MetadataValue::from_static("Bearer secret"))
        );
    }

    #[test]
    fn request_with_bearer_is_noop_without_token() {
        let request = request_with_bearer("payload", None).unwrap();
        assert!(request.metadata().get("authorization").is_none());
    }

    #[test]
    fn request_with_bearer_rejects_invalid_token() {
        let err = request_with_bearer("payload", Some("bad\nvalue")).unwrap_err();
        assert!(err.to_string().contains("Invalid bearer token"));
    }
}
