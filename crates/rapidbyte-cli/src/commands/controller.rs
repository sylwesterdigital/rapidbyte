//! Controller server subcommand.

use anyhow::Result;

pub async fn execute(
    listen: &str,
    signing_key: Option<&str>,
    auth_token: Option<&str>,
) -> Result<()> {
    let config = build_config(listen, signing_key, auth_token)?;
    rapidbyte_controller::run(config).await
}

fn build_config(
    listen: &str,
    signing_key: Option<&str>,
    auth_token: Option<&str>,
) -> Result<rapidbyte_controller::ControllerConfig> {
    let addr = listen
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid listen address: {e}"))?;
    let mut config = rapidbyte_controller::ControllerConfig {
        listen_addr: addr,
        ..Default::default()
    };
    if let Some(key) = signing_key {
        config.signing_key = key.as_bytes().to_vec();
    }
    if let Some(token) = auth_token {
        config.auth_tokens = vec![token.to_string()];
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_execute_uses_auth_token() {
        let config = build_config("[::]:9090", Some("signing"), Some("secret")).unwrap();
        assert_eq!(config.auth_tokens, vec!["secret".to_string()]);
        assert_eq!(config.signing_key, b"signing".to_vec());
    }
}
