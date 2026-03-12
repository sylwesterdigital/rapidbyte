//! Controller server subcommand.

use anyhow::Result;

pub async fn execute(listen: &str, signing_key: Option<&str>) -> Result<()> {
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
    rapidbyte_controller::run(config).await
}
