//! Controller server subcommand.

use anyhow::Result;

pub async fn execute(listen: &str) -> Result<()> {
    let addr = listen
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid listen address: {e}"))?;
    let config = rapidbyte_controller::ControllerConfig {
        listen_addr: addr,
        ..Default::default()
    };
    rapidbyte_controller::run(config).await
}
