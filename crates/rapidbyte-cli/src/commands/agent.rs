//! Agent worker subcommand.

use anyhow::Result;

pub async fn execute(
    controller: &str,
    flight_listen: &str,
    flight_advertise: &str,
    max_tasks: u32,
    signing_key: Option<&str>,
) -> Result<()> {
    let config = rapidbyte_agent::AgentConfig {
        controller_url: controller.into(),
        flight_listen: flight_listen.into(),
        flight_advertise: flight_advertise.into(),
        max_tasks,
        signing_key: signing_key.map_or_else(Vec::new, |k| k.as_bytes().to_vec()),
        ..Default::default()
    };
    rapidbyte_agent::run(config).await
}
