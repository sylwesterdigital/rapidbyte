//! Agent worker subcommand.

use std::time::Duration;

use anyhow::Result;

pub async fn execute(
    controller: &str,
    flight_listen: &str,
    flight_advertise: &str,
    max_tasks: u32,
) -> Result<()> {
    let config = rapidbyte_agent::AgentConfig {
        controller_url: controller.into(),
        flight_listen: flight_listen.into(),
        flight_advertise: flight_advertise.into(),
        max_tasks,
        heartbeat_interval: Duration::from_secs(10),
        poll_wait_seconds: 30,
    };
    rapidbyte_agent::run(config).await
}
