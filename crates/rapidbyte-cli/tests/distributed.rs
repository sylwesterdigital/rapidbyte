//! Integration test for the distributed pipeline flow.
//!
//! Starts a controller and agent in-process, submits a pipeline,
//! and verifies the coordination (submit -> assign -> execute -> report).

use std::time::Duration;

use rapidbyte_agent::AgentConfig;
use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{GetRunRequest, RunState, SubmitPipelineRequest};
use rapidbyte_controller::ControllerConfig;

/// A minimal pipeline YAML that references a non-existent plugin.
/// The agent will pick it up, fail at plugin resolution, and report
/// the failure back — exercising the full distributed coordination path.
const TEST_PIPELINE_YAML: &str = r#"
version: "1.0"
pipeline: integration-test
source:
  use: test-nonexistent-source
  config: {}
  streams: []
destination:
  use: test-nonexistent-dest
  config: {}
state:
  backend: postgres
  connection: "host=localhost dbname=rapidbyte_test_nonexistent"
"#;

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn distributed_submit_and_complete() {
    let _ = tracing_subscriber::fmt::try_init();

    let ctrl_port = free_port();
    let flight_port = free_port();
    let ctrl_addr = format!("127.0.0.1:{ctrl_port}");
    let ctrl_url = format!("http://{ctrl_addr}");
    let flight_addr = format!("127.0.0.1:{flight_port}");

    // Start controller
    let ctrl_addr_parsed = ctrl_addr.parse().unwrap();
    tokio::spawn(async move {
        let config = ControllerConfig {
            listen_addr: ctrl_addr_parsed,
            agent_reap_interval: Duration::from_secs(60),
            agent_reap_timeout: Duration::from_secs(120),
            lease_check_interval: Duration::from_secs(60),
            allow_unauthenticated: true,
            ..Default::default()
        };
        let _ = rapidbyte_controller::run(config).await;
    });

    // Give the controller a moment to bind
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start agent
    let agent_ctrl_url = ctrl_url.clone();
    let agent_flight_addr = flight_addr.clone();
    let agent_flight_advertise = flight_addr.clone();
    tokio::spawn(async move {
        let config = AgentConfig {
            controller_url: agent_ctrl_url,
            flight_listen: agent_flight_addr,
            flight_advertise: agent_flight_advertise,
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(5),
            poll_wait_seconds: 5,
            signing_key: Vec::new(),
            preview_ttl: Duration::from_secs(60),
            auth_token: None,
            controller_tls: None,
            flight_tls: None,
        };
        let _ = rapidbyte_agent::run(config).await;
    });

    // Give the agent a moment to register
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect as a pipeline client
    let channel = tonic::transport::Channel::from_shared(ctrl_url)
        .unwrap()
        .connect()
        .await
        .expect("Failed to connect to controller");

    let mut client = PipelineServiceClient::new(channel);

    // Submit the pipeline
    let resp = client
        .submit_pipeline(SubmitPipelineRequest {
            pipeline_yaml_utf8: TEST_PIPELINE_YAML.as_bytes().to_vec(),
            execution: None,
            idempotency_key: String::new(),
        })
        .await
        .expect("SubmitPipeline failed");

    let run_id = resp.into_inner().run_id;
    assert!(!run_id.is_empty(), "run_id should not be empty");

    // Poll GetRun until terminal state (the agent will fail because the
    // plugins don't exist, which still exercises the full distributed path)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut final_state = 0i32;

    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("Timed out waiting for run to reach terminal state (last state={final_state})");
        }

        let run_resp = client
            .get_run(GetRunRequest {
                run_id: run_id.clone(),
            })
            .await
            .expect("GetRun failed")
            .into_inner();

        final_state = run_resp.state;

        if final_state == RunState::Completed as i32
            || final_state == RunState::Failed as i32
            || final_state == RunState::Cancelled as i32
        {
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The run should have failed (non-existent plugins), proving the full
    // submit -> assign -> agent poll -> execute -> report flow works
    assert!(
        final_state == RunState::Failed as i32,
        "Expected run to fail (non-existent plugins), got state={final_state}"
    );
}
