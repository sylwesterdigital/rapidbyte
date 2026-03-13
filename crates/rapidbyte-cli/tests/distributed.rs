//! Integration test for the distributed pipeline flow.
//!
//! Starts a controller and agent in-process, submits a pipeline,
//! and verifies the coordination (submit -> assign -> execute -> report).

use std::time::Duration;

use rapidbyte_agent::AgentConfig;
use rapidbyte_controller::proto::rapidbyte::v1::agent_service_client::AgentServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{
    poll_task_response, CompleteTaskRequest, GetRunRequest, PollTaskRequest, ProgressUpdate,
    RegisterAgentRequest, ReportProgressRequest, RunState, SubmitPipelineRequest, TaskError,
    TaskOutcome,
};
use rapidbyte_controller::ControllerConfig;
use tokio_postgres::NoTls;

/// A minimal pipeline YAML with a deliberately unreachable Postgres state backend.
/// The agent will pick it up, fail fast during task execution, and report
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
  connection: "host=127.0.0.1 port=1 dbname=rapidbyte_test_nonexistent connect_timeout=1"
"#;

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

struct TestMetadataSchema {
    admin_client: tokio_postgres::Client,
    connection_task: tokio::task::JoinHandle<()>,
    schema: String,
    scoped_url: String,
}

impl TestMetadataSchema {
    async fn create() -> Self {
        let admin_url = std::env::var("RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL")
            .expect("RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL must be set for this test");
        let (admin_client, connection) = tokio_postgres::connect(&admin_url, NoTls)
            .await
            .expect("admin connection should succeed");
        let connection_task = tokio::spawn(async move {
            connection
                .await
                .expect("admin connection task should stay healthy");
        });

        let schema = format!("cli_distributed_test_{}", uuid::Uuid::new_v4().simple());
        admin_client
            .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
            .await
            .expect("schema creation should succeed");

        Self {
            scoped_url: format!("{admin_url} options='-c search_path={schema}'"),
            admin_client,
            connection_task,
            schema,
        }
    }

    async fn cleanup(self) {
        self.admin_client
            .batch_execute(&format!("DROP SCHEMA \"{}\" CASCADE", self.schema))
            .await
            .expect("schema cleanup should succeed");
        self.connection_task.abort();
    }
}

async fn wait_for_controller(ctrl_url: &str) -> tonic::transport::Channel {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    loop {
        match tonic::transport::Channel::from_shared(ctrl_url.to_string())
            .unwrap()
            .connect()
            .await
        {
            Ok(channel) => return channel,
            Err(error) if tokio::time::Instant::now() < deadline => {
                tracing::debug!(?error, ctrl_url, "controller not ready yet");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("Failed to connect to controller: {error}"),
        }
    }
}

fn spawn_controller(
    ctrl_addr: String,
    metadata_database_url: String,
    signing_key: Vec<u8>,
    lease_check_interval: Duration,
    reconciliation_timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let config = ControllerConfig {
            listen_addr: ctrl_addr.parse().unwrap(),
            metadata_database_url: Some(metadata_database_url),
            agent_reap_interval: Duration::from_secs(60),
            agent_reap_timeout: Duration::from_secs(120),
            lease_check_interval,
            reconciliation_timeout,
            allow_unauthenticated: true,
            signing_key,
            ..Default::default()
        };
        rapidbyte_controller::run(config)
            .await
            .expect("controller should stay running");
    })
}

async fn wait_for_run_state(
    client: &mut PipelineServiceClient<tonic::transport::Channel>,
    run_id: &str,
    expected_state: RunState,
    timeout: Duration,
) -> rapidbyte_controller::proto::rapidbyte::v1::GetRunResponse {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        assert!(
            tokio::time::Instant::now() <= deadline,
            "Timed out waiting for run {run_id} to reach state {:?}",
            expected_state
        );

        let response = client
            .get_run(GetRunRequest {
                run_id: run_id.to_string(),
            })
            .await
            .expect("GetRun failed")
            .into_inner();

        if response.state == expected_state as i32 {
            return response;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ignore = "requires RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL"]
async fn distributed_submit_and_complete() {
    let _ = tracing_subscriber::fmt::try_init();
    let metadata = TestMetadataSchema::create().await;

    let ctrl_port = free_port();
    let flight_port = free_port();
    let ctrl_addr = format!("127.0.0.1:{ctrl_port}");
    let ctrl_url = format!("http://{ctrl_addr}");
    let flight_addr = format!("127.0.0.1:{flight_port}");
    let signing_key = b"distributed-test-signing-key".to_vec();

    // Start controller
    let controller_task = spawn_controller(
        ctrl_addr.clone(),
        metadata.scoped_url.clone(),
        signing_key.clone(),
        Duration::from_secs(60),
        ControllerConfig::default().reconciliation_timeout,
    );

    let controller_probe = wait_for_controller(&ctrl_url).await;
    drop(controller_probe);

    // Start agent
    let agent_ctrl_url = ctrl_url.clone();
    let agent_flight_addr = flight_addr.clone();
    let agent_flight_advertise = flight_addr.clone();
    let agent_signing_key = signing_key.clone();
    let agent_task = tokio::spawn(async move {
        let config = AgentConfig {
            controller_url: agent_ctrl_url,
            flight_listen: agent_flight_addr,
            flight_advertise: agent_flight_advertise,
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(5),
            poll_wait_seconds: 5,
            signing_key: agent_signing_key,
            preview_ttl: Duration::from_secs(60),
            auth_token: None,
            allow_insecure_default_signing_key: false,
            controller_tls: None,
            flight_tls: None,
        };
        rapidbyte_agent::run(config)
            .await
            .expect("agent should stay running");
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect as a pipeline client
    let channel = wait_for_controller(&ctrl_url).await;
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

    // Poll GetRun until terminal state. The agent will fail quickly when it
    // cannot open the configured Postgres state backend.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut final_state = 0i32;

    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("Timed out waiting for run to reach terminal state (last state={final_state})");
        }
        if controller_task.is_finished() {
            match controller_task.await {
                Ok(()) => panic!("controller exited unexpectedly"),
                Err(error) => panic!("controller task failed: {error}"),
            }
        }
        if agent_task.is_finished() {
            match agent_task.await {
                Ok(()) => panic!("agent exited unexpectedly"),
                Err(error) => panic!("agent task failed: {error}"),
            }
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

    // The run should have failed, proving the full
    // submit -> assign -> agent poll -> execute -> report flow works
    assert!(
        final_state == RunState::Failed as i32,
        "Expected run to fail (non-existent plugins), got state={final_state}"
    );

    controller_task.abort();
    agent_task.abort();
    metadata.cleanup().await;
}

#[tokio::test]
#[ignore = "requires RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL"]
async fn distributed_restart_reconciles_then_times_out() {
    let _ = tracing_subscriber::fmt::try_init();
    let metadata = TestMetadataSchema::create().await;

    let ctrl_port = free_port();
    let ctrl_addr = format!("127.0.0.1:{ctrl_port}");
    let ctrl_url = format!("http://{ctrl_addr}");
    let signing_key = b"distributed-test-signing-key".to_vec();
    let reconciliation_timeout = Duration::from_secs(2);
    let lease_check_interval = Duration::from_millis(100);

    let controller_task = spawn_controller(
        ctrl_addr.clone(),
        metadata.scoped_url.clone(),
        signing_key,
        lease_check_interval,
        reconciliation_timeout,
    );

    let controller_probe = wait_for_controller(&ctrl_url).await;
    drop(controller_probe);

    let channel = wait_for_controller(&ctrl_url).await;
    let mut pipeline_client = PipelineServiceClient::new(channel.clone());
    let mut agent_client = AgentServiceClient::new(channel);

    let submit_response = pipeline_client
        .submit_pipeline(SubmitPipelineRequest {
            pipeline_yaml_utf8: TEST_PIPELINE_YAML.as_bytes().to_vec(),
            execution: None,
            idempotency_key: String::new(),
        })
        .await
        .expect("SubmitPipeline failed")
        .into_inner();
    let run_id = submit_response.run_id;

    let register_response = agent_client
        .register_agent(RegisterAgentRequest {
            max_tasks: 1,
            flight_advertise_endpoint: "grpc://127.0.0.1:9999".into(),
            plugin_bundle_hash: "fake-agent".into(),
            available_plugins: Vec::new(),
            memory_bytes: 0,
        })
        .await
        .expect("RegisterAgent failed")
        .into_inner();

    let poll_response = agent_client
        .poll_task(PollTaskRequest {
            agent_id: register_response.agent_id,
            wait_seconds: 1,
        })
        .await
        .expect("PollTask failed")
        .into_inner();

    let assignment = match poll_response.result {
        Some(poll_task_response::Result::Task(task)) => task,
        other => panic!("expected task assignment, got {other:?}"),
    };
    assert_eq!(assignment.run_id, run_id);

    let assigned_run = wait_for_run_state(
        &mut pipeline_client,
        &run_id,
        RunState::Assigned,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        assigned_run.current_task.unwrap().task_id,
        assignment.task_id
    );

    controller_task.abort();
    let _ = controller_task.await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let restarted_controller_task = spawn_controller(
        ctrl_addr,
        metadata.scoped_url.clone(),
        b"distributed-test-signing-key".to_vec(),
        lease_check_interval,
        reconciliation_timeout,
    );

    let restarted_channel = wait_for_controller(&ctrl_url).await;
    let mut restarted_pipeline_client = PipelineServiceClient::new(restarted_channel);

    let reconciling_run = wait_for_run_state(
        &mut restarted_pipeline_client,
        &run_id,
        RunState::Reconciling,
        Duration::from_secs(5),
    )
    .await;
    assert!(reconciling_run.last_error.is_none());

    let recovery_failed_run = wait_for_run_state(
        &mut restarted_pipeline_client,
        &run_id,
        RunState::RecoveryFailed,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(
        recovery_failed_run
            .last_error
            .expect("recovery_failed run should expose last_error")
            .code,
        "RECOVERY_TIMEOUT"
    );

    restarted_controller_task.abort();
    metadata.cleanup().await;
}

#[tokio::test]
#[ignore = "requires RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL"]
async fn distributed_restart_reconciles_and_resumes_execution() {
    let _ = tracing_subscriber::fmt::try_init();
    let metadata = TestMetadataSchema::create().await;

    let ctrl_port = free_port();
    let ctrl_addr = format!("127.0.0.1:{ctrl_port}");
    let ctrl_url = format!("http://{ctrl_addr}");
    let signing_key = b"distributed-test-signing-key".to_vec();

    let controller_task = spawn_controller(
        ctrl_addr.clone(),
        metadata.scoped_url.clone(),
        signing_key,
        Duration::from_secs(60),
        Duration::from_secs(30),
    );

    let controller_probe = wait_for_controller(&ctrl_url).await;
    drop(controller_probe);

    let channel = wait_for_controller(&ctrl_url).await;
    let mut pipeline_client = PipelineServiceClient::new(channel.clone());
    let mut agent_client = AgentServiceClient::new(channel);

    let submit_response = pipeline_client
        .submit_pipeline(SubmitPipelineRequest {
            pipeline_yaml_utf8: TEST_PIPELINE_YAML.as_bytes().to_vec(),
            execution: None,
            idempotency_key: String::new(),
        })
        .await
        .expect("SubmitPipeline failed")
        .into_inner();
    let run_id = submit_response.run_id;

    let register_response = agent_client
        .register_agent(RegisterAgentRequest {
            max_tasks: 1,
            flight_advertise_endpoint: "grpc://127.0.0.1:9999".into(),
            plugin_bundle_hash: "fake-agent".into(),
            available_plugins: Vec::new(),
            memory_bytes: 0,
        })
        .await
        .expect("RegisterAgent failed")
        .into_inner();
    let agent_id = register_response.agent_id;

    let poll_response = agent_client
        .poll_task(PollTaskRequest {
            agent_id: agent_id.clone(),
            wait_seconds: 1,
        })
        .await
        .expect("PollTask failed")
        .into_inner();

    let assignment = match poll_response.result {
        Some(poll_task_response::Result::Task(task)) => task,
        other => panic!("expected task assignment, got {other:?}"),
    };
    assert_eq!(assignment.run_id, run_id);

    let assigned_run = wait_for_run_state(
        &mut pipeline_client,
        &run_id,
        RunState::Assigned,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        assigned_run.current_task.unwrap().task_id,
        assignment.task_id
    );

    controller_task.abort();
    let _ = controller_task.await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let restarted_controller_task = spawn_controller(
        ctrl_addr,
        metadata.scoped_url.clone(),
        b"distributed-test-signing-key".to_vec(),
        Duration::from_secs(60),
        Duration::from_secs(30),
    );

    let restarted_channel = wait_for_controller(&ctrl_url).await;
    let mut restarted_pipeline_client = PipelineServiceClient::new(restarted_channel.clone());
    let mut restarted_agent_client = AgentServiceClient::new(restarted_channel);

    let reconciling_run = wait_for_run_state(
        &mut restarted_pipeline_client,
        &run_id,
        RunState::Reconciling,
        Duration::from_secs(5),
    )
    .await;
    assert!(reconciling_run.last_error.is_none());

    restarted_agent_client
        .report_progress(ReportProgressRequest {
            agent_id: agent_id.clone(),
            task_id: assignment.task_id.clone(),
            lease_epoch: assignment.lease_epoch,
            progress: Some(ProgressUpdate {
                stream: "users".into(),
                phase: "running".into(),
                records: 5,
                bytes: 10,
            }),
        })
        .await
        .expect("ReportProgress should be accepted after restart");

    let running_run = wait_for_run_state(
        &mut restarted_pipeline_client,
        &run_id,
        RunState::Running,
        Duration::from_secs(5),
    )
    .await;
    assert!(running_run.last_error.is_none());

    let completion = restarted_agent_client
        .complete_task(CompleteTaskRequest {
            agent_id,
            task_id: assignment.task_id,
            lease_epoch: assignment.lease_epoch,
            outcome: TaskOutcome::Failed.into(),
            error: Some(TaskError {
                code: "TEST_EXECUTION_FAILED".into(),
                message: "resume path injected failure".into(),
                retryable: false,
                safe_to_retry: false,
                commit_state: "before_commit".into(),
            }),
            metrics: None,
            preview: None,
            backend_run_id: 0,
        })
        .await
        .expect("CompleteTask should be accepted after restart")
        .into_inner();
    assert!(completion.acknowledged);

    let failed_run = wait_for_run_state(
        &mut restarted_pipeline_client,
        &run_id,
        RunState::Failed,
        Duration::from_secs(5),
    )
    .await;
    let error = failed_run
        .last_error
        .expect("failed run should expose last_error");
    assert_eq!(error.code, "TEST_EXECUTION_FAILED");
    assert_ne!(failed_run.state, RunState::RecoveryFailed as i32);

    restarted_controller_task.abort();
    metadata.cleanup().await;
}
