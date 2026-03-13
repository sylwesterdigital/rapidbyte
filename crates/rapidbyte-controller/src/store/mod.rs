//! Durable controller metadata store bootstrap and persistence helpers.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, Utc};
use tokio_postgres::{Client, NoTls, Row};

use crate::lease::Lease;
use crate::preview::{PreviewEntry, PreviewStreamEntry};
use crate::registry::AgentRecord;
use crate::run_state::{CurrentTask, RunRecord, RunState};
use crate::scheduler::{TaskRecord, TaskState};

pub const CONTROLLER_METADATA_MIGRATIONS: &str =
    include_str!("migrations/0001_controller_metadata.sql");

#[derive(Debug, Default)]
pub struct MetadataSnapshot {
    pub runs: Vec<RunRecord>,
    pub tasks: Vec<TaskRecord>,
    pub agents: Vec<AgentRecord>,
    pub previews: Vec<PreviewEntry>,
    pub max_lease_epoch: u64,
}

#[derive(Clone)]
pub struct MetadataStore {
    client: Arc<Client>,
}

impl MetadataStore {
    /// Connect to the metadata store and apply the checked-in migration set.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be reached or the schema
    /// migration fails.
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(?error, "controller metadata store connection failed");
            }
        });
        client.batch_execute(CONTROLLER_METADATA_MIGRATIONS).await?;
        Ok(Self {
            client: Arc::new(client),
        })
    }

    /// Load the durable controller state snapshot used to seed in-memory state
    /// after restart.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot query fails or contains invalid data.
    pub async fn load_snapshot(&self) -> anyhow::Result<MetadataSnapshot> {
        let run_rows = self
            .client
            .query("SELECT * FROM controller_runs ORDER BY created_at ASC", &[])
            .await?;
        let task_rows = self
            .client
            .query(
                "SELECT * FROM controller_tasks ORDER BY created_at ASC",
                &[],
            )
            .await?;
        let agent_rows = self
            .client
            .query(
                "SELECT * FROM controller_agents ORDER BY created_at ASC",
                &[],
            )
            .await?;
        let preview_rows = self
            .client
            .query(
                "SELECT * FROM controller_previews ORDER BY created_at ASC",
                &[],
            )
            .await?;

        let runs = run_rows
            .iter()
            .map(run_record_from_row)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let tasks = task_rows
            .iter()
            .map(task_record_from_row)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let agents = agent_rows
            .iter()
            .map(agent_record_from_row)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let previews = preview_rows
            .iter()
            .filter_map(|row| match preview_entry_from_row(row) {
                Ok(Some(entry)) => Some(Ok(entry)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let max_lease_epoch = tasks
            .iter()
            .filter_map(|task| task.lease.as_ref().map(|lease| lease.epoch))
            .max()
            .unwrap_or(0);

        Ok(MetadataSnapshot {
            runs,
            tasks,
            agents,
            previews,
            max_lease_epoch,
        })
    }

    /// Upsert a durable run record.
    ///
    /// # Errors
    ///
    /// Returns an error if the database write fails.
    pub async fn upsert_run(&self, run: &RunRecord) -> anyhow::Result<()> {
        let current_task = run.current_task.as_ref();
        let current_attempt = current_task
            .map(|task| i32::try_from(task.attempt))
            .transpose()?;
        let current_lease_epoch = current_task
            .map(|task| i64::try_from(task.lease_epoch))
            .transpose()?;
        let attempt = i32::try_from(run.attempt)?;
        let total_records = i64::try_from(run.total_records)?;
        let total_bytes = i64::try_from(run.total_bytes)?;
        let cursors_advanced = i64::try_from(run.cursors_advanced)?;

        self.client
            .execute(
                "INSERT INTO controller_runs (
                    run_id, pipeline_name, state, created_at, updated_at, started_at, completed_at,
                    recovery_started_at,
                    current_task_id, current_agent_id, current_attempt, current_lease_epoch,
                    current_task_assigned_at, error_code, error_message, error_retryable,
                    error_safe_to_retry, error_commit_state, attempt, idempotency_key,
                    total_records, total_bytes, elapsed_seconds, cursors_advanced
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8,
                    $9, $10, $11, $12,
                    $13, $14, $15, $16,
                    $17, $18, $19, $20,
                    $21, $22, $23, $24
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    pipeline_name = EXCLUDED.pipeline_name,
                    state = EXCLUDED.state,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    recovery_started_at = EXCLUDED.recovery_started_at,
                    current_task_id = EXCLUDED.current_task_id,
                    current_agent_id = EXCLUDED.current_agent_id,
                    current_attempt = EXCLUDED.current_attempt,
                    current_lease_epoch = EXCLUDED.current_lease_epoch,
                    current_task_assigned_at = EXCLUDED.current_task_assigned_at,
                    error_code = EXCLUDED.error_code,
                    error_message = EXCLUDED.error_message,
                    error_retryable = EXCLUDED.error_retryable,
                    error_safe_to_retry = EXCLUDED.error_safe_to_retry,
                    error_commit_state = EXCLUDED.error_commit_state,
                    attempt = EXCLUDED.attempt,
                    idempotency_key = EXCLUDED.idempotency_key,
                    total_records = EXCLUDED.total_records,
                    total_bytes = EXCLUDED.total_bytes,
                    elapsed_seconds = EXCLUDED.elapsed_seconds,
                    cursors_advanced = EXCLUDED.cursors_advanced",
                &[
                    &run.run_id,
                    &run.pipeline_name,
                    &run_state_to_db(run.state),
                    &to_datetime(run.created_at),
                    &to_datetime(run.updated_at),
                    &run.started_at.map(to_datetime),
                    &run.completed_at.map(to_datetime),
                    &run.recovery_started_at.map(to_datetime),
                    &current_task.map(|task| task.task_id.clone()),
                    &current_task.map(|task| task.agent_id.clone()),
                    &current_attempt,
                    &current_lease_epoch,
                    &current_task.map(|task| to_datetime(task.assigned_at)),
                    &run.error_code,
                    &run.error_message,
                    &run.error_retryable,
                    &run.error_safe_to_retry,
                    &run.error_commit_state,
                    &attempt,
                    &run.idempotency_key,
                    &total_records,
                    &total_bytes,
                    &run.elapsed_seconds,
                    &cursors_advanced,
                ],
            )
            .await?;
        Ok(())
    }

    /// Upsert a durable task record.
    ///
    /// # Errors
    ///
    /// Returns an error if the database write fails.
    pub async fn upsert_task(&self, task: &TaskRecord) -> anyhow::Result<()> {
        let attempt = i32::try_from(task.attempt)?;
        let limit_rows = task.limit.map(i64::try_from).transpose()?;
        let lease_epoch = task
            .lease
            .as_ref()
            .map(|lease| i64::try_from(lease.epoch))
            .transpose()?;
        let lease_expires_at = task.lease.as_ref().map(lease_expiry_to_datetime);

        self.client
            .execute(
                "INSERT INTO controller_tasks (
                    task_id, run_id, attempt, state, pipeline_yaml, dry_run, limit_rows,
                    assigned_agent_id, lease_epoch, lease_expires_at, created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, NOW(), NOW()
                )
                ON CONFLICT (task_id) DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    attempt = EXCLUDED.attempt,
                    state = EXCLUDED.state,
                    pipeline_yaml = EXCLUDED.pipeline_yaml,
                    dry_run = EXCLUDED.dry_run,
                    limit_rows = EXCLUDED.limit_rows,
                    assigned_agent_id = EXCLUDED.assigned_agent_id,
                    lease_epoch = EXCLUDED.lease_epoch,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    updated_at = NOW()",
                &[
                    &task.task_id,
                    &task.run_id,
                    &attempt,
                    &task_state_to_db(task.state),
                    &task.pipeline_yaml,
                    &task.dry_run,
                    &limit_rows,
                    &task.assigned_agent_id,
                    &lease_epoch,
                    &lease_expires_at,
                ],
            )
            .await?;
        Ok(())
    }

    /// Upsert durable preview metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the database write fails.
    pub async fn upsert_preview(
        &self,
        run_id: &str,
        task_id: &str,
        flight_endpoint: &str,
        ticket: &[u8],
        streams: &[PreviewStreamEntry],
        created_at: SystemTime,
        ttl: Duration,
    ) -> anyhow::Result<()> {
        let expires_at = to_datetime(created_at + ttl);
        let streams_json = serde_json::to_value(
            streams
                .iter()
                .map(|stream| {
                    serde_json::json!({
                        "stream": stream.stream,
                        "rows": stream.rows,
                        "ticket": stream.ticket.to_vec(),
                    })
                })
                .collect::<Vec<_>>(),
        )?;

        self.client
            .execute(
                "INSERT INTO controller_previews (
                    run_id, task_id, flight_endpoint, ticket, streams_json, created_at, expires_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (run_id) DO UPDATE SET
                    task_id = EXCLUDED.task_id,
                    flight_endpoint = EXCLUDED.flight_endpoint,
                    ticket = EXCLUDED.ticket,
                    streams_json = EXCLUDED.streams_json,
                    created_at = EXCLUDED.created_at,
                    expires_at = EXCLUDED.expires_at",
                &[
                    &run_id,
                    &task_id,
                    &flight_endpoint,
                    &ticket,
                    &streams_json,
                    &to_datetime(created_at),
                    &expires_at,
                ],
            )
            .await?;
        Ok(())
    }

    /// Upsert a durable agent record.
    ///
    /// # Errors
    ///
    /// Returns an error if the database write fails.
    pub async fn upsert_agent(&self, agent: &AgentRecord) -> anyhow::Result<()> {
        let max_tasks = i32::try_from(agent.max_tasks)?;
        let active_tasks = i32::try_from(agent.active_tasks)?;
        let memory_bytes = i64::try_from(agent.memory_bytes)?;
        let last_heartbeat_at = to_datetime(system_time_from_instant(agent.last_heartbeat));

        self.client
            .execute(
                "INSERT INTO controller_agents (
                    agent_id, max_tasks, active_tasks, flight_endpoint, plugin_bundle_hash,
                    available_plugins, memory_bytes, last_heartbeat_at, created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, NOW(), NOW()
                )
                ON CONFLICT (agent_id) DO UPDATE SET
                    max_tasks = EXCLUDED.max_tasks,
                    active_tasks = EXCLUDED.active_tasks,
                    flight_endpoint = EXCLUDED.flight_endpoint,
                    plugin_bundle_hash = EXCLUDED.plugin_bundle_hash,
                    available_plugins = EXCLUDED.available_plugins,
                    memory_bytes = EXCLUDED.memory_bytes,
                    last_heartbeat_at = EXCLUDED.last_heartbeat_at,
                    updated_at = NOW()",
                &[
                    &agent.agent_id,
                    &max_tasks,
                    &active_tasks,
                    &agent.flight_endpoint,
                    &agent.plugin_bundle_hash,
                    &agent.available_plugins,
                    &memory_bytes,
                    &last_heartbeat_at,
                ],
            )
            .await?;
        Ok(())
    }

    /// Delete a durable agent record.
    ///
    /// # Errors
    ///
    /// Returns an error if the database write fails.
    pub async fn delete_agent(&self, agent_id: &str) -> anyhow::Result<()> {
        self.client
            .execute(
                "DELETE FROM controller_agents WHERE agent_id = $1",
                &[&agent_id],
            )
            .await?;
        Ok(())
    }
}

pub async fn initialize_metadata_store(database_url: &str) -> anyhow::Result<MetadataStore> {
    MetadataStore::connect(database_url).await
}

fn run_state_to_db(state: RunState) -> &'static str {
    match state {
        RunState::Pending => "pending",
        RunState::Assigned => "assigned",
        RunState::Reconciling => "reconciling",
        RunState::Running => "running",
        RunState::PreviewReady => "preview_ready",
        RunState::Completed => "completed",
        RunState::Failed => "failed",
        RunState::RecoveryFailed => "recovery_failed",
        RunState::Cancelling => "cancelling",
        RunState::Cancelled => "cancelled",
        RunState::TimedOut => "timed_out",
    }
}

fn run_state_from_db(state: &str) -> anyhow::Result<RunState> {
    match state {
        "pending" => Ok(RunState::Pending),
        "assigned" => Ok(RunState::Assigned),
        "reconciling" => Ok(RunState::Reconciling),
        "running" => Ok(RunState::Running),
        "preview_ready" => Ok(RunState::PreviewReady),
        "completed" => Ok(RunState::Completed),
        "failed" => Ok(RunState::Failed),
        "recovery_failed" => Ok(RunState::RecoveryFailed),
        "cancelling" => Ok(RunState::Cancelling),
        "cancelled" => Ok(RunState::Cancelled),
        "timed_out" => Ok(RunState::TimedOut),
        other => anyhow::bail!("unknown run state {other}"),
    }
}

fn task_state_to_db(state: TaskState) -> &'static str {
    match state {
        TaskState::Pending => "pending",
        TaskState::Assigned => "assigned",
        TaskState::Running => "running",
        TaskState::Completed => "completed",
        TaskState::Failed => "failed",
        TaskState::Cancelled => "cancelled",
        TaskState::TimedOut => "timed_out",
    }
}

fn task_state_from_db(state: &str) -> anyhow::Result<TaskState> {
    match state {
        "pending" => Ok(TaskState::Pending),
        "assigned" => Ok(TaskState::Assigned),
        "running" => Ok(TaskState::Running),
        "completed" => Ok(TaskState::Completed),
        "failed" => Ok(TaskState::Failed),
        "cancelled" => Ok(TaskState::Cancelled),
        "timed_out" => Ok(TaskState::TimedOut),
        other => anyhow::bail!("unknown task state {other}"),
    }
}

fn run_record_from_row(row: &Row) -> anyhow::Result<RunRecord> {
    Ok(RunRecord {
        run_id: row.get("run_id"),
        pipeline_name: row.get("pipeline_name"),
        state: run_state_from_db(row.get::<_, String>("state").as_str())?,
        created_at: datetime_to_system_time(row.get("created_at")),
        updated_at: datetime_to_system_time(row.get("updated_at")),
        started_at: row
            .get::<_, Option<DateTime<Utc>>>("started_at")
            .map(datetime_to_system_time),
        completed_at: row
            .get::<_, Option<DateTime<Utc>>>("completed_at")
            .map(datetime_to_system_time),
        recovery_started_at: row
            .get::<_, Option<DateTime<Utc>>>("recovery_started_at")
            .map(datetime_to_system_time),
        current_task: current_task_from_row(row)?,
        error_code: row.get("error_code"),
        error_message: row.get("error_message"),
        error_retryable: row.get("error_retryable"),
        error_safe_to_retry: row.get("error_safe_to_retry"),
        error_commit_state: row.get("error_commit_state"),
        attempt: u32::try_from(row.get::<_, i32>("attempt"))?,
        idempotency_key: row.get("idempotency_key"),
        total_records: u64::try_from(row.get::<_, i64>("total_records"))?,
        total_bytes: u64::try_from(row.get::<_, i64>("total_bytes"))?,
        elapsed_seconds: row.get("elapsed_seconds"),
        cursors_advanced: u64::try_from(row.get::<_, i64>("cursors_advanced"))?,
    })
}

fn current_task_from_row(row: &Row) -> anyhow::Result<Option<CurrentTask>> {
    let Some(task_id) = row.get::<_, Option<String>>("current_task_id") else {
        return Ok(None);
    };
    let Some(agent_id) = row.get::<_, Option<String>>("current_agent_id") else {
        return Ok(None);
    };
    let Some(attempt) = row.get::<_, Option<i32>>("current_attempt") else {
        return Ok(None);
    };
    let Some(lease_epoch) = row.get::<_, Option<i64>>("current_lease_epoch") else {
        return Ok(None);
    };
    let Some(assigned_at) = row.get::<_, Option<DateTime<Utc>>>("current_task_assigned_at") else {
        return Ok(None);
    };

    Ok(Some(CurrentTask {
        task_id,
        agent_id,
        attempt: u32::try_from(attempt)?,
        lease_epoch: u64::try_from(lease_epoch)?,
        assigned_at: datetime_to_system_time(assigned_at),
    }))
}

fn task_record_from_row(row: &Row) -> anyhow::Result<TaskRecord> {
    let lease_epoch = row.get::<_, Option<i64>>("lease_epoch");
    let lease_expires_at = row.get::<_, Option<DateTime<Utc>>>("lease_expires_at");
    let lease = match (lease_epoch, lease_expires_at) {
        (Some(epoch), Some(expires_at)) => Some(Lease {
            epoch: u64::try_from(epoch)?,
            expires_at: lease_expiry_from_datetime(expires_at),
        }),
        _ => None,
    };

    Ok(TaskRecord {
        task_id: row.get("task_id"),
        run_id: row.get("run_id"),
        attempt: u32::try_from(row.get::<_, i32>("attempt"))?,
        lease,
        state: task_state_from_db(row.get::<_, String>("state").as_str())?,
        pipeline_yaml: row.get("pipeline_yaml"),
        dry_run: row.get("dry_run"),
        limit: row
            .get::<_, Option<i64>>("limit_rows")
            .map(u64::try_from)
            .transpose()?,
        assigned_agent_id: row.get("assigned_agent_id"),
    })
}

fn agent_record_from_row(row: &Row) -> anyhow::Result<AgentRecord> {
    Ok(AgentRecord {
        agent_id: row.get("agent_id"),
        max_tasks: u32::try_from(row.get::<_, i32>("max_tasks"))?,
        active_tasks: u32::try_from(row.get::<_, i32>("active_tasks"))?,
        flight_endpoint: row.get("flight_endpoint"),
        plugin_bundle_hash: row.get("plugin_bundle_hash"),
        last_heartbeat: instant_from_system_time(datetime_to_system_time(
            row.get("last_heartbeat_at"),
        )),
        available_plugins: row.get("available_plugins"),
        memory_bytes: u64::try_from(row.get::<_, i64>("memory_bytes"))?,
    })
}

fn preview_entry_from_row(row: &Row) -> anyhow::Result<Option<PreviewEntry>> {
    #[derive(serde::Deserialize)]
    struct StoredPreviewStream {
        stream: String,
        rows: u64,
        ticket: Vec<u8>,
    }

    let created_at = datetime_to_system_time(row.get("created_at"));
    let expires_at = datetime_to_system_time(row.get("expires_at"));
    let Some(ttl) = expires_at.duration_since(created_at).ok() else {
        return Ok(None);
    };
    if expires_at <= SystemTime::now() {
        return Ok(None);
    }

    let streams_json: serde_json::Value = row.get("streams_json");
    let streams = serde_json::from_value::<Vec<StoredPreviewStream>>(streams_json)?
        .into_iter()
        .map(|stream| PreviewStreamEntry {
            stream: stream.stream,
            rows: stream.rows,
            ticket: bytes::Bytes::from(stream.ticket),
        })
        .collect();

    Ok(Some(PreviewEntry {
        run_id: row.get("run_id"),
        task_id: row.get("task_id"),
        flight_endpoint: row.get("flight_endpoint"),
        ticket: bytes::Bytes::from(row.get::<_, Vec<u8>>("ticket")),
        streams,
        created_at: instant_from_system_time(created_at),
        ttl,
    }))
}

fn to_datetime(time: SystemTime) -> DateTime<Utc> {
    time.into()
}

fn datetime_to_system_time(time: DateTime<Utc>) -> SystemTime {
    time.into()
}

fn lease_expiry_to_datetime(lease: &Lease) -> DateTime<Utc> {
    let ttl = lease.expires_at.saturating_duration_since(Instant::now());
    to_datetime(SystemTime::now() + ttl)
}

fn lease_expiry_from_datetime(expires_at: DateTime<Utc>) -> Instant {
    let system_time = datetime_to_system_time(expires_at);
    match system_time.duration_since(SystemTime::now()) {
        Ok(remaining) => Instant::now() + remaining,
        Err(_) => Instant::now(),
    }
}

fn instant_from_system_time(time: SystemTime) -> Instant {
    match SystemTime::now().duration_since(time) {
        Ok(elapsed) => Instant::now()
            .checked_sub(elapsed)
            .unwrap_or_else(Instant::now),
        Err(_) => Instant::now(),
    }
}

fn system_time_from_instant(time: Instant) -> SystemTime {
    SystemTime::now()
        .checked_sub(time.elapsed())
        .unwrap_or_else(SystemTime::now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[tokio::test]
    #[ignore = "requires RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL"]
    async fn metadata_store_roundtrips_runs_and_tasks() {
        let admin_url = env::var("RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL")
            .expect("test database URL env var must be set");
        let (admin_client, admin_connection) = tokio_postgres::connect(&admin_url, NoTls)
            .await
            .expect("admin connection should succeed");
        tokio::spawn(async move {
            admin_connection
                .await
                .expect("admin connection task should stay healthy");
        });

        let schema = format!("controller_store_test_{}", uuid::Uuid::new_v4().simple());
        admin_client
            .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
            .await
            .expect("schema creation should succeed");

        let scoped_url = format!("{admin_url} options='-c search_path={schema}'");
        let store = MetadataStore::connect(&scoped_url)
            .await
            .expect("metadata store connect should succeed");

        let now = SystemTime::now();
        let task = TaskRecord {
            task_id: "task-1".into(),
            run_id: "run-1".into(),
            attempt: 1,
            lease: Some(Lease::new(7, Duration::from_secs(60))),
            state: TaskState::Assigned,
            pipeline_yaml: b"pipeline: test".to_vec(),
            dry_run: false,
            limit: Some(25),
            assigned_agent_id: Some("agent-1".into()),
        };
        let run = RunRecord {
            run_id: "run-1".into(),
            pipeline_name: "pipe".into(),
            state: RunState::Assigned,
            created_at: now,
            updated_at: now,
            started_at: None,
            completed_at: None,
            current_task: Some(CurrentTask {
                task_id: "task-1".into(),
                agent_id: "agent-1".into(),
                attempt: 1,
                lease_epoch: 7,
                assigned_at: now,
            }),
            recovery_started_at: Some(now),
            error_code: Some("TEST_ERROR".into()),
            error_message: None,
            error_retryable: Some(false),
            error_safe_to_retry: Some(false),
            error_commit_state: Some("before_commit".into()),
            attempt: 1,
            idempotency_key: Some("idem-key".into()),
            total_records: 0,
            total_bytes: 0,
            elapsed_seconds: 0.0,
            cursors_advanced: 0,
        };

        store
            .upsert_run(&run)
            .await
            .expect("run upsert should succeed");
        store
            .upsert_task(&task)
            .await
            .expect("task upsert should succeed");
        store
            .upsert_agent(&AgentRecord {
                agent_id: "agent-1".into(),
                max_tasks: 4,
                active_tasks: 1,
                flight_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: "hash".into(),
                last_heartbeat: Instant::now(),
                available_plugins: vec!["source-postgres".into()],
                memory_bytes: 1024,
            })
            .await
            .expect("agent upsert should succeed");
        store
            .upsert_preview(
                "run-1",
                "task-1",
                "localhost:9091",
                b"ticket",
                &[PreviewStreamEntry {
                    stream: "users".into(),
                    rows: 3,
                    ticket: bytes::Bytes::from_static(b"users-ticket"),
                }],
                now,
                Duration::from_secs(60),
            )
            .await
            .expect("preview upsert should succeed");

        let snapshot = store
            .load_snapshot()
            .await
            .expect("snapshot load should succeed");

        assert_eq!(snapshot.runs.len(), 1);
        assert_eq!(snapshot.tasks.len(), 1);
        assert_eq!(snapshot.agents.len(), 1);
        assert_eq!(snapshot.previews.len(), 1);
        assert_eq!(snapshot.max_lease_epoch, 7);
        assert_eq!(
            snapshot.runs[0].idempotency_key.as_deref(),
            Some("idem-key")
        );
        assert_eq!(
            snapshot.tasks[0].assigned_agent_id.as_deref(),
            Some("agent-1")
        );
        assert_eq!(snapshot.tasks[0].limit, Some(25));
        assert!(snapshot.runs[0].recovery_started_at.is_some());
        assert_eq!(snapshot.agents[0].agent_id, "agent-1");
        assert_eq!(snapshot.previews[0].streams[0].stream, "users");

        admin_client
            .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
            .await
            .expect("schema cleanup should succeed");
    }
}
