CREATE TABLE IF NOT EXISTS controller_runs (
    run_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    recovery_started_at TIMESTAMPTZ,
    current_task_id TEXT,
    current_agent_id TEXT,
    current_attempt INTEGER,
    current_lease_epoch BIGINT,
    current_task_assigned_at TIMESTAMPTZ,
    error_code TEXT,
    error_message TEXT,
    error_retryable BOOLEAN,
    error_safe_to_retry BOOLEAN,
    error_commit_state TEXT,
    attempt INTEGER NOT NULL DEFAULT 1,
    idempotency_key TEXT UNIQUE,
    total_records BIGINT NOT NULL DEFAULT 0,
    total_bytes BIGINT NOT NULL DEFAULT 0,
    elapsed_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
    cursors_advanced BIGINT NOT NULL DEFAULT 0
);

ALTER TABLE controller_runs
    ADD COLUMN IF NOT EXISTS recovery_started_at TIMESTAMPTZ;
ALTER TABLE controller_runs
    ADD COLUMN IF NOT EXISTS error_code TEXT;
ALTER TABLE controller_runs
    ADD COLUMN IF NOT EXISTS error_retryable BOOLEAN;
ALTER TABLE controller_runs
    ADD COLUMN IF NOT EXISTS error_safe_to_retry BOOLEAN;
ALTER TABLE controller_runs
    ADD COLUMN IF NOT EXISTS error_commit_state TEXT;

CREATE TABLE IF NOT EXISTS controller_tasks (
    task_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES controller_runs(run_id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL,
    state TEXT NOT NULL,
    recovery_state TEXT,
    pipeline_yaml BYTEA NOT NULL,
    dry_run BOOLEAN NOT NULL DEFAULT FALSE,
    limit_rows BIGINT,
    assigned_agent_id TEXT,
    lease_epoch BIGINT,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_controller_tasks_pending
    ON controller_tasks (created_at)
    WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS idx_controller_tasks_run_id
    ON controller_tasks (run_id);

CREATE INDEX IF NOT EXISTS idx_controller_tasks_assigned_agent
    ON controller_tasks (assigned_agent_id)
    WHERE assigned_agent_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS controller_agents (
    agent_id TEXT PRIMARY KEY,
    max_tasks INTEGER NOT NULL,
    active_tasks INTEGER NOT NULL DEFAULT 0,
    flight_endpoint TEXT NOT NULL,
    plugin_bundle_hash TEXT NOT NULL,
    available_plugins TEXT[] NOT NULL DEFAULT '{}',
    memory_bytes BIGINT NOT NULL DEFAULT 0,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS controller_previews (
    run_id TEXT PRIMARY KEY REFERENCES controller_runs(run_id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES controller_tasks(task_id) ON DELETE CASCADE,
    flight_endpoint TEXT NOT NULL,
    ticket BYTEA NOT NULL,
    streams_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS controller_task_history (
    history_id BIGSERIAL PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES controller_tasks(task_id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES controller_runs(run_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    state TEXT,
    actor_agent_id TEXT,
    lease_epoch BIGINT,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_controller_task_history_run_id
    ON controller_task_history (run_id, created_at DESC);
