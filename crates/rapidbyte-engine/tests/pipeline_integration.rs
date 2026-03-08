//! Integration tests for pipeline parsing, validation, and state backend.
//!
//! These tests verify the full pipeline processing path from YAML parsing
//! through validation, using real fixture files.

use rapidbyte_engine::config::parser;
use rapidbyte_engine::config::types::{PipelineWriteMode, StateBackendKind};
use rapidbyte_engine::config::validator;
use rapidbyte_state::{SqliteStateBackend, StateBackend};
use rapidbyte_types::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};

/// Test parsing and validating a well-formed pipeline YAML fixture.
#[test]
fn test_parse_and_validate_fixture_pipeline() {
    // Set required env vars for the fixture
    std::env::set_var("TEST_SOURCE_PG_HOST", "localhost");
    std::env::set_var("TEST_SOURCE_PG_PORT", "5432");
    std::env::set_var("TEST_DEST_PG_HOST", "localhost");
    std::env::set_var("TEST_DEST_PG_PORT", "5433");

    let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("tests/fixtures/pipelines/simple_pg_to_pg.yaml");

    let config = parser::parse_pipeline(&fixture_path).expect("Failed to parse fixture pipeline");

    assert_eq!(config.pipeline, "test_pg_to_pg");
    assert_eq!(config.source.use_ref, "postgres");
    assert_eq!(config.source.streams.len(), 2);
    assert_eq!(config.source.streams[0].name, "users");
    assert_eq!(config.source.streams[1].name, "orders");
    assert_eq!(config.destination.use_ref, "postgres");
    assert_eq!(config.destination.write_mode, PipelineWriteMode::Append);
    assert_eq!(config.destination.config["schema"], "raw");
    assert_eq!(config.state.backend, StateBackendKind::Sqlite);

    // Validate should pass
    validator::validate_pipeline(&config).expect("Validation should pass");

    // Clean up
    std::env::remove_var("TEST_SOURCE_PG_HOST");
    std::env::remove_var("TEST_SOURCE_PG_PORT");
    std::env::remove_var("TEST_DEST_PG_HOST");
    std::env::remove_var("TEST_DEST_PG_PORT");
}

/// Test that an invalid pipeline fixture fails validation.
#[test]
fn test_parse_and_validate_invalid_fixture() {
    let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("tests/fixtures/pipelines/invalid_pipeline.yaml");

    let result = parser::parse_pipeline(&fixture_path);
    assert!(
        result.is_err(),
        "Invalid pipeline should fail at parse-time"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("unknown variant") || err.contains("Failed to parse pipeline YAML"),
        "Expected serde enum parse error, got: {err}"
    );
}

/// Test full state backend lifecycle: create run, set cursors, complete run.
#[test]
fn test_state_backend_full_lifecycle() {
    let state = SqliteStateBackend::in_memory().expect("Failed to create in-memory state");

    // Start a run
    let run_id = state
        .start_run(&PipelineId::new("my_pipeline"), &StreamName::new("all"))
        .expect("Failed to start run");
    assert!(run_id > 0);

    // Set cursor for a stream
    let cursor = CursorState {
        cursor_field: Some("updated_at".to_string()),
        cursor_value: Some("2024-01-15T10:30:00Z".to_string()),
        updated_at: chrono::Utc::now().to_rfc3339(),
    };
    state
        .set_cursor(
            &PipelineId::new("my_pipeline"),
            &StreamName::new("users"),
            &cursor,
        )
        .expect("Failed to set cursor");

    // Read cursor back
    let loaded = state
        .get_cursor(&PipelineId::new("my_pipeline"), &StreamName::new("users"))
        .expect("Failed to get cursor")
        .expect("Cursor should exist");
    assert_eq!(loaded.cursor_field, Some("updated_at".to_string()));
    assert_eq!(
        loaded.cursor_value,
        Some("2024-01-15T10:30:00Z".to_string())
    );

    // Complete the run
    state
        .complete_run(
            run_id,
            RunStatus::Completed,
            &RunStats {
                records_read: 100,
                records_written: 100,
                bytes_read: 5000,
                bytes_written: 5000,
                error_message: None,
            },
        )
        .expect("Failed to complete run");

    // Start another run and fail it
    let run_id2 = state
        .start_run(&PipelineId::new("my_pipeline"), &StreamName::new("all"))
        .expect("Failed to start second run");

    state
        .complete_run(
            run_id2,
            RunStatus::Failed,
            &RunStats {
                records_read: 50,
                records_written: 0,
                bytes_read: 2500,
                bytes_written: 0,
                error_message: Some("Connection lost".to_string()),
            },
        )
        .expect("Failed to complete failed run");
}

/// Test that plugin path resolution works with RAPIDBYTE_PLUGIN_DIR.
#[test]
fn test_plugin_path_resolution_with_env() {
    use rapidbyte_runtime::resolve_plugin_path;
    use rapidbyte_types::wire::PluginKind;

    // Create a temp directory with a fake .wasm file in sources/ subdir
    let tmp = std::env::temp_dir().join("rapidbyte_test_plugins");
    let sources_dir = tmp.join("sources");
    std::fs::create_dir_all(&sources_dir).unwrap();
    let fake_wasm = sources_dir.join("postgres.wasm");
    std::fs::write(&fake_wasm, b"fake wasm").unwrap();

    std::env::set_var("RAPIDBYTE_PLUGIN_DIR", tmp.to_str().unwrap());

    let result = resolve_plugin_path("rapidbyte/postgres@v0.1.0", PluginKind::Source);
    assert!(
        result.is_ok(),
        "Should resolve with RAPIDBYTE_PLUGIN_DIR set"
    );
    assert_eq!(result.unwrap(), fake_wasm);

    // Clean up
    std::fs::remove_dir_all(&tmp).unwrap();
    std::env::remove_var("RAPIDBYTE_PLUGIN_DIR");
}

/// Test that plugin path resolution fails gracefully for missing plugins.
#[test]
fn test_plugin_path_resolution_missing() {
    use rapidbyte_runtime::resolve_plugin_path;
    use rapidbyte_types::wire::PluginKind;

    // Ensure RAPIDBYTE_PLUGIN_DIR points nowhere useful
    std::env::set_var("RAPIDBYTE_PLUGIN_DIR", "/tmp/nonexistent_dir_rapidbyte");

    let result = resolve_plugin_path("rapidbyte/nonexistent-plugin@v0.1.0", PluginKind::Source);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("not found"));

    std::env::remove_var("RAPIDBYTE_PLUGIN_DIR");
}

/// Test Arrow IPC round-trip with realistic schema (similar to PG users table).
#[test]
fn test_arrow_ipc_realistic_schema() {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    // Build a schema that mirrors the source_seed.sql users table
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("created_at", DataType::Utf8, true), // timestamps as Utf8 for v0.1
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
            Arc::new(StringArray::from(vec![
                Some("alice@example.com"),
                Some("bob@example.com"),
                Some("carol@example.com"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("2024-01-15T10:30:00"),
                Some("2024-01-15T10:31:00"),
                Some("2024-01-15T10:32:00"),
            ])),
        ],
    )
    .unwrap();

    // Encode to IPC
    let ipc_bytes = rapidbyte_engine::arrow::record_batch_to_ipc(&batch).unwrap();
    assert!(!ipc_bytes.is_empty());

    // Decode back
    let decoded = rapidbyte_engine::arrow::ipc_to_record_batches(&ipc_bytes).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].num_rows(), 3);
    assert_eq!(decoded[0].num_columns(), 4);

    // Verify column names preserved
    let schema = decoded[0].schema();
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(2).name(), "email");
    assert_eq!(schema.field(3).name(), "created_at");
}
