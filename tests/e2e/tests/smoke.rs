#[tokio::test]
async fn harness_bootstrap_exposes_postgres_port_and_plugin_dir() {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");

    assert!(context.postgres_port > 0);
    assert!(context.plugin_dir.exists());
}
