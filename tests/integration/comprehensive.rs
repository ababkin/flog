use flog::ckz::{CkzLayer, Config};
use futures_util::StreamExt;
use serial_test::serial;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::clickhouse::ClickHouse;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, Registry};

#[tokio::test]
#[serial]
async fn test_flog_logs_to_clickhouse() {
    let _ = env_logger::builder().is_test(true).try_init();

    // Start ClickHouse container for testing
    let container = ClickHouse::default()
        .start()
        .await
        .expect("failed to start ClickHouse container");

    let host = container.get_host().await.expect("failed to get container host");
    let port = container.get_host_port_ipv4(9000).await.expect("failed to get container port");

    // Set up environment variables required by CkzLayer
    std::env::set_var("DEPLOYMENT_ID", "test_deployment");
    std::env::set_var("CKZ_LOG_SOURCE", "test_source");
    std::env::set_var("CKZ_DB_NAME", "default");
    std::env::set_var("CKZ_USER", "default");
    std::env::set_var("CKZ_PASS", "");

    // Create test config with ClickHouse container
    let config = Config {
        url: format!("{}:{}", host, port),
        flush_interval_secs: 1,
        buffer_size: 100,
        receive_timeout_millis: 100,
        max_batch_size: 10,
    };

    // Create CkzLayer
    let ckz_layer = CkzLayer::new(config);

    // Set up tracing subscriber with the CkzLayer
    let subscriber = Registry::default().with(ckz_layer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set global default subscriber");

    // Log a test message with a unique identifier
    let test_id = chrono::Utc::now().timestamp_nanos_opt().expect("timestamp should always be valid");
    let test_message = format!("test_log_message_{}", test_id);
    info!("{}", test_message);

    // Yield to async to allow the log entry to be processed and flushed
    // Wait a bit longer than flush_interval_secs to ensure it's flushed
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Check that the data has been logged to ClickHouse
    // Create a ClickHouse client to query the database
    let mut client_options = klickhouse::ClientOptions::default();
    client_options.username = "default".to_string();
    client_options.password = "".to_string();
    client_options.default_database = "default".to_string();

    let client = klickhouse::Client::connect(&format!("{}:{}", host, port), client_options)
        .await
        .expect("failed to connect to ClickHouse");

    // Verify logs table exists and has at least one entry
    let count_query = "SELECT COUNT(*) as count FROM logs";
    let mut rows = client
        .query::<klickhouse::RawRow>(count_query)
        .await
        .expect("failed to query logs count");
    let mut row = rows
        .next()
        .await
        .expect("no rows returned")
        .expect("row error");
    let count: u64 = row.get::<&str, u64>("count");
    assert!(
        count >= 1,
        "Expected at least 1 log entry, but found {}",
        count
    );

    // Verify the specific log entry we just created
    let query = format!(
        "SELECT message, deployment_id, log_level, target FROM logs WHERE message = '{}' LIMIT 1",
        test_message
    );
    let mut rows = client
        .query::<klickhouse::RawRow>(&query)
        .await
        .expect("failed to query logs");
    
    if let Some(row_result) = rows.next().await {
        let mut row = row_result.expect("row error");
        let message: String = row.get("message");
        let deployment_id: String = row.get("deployment_id");
        let log_level: String = row.get("log_level");
        let target: String = row.get("target");

        assert_eq!(message, test_message, "Message should match");
        assert_eq!(
            deployment_id,
            "test_deployment__test_source",
            "Deployment ID should match"
        );
        assert_eq!(log_level, "INFO", "Log level should be INFO");
        assert!(!target.is_empty(), "Target should not be empty");
    } else {
        panic!("Expected to find the test log entry in ClickHouse");
    }

    // Verify log entry structure - check that all required fields are present
    let query = "SELECT timestamp, deployment_id, log_level, message, target, file, line FROM logs LIMIT 1";
    let mut rows = client
        .query::<klickhouse::RawRow>(query)
        .await
        .expect("failed to query logs structure");
    let mut row = rows.next().await.expect("no first row").expect("row error");
    
    let timestamp: i64 = row.get("timestamp");
    let deployment_id: String = row.get("deployment_id");
    let log_level: String = row.get("log_level");
    let message: String = row.get("message");
    let target: String = row.get("target");
    let _file: String = row.get("file");
    let _line: u32 = row.get("line");

    assert!(timestamp > 0, "Timestamp should be positive");
    assert!(!deployment_id.is_empty(), "Deployment ID should not be empty");
    assert!(!log_level.is_empty(), "Log level should not be empty");
    assert!(!message.is_empty(), "Message should not be empty");
    assert!(!target.is_empty(), "Target should not be empty");
    // file and line may be empty/default, but they should exist in the row

    // Container will be automatically stopped when dropped
}

