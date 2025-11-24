
use klickhouse::{Client, ClientOptions};
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer, registry::LookupSpan};
use tracing_core::field::Field;
use std::fmt::Debug;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, MissedTickBehavior};
use chrono::Utc;
use chrono::NaiveDateTime;
use std::env;
use anyhow::{anyhow, Result};
use url::Url;


#[macro_export]
macro_rules! elog {
    ($source:expr, $($arg:tt)*) => {{
        use chrono::Local;
        let now = Local::now();
        eprintln!("[{}][{}] {}", now.format("%Y-%m-%d %H:%M:%S"), $source, format_args!($($arg)*));
    }};
}

#[derive(Clone)]
pub struct Config {
    pub url: String,
    pub flush_interval_secs: u64,
    pub buffer_size: usize,
    pub receive_timeout_millis: u64,
    pub max_batch_size: usize,
}

#[derive(klickhouse::Row, Debug, Clone)]
pub struct LogEntry {
    pub timestamp: i64, // Nanoseconds since epoch
    pub deployment_id: String,
    pub log_level: String,
    pub message: String,
    pub target: String,
    pub file: String,
    pub line: u32,
}

pub struct CkzLayer {
    sender: mpsc::Sender<LogEntry>,
    deployment_id: String,
    source: String,
}

impl CkzLayer {
    pub fn new(config: Config) -> Self {
        let (sender, mut receiver) = mpsc::channel(config.buffer_size);
        let url = config.url.clone();
        let deployment_id = env::var("DEPLOYMENT_ID").expect("DEPLOYMENT_ID must be set");
        let source = env::var("CKZ_LOG_SOURCE").expect("CKZ_LOG_SOURCE must be set");
        
        // Clone the config to move it into the task
        let config_clone = config.clone();
        
        // Spawn initialization and processing task
        // The table will be created before we start processing logs, ensuring it exists on startup
        tokio::spawn(async move {
            // Create klickhouse client
            let client = match create_client(&url).await {
                Ok(client) => client,
                Err(e) => {
                    elog!("ckz", "Failed to create ClickHouse client: {:?}", e);
                    return;
                }
            };
            
            // Ensure the logs table exists before starting to process logs
            // This happens on startup, before any logs are processed
            if let Err(e) = ensure_logs_table_exists(&client).await {
                elog!("ckz", "Failed to ensure logs table exists: {:?}", e);
                // Continue anyway - logs will be buffered and can be retried later
            } else {
                elog!("ckz", "Successfully ensured logs table exists");
            }
            
            // Start processing logs (table is now ready)
            process_logs(client, &mut receiver, config_clone).await;
        });

        CkzLayer { sender, deployment_id, source }
    }
}

impl<S> Layer<S> for CkzLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event, _ctx: Context<S>) {
        let timestamp = naive_to_i64(Utc::now().naive_utc());
        let metadata = event.metadata();
        let mut message = String::new();
        event.record(&mut |field: &Field, value: &dyn Debug| {
            if field.name() == "message" {
                message = format!("{:?}", value);
            }
        });

        let log_entry = LogEntry {
            timestamp,
            // TODO: make this nicer
            deployment_id: format!("{}__{}", self.deployment_id, self.source),
            log_level: metadata.level().to_string(),
            message,
            target: metadata.target().to_string(),
            file: metadata.file().map(String::from).unwrap_or_default(),
            line: metadata.line().unwrap_or(0),
        };

        // Use try_send instead of send to drop logs if the channel is full
        if let Err(e) = self.sender.try_send(log_entry) {
            match e {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    // Log entry dropped due to full channel
                    elog!("ckz", "Log entry dropped due to full channel.");
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    elog!("ckz", "Failed to send log entry to logging task: channel closed");
                }
            }
        }
    }
}

async fn create_client(url: &str) -> Result<Client> {
    // Parse URL to extract host and port for klickhouse
    // klickhouse uses native protocol on port 9000, not HTTP on 8123
    let address = if url.contains("://") {
        let parsed_url = Url::parse(url).map_err(|e| anyhow!("Invalid URL: {}", e))?;
        
        let host = parsed_url
            .host_str()
            .ok_or_else(|| anyhow!("Missing host in URL"))?;
        
        // For HTTP URLs, always use port 9000 (native protocol) instead of 8123 (HTTP)
        // If the URL specifies port 8123, convert it to 9000 for native protocol
        let port = match parsed_url.port() {
            Some(8123) => 9000,  // Convert HTTP port to native protocol port
            Some(p) => p,         // Use specified port if it's not 8123
            None => 9000,         // Default to native protocol port
        };
        
        format!("{}:{}", host, port)
    } else {
        // Already in host:port format
        url.to_string()
    };

    // Get credentials from environment
    let username = env::var("CKZ_USER").unwrap_or_else(|_| "default".to_string());
    let password = env::var("CKZ_PASS").unwrap_or_else(|_| "".to_string());
    let db_name = env::var("CKZ_DB_NAME").expect("CKZ_DB_NAME must be set");

    // First connect without specifying a default database to create the database
    let mut options = ClientOptions::default();
    options.username = username.clone();
    options.password = password.clone();
    // Don't set default_database yet - connect to 'default' database first

    let client = Client::connect(&address, options)
        .await
        .map_err(|e| anyhow!("Failed to connect: {}", e))?;

    // Create the database if it doesn't exist
    let create_database_sql = format!("CREATE DATABASE IF NOT EXISTS {}", db_name);
    client
        .execute(&create_database_sql)
        .await
        .map_err(|e| anyhow!("Failed to create database: {}", e))?;

    // Now reconnect with the target database as default
    let mut options_with_db = ClientOptions::default();
    options_with_db.username = username;
    options_with_db.password = password;
    options_with_db.default_database = db_name;

    Client::connect(&address, options_with_db)
        .await
        .map_err(|e| anyhow!("Failed to connect with database: {}", e))
}

async fn ensure_logs_table_exists(client: &Client) -> Result<()> {
    let db_name = env::var("CKZ_DB_NAME").expect("CKZ_DB_NAME must be set");
    
    // Create logs table if it doesn't exist
    let create_table_query = format!(
        "CREATE TABLE IF NOT EXISTS {}.logs (
            timestamp Int64,
            deployment_id String CODEC(ZSTD(1)),
            log_level String CODEC(ZSTD(1)),
            message String CODEC(ZSTD(1)),
            target String CODEC(ZSTD(1)),
            file String CODEC(ZSTD(1)),
            line UInt32 CODEC(Delta, ZSTD(1))
        ) ENGINE = MergeTree()
        PARTITION BY (toYYYYMM(toDateTime(timestamp / 1000000000)))
        ORDER BY (timestamp)
        TTL toDateTime(timestamp / 1000000000) + INTERVAL 3 MONTH
        SETTINGS index_granularity = 8192",
        db_name
    );
    
    client
        .execute(&create_table_query)
        .await
        .map_err(|e| {
            elog!("ckz", "Failed to create logs table: {:?}", e);
            anyhow!("Failed to create logs table: {}", e)
        })?;
    
    elog!("ckz", "Successfully ensured logs table exists for database: {}", db_name);
    Ok(())
}

async fn process_logs(client: Client, receiver: &mut mpsc::Receiver<LogEntry>, config: Config) {
    let mut buffer = Vec::new();
    let max_interval = Duration::from_secs(config.flush_interval_secs);
    let mut last_send = Instant::now();
    
    // Create a periodic flush interval that fires every 100ms
    let mut flush_interval = tokio::time::interval(Duration::from_millis(100));
    flush_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // Receive log entries
            entry = receiver.recv() => {
                match entry {
                    Some(log_entry) => {
                        buffer.push(log_entry);
                        if buffer.len() >= config.max_batch_size {
                            send_batch(&client, &mut buffer).await;
                            last_send = Instant::now();
                        }
                    },
                    None => break, // Channel closed
                }
            },
            
            // Periodic flush based on timeout
            _ = flush_interval.tick() => {
                if !buffer.is_empty() && last_send.elapsed() >= max_interval {
                    send_batch(&client, &mut buffer).await;
                    last_send = Instant::now();
                }
            },
        }
    }

    // Send remaining logs before exiting
    if !buffer.is_empty() {
        send_batch(&client, &mut buffer).await;
    }
}

async fn send_batch(client: &Client, buffer: &mut Vec<LogEntry>) {
    if buffer.is_empty() {
        return;
    }

    let records = std::mem::take(buffer);

    // Use INSERT with FORMAT Native via klickhouse::Row derive macro
    let insert_query = "INSERT INTO logs FORMAT NATIVE";
    match client.insert_native_block(insert_query, records).await {
        Ok(()) => {
            // Successfully sent log entries (suppressed for noise reduction)
        }
        Err(e) => {
            elog!("ckz", "Error sending log entries: {:?}", e);
            // Clear the buffer on error, so the channel doesn't overfill and block the upstream stuff
            // due to bounded channel backpressure
        }
    }
}

pub fn naive_to_i64(ts: NaiveDateTime) -> i64 {
    ts.and_utc().timestamp_nanos_opt().expect("could not convert timestamp to nanos")
}

pub fn naive_to_u64(ts: NaiveDateTime) -> u64 {
    naive_to_i64(ts) as u64
}
