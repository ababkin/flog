/*
CREATE DATABASE IF NOT EXISTS event_server;

CREATE TABLE IF NOT EXISTS event_server.logs (
	timestamp DateTime64(9) CODEC(Delta, ZSTD(3)),
	deployment_id String CODEC(ZSTD(3)),
	log_level String CODEC(ZSTD(3)),
	message String CODEC(ZSTD(3)),
	target String CODEC(ZSTD(3)),
	file String CODEC(ZSTD(3)),
	line UInt32 CODEC(Delta, ZSTD(3))
) ENGINE = MergeTree()
PARTITION BY (deployment_id, toYYYYMM(timestamp))
ORDER BY (timestamp, deployment_id)
TTL toDateTime(timestamp) + INTERVAL 6 MONTH
SETTINGS index_granularity = 8192;
*/

use reqwest::blocking::Client;
use serde::Serialize;
use tracing::{Event, Subscriber};
use tracing_subscriber::{layer::Context, Layer, registry::LookupSpan};
use tracing_core::field::Field;
use std::fmt::Debug;
use std::sync::mpsc::{self, SyncSender, Receiver};
use std::thread;
use std::time::{ Instant, Duration };
use chrono::Utc;
use chrono::NaiveDateTime;
use std::env;


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

#[derive(Serialize, Debug)]
struct LogEntry {
    timestamp: u64,
    deployment_id: String,
    log_level: String,
    message: String,
    target: String,
    file: Option<String>,
    line: Option<u32>,
}

pub struct CkzLayer {
    sender: SyncSender<LogEntry>,
    deployment_id: String,
}

impl CkzLayer {
    pub fn new(config: Config) -> Self {
        let (sender, receiver) = mpsc::sync_channel(config.buffer_size);
        let endpoint = config.url.to_string();
        let deployment_id = env::var("DEPLOYMENT_ID").expect("DEPLOYMENT_ID must be set");
        
        // Clone the config to move it into the thread
        let config_clone = config.clone();
        
        thread::spawn(move || {
            let client = Client::new();
            process_logs(client, receiver, endpoint, config_clone);
        });

        CkzLayer { sender, deployment_id }
    }
}
impl<S> Layer<S> for CkzLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event, _ctx: Context<S>) {
        let timestamp = naive_to_u64(Utc::now().naive_utc());
        let metadata = event.metadata();
        let mut message = String::new();
        event.record(&mut |field: &Field, value: &dyn Debug| {
            if field.name() == "message" {
                message = format!("{:?}", value);
            }
        });

        let log_entry = LogEntry {
            timestamp,
            deployment_id: self.deployment_id.clone(),
            log_level: metadata.level().to_string(),
            message,
            target: metadata.target().to_string(),
            file: metadata.file().map(String::from),
            line: metadata.line(),
        };

        // Use try_send instead of send to drop logs if the channel is full
        if let Err(e) = self.sender.try_send(log_entry) {
            if let mpsc::TrySendError::Full(_) = e {
                // Log entry dropped due to full channel
                elog!("ckz", "Log entry dropped due to full channel.");
            } else {
                elog!("ckz", "Failed to send log entry to logging thread: {:?}", e);
            }
        }
    }
}

fn process_logs(client: Client, receiver: Receiver<LogEntry>, endpoint: String, config: Config) {
    let mut buffer = Vec::new();
    let max_interval = Duration::from_secs(config.flush_interval_secs);
    let mut last_send = Instant::now();

    loop {
        let entry = receiver.recv_timeout(Duration::from_millis(config.receive_timeout_millis));
        match entry {
            Ok(log_entry) => {
                buffer.push(log_entry);
                if buffer.len() >= config.max_batch_size || last_send.elapsed() >= max_interval {
                    send_batch(&client, &endpoint, &mut buffer);
                    last_send = Instant::now();
                }
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if !buffer.is_empty() && last_send.elapsed() >= max_interval {
                    send_batch(&client, &endpoint, &mut buffer);
                    last_send = Instant::now();
                }
            },
            Err(_) => break, // Channel disconnected
        }
    }

    // Send remaining logs before exiting
    if !buffer.is_empty() {
        send_batch(&client, &endpoint, &mut buffer);
    }
}

fn send_batch(client: &Client, endpoint: &str, buffer: &mut Vec<LogEntry>) {
    if buffer.is_empty() {
        return;
    }

    // Create a request with a timeout of 10 seconds
    let request = client
        .post(endpoint)
        .json(&buffer)
        .timeout(Duration::from_secs(10)); // Set the timeout to 10 seconds

    match request.send() {
        Ok(response) => {
            if response.status().is_success() {
                // Successfully sent log entries
                buffer.clear();
            } else {
                elog!("ckz", "Failed to send log entries: {:?}", response.text().unwrap_or_else(|_| "Unknown error".to_string()));
            }
        }
        Err(e) => {
            elog!("ckz", "Error sending log entries (possibly due to timeout): {:?}", e);
            // Clear the buffer on timeout or any other error, so the channel doesn't overfill and block the upstream stuff
            // due to bounded channel backpressure
            buffer.clear();
        }
    }
}

pub fn naive_to_i64(ts: NaiveDateTime) -> i64 {
    ts.and_utc().timestamp_nanos_opt().expect("could not convert timestamp to nanos")
}

pub fn naive_to_u64(ts: NaiveDateTime) -> u64 {
    naive_to_i64(ts) as u64
}
