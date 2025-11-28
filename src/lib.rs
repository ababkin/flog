use tracing_subscriber::{fmt, layer::SubscriberExt, Registry};
use std::env;
use tracing_subscriber::layer::Layer;
use tracing_subscriber::filter::LevelFilter;

pub mod ckz; use ckz::CkzLayer;

const DEFAULT_STDOUT_LOG_LEVEL: &str = "INFO";
const DEFAULT_CKZ_LOG_LEVEL: &str = "DEBUG";

pub struct LogConfig {
    pub ckz: ckz::Config,
}

pub fn setup(config: LogConfig) {
    let stdout_log_level_str = env::var("STDOUT_LOG_LEVEL").unwrap_or_else(|_| DEFAULT_STDOUT_LOG_LEVEL.to_string());
    let stdout_log_level: LevelFilter = stdout_log_level_str.parse().expect("STDOUT_LOG_LEVEL not a string");

    let ckz_log_level_str = env::var("CKZ_LOG_LEVEL").unwrap_or_else(|_| DEFAULT_CKZ_LOG_LEVEL.to_string());
    let ckz_log_level: LevelFilter = ckz_log_level_str.parse().expect("CKZ_LOG_LEVEL not a string");

    // Simple filters: just use LevelFilter directly - it matches all targets at the specified level
    let stdout_layer = fmt::Layer::new()
        .with_writer(std::io::stdout)
        .with_filter(stdout_log_level);

    let ckz_layer = CkzLayer::new(config.ckz)
        .with_filter(ckz_log_level);

    let subscriber = Registry::default()
        .with(ckz_layer)
        .with(stdout_layer);

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
}


// pub fn setup_tracing() {
//     let log_level = Level::from_str(var("LOG_LEVEL").unwrap_or("DEBUG".to_string()).as_str())
//         .expect("LOG_LEVEL not a string");
//     let subscriber = FmtSubscriber::builder()
//         .with_max_level(log_level)
//         .finish();

//     tracing::subscriber::set_global_default(subscriber)
//         .expect("setting default subscriber failed");
// }