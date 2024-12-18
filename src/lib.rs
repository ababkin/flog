use tracing::Level;
use tracing_subscriber::{fmt, layer::SubscriberExt, Registry};
use std::env;
use tracing_subscriber::layer::Layer;
use tracing_subscriber::filter::{Targets, LevelFilter};

pub mod ckz; use ckz::CkzLayer;


pub struct LogConfig {
    pub ckz: ckz::Config,
}

pub fn setup(config: LogConfig) {
    let log_level_str = env::var("LOG_LEVEL").unwrap_or_else(|_| "DEBUG".to_string());
    let log_level: Level = log_level_str.parse().unwrap_or(Level::DEBUG);

    let common_filter = Targets::new()
        .with_target("hyper_util", LevelFilter::OFF)
        .with_target("hyper", LevelFilter::INFO)
        .with_target("h2", LevelFilter::INFO);

    let stdout_filter = common_filter
        .clone()
        .with_target("", log_level);

    let ckz_filter = common_filter
        .with_target("", LevelFilter::DEBUG);

    let stdout_layer = fmt::Layer::new()
        .with_writer(std::io::stdout)
        .with_filter(stdout_filter);

    let ckz_layer = CkzLayer::new(config.ckz)
        .with_filter(ckz_filter);

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