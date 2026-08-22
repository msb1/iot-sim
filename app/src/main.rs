use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use iot_sim::config::AppConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("iot_sim=info".parse()?),
        )
        .init();

    let config_path = config_path_from_args()?;
    let config = AppConfig::load(&config_path)?;
    tracing::info!(
        path = %config_path.display(),
        config = %config.config_name,
        prometheus_enabled = config.exporters.prometheus.enabled,
        kafka_enabled = config.exporters.kafka.enabled,
        simulator_stream = config.logging.simulator_stream.enabled,
        "configuration loaded and validated"
    );
    let duration = config.runtime.run_duration_seconds;
    let running = iot_sim::build_runtime(&config).await?.start().await?;
    tracing::info!(config = %config.config_name, sensors = config.entities.iter().map(|e| e.sensors.iter().map(|s| s.quantity).sum::<usize>()).sum::<usize>(), "simulation started");

    if let Some(seconds) = duration {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(seconds)) => {},
            result = tokio::signal::ctrl_c() => result?,
        }
    } else {
        tokio::signal::ctrl_c().await?;
    }

    running.stop().await?;
    tracing::info!("simulation stopped cleanly");
    Ok(())
}

fn config_path_from_args() -> Result<PathBuf, Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    match args.next() {
        None => Ok(PathBuf::from("config/simulation.yaml")),
        Some(flag) if flag == "--config" || flag == "-c" => args
            .next()
            .map(PathBuf::from)
            .ok_or_else(|| "--config requires a path".into()),
        Some(path) => Ok(PathBuf::from(path)),
    }
}
