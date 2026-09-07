pub mod anomalies;
pub mod config;
#[path = "impl.rs"]
pub mod domain;
pub mod export;
pub mod models;
pub mod services;
pub mod telemetry;

use std::sync::Arc;
use std::time::Duration;

use anomalies::AnomalyInjector;
use chrono::DateTime;
use config::AppConfig;
use export::dataset::DatasetExporter;
#[cfg(feature = "kafka")]
use export::kafka::KafkaExporter;
use export::prometheus::PrometheusExporter;
use export::TelemetryExporter;
use services::{build_services, SimulationError, SimulationRuntime};

pub async fn build_runtime(config: &AppConfig) -> Result<SimulationRuntime, SimulationError> {
    let system = domain::SimulationSystem::from_config(config);
    let services = build_services(config, &system);
    let anomaly_injector = AnomalyInjector::from_config(&config.anomalies);
    let mut exporters: Vec<Arc<dyn TelemetryExporter>> = Vec::new();
    let dataset_start_ms = if config.exporters.dataset.enabled {
        Some(
            DateTime::parse_from_rfc3339(
                config
                    .exporters
                    .dataset
                    .start_time
                    .as_deref()
                    .expect("validated dataset start time"),
            )
            .expect("validated dataset start time")
            .timestamp_millis(),
        )
    } else {
        None
    };
    if config.exporters.prometheus.enabled {
        exporters.push(Arc::new(
            PrometheusExporter::new(
                config.exporters.prometheus.clone(),
                config.config_name.clone(),
            )
            .map_err(|source| SimulationError::Exporter {
                exporter: "prometheus",
                source,
            })?,
        ));
    }
    if config.exporters.kafka.enabled {
        #[cfg(feature = "kafka")]
        exporters.push(Arc::new(
            KafkaExporter::new(&config.exporters.kafka).map_err(|source| {
                SimulationError::Exporter {
                    exporter: "kafka",
                    source,
                }
            })?,
        ));
        #[cfg(not(feature = "kafka"))]
        return Err(SimulationError::KafkaFeatureDisabled);
    }
    if let Some(start_ms) = dataset_start_ms {
        exporters.push(Arc::new(
            DatasetExporter::new(config.exporters.dataset.clone(), start_ms).map_err(|source| {
                SimulationError::Exporter {
                    exporter: "dataset",
                    source,
                }
            })?,
        ));
    }
    tracing::info!(
        config = %config.config_name,
        services = services.len(),
        exporters = exporters.len(),
        simulator_stream = config.logging.simulator_stream.enabled,
        anomaly_profiles = anomaly_injector.active_profile_count(),
        "simulation runtime assembled"
    );
    Ok(SimulationRuntime::new(
        services,
        exporters,
        Duration::from_secs(config.runtime.shutdown_timeout_seconds),
        config.logging.simulator_stream.enabled,
        anomaly_injector,
        dataset_start_ms,
    ))
}
