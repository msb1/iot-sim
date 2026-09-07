use std::sync::Arc;
use std::time::Instant;

use thiserror::Error;
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, timeout};
use tokio_util::sync::CancellationToken;

use crate::anomalies::AnomalyInjector;
use crate::export::{ExportError, TelemetryExporter};

use super::base::{epoch_millis, SimulatorService};

pub struct SimulationRuntime {
    services: Vec<Box<dyn SimulatorService>>,
    exporters: Vec<Arc<dyn TelemetryExporter>>,
    shutdown_timeout: std::time::Duration,
    simulator_stream_enabled: bool,
    anomaly_injector: AnomalyInjector,
    dataset_start_ms: Option<i64>,
}

impl SimulationRuntime {
    pub fn new(
        services: Vec<Box<dyn SimulatorService>>,
        exporters: Vec<Arc<dyn TelemetryExporter>>,
        shutdown_timeout: std::time::Duration,
        simulator_stream_enabled: bool,
        anomaly_injector: AnomalyInjector,
        dataset_start_ms: Option<i64>,
    ) -> Self {
        Self {
            services,
            exporters,
            shutdown_timeout,
            simulator_stream_enabled,
            anomaly_injector,
            dataset_start_ms,
        }
    }

    pub async fn start(mut self) -> Result<RunningSimulation, SimulationError> {
        if self.services.is_empty() {
            return Err(SimulationError::NoSensors);
        }
        let cancellation = CancellationToken::new();
        for exporter in &self.exporters {
            tracing::info!(exporter = exporter.name(), "starting telemetry exporter");
            exporter
                .start(cancellation.child_token())
                .await
                .map_err(|source| SimulationError::Exporter {
                    exporter: exporter.name(),
                    source,
                })?;
            tracing::info!(exporter = exporter.name(), "telemetry exporter started");
        }

        tracing::info!(
            services = self.services.len(),
            exporters = self.exporters.len(),
            "simulation scheduler started"
        );
        let loop_cancellation = cancellation.clone();
        let simulator_stream_enabled = self.simulator_stream_enabled;
        let dataset_start_ms = self.dataset_start_ms;
        let handle = tokio::spawn(async move {
            if let Some(dataset_start_ms) = dataset_start_ms {
                let dataset_end_ms = epoch_millis();
                let start = Instant::now();
                let span_ms = (dataset_end_ms - dataset_start_ms).max(0) as u64;
                let end = start + std::time::Duration::from_millis(span_ms);
                tracing::info!(
                    dataset_start_ms,
                    dataset_end_ms,
                    "generating historical dataset using real source timestamps"
                );
                loop {
                    if loop_cancellation.is_cancelled() {
                        break;
                    }
                    let next_due = self
                        .services
                        .iter()
                        .filter_map(|service| service.next_due())
                        .min()
                        .ok_or(SimulationError::NoSensors)?;
                    if next_due > end {
                        break;
                    }
                    let offset_ms = next_due
                        .saturating_duration_since(start)
                        .as_millis()
                        .min(i64::MAX as u128) as i64;
                    let timestamp_ms = dataset_start_ms.saturating_add(offset_ms);
                    let mut readings = Vec::new();
                    for service in &mut self.services {
                        readings.extend(service.sample_due(next_due, timestamp_ms));
                    }
                    if readings.is_empty() {
                        continue;
                    }
                    self.anomaly_injector.inject(&mut readings);
                    for exporter in &self.exporters {
                        exporter.export(&readings).await.map_err(|source| {
                            SimulationError::Exporter {
                                exporter: exporter.name(),
                                source,
                            }
                        })?;
                    }
                    // Keep cancellation responsive even for multi-year datasets.
                    tokio::task::yield_now().await;
                }
            } else {
                loop {
                    if loop_cancellation.is_cancelled() {
                        break;
                    }
                    let next_due = self
                        .services
                        .iter()
                        .filter_map(|service| service.next_due())
                        .min()
                        .ok_or(SimulationError::NoSensors)?;
                    let now = Instant::now();
                    if next_due > now {
                        tokio::select! {
                            _ = loop_cancellation.cancelled() => break,
                            _ = sleep_until(next_due.into()) => {}
                        }
                        continue;
                    }

                    let now = Instant::now();
                    let timestamp_ms = epoch_millis();
                    let mut readings = Vec::new();
                    for service in &mut self.services {
                        readings.extend(service.sample_due(now, timestamp_ms));
                    }
                    if readings.is_empty() {
                        continue;
                    }
                    self.anomaly_injector.inject(&mut readings);
                    tracing::debug!(count = readings.len(), "generated sensor readings");
                    if simulator_stream_enabled {
                        for reading in &readings {
                            match serde_json::to_string(reading) {
                                Ok(reading) => tracing::info!(
                                    target: "iot_sim::simulator_stream",
                                    telemetry = %reading,
                                    "simulator reading"
                                ),
                                Err(error) => {
                                    tracing::warn!(%error, "could not serialize simulator reading for console stream")
                                }
                            }
                        }
                    }
                    for exporter in &self.exporters {
                        if let Err(source) = exporter.export(&readings).await {
                            tracing::error!(exporter = exporter.name(), %source, count = readings.len(), "telemetry export failed");
                            return Err(SimulationError::Exporter {
                                exporter: exporter.name(),
                                source,
                            });
                        }
                    }
                }
            }

            for exporter in &self.exporters {
                tracing::info!(exporter = exporter.name(), "stopping telemetry exporter");
                exporter
                    .shutdown()
                    .await
                    .map_err(|source| SimulationError::Exporter {
                        exporter: exporter.name(),
                        source,
                    })?;
                tracing::info!(exporter = exporter.name(), "telemetry exporter stopped");
            }
            Ok(())
        });
        Ok(RunningSimulation {
            cancellation,
            handle: Some(handle),
            shutdown_timeout: self.shutdown_timeout,
        })
    }
}

pub struct RunningSimulation {
    cancellation: CancellationToken,
    handle: Option<JoinHandle<Result<(), SimulationError>>>,
    shutdown_timeout: std::time::Duration,
}

impl RunningSimulation {
    pub fn is_running(&self) -> bool {
        self.handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    pub async fn stop(mut self) -> Result<(), SimulationError> {
        tracing::info!("stopping simulation scheduler");
        self.cancellation.cancel();
        let handle = self.handle.take().expect("simulation handle is present");
        timeout(self.shutdown_timeout, handle)
            .await
            .map_err(|_| SimulationError::ShutdownTimeout)?
            .map_err(SimulationError::TaskJoin)??;
        Ok(())
    }

    /// Wait for a finite dataset generation to complete without cancelling it.
    pub async fn wait(mut self) -> Result<(), SimulationError> {
        let handle = self.handle.take().expect("simulation handle is present");
        handle.await.map_err(SimulationError::TaskJoin)??;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum SimulationError {
    #[error("the configuration contains no enabled sensors")]
    NoSensors,
    #[error(
        "Kafka is enabled in configuration but the binary was not built with --features kafka"
    )]
    KafkaFeatureDisabled,
    #[error("{exporter} exporter failed: {source}")]
    Exporter {
        exporter: &'static str,
        #[source]
        source: ExportError,
    },
    #[error("simulation task failed: {0}")]
    TaskJoin(tokio::task::JoinError),
    #[error("simulation did not stop within the configured shutdown timeout")]
    ShutdownTimeout,
}
