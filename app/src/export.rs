#[cfg(feature = "kafka")]
pub mod kafka;
pub mod prometheus;

use async_trait::async_trait;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::telemetry::SensorReading;

#[async_trait]
pub trait TelemetryExporter: Send + Sync {
    fn name(&self) -> &'static str;
    async fn start(&self, cancellation: CancellationToken) -> Result<(), ExportError>;
    async fn export(&self, readings: &[SensorReading]) -> Result<(), ExportError>;
    async fn shutdown(&self) -> Result<(), ExportError> {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("Prometheus exporter error: {0}")]
    Prometheus(String),
    #[error("Kafka exporter error: {0}")]
    Kafka(String),
    #[error("telemetry serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
