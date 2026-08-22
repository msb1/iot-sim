use std::sync::Mutex;

use async_trait::async_trait;
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use prometheus::{Encoder, GaugeVec, Opts, Registry, TextEncoder};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::PrometheusConfig;
use crate::export::{ExportError, TelemetryExporter};
use crate::telemetry::SensorReading;

pub struct PrometheusExporter {
    config: PrometheusConfig,
    registry: Registry,
    value: GaugeVec,
    timestamp_ms: GaugeVec,
    sequence: GaugeVec,
    config_name: String,
    server: Mutex<Option<JoinHandle<()>>>,
}

impl PrometheusExporter {
    pub fn new(config: PrometheusConfig, config_name: String) -> Result<Self, ExportError> {
        let registry = Registry::new();
        let labels = &[
            "config_name",
            "entity_type",
            "entity_id",
            "sensor_id",
            "sensor_type",
            "metric",
        ];
        let value = GaugeVec::new(
            Opts::new("iot_sensor_value", "Latest simulated IoT sensor value"),
            labels,
        )
        .map_err(|error| ExportError::Prometheus(error.to_string()))?;
        let sample_labels = &[
            "config_name",
            "entity_type",
            "entity_id",
            "sensor_id",
            "sensor_type",
        ];
        let timestamp_ms = GaugeVec::new(
            Opts::new(
                "iot_sensor_timestamp_milliseconds",
                "Unix timestamp of the latest simulated sample",
            ),
            sample_labels,
        )
        .map_err(|error| ExportError::Prometheus(error.to_string()))?;
        let sequence = GaugeVec::new(
            Opts::new(
                "iot_sensor_sequence",
                "Monotonic sequence number for a simulated sensor",
            ),
            sample_labels,
        )
        .map_err(|error| ExportError::Prometheus(error.to_string()))?;
        registry
            .register(Box::new(value.clone()))
            .map_err(|error| ExportError::Prometheus(error.to_string()))?;
        registry
            .register(Box::new(timestamp_ms.clone()))
            .map_err(|error| ExportError::Prometheus(error.to_string()))?;
        registry
            .register(Box::new(sequence.clone()))
            .map_err(|error| ExportError::Prometheus(error.to_string()))?;
        Ok(Self {
            config,
            registry,
            value,
            timestamp_ms,
            sequence,
            config_name,
            server: Mutex::new(None),
        })
    }
}

#[derive(Clone)]
struct MetricsState(Registry);

async fn metrics(State(state): State<MetricsState>) -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let families = state.0.gather();
    let mut buffer = Vec::new();
    match encoder.encode(&families, &mut buffer) {
        Ok(()) => (
            StatusCode::OK,
            [("content-type", encoder.format_type())],
            buffer,
        )
            .into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

#[async_trait]
impl TelemetryExporter for PrometheusExporter {
    fn name(&self) -> &'static str {
        "prometheus"
    }

    async fn start(&self, cancellation: CancellationToken) -> Result<(), ExportError> {
        let listener = tokio::net::TcpListener::bind(&self.config.bind)
            .await
            .map_err(|error| {
                ExportError::Prometheus(format!("cannot bind {}: {error}", self.config.bind))
            })?;
        let path = if self.config.path.starts_with('/') {
            self.config.path.clone()
        } else {
            format!("/{}", self.config.path)
        };
        let app = Router::new()
            .route(&path, get(metrics))
            .with_state(MetricsState(self.registry.clone()));
        let handle = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app)
                .with_graceful_shutdown(cancellation.cancelled_owned())
                .await
            {
                tracing::error!(%error, "Prometheus HTTP server stopped unexpectedly");
            }
        });
        *self
            .server
            .lock()
            .map_err(|_| ExportError::Prometheus("server lock poisoned".into()))? = Some(handle);
        tracing::info!(bind = %self.config.bind, path = %path, "Prometheus metrics endpoint listening");
        Ok(())
    }

    async fn export(&self, readings: &[SensorReading]) -> Result<(), ExportError> {
        for reading in readings {
            let sensor_type = serde_json::to_value(reading.sensor_type)?
                .as_str()
                .unwrap_or("unknown")
                .to_owned();
            let sample_labels = [
                self.config_name.as_str(),
                reading.entity_type.as_str(),
                reading.entity_id.as_str(),
                reading.sensor_id.as_str(),
                sensor_type.as_str(),
            ];
            self.timestamp_ms
                .with_label_values(&sample_labels)
                .set(reading.timestamp_ms as f64);
            self.sequence
                .with_label_values(&sample_labels)
                .set(reading.sequence as f64);
            for (metric, value) in &reading.metrics {
                let labels = [
                    sample_labels[0],
                    sample_labels[1],
                    sample_labels[2],
                    sample_labels[3],
                    sample_labels[4],
                    metric.as_str(),
                ];
                self.value.with_label_values(&labels).set(*value);
            }
        }
        tracing::debug!(
            count = readings.len(),
            "updated Prometheus metrics from telemetry batch"
        );
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), ExportError> {
        let handle = self
            .server
            .lock()
            .map_err(|_| ExportError::Prometheus("server lock poisoned".into()))?
            .take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        tracing::info!("Prometheus metrics endpoint stopped");
        Ok(())
    }
}
