use std::sync::Arc;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{primitives::ByteStream, Client};
use tokio_util::sync::CancellationToken;
use crate::anomalies::AnomalyEventCollector;
use crate::config::DatasetConfig;
use crate::export::{ExportError, TelemetryExporter};
use crate::telemetry::SensorReading;

pub struct MetadataExporter {
    config: DatasetConfig,
    key: String,
    events: Arc<AnomalyEventCollector>,
}

impl MetadataExporter {
    pub fn new(config: DatasetConfig, start_ms: i64, events: Arc<AnomalyEventCollector>) -> Self {
        let start = chrono::DateTime::from_timestamp_millis(start_ms).expect("validated timestamp")
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        Self { config, key: format!("metadata/anomalies-{start}.json"), events }
    }
}

#[async_trait]
impl TelemetryExporter for MetadataExporter {
    fn name(&self) -> &'static str { "metadata" }
    async fn start(&self, _token: CancellationToken) -> Result<(), ExportError> { Ok(()) }
    async fn export(&self, _readings: &[SensorReading]) -> Result<(), ExportError> { Ok(()) }
    async fn shutdown(&self) -> Result<(), ExportError> {
        let body = serde_json::to_vec_pretty(&self.events.snapshot())?;
        let credentials = aws_sdk_s3::config::Credentials::new(
            self.config.s3_access_key.clone(), self.config.s3_secret_key.clone(), None, None,
            "iot-sim-metadata-config",
        );
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .credentials_provider(credentials).endpoint_url(&self.config.s3_endpoint_url).load().await;
        let client = Client::from_conf(aws_sdk_s3::config::Builder::from(&shared).force_path_style(true).build());
        client.put_object().bucket(&self.config.s3_bucket_name).key(&self.key)
            .content_type("application/json").body(ByteStream::from(body)).send().await
            .map_err(|e| ExportError::Metadata(e.to_string()))?;
        tracing::info!(bucket = %self.config.s3_bucket_name, key = %self.key, events = self.events.snapshot().len(), "anomaly metadata uploaded");
        Ok(())
    }
}
