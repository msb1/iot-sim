use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio_util::sync::CancellationToken;

use crate::config::KafkaConfig;
use crate::export::{ExportError, TelemetryExporter};
use crate::telemetry::{KafkaMetricMessage, SensorReading};

pub struct KafkaExporter {
    producer: FutureProducer,
    topic: String,
}

impl KafkaExporter {
    pub fn new(config: &KafkaConfig) -> Result<Self, ExportError> {
        let mut client = ClientConfig::new();
        client
            .set("bootstrap.servers", &config.brokers)
            .set("client.id", &config.client_id)
            .set("message.timeout.ms", "5000")
            .set("enable.idempotence", "true")
            .set("acks", "all");
        if let Some(value) = &config.security.security_protocol {
            client.set("security.protocol", value);
        }
        if let Some(value) = &config.security.sasl_mechanism {
            client.set("sasl.mechanism", value);
        }
        if let Some(value) = &config.security.username {
            client.set("sasl.username", value);
        }
        if let Some(value) = &config.security.password {
            client.set("sasl.password", value);
        }
        let producer = client
            .create::<FutureProducer>()
            .map_err(|error| ExportError::Kafka(error.to_string()))?;
        tracing::info!(brokers = %config.brokers, topic = %config.topic, client_id = %config.client_id, "Kafka producer configured");
        Ok(Self {
            producer,
            topic: config.topic.clone(),
        })
    }
}

#[async_trait]
impl TelemetryExporter for KafkaExporter {
    fn name(&self) -> &'static str {
        "kafka"
    }
    async fn start(&self, _cancellation: CancellationToken) -> Result<(), ExportError> {
        tracing::info!(topic = %self.topic, "Kafka telemetry exporter ready to produce");
        Ok(())
    }

    async fn export(&self, readings: &[SensorReading]) -> Result<(), ExportError> {
        for reading in readings {
            for (metric, value) in &reading.metrics {
                let message = KafkaMetricMessage {
                    entity_type: &reading.entity_type,
                    entity_id: &reading.entity_id,
                    sensor_id: &reading.sensor_id,
                    sensor_type: reading.sensor_type,
                    timestamp_ms: reading.timestamp_ms,
                    interval_ms: reading.interval_ms,
                    metric,
                    value: *value,
                };
                let payload = serde_json::to_string(&message)?;
                let sensor_type = serde_json::to_string(&message.sensor_type)?;
                let sensor_type = sensor_type.trim_matches('"');
                let headers = OwnedHeaders::new()
                    .insert(Header {
                        key: "entity_id",
                        value: Some(message.entity_id),
                    })
                    .insert(Header {
                        key: "sensor_id",
                        value: Some(message.sensor_id),
                    })
                    .insert(Header {
                        key: "sensor_type",
                        value: Some(sensor_type),
                    })
                    .insert(Header {
                        key: "metric",
                        value: Some(message.metric),
                    });
                let record = FutureRecord::to(&self.topic)
                    .key(message.entity_id)
                    .payload(&payload)
                    .headers(headers);
                self.producer
                    .send(record, Timeout::After(Duration::from_secs(5)))
                    .await
                    .map_err(|(error, _)| ExportError::Kafka(error.to_string()))?;
                tracing::debug!(topic = %self.topic, entity_id = message.entity_id, sensor_id = message.sensor_id, metric = message.metric, bytes = payload.len(), "Kafka telemetry record delivered");
            }
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), ExportError> {
        use rdkafka::producer::Producer;
        self.producer
            .flush(Timeout::After(Duration::from_secs(5)))
            .map_err(|error| ExportError::Kafka(error.to_string()))?;
        tracing::info!(topic = %self.topic, "Kafka producer flushed");
        Ok(())
    }
}

// kcat -b 192.168.1.50:9092,192.168.1.50:9094,192.168.1.50:9096 -C -t iot.telemetry.v1 -o beginning -f 'key=%k headers=%h value=%s\n'
//
// kafka-topics --bootstrap-server 192.168.1.50:9092,192.168.1.50:9094,192.168.1.50:9096 --list
// kafka-topics --create --bootstrap-server 192.168.1.50:9092,192.168.1.50:9094,192.168.1.50:9096 --topic iot.telemetry.v1 \
// --partitions 3 \
// --replication-factor 3 \
// --topic iot.telemetry.v1  \
// --config retention.ms=604800000 \
// --config min.insync.replicas=2
