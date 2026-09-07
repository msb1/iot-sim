use serde::Serialize;
use std::collections::BTreeMap;

use crate::config::SensorType;

#[derive(Debug, Clone, Serialize)]
pub struct SensorReading {
    pub entity_type: String,
    pub entity_id: String,
    pub sensor_id: String,
    pub sensor_type: SensorType,
    pub timestamp_ms: i64,
    pub interval_ms: i64,
    pub sequence: u64,
    pub metrics: BTreeMap<String, f64>,
}

/// The compact Kafka value emitted for one sensor metric.
///
/// Kafka headers deliberately remain transport metadata and are not duplicated
/// in this payload.
#[derive(Debug, Serialize)]
pub struct KafkaMetricMessage<'a> {
    pub entity_type: &'a str,
    pub entity_id: &'a str,
    pub sensor_id: &'a str,
    pub sensor_type: SensorType,
    pub timestamp_ms: i64,
    pub interval_ms: i64,
    pub metric: &'a str,
    pub value: f64,
}

#[cfg(test)]
mod tests {
    use crate::config::SensorType;

    use super::KafkaMetricMessage;

    #[test]
    fn kafka_metric_value_contains_only_metric_data() {
        let message = KafkaMetricMessage {
            entity_type: "water_treatment_skid",
            entity_id: "water_quality_skid_01",
            sensor_id: "cond-01",
            sensor_type: SensorType::Conductivity,
            timestamp_ms: 1_787_336_289_652,
            interval_ms: 5_000,
            metric: "conductivity_ms_cm",
            value: 1.3956547378189113,
        };

        let value = serde_json::to_value(message).unwrap();
        assert_eq!(value["metric"], "conductivity_ms_cm");
        assert_eq!(value["value"], 1.3956547378189113);
        assert_eq!(value["interval_ms"], 5_000);
        assert!(value.get("headers").is_none());
        assert_eq!(value.as_object().unwrap().len(), 8);
    }
}
