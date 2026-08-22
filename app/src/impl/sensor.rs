use std::time::Duration;

use crate::config::{SensorConfig, SensorType};

#[derive(Debug, Clone)]
pub struct IoTSensor {
    pub sensor_id: String,
    pub sensor_type: SensorType,
    pub timestep: Duration,
    pub min_value: f64,
    pub max_value: f64,
}

impl IoTSensor {
    pub fn from_config(config: &SensorConfig, index: usize) -> Self {
        Self {
            sensor_id: config.expanded_id(index),
            sensor_type: config.sensor_type,
            timestep: Duration::from_millis(config.timestep_ms),
            min_value: config.min_value,
            max_value: config.max_value,
        }
    }

    pub fn clamp(&self, value: f64) -> f64 {
        value.clamp(self.min_value, self.max_value)
    }
}
