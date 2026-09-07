use std::time::Duration;

use crate::config::{DataCenterRackConfig, ScenarioSignalConfig, SensorConfig, SensorType};

#[derive(Debug, Clone)]
pub struct IoTSensor {
    pub sensor_id: String,
    pub sensor_type: SensorType,
    pub timestep: Duration,
    pub min_value: f64,
    pub max_value: f64,
    pub scenario: Option<ScenarioSignalConfig>,
    pub data_center_rack: Option<DataCenterRackConfig>,
}

impl IoTSensor {
    pub fn from_config(config: &SensorConfig, index: usize) -> Self {
        Self {
            sensor_id: config.expanded_id(index),
            sensor_type: config.sensor_type,
            timestep: Duration::from_millis(config.timestep_ms),
            min_value: config.min_value,
            max_value: config.max_value,
            scenario: config.scenario.clone(),
            data_center_rack: config.data_center_rack.clone(),
        }
    }

    pub fn clamp(&self, value: f64) -> f64 {
        value.clamp(self.min_value, self.max_value)
    }
}
