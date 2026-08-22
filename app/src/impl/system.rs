use std::collections::HashMap;

use crate::config::AppConfig;

use super::{IoTEntity, IoTSensor};

#[derive(Debug, Clone)]
pub struct SimulationSystem {
    pub config_name: String,
    pub schema_version: String,
    pub entities: Vec<IoTEntity>,
}

impl SimulationSystem {
    pub fn from_config(config: &AppConfig) -> Self {
        Self {
            config_name: config.config_name.clone(),
            schema_version: config.schema_version.clone(),
            entities: config.entities.iter().map(IoTEntity::from_config).collect(),
        }
    }

    pub fn sensors_by_type(&self) -> HashMap<crate::config::SensorType, Vec<ScheduledSensor>> {
        let mut sensors = HashMap::new();
        for entity in &self.entities {
            for sensor in &entity.sensors {
                sensors
                    .entry(sensor.sensor_type)
                    .or_insert_with(Vec::new)
                    .push(ScheduledSensor {
                        entity_type: entity.entity_type.clone(),
                        entity_id: entity.entity_id.clone(),
                        sensor: sensor.clone(),
                    });
            }
        }
        sensors
    }
}

#[derive(Debug, Clone)]
pub struct ScheduledSensor {
    pub entity_type: String,
    pub entity_id: String,
    pub sensor: IoTSensor,
}
