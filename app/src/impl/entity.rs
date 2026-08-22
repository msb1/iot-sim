use crate::config::EntityConfig;

use super::IoTSensor;

#[derive(Debug, Clone)]
pub struct IoTEntity {
    pub entity_type: String,
    pub entity_id: String,
    pub description: String,
    pub sensors: Vec<IoTSensor>,
}

impl IoTEntity {
    pub fn from_config(config: &EntityConfig) -> Self {
        let sensors = config
            .sensors
            .iter()
            .filter(|sensor| sensor.enabled)
            .flat_map(|sensor| {
                (1..=sensor.quantity).map(move |index| IoTSensor::from_config(sensor, index))
            })
            .collect();
        Self {
            entity_type: config.entity_type.clone(),
            entity_id: config.entity_id.clone(),
            description: config.description.clone(),
            sensors,
        }
    }
}
