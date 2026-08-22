use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::domain::ScheduledSensor;
use crate::telemetry::SensorReading;

/// Common contract shared by every simulator domain.
pub trait SimulatorService: Send {
    fn name(&self) -> &'static str;
    fn next_due(&self) -> Option<Instant>;
    fn sample_due(&mut self, now: Instant, timestamp_ms: i64) -> Vec<SensorReading>;
}

#[derive(Debug)]
pub struct SensorSchedule {
    pub definition: ScheduledSensor,
    next_due: Instant,
    sequence: u64,
}

impl SensorSchedule {
    pub fn new(definition: ScheduledSensor, start: Instant) -> Self {
        Self {
            definition,
            next_due: start,
            sequence: 0,
        }
    }

    pub fn next_due(&self) -> Instant {
        self.next_due
    }

    pub fn is_due(&self, now: Instant) -> bool {
        self.next_due <= now
    }

    pub fn emit(
        &mut self,
        now: Instant,
        timestamp_ms: i64,
        metrics: impl IntoIterator<Item = (impl Into<String>, f64)>,
    ) -> SensorReading {
        self.sequence += 1;
        while self.next_due <= now {
            self.next_due += self.definition.sensor.timestep;
        }
        SensorReading {
            entity_type: self.definition.entity_type.clone(),
            entity_id: self.definition.entity_id.clone(),
            sensor_id: self.definition.sensor.sensor_id.clone(),
            sensor_type: self.definition.sensor.sensor_type,
            timestamp_ms,
            sequence: self.sequence,
            metrics: metrics
                .into_iter()
                .map(|(name, value)| (name.into(), self.definition.sensor.clamp(value)))
                .collect::<BTreeMap<_, _>>(),
        }
    }
}

pub fn earliest(schedules: &[SensorSchedule]) -> Option<Instant> {
    schedules.iter().map(SensorSchedule::next_due).min()
}

pub fn earliest_groups(groups: &[&[SensorSchedule]]) -> Option<Instant> {
    groups.iter().filter_map(|group| earliest(group)).min()
}

pub fn elapsed_seconds(last: &mut Instant, now: Instant, fallback: Duration) -> f64 {
    let elapsed = now.saturating_duration_since(*last);
    *last = now;
    if elapsed.is_zero() {
        fallback.as_secs_f64()
    } else {
        elapsed.as_secs_f64()
    }
}

pub fn epoch_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SensorType;
    use crate::domain::IoTSensor;

    #[test]
    fn schedule_emits_only_at_its_configured_timestep() {
        let start = Instant::now();
        let sensor = ScheduledSensor {
            entity_type: "test".into(),
            entity_id: "entity-1".into(),
            sensor: IoTSensor {
                sensor_id: "sensor-1".into(),
                sensor_type: SensorType::Ph,
                timestep: Duration::from_millis(250),
                min_value: 0.0,
                max_value: 14.0,
            },
        };
        let mut schedule = SensorSchedule::new(sensor, start);
        assert!(schedule.is_due(start));
        let first = schedule.emit(start, 1000, [("ph_units", 7.0)]);
        assert_eq!(first.sequence, 1);
        assert!(!schedule.is_due(start + Duration::from_millis(249)));
        assert!(schedule.is_due(start + Duration::from_millis(250)));
    }

    #[test]
    fn schedule_catches_up_without_emitting_a_burst() {
        let start = Instant::now();
        let sensor = ScheduledSensor {
            entity_type: "test".into(),
            entity_id: "entity-1".into(),
            sensor: IoTSensor {
                sensor_id: "sensor-1".into(),
                sensor_type: SensorType::Sound,
                timestep: Duration::from_millis(100),
                min_value: 30.0,
                max_value: 120.0,
            },
        };
        let mut schedule = SensorSchedule::new(sensor, start);
        let delayed = start + Duration::from_millis(450);
        schedule.emit(delayed, 1000, [("decibels_db", 50.0)]);
        assert!(!schedule.is_due(delayed));
        assert_eq!(schedule.next_due(), start + Duration::from_millis(500));
    }
}
