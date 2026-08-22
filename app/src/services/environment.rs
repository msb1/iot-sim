use std::time::Instant;

use crate::config::{SoundModelConfig, WeatherModelConfig};
use crate::domain::ScheduledSensor;
use crate::models::{SoundSensorSimulator, WeatherStationSimulator};
use crate::telemetry::SensorReading;

use super::base::{earliest, elapsed_seconds, SensorSchedule, SimulatorService};

pub struct WeatherService {
    model: WeatherStationSimulator,
    schedules: Vec<SensorSchedule>,
    last_sample: Instant,
}

impl WeatherService {
    pub fn new(config: &WeatherModelConfig, sensors: Vec<ScheduledSensor>, start: Instant) -> Self {
        Self {
            model: WeatherStationSimulator::from_config(config),
            schedules: sensors
                .into_iter()
                .map(|s| SensorSchedule::new(s, start))
                .collect(),
            last_sample: start,
        }
    }
}

impl SimulatorService for WeatherService {
    fn name(&self) -> &'static str {
        "weather"
    }
    fn next_due(&self) -> Option<Instant> {
        earliest(&self.schedules)
    }

    fn sample_due(&mut self, now: Instant, timestamp_ms: i64) -> Vec<SensorReading> {
        if !self.schedules.iter().any(|s| s.is_due(now)) {
            return Vec::new();
        }
        let fallback = self.schedules[0].definition.sensor.timestep;
        let dt_hours = elapsed_seconds(&mut self.last_sample, now, fallback) / 3600.0;
        let value = self.model.next_step(dt_hours);
        self.schedules
            .iter_mut()
            .filter(|s| s.is_due(now))
            .map(|s| {
                s.emit(
                    now,
                    timestamp_ms,
                    [
                        ("temperature_c", value.temperature_c),
                        ("humidity_pct", value.humidity_pct),
                        ("pressure_hpa", value.pressure_hpa),
                    ],
                )
            })
            .collect()
    }
}

pub struct SoundService {
    model: SoundSensorSimulator,
    schedules: Vec<SensorSchedule>,
    last_sample: Instant,
}

impl SoundService {
    pub fn new(config: &SoundModelConfig, sensors: Vec<ScheduledSensor>, start: Instant) -> Self {
        Self {
            model: SoundSensorSimulator::from_config(config),
            schedules: sensors
                .into_iter()
                .map(|s| SensorSchedule::new(s, start))
                .collect(),
            last_sample: start,
        }
    }
}

impl SimulatorService for SoundService {
    fn name(&self) -> &'static str {
        "sound"
    }
    fn next_due(&self) -> Option<Instant> {
        earliest(&self.schedules)
    }
    fn sample_due(&mut self, now: Instant, timestamp_ms: i64) -> Vec<SensorReading> {
        if !self.schedules.iter().any(|s| s.is_due(now)) {
            return Vec::new();
        }
        let fallback = self.schedules[0].definition.sensor.timestep;
        let value = self
            .model
            .next_sample(elapsed_seconds(&mut self.last_sample, now, fallback));
        self.schedules
            .iter_mut()
            .filter(|s| s.is_due(now))
            .map(|s| s.emit(now, timestamp_ms, [("decibels_db", value)]))
            .collect()
    }
}
