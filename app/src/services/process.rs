use std::time::Instant;

use crate::config::ProcessModelConfig;
use crate::domain::ScheduledSensor;
use crate::models::ProcessAnalyticsSimulator;
use crate::telemetry::SensorReading;

use super::base::{earliest_groups, elapsed_seconds, SensorSchedule, SimulatorService};

pub struct ProcessService {
    model: ProcessAnalyticsSimulator,
    concentration: Vec<SensorSchedule>,
    ftir: Vec<SensorSchedule>,
    last_sample: Instant,
}

impl ProcessService {
    pub fn new(
        config: &ProcessModelConfig,
        concentration: Vec<ScheduledSensor>,
        ftir: Vec<ScheduledSensor>,
        start: Instant,
    ) -> Self {
        Self {
            model: ProcessAnalyticsSimulator::from_config(config),
            concentration: concentration
                .into_iter()
                .map(|s| SensorSchedule::new(s, start))
                .collect(),
            ftir: ftir
                .into_iter()
                .map(|s| SensorSchedule::new(s, start))
                .collect(),
            last_sample: start,
        }
    }
}

impl SimulatorService for ProcessService {
    fn name(&self) -> &'static str {
        "process_analytics"
    }
    fn next_due(&self) -> Option<Instant> {
        earliest_groups(&[&self.concentration, &self.ftir])
    }
    fn sample_due(&mut self, now: Instant, timestamp_ms: i64) -> Vec<SensorReading> {
        if !self
            .concentration
            .iter()
            .chain(&self.ftir)
            .any(|s| s.is_due(now))
        {
            return Vec::new();
        }
        let fallback = self
            .concentration
            .iter()
            .chain(&self.ftir)
            .map(|s| s.definition.sensor.timestep)
            .min()
            .unwrap();
        let value =
            self.model
                .sample_analytics(elapsed_seconds(&mut self.last_sample, now, fallback));
        let mut readings: Vec<_> = self
            .concentration
            .iter_mut()
            .filter(|s| s.is_due(now))
            .map(|s| {
                s.emit(
                    now,
                    timestamp_ms,
                    [("concentration_mol_l", value.concentration_mol_l)],
                )
            })
            .collect();
        readings.extend(self.ftir.iter_mut().filter(|s| s.is_due(now)).map(|s| {
            s.emit(
                now,
                timestamp_ms,
                [
                    ("absorbance_lambda_1", value.ftir_absorbance_array[0]),
                    ("absorbance_lambda_2", value.ftir_absorbance_array[1]),
                    ("absorbance_lambda_3", value.ftir_absorbance_array[2]),
                    ("absorbance_lambda_4", value.ftir_absorbance_array[3]),
                    ("optical_path_length_cm", value.optical_path_length_cm),
                ],
            )
        }));
        readings
    }
}
