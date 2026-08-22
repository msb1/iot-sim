use std::time::Instant;

use crate::config::GasModelConfig;
use crate::domain::ScheduledSensor;
use crate::models::GasEnvironmentalSafetySimulator;
use crate::telemetry::SensorReading;

use super::base::{earliest_groups, elapsed_seconds, SensorSchedule, SimulatorService};

pub struct GasSafetyService {
    model: GasEnvironmentalSafetySimulator,
    dissolved_oxygen: Vec<SensorSchedule>,
    toxic: Vec<SensorSchedule>,
    combustible: Vec<SensorSchedule>,
    voc: Vec<SensorSchedule>,
    last_sample: Instant,
}

impl GasSafetyService {
    pub fn new(config: &GasModelConfig, groups: [Vec<ScheduledSensor>; 4], start: Instant) -> Self {
        let [dissolved_oxygen, toxic, combustible, voc] = groups;
        let schedules = |items: Vec<ScheduledSensor>| {
            items
                .into_iter()
                .map(|s| SensorSchedule::new(s, start))
                .collect()
        };
        Self {
            model: GasEnvironmentalSafetySimulator::from_config(config),
            dissolved_oxygen: schedules(dissolved_oxygen),
            toxic: schedules(toxic),
            combustible: schedules(combustible),
            voc: schedules(voc),
            last_sample: start,
        }
    }
}

impl SimulatorService for GasSafetyService {
    fn name(&self) -> &'static str {
        "gas_environmental_safety"
    }
    fn next_due(&self) -> Option<Instant> {
        earliest_groups(&[
            &self.dissolved_oxygen,
            &self.toxic,
            &self.combustible,
            &self.voc,
        ])
    }
    fn sample_due(&mut self, now: Instant, timestamp_ms: i64) -> Vec<SensorReading> {
        if !self
            .dissolved_oxygen
            .iter()
            .chain(&self.toxic)
            .chain(&self.combustible)
            .chain(&self.voc)
            .any(|s| s.is_due(now))
        {
            return Vec::new();
        }
        let fallback = self
            .dissolved_oxygen
            .iter()
            .chain(&self.toxic)
            .chain(&self.combustible)
            .chain(&self.voc)
            .map(|s| s.definition.sensor.timestep)
            .min()
            .unwrap();
        let value =
            self.model
                .sample_safety_block(elapsed_seconds(&mut self.last_sample, now, fallback));
        let mut readings: Vec<_> = self
            .dissolved_oxygen
            .iter_mut()
            .filter(|s| s.is_due(now))
            .map(|s| {
                s.emit(
                    now,
                    timestamp_ms,
                    [("dissolved_oxygen_mg_l", value.dissolved_oxygen_mg_l)],
                )
            })
            .collect();
        readings.extend(self.toxic.iter_mut().filter(|s| s.is_due(now)).map(|s| {
            s.emit(
                now,
                timestamp_ms,
                [
                    ("carbon_monoxide_co_ppm", value.carbon_monoxide_co_ppm),
                    (
                        "sensor_health_warning",
                        if value.sensor_health_warning {
                            1.0
                        } else {
                            0.0
                        },
                    ),
                ],
            )
        }));
        readings.extend(
            self.combustible
                .iter_mut()
                .filter(|s| s.is_due(now))
                .map(|s| {
                    s.emit(
                        now,
                        timestamp_ms,
                        [("combustible_gas_lel_pct", value.combustible_gas_lel_pct)],
                    )
                }),
        );
        readings.extend(
            self.voc
                .iter_mut()
                .filter(|s| s.is_due(now))
                .map(|s| s.emit(now, timestamp_ms, [("voc_pid_ppm", value.voc_pid_ppm)])),
        );
        readings
    }
}
