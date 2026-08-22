use std::time::Instant;

use crate::config::{DriftingModelConfig, SensorType};
use crate::domain::ScheduledSensor;
use crate::models::base_models::SensorSimulator;
use crate::telemetry::SensorReading;

use super::base::{earliest_groups, elapsed_seconds, SensorSchedule, SimulatorService};

struct DriftingChannel {
    metric_name: &'static str,
    model: SensorSimulator,
    schedules: Vec<SensorSchedule>,
    last_sample: Instant,
}

impl DriftingChannel {
    fn new(
        metric_name: &'static str,
        config: &DriftingModelConfig,
        sensors: Vec<ScheduledSensor>,
        start: Instant,
    ) -> Self {
        Self {
            metric_name,
            model: SensorSimulator::new(
                config.target_mean,
                config.measurement_noise_sigma,
                config.drift_sigma,
                0.0,
            ),
            schedules: sensors
                .into_iter()
                .map(|s| SensorSchedule::new(s, start))
                .collect(),
            last_sample: start,
        }
    }
}

pub struct ElectrochemistryService {
    channels: Vec<DriftingChannel>,
}

impl ElectrochemistryService {
    pub fn new(
        ph: (&DriftingModelConfig, Vec<ScheduledSensor>),
        orp: (&DriftingModelConfig, Vec<ScheduledSensor>),
        conductivity: (&DriftingModelConfig, Vec<ScheduledSensor>),
        start: Instant,
    ) -> Self {
        let candidates = [
            ("ph_units", ph.0, ph.1),
            ("orp_mv", orp.0, orp.1),
            ("conductivity_ms_cm", conductivity.0, conductivity.1),
        ];
        Self {
            channels: candidates
                .into_iter()
                .filter(|(_, _, sensors)| !sensors.is_empty())
                .map(|(name, config, sensors)| DriftingChannel::new(name, config, sensors, start))
                .collect(),
        }
    }
}

impl SimulatorService for ElectrochemistryService {
    fn name(&self) -> &'static str {
        "electrochemistry"
    }
    fn next_due(&self) -> Option<Instant> {
        earliest_groups(
            &self
                .channels
                .iter()
                .map(|c| c.schedules.as_slice())
                .collect::<Vec<_>>(),
        )
    }
    fn sample_due(&mut self, now: Instant, timestamp_ms: i64) -> Vec<SensorReading> {
        let mut readings = Vec::new();
        for channel in &mut self.channels {
            if !channel.schedules.iter().any(|s| s.is_due(now)) {
                continue;
            }
            let fallback = channel.schedules[0].definition.sensor.timestep;
            let value = channel.model.next_drifting_with_dt(elapsed_seconds(
                &mut channel.last_sample,
                now,
                fallback,
            ));
            readings.extend(
                channel
                    .schedules
                    .iter_mut()
                    .filter(|s| s.is_due(now))
                    .map(|s| s.emit(now, timestamp_ms, [(channel.metric_name, value)])),
            );
        }
        readings
    }
}

#[allow(dead_code)]
fn _assert_types() {
    let _ = [SensorType::Ph, SensorType::Orp, SensorType::Conductivity];
}
