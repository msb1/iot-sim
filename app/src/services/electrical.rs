use std::time::Instant;

use crate::config::ElectricalModelConfig;
use crate::domain::ScheduledSensor;
use crate::models::ThreePhaseSensorSimulator;
use crate::telemetry::SensorReading;

use super::base::{earliest, elapsed_seconds, SensorSchedule, SimulatorService};

pub struct ElectricalService {
    model: ThreePhaseSensorSimulator,
    schedules: Vec<SensorSchedule>,
    last_sample: Instant,
}

impl ElectricalService {
    pub fn new(
        config: &ElectricalModelConfig,
        sensors: Vec<ScheduledSensor>,
        start: Instant,
    ) -> Self {
        Self {
            model: ThreePhaseSensorSimulator::from_config(config),
            schedules: sensors
                .into_iter()
                .map(|s| SensorSchedule::new(s, start))
                .collect(),
            last_sample: start,
        }
    }
}

impl SimulatorService for ElectricalService {
    fn name(&self) -> &'static str {
        "electrical"
    }
    fn next_due(&self) -> Option<Instant> {
        earliest(&self.schedules)
    }
    fn sample_due(&mut self, now: Instant, timestamp_ms: i64) -> Vec<SensorReading> {
        if !self.schedules.iter().any(|s| s.is_due(now)) {
            return Vec::new();
        }
        let fallback = self.schedules[0].definition.sensor.timestep;
        let dt = elapsed_seconds(&mut self.last_sample, now, fallback);
        let (a, b, c) = self.model.sample_phases(dt);
        let tripped = if self.model.check_safety_relay() {
            1.0
        } else {
            0.0
        };
        self.schedules
            .iter_mut()
            .filter(|s| s.is_due(now))
            .map(|s| {
                s.emit(
                    now,
                    timestamp_ms,
                    [
                        ("voltage_a_v", a.voltage_v),
                        ("current_a_a", a.current_a),
                        ("power_a_kw", a.real_power_kw),
                        ("apparent_power_a_kva", a.apparent_power_kva),
                        ("power_factor_a", a.power_factor),
                        ("voltage_thd_a_pct", a.voltage_thd_pct),
                        ("active_energy_a_kwh", a.cumulative_active_energy_kwh),
                        ("apparent_energy_a_kvah", a.cumulative_apparent_energy_kvah),
                        ("voltage_b_v", b.voltage_v),
                        ("current_b_a", b.current_a),
                        ("power_b_kw", b.real_power_kw),
                        ("apparent_power_b_kva", b.apparent_power_kva),
                        ("power_factor_b", b.power_factor),
                        ("voltage_thd_b_pct", b.voltage_thd_pct),
                        ("active_energy_b_kwh", b.cumulative_active_energy_kwh),
                        ("apparent_energy_b_kvah", b.cumulative_apparent_energy_kvah),
                        ("voltage_c_v", c.voltage_v),
                        ("current_c_a", c.current_a),
                        ("power_c_kw", c.real_power_kw),
                        ("apparent_power_c_kva", c.apparent_power_kva),
                        ("power_factor_c", c.power_factor),
                        ("voltage_thd_c_pct", c.voltage_thd_pct),
                        ("active_energy_c_kwh", c.cumulative_active_energy_kwh),
                        ("apparent_energy_c_kvah", c.cumulative_apparent_energy_kvah),
                        ("safety_relay_tripped", tripped),
                    ],
                )
            })
            .collect()
    }
}
