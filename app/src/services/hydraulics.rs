use std::time::Instant;

use crate::config::HydraulicsModelConfig;
use crate::domain::ScheduledSensor;
use crate::models::FluidHydraulicsSimulator;
use crate::telemetry::SensorReading;

use super::base::{earliest_groups, elapsed_seconds, SensorSchedule, SimulatorService};

pub struct HydraulicsService {
    model: FluidHydraulicsSimulator,
    level: Vec<SensorSchedule>,
    flow: Vec<SensorSchedule>,
    load: Vec<SensorSchedule>,
    last_sample: Instant,
    started: Instant,
    mixer_start_after_seconds: Option<f64>,
    high_discharge_after_seconds: Option<f64>,
    mixer_started: bool,
    discharge_started: bool,
}

impl HydraulicsService {
    pub fn new(
        config: &HydraulicsModelConfig,
        groups: [Vec<ScheduledSensor>; 3],
        start: Instant,
    ) -> Self {
        let [level, flow, load] = groups;
        let schedules = |items: Vec<ScheduledSensor>| {
            items
                .into_iter()
                .map(|s| SensorSchedule::new(s, start))
                .collect()
        };
        Self {
            model: FluidHydraulicsSimulator::from_config(config),
            level: schedules(level),
            flow: schedules(flow),
            load: schedules(load),
            last_sample: start,
            started: start,
            mixer_start_after_seconds: config.mixer_start_after_seconds,
            high_discharge_after_seconds: config.high_discharge_after_seconds,
            mixer_started: false,
            discharge_started: false,
        }
    }
}

impl SimulatorService for HydraulicsService {
    fn name(&self) -> &'static str {
        "fluid_hydraulics"
    }
    fn next_due(&self) -> Option<Instant> {
        earliest_groups(&[&self.level, &self.flow, &self.load])
    }
    fn sample_due(&mut self, now: Instant, timestamp_ms: i64) -> Vec<SensorReading> {
        if !self
            .level
            .iter()
            .chain(&self.flow)
            .chain(&self.load)
            .any(|s| s.is_due(now))
        {
            return Vec::new();
        }
        let elapsed = now.saturating_duration_since(self.started).as_secs_f64();
        if !self.mixer_started
            && self
                .mixer_start_after_seconds
                .is_some_and(|at| elapsed >= at)
        {
            self.model.set_mixer_state(true);
            self.mixer_started = true;
        }
        if !self.discharge_started
            && self
                .high_discharge_after_seconds
                .is_some_and(|at| elapsed >= at)
        {
            self.model.trigger_high_discharge_draw();
            self.discharge_started = true;
        }
        let fallback = self
            .level
            .iter()
            .chain(&self.flow)
            .chain(&self.load)
            .map(|s| s.definition.sensor.timestep)
            .min()
            .unwrap();
        let value =
            self.model
                .sample_hydraulics(elapsed_seconds(&mut self.last_sample, now, fallback));
        let mut readings: Vec<_> = self
            .level
            .iter_mut()
            .filter(|s| s.is_due(now))
            .map(|s| {
                s.emit(
                    now,
                    timestamp_ms,
                    [
                        ("tank_level_pct", value.tank_level_pct),
                        ("fluid_volume_liters", value.fluid_volume_liters),
                    ],
                )
            })
            .collect();
        readings.extend(self.flow.iter_mut().filter(|s| s.is_due(now)).map(|s| {
            s.emit(
                now,
                timestamp_ms,
                [("mass_flow_rate_l_min", value.mass_flow_rate_l_min)],
            )
        }));
        readings.extend(self.load.iter_mut().filter(|s| s.is_due(now)).map(|s| {
            s.emit(
                now,
                timestamp_ms,
                [
                    ("load_cell_weight_kg", value.load_cell_weight_kg),
                    ("mixer_active", if value.mixer_active { 1.0 } else { 0.0 }),
                ],
            )
        }));
        readings
    }
}
