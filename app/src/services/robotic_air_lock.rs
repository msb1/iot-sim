use std::sync::Arc;
use std::time::Instant;

use rand::rngs::StdRng;
use rand::SeedableRng;
use rand_distr::{Distribution, Normal};

use crate::anomalies::ModelAnomalyController;
use crate::config::RoboticAirLockConfig;
use crate::domain::ScheduledSensor;
use crate::models::RoboticAirLockTelemetry;
use crate::telemetry::SensorReading;

use super::base::{SensorSchedule, SimulatorService};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EquipmentState {
    Nominal,
    FaultedSealActive,
    MaintenanceOffline,
}

impl EquipmentState {
    fn code(self) -> f64 {
        match self {
            Self::Nominal => 0.0,
            Self::FaultedSealActive => 1.0,
            Self::MaintenanceOffline => 2.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CycleStatus {
    Ingress,
    Sealing,
    Purging,
    Egress,
    Reset,
    Maintenance,
}

impl CycleStatus {
    fn code(self) -> f64 {
        match self {
            Self::Ingress => 0.0,
            Self::Sealing => 1.0,
            Self::Purging => 2.0,
            Self::Egress => 3.0,
            Self::Reset => 4.0,
            Self::Maintenance => 5.0,
        }
    }
}

struct AirLockSchedule {
    schedule: SensorSchedule,
    config: RoboticAirLockConfig,
    sample_index: u64,
    rng: StdRng,
}

/// Shared-physics simulator for a robotic battery dry-room material air lock.
/// The entire sensor frame is emitted together so door motion, pressure,
/// purging, seal health, and dew-point recovery remain physically coherent.
pub struct RoboticAirLockService {
    airlocks: Vec<AirLockSchedule>,
    anomalies: Arc<ModelAnomalyController>,
}

impl RoboticAirLockService {
    pub fn new(
        sensors: Vec<ScheduledSensor>,
        start: Instant,
        anomalies: Arc<ModelAnomalyController>,
    ) -> Self {
        let airlocks = sensors
            .into_iter()
            .map(|sensor| {
                let config = sensor
                    .sensor
                    .robotic_air_lock
                    .clone()
                    .expect("robotic_air_lock configuration was validated");
                let rng = config
                    .seed
                    .map(StdRng::seed_from_u64)
                    .unwrap_or_else(StdRng::from_entropy);
                AirLockSchedule {
                    schedule: SensorSchedule::new(sensor, start),
                    config,
                    sample_index: 0,
                    rng,
                }
            })
            .collect();
        Self {
            airlocks,
            anomalies,
        }
    }
}

impl SimulatorService for RoboticAirLockService {
    fn name(&self) -> &'static str {
        "robotic_air_lock"
    }

    fn next_due(&self) -> Option<Instant> {
        self.airlocks
            .iter()
            .map(|airlock| airlock.schedule.next_due())
            .min()
    }

    fn sample_due(&mut self, now: Instant, timestamp_ms: i64) -> Vec<SensorReading> {
        self.airlocks
            .iter_mut()
            .filter(|airlock| airlock.schedule.is_due(now))
            .map(|airlock| {
                let telemetry = telemetry_at(airlock, &self.anomalies, timestamp_ms);
                airlock.sample_index += 1;
                airlock
                    .schedule
                    .emit(now, timestamp_ms, telemetry.metrics())
            })
            .collect()
    }
}

fn telemetry_at(
    airlock: &mut AirLockSchedule,
    anomalies: &ModelAnomalyController,
    timestamp_ms: i64,
) -> RoboticAirLockTelemetry {
    let cfg = &airlock.config;
    let elapsed_seconds =
        airlock.sample_index as f64 * airlock.schedule.definition.sensor.timestep.as_secs_f64();
    let cycle_duration = cfg.entry_duration_seconds
        + cfg.sealing_duration_seconds
        + cfg.purge_duration_seconds
        + cfg.exit_duration_seconds
        + cfg.reset_duration_seconds;
    let cycle_number = (elapsed_seconds / cycle_duration).floor() as u64 + 1;
    let cycle_offset = elapsed_seconds.rem_euclid(cycle_duration);
    let anomaly_state = anomalies
        .robotic_air_lock_state(&airlock.schedule.definition.sensor.sensor_id, cycle_number, timestamp_ms);
    let state = if anomaly_state.maintenance_offline {
        EquipmentState::MaintenanceOffline
    } else if anomaly_state.seal_faulted {
        EquipmentState::FaultedSealActive
    } else {
        EquipmentState::Nominal
    };
    let wet_payload_recovery_multiplier = if state == EquipmentState::Nominal {
        anomaly_state.wet_payload_recovery_multiplier
    } else {
        None
    };
    let (status, phase_progress) = phase(cfg, cycle_offset, state);

    let mut telemetry = if state == EquipmentState::MaintenanceOffline {
        RoboticAirLockTelemetry {
            dew_point_c: cfg.nominal_dew_point_c,
            ambient_temperature_c: cfg.ambient_temperature_c,
            relative_humidity_pct: cfg.ambient_relative_humidity_pct,
            differential_pressure_pa: 0.0,
            purge_flow_rate_cfm: 0.0,
            outer_door_open: 0.0,
            inner_door_open: 0.0,
            inflatable_seal_pressure_bar: 0.0,
            cart_presence_detected: 0.0,
            cart_speed_m_s: 0.0,
            cycle_status_code: CycleStatus::Maintenance.code(),
            equipment_state_code: state.code(),
        }
    } else if state == EquipmentState::FaultedSealActive {
        faulted_telemetry(cfg, status, phase_progress, state)
    } else {
        nominal_telemetry(
            cfg,
            status,
            phase_progress,
            wet_payload_recovery_multiplier,
            state,
        )
    };
    apply_measurement_noise(
        &mut telemetry,
        cfg.measurement_noise_sigma,
        &mut airlock.rng,
    );
    telemetry
}

fn phase(cfg: &RoboticAirLockConfig, offset: f64, state: EquipmentState) -> (CycleStatus, f64) {
    if state == EquipmentState::MaintenanceOffline {
        return (CycleStatus::Maintenance, 0.0);
    }
    let phases = [
        (CycleStatus::Ingress, cfg.entry_duration_seconds),
        (CycleStatus::Sealing, cfg.sealing_duration_seconds),
        (CycleStatus::Purging, cfg.purge_duration_seconds),
        (CycleStatus::Egress, cfg.exit_duration_seconds),
        (CycleStatus::Reset, cfg.reset_duration_seconds),
    ];
    let mut cursor = 0.0;
    for (status, duration) in phases {
        if offset < cursor + duration {
            return (status, (offset - cursor) / duration);
        }
        cursor += duration;
    }
    (CycleStatus::Reset, 1.0)
}

fn nominal_telemetry(
    cfg: &RoboticAirLockConfig,
    status: CycleStatus,
    progress: f64,
    wet_payload_recovery_multiplier: Option<f64>,
    state: EquipmentState,
) -> RoboticAirLockTelemetry {
    let recovery_multiplier = wet_payload_recovery_multiplier.unwrap_or(1.0);
    let dew_point_c = match status {
        CycleStatus::Ingress => -20.0,
        CycleStatus::Sealing => -25.0,
        CycleStatus::Purging => {
            -20.0
                + (cfg.nominal_dew_point_c + 20.0)
                    * (1.0 - (-4.6 * progress / recovery_multiplier).exp())
        }
        CycleStatus::Egress | CycleStatus::Reset => cfg.nominal_dew_point_c,
        CycleStatus::Maintenance => cfg.nominal_dew_point_c,
    };
    let sealed = matches!(
        status,
        CycleStatus::Sealing | CycleStatus::Purging | CycleStatus::Reset
    );
    RoboticAirLockTelemetry {
        dew_point_c,
        ambient_temperature_c: cfg.ambient_temperature_c,
        relative_humidity_pct: cfg.ambient_relative_humidity_pct,
        differential_pressure_pa: if status == CycleStatus::Ingress {
            2.0
        } else if status == CycleStatus::Sealing {
            5.0
        } else if status == CycleStatus::Egress {
            15.0
        } else {
            cfg.nominal_differential_pressure_pa
        },
        purge_flow_rate_cfm: if status == CycleStatus::Purging {
            cfg.purge_flow_rate_cfm
        } else {
            0.0
        },
        outer_door_open: f64::from(status == CycleStatus::Ingress),
        inner_door_open: f64::from(status == CycleStatus::Egress),
        inflatable_seal_pressure_bar: if sealed {
            cfg.nominal_seal_pressure_bar
        } else {
            0.0
        },
        cart_presence_detected: f64::from(matches!(
            status,
            CycleStatus::Ingress | CycleStatus::Egress
        )),
        cart_speed_m_s: if matches!(status, CycleStatus::Ingress | CycleStatus::Egress) {
            0.8
        } else {
            0.0
        },
        cycle_status_code: status.code(),
        equipment_state_code: state.code(),
    }
}

fn faulted_telemetry(
    cfg: &RoboticAirLockConfig,
    status: CycleStatus,
    progress: f64,
    state: EquipmentState,
) -> RoboticAirLockTelemetry {
    let dew_point_c = match status {
        CycleStatus::Ingress => -20.0,
        CycleStatus::Sealing => -18.0,
        CycleStatus::Purging => -18.0 + 6.0 * progress,
        CycleStatus::Egress | CycleStatus::Reset | CycleStatus::Maintenance => -10.0,
    };
    RoboticAirLockTelemetry {
        dew_point_c,
        ambient_temperature_c: cfg.ambient_temperature_c,
        relative_humidity_pct: cfg.ambient_relative_humidity_pct,
        differential_pressure_pa: 4.5,
        purge_flow_rate_cfm: if status == CycleStatus::Purging {
            cfg.purge_flow_rate_cfm
        } else {
            0.0
        },
        outer_door_open: f64::from(status == CycleStatus::Ingress),
        inner_door_open: 0.0, // Interlock prevents dry-room contamination.
        inflatable_seal_pressure_bar: 0.0,
        cart_presence_detected: f64::from(status == CycleStatus::Ingress),
        cart_speed_m_s: if status == CycleStatus::Ingress {
            0.8
        } else {
            0.0
        },
        cycle_status_code: status.code(),
        equipment_state_code: state.code(),
    }
}

fn apply_measurement_noise(telemetry: &mut RoboticAirLockTelemetry, sigma: f64, rng: &mut StdRng) {
    if sigma == 0.0 {
        return;
    }
    let noise = |scale: f64, rng: &mut StdRng| {
        Normal::new(0.0, sigma * scale)
            .expect("validated noise")
            .sample(rng)
    };
    telemetry.dew_point_c += noise(1.0, rng);
    telemetry.ambient_temperature_c += noise(0.2, rng);
    telemetry.relative_humidity_pct = (telemetry.relative_humidity_pct + noise(0.05, rng)).max(0.0);
    telemetry.differential_pressure_pa =
        (telemetry.differential_pressure_pa + noise(0.5, rng)).max(0.0);
    telemetry.inflatable_seal_pressure_bar =
        (telemetry.inflatable_seal_pressure_bar + noise(0.05, rng)).max(0.0);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> RoboticAirLockConfig {
        RoboticAirLockConfig {
            air_lock_id: "MAL-ROBOTIC-04".into(),
            entry_duration_seconds: 45.0,
            sealing_duration_seconds: 15.0,
            purge_duration_seconds: 105.0,
            exit_duration_seconds: 45.0,
            reset_duration_seconds: 90.0,
            nominal_dew_point_c: -50.0,
            ambient_temperature_c: 20.0,
            ambient_relative_humidity_pct: 0.45,
            nominal_differential_pressure_pa: 22.0,
            nominal_seal_pressure_bar: 3.1,
            purge_flow_rate_cfm: 350.0,
            measurement_noise_sigma: 0.0,
            seed: Some(1),
        }
    }

    #[test]
    fn wet_payload_recovery_is_slower_than_nominal() {
        let cfg = config();
        let normal = nominal_telemetry(
            &cfg,
            CycleStatus::Purging,
            0.75,
            None,
            EquipmentState::Nominal,
        );
        let wet = nominal_telemetry(
            &cfg,
            CycleStatus::Purging,
            0.75,
            Some(3.0),
            EquipmentState::Nominal,
        );
        assert!(wet.dew_point_c > normal.dew_point_c);
        // The same exponential coefficient is divided by three, so reaching
        // a given low-moisture setpoint takes three times as long.
        assert!((wet.dew_point_c + 20.0).abs() < (normal.dew_point_c + 20.0).abs());
    }
}
