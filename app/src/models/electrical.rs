use rand::thread_rng;
use rand_distr::{Distribution, Normal};

use crate::config::ElectricalModelConfig;

/// Holds data for a single electrical phase channel.
#[derive(Debug, Clone, Copy)]
pub struct PhaseReading {
    pub voltage_v: f64,
    pub current_a: f64,
    pub power_factor: f64,
    pub real_power_kw: f64,
    pub apparent_power_kva: f64,
    pub voltage_thd_pct: f64,
    pub cumulative_active_energy_kwh: f64,
    pub cumulative_apparent_energy_kvah: f64,
}

/// Simulates a three-phase industrial power line with energy accumulation,
/// harmonic distortion, and a latched over-current safety relay.
pub struct ThreePhaseSensorSimulator {
    nominal_voltage: f64,
    grid_impedance: f64,
    trip_threshold_amps: f64,
    is_tripped: bool,

    pub impedance_a: f64,
    pub impedance_b: f64,
    pub impedance_c: f64,
    pub background_v_thd_pct: f64,
    normal_background_v_thd_pct: f64,
    polluted_voltage_thd_pct: f64,
    fault_impedance_ohm: f64,
    base_power_factor: [f64; 3],
    normal_phase_a_impedance_ohm: f64,
    current_thd_divisor_amps: f64,
    current_thd_multiplier: f64,
    thd_power_factor_penalty: f64,

    pub energy_active_kwh_a: f64,
    pub energy_apparent_kvah_a: f64,
    pub energy_active_kwh_b: f64,
    pub energy_apparent_kvah_b: f64,
    pub energy_active_kwh_c: f64,
    pub energy_apparent_kvah_c: f64,

    v_noise: Normal<f64>,
    i_noise: Normal<f64>,
    pf_noise: Normal<f64>,
    thd_noise: Normal<f64>,
}

impl ThreePhaseSensorSimulator {
    pub fn from_config(config: &ElectricalModelConfig) -> Self {
        Self {
            nominal_voltage: config.nominal_voltage_v,
            grid_impedance: config.grid_impedance_ohm,
            trip_threshold_amps: config.trip_threshold_amps,
            is_tripped: false,
            impedance_a: config.phase_impedance_ohm[0],
            impedance_b: config.phase_impedance_ohm[1],
            impedance_c: config.phase_impedance_ohm[2],
            background_v_thd_pct: config.background_voltage_thd_pct,
            normal_background_v_thd_pct: config.background_voltage_thd_pct,
            polluted_voltage_thd_pct: config.polluted_voltage_thd_pct,
            fault_impedance_ohm: config.fault_impedance_ohm,
            base_power_factor: config.base_power_factor,
            normal_phase_a_impedance_ohm: config.phase_impedance_ohm[0],
            current_thd_divisor_amps: config.current_thd_divisor_amps,
            current_thd_multiplier: config.current_thd_multiplier,
            thd_power_factor_penalty: config.thd_power_factor_penalty,
            energy_active_kwh_a: 0.0,
            energy_apparent_kvah_a: 0.0,
            energy_active_kwh_b: 0.0,
            energy_apparent_kvah_b: 0.0,
            energy_active_kwh_c: 0.0,
            energy_apparent_kvah_c: 0.0,
            v_noise: Normal::new(0.0, config.voltage_noise_sigma).unwrap(),
            i_noise: Normal::new(0.0, config.current_noise_sigma).unwrap(),
            pf_noise: Normal::new(0.0, config.power_factor_noise_sigma).unwrap(),
            thd_noise: Normal::new(0.0, config.thd_noise_sigma).unwrap(),
        }
    }

    /// Forces a severe over-current spike on Phase A.
    pub fn inject_fault_spike(&mut self) {
        self.impedance_a = self.fault_impedance_ohm;
    }

    /// Enables or disables elevated industrial background harmonics.
    pub fn set_industrial_pollution(&mut self, enabled: bool) {
        self.background_v_thd_pct = if enabled {
            self.polluted_voltage_thd_pct
        } else {
            self.normal_background_v_thd_pct
        };
    }

    /// Resets the latched safety relay and restores Phase A's normal impedance.
    pub fn reset_breaker(&mut self) {
        self.is_tripped = false;
        self.impedance_a = self.normal_phase_a_impedance_ohm;
    }

    pub fn check_safety_relay(&self) -> bool {
        self.is_tripped
    }

    /// Samples all three phases, calculates power, and accumulates energy.
    pub fn sample_phases(&mut self, dt_seconds: f64) -> (PhaseReading, PhaseReading, PhaseReading) {
        assert!(dt_seconds >= 0.0, "dt_seconds must not be negative");

        let mut rng = thread_rng();
        let hours_delta = dt_seconds / 3600.0;

        let mut currents = [
            if self.is_tripped {
                0.0
            } else {
                self.nominal_voltage / self.impedance_a + self.i_noise.sample(&mut rng)
            },
            if self.is_tripped {
                0.0
            } else {
                self.nominal_voltage / self.impedance_b + self.i_noise.sample(&mut rng)
            },
            if self.is_tripped {
                0.0
            } else {
                self.nominal_voltage / self.impedance_c + self.i_noise.sample(&mut rng)
            },
        ];
        for current in &mut currents {
            *current = current.max(0.0);
        }

        if !self.is_tripped
            && currents
                .iter()
                .any(|&current| current > self.trip_threshold_amps)
        {
            self.is_tripped = true;
            currents = [0.0; 3];
        }

        let thd: [f64; 3] = currents.map(|current| {
            if self.is_tripped {
                0.0
            } else {
                (self.background_v_thd_pct
                    + (current / self.current_thd_divisor_amps * self.current_thd_multiplier)
                    + self.thd_noise.sample(&mut rng))
                .max(0.0)
            }
        });
        let power_factors: [f64; 3] = std::array::from_fn(|index| {
            if self.is_tripped {
                0.0
            } else {
                (self.base_power_factor[index]
                    - (thd[index] / 100.0 * self.thd_power_factor_penalty)
                    + self.pf_noise.sample(&mut rng))
                .clamp(0.0, 1.0)
            }
        });
        let voltages: [f64; 3] = currents.map(|current| {
            if self.is_tripped {
                self.nominal_voltage
            } else {
                self.nominal_voltage - current * self.grid_impedance + self.v_noise.sample(&mut rng)
            }
        });

        let apparent_power: [f64; 3] =
            std::array::from_fn(|index| voltages[index] * currents[index] / 1000.0);
        let real_power: [f64; 3] =
            std::array::from_fn(|index| apparent_power[index] * power_factors[index]);
        self.energy_active_kwh_a += real_power[0] * hours_delta;
        self.energy_apparent_kvah_a += apparent_power[0] * hours_delta;
        self.energy_active_kwh_b += real_power[1] * hours_delta;
        self.energy_apparent_kvah_b += apparent_power[1] * hours_delta;
        self.energy_active_kwh_c += real_power[2] * hours_delta;
        self.energy_apparent_kvah_c += apparent_power[2] * hours_delta;

        (
            PhaseReading {
                voltage_v: voltages[0],
                current_a: currents[0],
                power_factor: power_factors[0],
                real_power_kw: real_power[0],
                apparent_power_kva: apparent_power[0],
                voltage_thd_pct: thd[0],
                cumulative_active_energy_kwh: self.energy_active_kwh_a,
                cumulative_apparent_energy_kvah: self.energy_apparent_kvah_a,
            },
            PhaseReading {
                voltage_v: voltages[1],
                current_a: currents[1],
                power_factor: power_factors[1],
                real_power_kw: real_power[1],
                apparent_power_kva: apparent_power[1],
                voltage_thd_pct: thd[1],
                cumulative_active_energy_kwh: self.energy_active_kwh_b,
                cumulative_apparent_energy_kvah: self.energy_apparent_kvah_b,
            },
            PhaseReading {
                voltage_v: voltages[2],
                current_a: currents[2],
                power_factor: power_factors[2],
                real_power_kw: real_power[2],
                apparent_power_kva: apparent_power[2],
                voltage_thd_pct: thd[2],
                cumulative_active_energy_kwh: self.energy_active_kwh_c,
                cumulative_apparent_energy_kvah: self.energy_apparent_kvah_c,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::ThreePhaseSensorSimulator;

    #[test]
    fn over_current_trip_latches_and_zeroes_all_phases() {
        let config = crate::config::AppConfig::load(crate::config::sample_config_path()).unwrap();
        let mut sensor = ThreePhaseSensorSimulator::from_config(&config.models.electrical);
        sensor.inject_fault_spike();

        let (phase_a, phase_b, phase_c) = sensor.sample_phases(900.0);

        assert!(sensor.check_safety_relay());
        assert_eq!(phase_a.current_a, 0.0);
        assert_eq!(phase_b.current_a, 0.0);
        assert_eq!(phase_c.current_a, 0.0);
        assert_eq!(phase_a.real_power_kw, 0.0);
    }

    #[test]
    fn energy_accumulates_for_each_phase() {
        let config = crate::config::AppConfig::load(crate::config::sample_config_path()).unwrap();
        let mut sensor = ThreePhaseSensorSimulator::from_config(&config.models.electrical);

        let (phase_a, phase_b, phase_c) = sensor.sample_phases(3600.0);

        assert!(phase_a.cumulative_active_energy_kwh > 0.0);
        assert!(phase_b.cumulative_active_energy_kwh > 0.0);
        assert!(phase_c.cumulative_active_energy_kwh > 0.0);
        assert!(phase_a.cumulative_apparent_energy_kvah > phase_a.cumulative_active_energy_kwh);
    }
}
