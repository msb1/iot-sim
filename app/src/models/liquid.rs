use rand::thread_rng;
use rand_distr::{Distribution, Normal};
use std::f64::consts::PI;

use crate::config::HydraulicsModelConfig;

/// Unified structural payload containing the synchronized fluid hydraulic metrics.
#[derive(Debug, Clone)]
pub struct HydraulicsTelemetry {
    pub tank_level_pct: f64,
    pub fluid_volume_liters: f64,
    pub mass_flow_rate_l_min: f64,
    pub load_cell_weight_kg: f64,
    pub mixer_active: bool,
}

/// Simulates a coupled industrial tank loop where flow drives level and volume drives weight.
pub struct FluidHydraulicsSimulator {
    current_time_ticks: f64,
    max_capacity_liters: f64,
    current_volume_liters: f64,
    fluid_density_kg_l: f64,
    tank_tare_weight_kg: f64,
    base_inflow_l_min: f64,
    current_outflow_l_min: f64,
    normal_outflow_l_min: f64,
    high_discharge_l_min: f64,
    is_mixer_running: bool,
    mixer_rpm: f64,
    mixer_harmonic_amplitude_kg: f64,
    level_sensor_noise: Normal<f64>,
    flow_sensor_noise: Normal<f64>,
    load_cell_noise: Normal<f64>,
}

impl FluidHydraulicsSimulator {
    pub fn from_config(config: &HydraulicsModelConfig) -> Self {
        let initial_volume = config.max_capacity_liters * (config.initial_fill_pct / 100.0);
        Self {
            current_time_ticks: 0.0,
            max_capacity_liters: config.max_capacity_liters,
            current_volume_liters: initial_volume,
            fluid_density_kg_l: config.fluid_density_kg_l,
            tank_tare_weight_kg: config.tank_tare_weight_kg,
            base_inflow_l_min: config.base_inflow_l_min,
            current_outflow_l_min: config.base_outflow_l_min,
            normal_outflow_l_min: config.base_outflow_l_min,
            high_discharge_l_min: config.high_discharge_l_min,
            is_mixer_running: false,
            mixer_rpm: config.mixer_rpm,
            mixer_harmonic_amplitude_kg: config.mixer_harmonic_amplitude_kg,
            level_sensor_noise: Normal::new(0.0, config.level_noise_sigma).unwrap(),
            flow_sensor_noise: Normal::new(0.0, config.flow_noise_sigma).unwrap(),
            load_cell_noise: Normal::new(0.0, config.load_cell_noise_sigma).unwrap(),
        }
    }

    /// Simulates a downstream discharge valve opening wide.
    pub fn trigger_high_discharge_draw(&mut self) {
        self.current_outflow_l_min = self.high_discharge_l_min;
    }

    pub fn clear_high_discharge_draw(&mut self) {
        self.current_outflow_l_min = self.normal_outflow_l_min;
    }

    /// Toggles the industrial mechanical mixing motor.
    pub fn set_mixer_state(&mut self, active: bool) {
        self.is_tripped_if_applicable();
        self.is_mixer_running = active;
    }

    /// Advances the mass-conservation model and returns synchronized sensor telemetry.
    pub fn sample_hydraulics(&mut self, dt_seconds: f64) -> HydraulicsTelemetry {
        let mut rng = thread_rng();
        self.current_time_ticks += dt_seconds;

        let net_flow_l_min = self.base_inflow_l_min - self.current_outflow_l_min;
        let flow_delta_liters = (net_flow_l_min / 60.0) * dt_seconds;
        self.current_volume_liters =
            (self.current_volume_liters + flow_delta_liters).clamp(0.0, self.max_capacity_liters);

        let actual_level_pct = (self.current_volume_liters / self.max_capacity_liters) * 100.0;
        let reported_level_pct =
            (actual_level_pct + self.level_sensor_noise.sample(&mut rng)).clamp(0.0, 100.0);
        let reported_flow =
            (self.current_outflow_l_min + self.flow_sensor_noise.sample(&mut rng)).max(0.0);

        let liquid_mass_kg = self.current_volume_liters * self.fluid_density_kg_l;
        let base_static_weight = self.tank_tare_weight_kg + liquid_mass_kg;
        let harmonic_shake = if self.is_mixer_running {
            let frequency_hz = self.mixer_rpm / 60.0;
            let angular_velocity = 2.0 * PI * frequency_hz;
            self.mixer_harmonic_amplitude_kg * (angular_velocity * self.current_time_ticks).sin()
        } else {
            0.0
        };
        let reported_weight =
            base_static_weight + harmonic_shake + self.load_cell_noise.sample(&mut rng);

        HydraulicsTelemetry {
            tank_level_pct: reported_level_pct,
            fluid_volume_liters: self.current_volume_liters,
            mass_flow_rate_l_min: reported_flow,
            load_cell_weight_kg: reported_weight,
            mixer_active: self.is_mixer_running,
        }
    }

    fn is_tripped_if_applicable(&self) {}
}
