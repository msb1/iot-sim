use rand::thread_rng;
use rand_distr::{Distribution, Normal};

use crate::config::GasModelConfig;

/// Synchronized environmental safety metrics for sensors 9 through 12.
#[derive(Debug, Clone)]
pub struct GasSafetyTelemetry {
    pub dissolved_oxygen_mg_l: f64,
    pub carbon_monoxide_co_ppm: f64,
    pub combustible_gas_lel_pct: f64,
    pub voc_pid_ppm: f64,
    pub sensor_health_warning: bool,
}

/// Simulates dissolved oxygen, toxic gas, combustible gas, and VOC sensors.
pub struct GasEnvironmentalSafetySimulator {
    pub ambient_temp_c: f64,
    pub ambient_rh_pct: f64,
    true_co_ppm: f64,
    true_lel_pct: f64,
    true_voc_ppm: f64,
    do_membrane_depletion_drift: f64,
    catalytic_bead_poisoning_loss: f64,
    safety_gaussian_noise: Normal<f64>,
    degradation_drift_noise: Normal<f64>,
    baseline_co_ppm: f64,
    baseline_lel_pct: f64,
    baseline_voc_ppm: f64,
    hazard_co_ppm: f64,
    hazard_lel_pct: f64,
    hazard_voc_ppm: f64,
    co_warning_ppm: f64,
    lel_warning_pct: f64,
    humidity_quenching_per_pct: f64,
    humidity_quenching_threshold_pct: f64,
    dissolved_oxygen_saturation_coefficients: [f64; 3],
    dissolved_oxygen_noise_multiplier: f64,
    co_noise_multiplier: f64,
    lel_noise_multiplier: f64,
    voc_noise_multiplier: f64,
    ventilation_poisoning_retention: f64,
}

impl GasEnvironmentalSafetySimulator {
    pub fn from_config(config: &GasModelConfig) -> Self {
        Self {
            ambient_temp_c: config.initial_temperature_c,
            ambient_rh_pct: config.initial_humidity_pct,
            true_co_ppm: config.baseline_co_ppm,
            true_lel_pct: config.baseline_lel_pct,
            true_voc_ppm: config.baseline_voc_ppm,
            do_membrane_depletion_drift: 0.0,
            catalytic_bead_poisoning_loss: 1.0,
            safety_gaussian_noise: Normal::new(0.0, config.safety_noise_sigma).unwrap(),
            degradation_drift_noise: Normal::new(0.0, config.degradation_drift_sigma).unwrap(),
            baseline_co_ppm: config.baseline_co_ppm,
            baseline_lel_pct: config.baseline_lel_pct,
            baseline_voc_ppm: config.baseline_voc_ppm,
            hazard_co_ppm: config.hazard_co_ppm,
            hazard_lel_pct: config.hazard_lel_pct,
            hazard_voc_ppm: config.hazard_voc_ppm,
            co_warning_ppm: config.co_warning_ppm,
            lel_warning_pct: config.lel_warning_pct,
            humidity_quenching_per_pct: config.humidity_quenching_per_pct,
            humidity_quenching_threshold_pct: config.humidity_quenching_threshold_pct,
            dissolved_oxygen_saturation_coefficients: config
                .dissolved_oxygen_saturation_coefficients,
            dissolved_oxygen_noise_multiplier: config.dissolved_oxygen_noise_multiplier,
            co_noise_multiplier: config.co_noise_multiplier,
            lel_noise_multiplier: config.lel_noise_multiplier,
            voc_noise_multiplier: config.voc_noise_multiplier,
            ventilation_poisoning_retention: config.ventilation_poisoning_retention,
        }
    }

    pub fn trigger_hazardous_gas_leak(&mut self) {
        self.true_co_ppm = self.hazard_co_ppm;
        self.true_lel_pct = self.hazard_lel_pct;
        self.true_voc_ppm = self.hazard_voc_ppm;
    }

    pub fn clear_and_ventilate_area(&mut self) {
        self.true_co_ppm = self.baseline_co_ppm;
        self.true_lel_pct = self.baseline_lel_pct;
        self.true_voc_ppm = self.baseline_voc_ppm;
        self.catalytic_bead_poisoning_loss *= self.ventilation_poisoning_retention;
    }

    pub fn sample_safety_block(&mut self, dt_seconds: f64) -> GasSafetyTelemetry {
        assert!(dt_seconds >= 0.0, "dt_seconds must not be negative");
        let mut rng = thread_rng();
        let hours_elapsed = dt_seconds / 3600.0;

        let [constant, linear, quadratic] = self.dissolved_oxygen_saturation_coefficients;
        let physical_do_saturation =
            constant + (linear * self.ambient_temp_c) + (quadratic * self.ambient_temp_c.powi(2));
        self.do_membrane_depletion_drift +=
            self.degradation_drift_noise.sample(&mut rng) * hours_elapsed.sqrt();
        let reported_do = (physical_do_saturation - self.do_membrane_depletion_drift
            + self.safety_gaussian_noise.sample(&mut rng) * self.dissolved_oxygen_noise_multiplier)
            .max(0.0);

        let reported_co = (self.true_co_ppm
            + self.safety_gaussian_noise.sample(&mut rng) * self.co_noise_multiplier)
            .max(0.0);
        let reported_lel = (self.true_lel_pct * self.catalytic_bead_poisoning_loss
            + self.safety_gaussian_noise.sample(&mut rng) * self.lel_noise_multiplier)
            .max(0.0);

        let humidity_quenching_penalty =
            if self.ambient_rh_pct > self.humidity_quenching_threshold_pct {
                1.0 - (self.humidity_quenching_per_pct
                    * (self.ambient_rh_pct - self.humidity_quenching_threshold_pct))
            } else {
                1.0
            };
        let reported_voc = (self.true_voc_ppm * humidity_quenching_penalty
            + self.safety_gaussian_noise.sample(&mut rng) * self.voc_noise_multiplier)
            .max(0.0);

        GasSafetyTelemetry {
            dissolved_oxygen_mg_l: reported_do,
            carbon_monoxide_co_ppm: reported_co,
            combustible_gas_lel_pct: reported_lel,
            voc_pid_ppm: reported_voc,
            sensor_health_warning: reported_co > self.co_warning_ppm
                || reported_lel > self.lel_warning_pct,
        }
    }
}
