use rand::thread_rng;
use rand_distr::{Distribution, Normal};

use crate::config::ProcessModelConfig;
use crate::models::base_models::SensorSimulator;

/// Represents the spectroscopic fingerprint of a specific molecule across
/// four infrared bands.
#[derive(Debug, Clone, Copy)]
pub struct MolarExtinctionProfile {
    pub lambda_1: f64,
    pub lambda_2: f64,
    pub lambda_3: f64,
    pub lambda_4: f64,
}

/// Holds the synchronized output of the concentration and FTIR sensors.
#[derive(Debug, Clone)]
pub struct ProcessAnalyticsReading {
    pub concentration_mol_l: f64,
    pub ftir_absorbance_array: [f64; 4],
    pub optical_path_length_cm: f64,
}

/// Simulates the chemical concentration loop and its coupled FTIR array.
pub struct ProcessAnalyticsSimulator {
    current_concentration: f64,
    target_equilibrium: f64,
    reversion_rate: f64,
    path_length_cm: f64,
    chemical_fingerprint: MolarExtinctionProfile,
    // Model B: each optical channel keeps its own persistent calibration /
    // fouling drift and adds measurement noise to the reading.
    optical_channels: [SensorSimulator; 4],
    concentration_noise: Normal<f64>,
}

impl ProcessAnalyticsSimulator {
    pub fn from_config(config: &ProcessModelConfig) -> Self {
        Self {
            current_concentration: config.initial_concentration_mol_l,
            target_equilibrium: config.target_equilibrium_mol_l,
            reversion_rate: config.reversion_rate,
            path_length_cm: config.optical_path_length_cm,
            chemical_fingerprint: MolarExtinctionProfile {
                lambda_1: config.molar_extinction[0],
                lambda_2: config.molar_extinction[1],
                lambda_3: config.molar_extinction[2],
                lambda_4: config.molar_extinction[3],
            },
            optical_channels: std::array::from_fn(|_| {
                // Model B parameters: persistent drift plus photodiode noise.
                SensorSimulator::new(
                    0.0,
                    config.optical_noise_sigma,
                    config.optical_drift_sigma,
                    0.0,
                )
            }),
            concentration_noise: Normal::new(0.0, config.concentration_noise_sigma).unwrap(),
        }
    }

    /// Injects a concentrated batch of reactant into the process tank.
    pub fn inject_reactant_spike(&mut self, amount_mol_l: f64) {
        self.current_concentration = (self.current_concentration + amount_mol_l).max(0.0);
    }

    /// Advances the process and returns one synchronized S7/S8 reading.
    pub fn sample_analytics(&mut self, dt_seconds: f64) -> ProcessAnalyticsReading {
        assert!(dt_seconds >= 0.0, "dt_seconds must not be negative");
        let mut rng = thread_rng();

        let pull = self.reversion_rate
            * (self.target_equilibrium - self.current_concentration)
            * dt_seconds;
        let fluctuation = self.concentration_noise.sample(&mut rng) * dt_seconds.sqrt();
        self.current_concentration = (self.current_concentration + pull + fluctuation).max(0.0);

        let concentration = self.current_concentration;
        let path_length = self.path_length_cm;
        let extinction = [
            self.chemical_fingerprint.lambda_1,
            self.chemical_fingerprint.lambda_2,
            self.chemical_fingerprint.lambda_3,
            self.chemical_fingerprint.lambda_4,
        ];

        let absorbance = std::array::from_fn(|index| {
            // Beer–Lambert signal plus Model B optical drift/noise.
            (extinction[index] * concentration * path_length
                + self.optical_channels[index].next_drifting_with_dt(dt_seconds))
            .max(0.0)
        });

        ProcessAnalyticsReading {
            concentration_mol_l: concentration,
            ftir_absorbance_array: absorbance,
            optical_path_length_cm: path_length,
        }
    }
}
