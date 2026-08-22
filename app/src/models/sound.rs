use rand::{thread_rng, Rng};
use rand_distr::{Distribution, Normal};

use crate::config::SoundModelConfig;

/// Simulates sound pressure readings in decibels (dB).
///
/// The simulator combines a noisy ambient floor with occasional loud events.
/// Active events decay exponentially over time and are combined with the
/// ambient sound using linear power addition before being converted back to
/// decibels.
pub struct SoundSensorSimulator {
    ambient_floor_db: f64,
    ambient_noise_dist: Normal<f64>,

    // Transient spike behavior parameters.
    current_spike_db: f64,
    spike_probability_per_step: f64,
    decay_rate: f64,
    spike_min_db: f64,
    spike_max_db: f64,
    spike_cutoff_db: f64,
}

impl SoundSensorSimulator {
    /// Creates a sound sensor simulator.
    ///
    /// `ambient_floor_db` and `noise_sigma` are measured in dB. `spike_prob`
    /// must be between 0 and 1, and `decay_rate` controls how quickly a loud
    /// event fades as time advances.
    pub fn from_config(config: &SoundModelConfig) -> Self {
        let ambient_floor_db = config.ambient_floor_db;
        let noise_sigma = config.noise_sigma;
        let spike_prob = config.spike_probability_per_sample;
        let decay_rate = config.decay_rate;
        assert!(noise_sigma > 0.0, "noise_sigma must be positive");
        assert!(
            (0.0..=1.0).contains(&spike_prob),
            "spike_prob must be between 0 and 1"
        );
        assert!(decay_rate >= 0.0, "decay_rate must not be negative");

        Self {
            ambient_floor_db,
            ambient_noise_dist: Normal::new(0.0, noise_sigma).unwrap(),
            current_spike_db: 0.0,
            spike_probability_per_step: spike_prob,
            decay_rate,
            spike_min_db: config.spike_min_db,
            spike_max_db: config.spike_max_db,
            spike_cutoff_db: config.spike_cutoff_db,
        }
    }

    /// Advances the simulator by `dt` and returns the next reading in dB.
    pub fn next_sample(&mut self, dt: f64) -> f64 {
        assert!(dt >= 0.0, "dt must not be negative");

        let mut rng = thread_rng();
        let ambient = self.ambient_floor_db + self.ambient_noise_dist.sample(&mut rng);

        if rng.gen_bool(self.spike_probability_per_step) {
            // A sudden loud event, such as machinery or an impact.
            self.current_spike_db = rng.gen_range(self.spike_min_db..self.spike_max_db);
        } else {
            self.current_spike_db *= (-self.decay_rate * dt).exp();
            if self.current_spike_db < self.spike_cutoff_db {
                self.current_spike_db = 0.0;
            }
        }

        if self.current_spike_db > 0.0 {
            let ambient_power = 10.0_f64.powf(ambient / 10.0);
            let spike_power = 10.0_f64.powf(self.current_spike_db / 10.0);
            10.0 * (ambient_power + spike_power).log10()
        } else {
            ambient
        }
    }
}
