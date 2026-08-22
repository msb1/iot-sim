use rand::thread_rng;
use rand_distr::{Distribution, Normal};

pub struct SensorSimulator {
    // Shared parameters
    target_mean: f64,

    // Model A: Stationary parameters
    stationary_noise: Normal<f64>,

    // Model B: Drifting/Aging parameters
    current_baseline: f64,
    drift_dist: Normal<f64>,
    measurement_noise_dist: Normal<f64>,

    // Model C: Mean-Reverting parameters
    current_ou_value: f64,
    reversion_rate: f64, // theta
    ou_noise_dist: Normal<f64>,
}

impl SensorSimulator {
    pub fn new(target_mean: f64, noise_sigma: f64, drift_sigma: f64, reversion_rate: f64) -> Self {
        Self {
            target_mean,
            stationary_noise: Normal::new(0.0, noise_sigma).unwrap(),

            current_baseline: target_mean,
            drift_dist: Normal::new(0.0, drift_sigma).unwrap(),
            measurement_noise_dist: Normal::new(0.0, noise_sigma).unwrap(),

            current_ou_value: target_mean, // start at the mean
            reversion_rate,
            ou_noise_dist: Normal::new(0.0, noise_sigma).unwrap(),
        }
    }

    // Model A: Constant fluctuation around a target mean
    pub fn next_stationary(&self) -> f64 {
        let mut rng = thread_rng();
        let noise = self.stationary_noise.sample(&mut rng);
        self.target_mean + noise
    }

    // Model B: Permanent calibration drift combined with temporary noise
    pub fn next_drifting(&mut self) -> f64 {
        self.next_drifting_with_dt(1.0)
    }

    /// Model B with a variable time step. Drift scales with sqrt(dt), as it
    /// does for a random-walk process.
    pub fn next_drifting_with_dt(&mut self, dt: f64) -> f64 {
        let baseline = self.next_drifting_baseline(dt);
        let mut rng = thread_rng();
        baseline + self.measurement_noise_dist.sample(&mut rng)
    }

    /// Advances Model B and returns its persistent baseline without adding
    /// measurement noise. This is useful when another model contributes a
    /// deterministic signal, such as a weather cycle.
    pub fn next_drifting_baseline(&mut self, dt: f64) -> f64 {
        assert!(dt >= 0.0, "dt must not be negative");
        let mut rng = thread_rng();
        self.current_baseline += self.drift_dist.sample(&mut rng) * dt.sqrt();
        self.current_baseline
    }

    // Model C: Random walk bounded by a mean-reverting pull
    pub fn next_mean_reverting(&mut self, dt: f64) -> f64 {
        let mut rng = thread_rng();
        let noise = self.ou_noise_dist.sample(&mut rng);

        // Ornstein-Uhlenbeck step formula
        let pull = self.reversion_rate * (self.target_mean - self.current_ou_value) * dt;
        let random_walk_step = noise * dt.sqrt();

        self.current_ou_value += pull + random_walk_step;
        self.current_ou_value
    }
}
