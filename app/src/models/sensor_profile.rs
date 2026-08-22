/// Encapsulates the 3 core simulator behaviors as distinct profiles.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SensorProfile {
    /// Model A: Stationary behavior around a target mean
    Stationary { mean: f64, noise: f64 },
    /// Model B: Drifting behavior with accumulating baseline drift
    Drifting {
        baseline: f64,
        drift: f64,
        noise: f64,
    },
    /// Model C: Mean-reverting Ornstein-Uhlenbeck behavior
    MeanReverting {
        value: f64,
        mean: f64,
        rate: f64,
        noise: f64,
    },
}

impl SensorProfile {
    /// Creates a stationary profile with the given mean and noise level.
    pub fn stationary(mean: f64, noise: f64) -> Self {
        Self::Stationary { mean, noise }
    }

    /// Creates a drifting profile starting from the given baseline.
    pub fn drifting(baseline: f64, drift: f64, noise: f64) -> Self {
        Self::Drifting {
            baseline,
            drift,
            noise,
        }
    }

    /// Creates a mean-reverting profile starting from the given value.
    pub fn mean_reverting(value: f64, mean: f64, rate: f64, noise: f64) -> Self {
        Self::MeanReverting {
            value,
            mean,
            rate,
            noise,
        }
    }
}
