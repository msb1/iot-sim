use std::f64::consts::TAU;
use std::time::Instant;

use rand::rngs::StdRng;
use rand::SeedableRng;
use rand_distr::{Distribution, Normal};

use crate::config::{ScenarioSignalConfig, ScenarioWaveformConfig};
use crate::domain::ScheduledSensor;
use crate::telemetry::SensorReading;

use super::base::{SensorSchedule, SimulatorService};

struct ScenarioSchedule {
    schedule: SensorSchedule,
    config: ScenarioSignalConfig,
    sample_index: u64,
    rng: StdRng,
}

pub struct ScenarioService {
    schedules: Vec<ScenarioSchedule>,
}

impl ScenarioService {
    pub fn new(sensors: Vec<ScheduledSensor>, start: Instant) -> Self {
        let schedules = sensors
            .into_iter()
            .map(|sensor| {
                let config = sensor
                    .sensor
                    .scenario
                    .clone()
                    .expect("scenario_signal configuration was validated");
                let rng = config
                    .seed
                    .map(StdRng::seed_from_u64)
                    .unwrap_or_else(StdRng::from_entropy);
                ScenarioSchedule {
                    schedule: SensorSchedule::new(sensor, start),
                    config,
                    sample_index: 0,
                    rng,
                }
            })
            .collect();
        Self { schedules }
    }
}

impl SimulatorService for ScenarioService {
    fn name(&self) -> &'static str {
        "industrial_scenario_signals"
    }

    fn next_due(&self) -> Option<Instant> {
        self.schedules
            .iter()
            .map(|item| item.schedule.next_due())
            .min()
    }

    fn sample_due(&mut self, now: Instant, timestamp_ms: i64) -> Vec<SensorReading> {
        self.schedules
            .iter_mut()
            .filter(|item| item.schedule.is_due(now))
            .map(|item| {
                let mut value = waveform_value(&item.config, item.sample_index);
                if item.config.noise_sigma > 0.0 {
                    value += Normal::new(0.0, item.config.noise_sigma)
                        .expect("scenario noise was validated")
                        .sample(&mut item.rng);
                }
                item.sample_index += 1;
                item.schedule
                    .emit(now, timestamp_ms, [(item.config.metric.clone(), value)])
            })
            .collect()
    }
}

fn waveform_value(config: &ScenarioSignalConfig, sample_index: u64) -> f64 {
    match &config.waveform {
        ScenarioWaveformConfig::Constant => config.baseline,
        ScenarioWaveformConfig::Sine {
            amplitude,
            period_samples,
            phase_radians,
        } => {
            config.baseline
                + amplitude
                    * (TAU * (sample_index % period_samples) as f64 / *period_samples as f64
                        + phase_radians)
                        .sin()
        }
        ScenarioWaveformConfig::StepCycle {
            values,
            samples_per_step,
        } => {
            let step = (sample_index / samples_per_step) as usize % values.len();
            values[step]
        }
        ScenarioWaveformConfig::BatchTemperature {
            low,
            peak,
            rise_samples,
            plateau_samples,
            cool_samples,
        } => {
            let cycle = rise_samples + plateau_samples + cool_samples;
            let position = sample_index % cycle;
            if position < *rise_samples {
                low + (peak - low) * position as f64 / *rise_samples as f64
            } else if position < rise_samples + plateau_samples {
                *peak
            } else {
                let cooling_position = position - rise_samples - plateau_samples;
                peak - (peak - low) * cooling_position as f64 / *cool_samples as f64
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_profile_rises_plateaus_and_cools() {
        let config = ScenarioSignalConfig {
            metric: "temperature_c".into(),
            baseline: 30.0,
            noise_sigma: 0.0,
            waveform: ScenarioWaveformConfig::BatchTemperature {
                low: 30.0,
                peak: 85.0,
                rise_samples: 2,
                plateau_samples: 2,
                cool_samples: 2,
            },
            seed: Some(1),
        };
        assert_eq!(waveform_value(&config, 0), 30.0);
        assert_eq!(waveform_value(&config, 2), 85.0);
        assert_eq!(waveform_value(&config, 4), 85.0);
        assert_eq!(waveform_value(&config, 5), 57.5);
    }
}
