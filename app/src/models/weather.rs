use rand::thread_rng;
use rand_distr::{Distribution, Normal};
use std::f64::consts::PI;

use super::base_models::SensorSimulator;
use crate::config::WeatherModelConfig;

/// The three values produced by a weather station at one point in time.
#[derive(Debug, Clone, Copy)]
pub struct WeatherReading {
    pub temperature_c: f64,
    pub pressure_hpa: f64,
    pub humidity_pct: f64,
}

/// The current phase of a storm transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StormStatus {
    Clear,
    Rising,
    Peak,
    Clearing,
}

struct StormModifier {
    active: bool,
    intensity: f64,
    transition_rate: f64,
}

/// Simulates correlated temperature, pressure, and relative humidity readings.
///
/// The simulator combines slowly drifting weather baselines, daily temperature
/// and pressure cycles, measurement noise, and a smoothly transitioning storm.
pub struct WeatherStationSimulator {
    current_time_hours: f64,
    /// Model B baselines for the two drifting environmental quantities.
    temp_model: SensorSimulator,
    pressure_model: SensorSimulator,
    humidity_baseline: f64,
    temp_noise: Normal<f64>,
    humidity_noise: Normal<f64>,
    pressure_noise: Normal<f64>,
    storm: StormModifier,
    storm_temperature_drop_c: f64,
    storm_pressure_drop_hpa: f64,
    storm_humidity_rise_pct: f64,
    diurnal_temperature_amplitude_c: f64,
    pressure_tide_amplitude_hpa: f64,
    storm_temperature_noise_multiplier: f64,
    storm_humidity_noise_multiplier: f64,
    storm_pressure_noise_multiplier: f64,
    storm_diurnal_amplitude_reduction_c: f64,
    temperature_cycle_phase_hour: f64,
    temperature_humidity_coupling: f64,
    storm_pressure_tide_reduction_hpa: f64,
    pressure_tides_per_day: f64,
}

impl WeatherStationSimulator {
    /// Creates a station using Celsius, hPa, and percent for its units.
    pub fn from_config(config: &WeatherModelConfig) -> Self {
        Self {
            current_time_hours: 0.0,
            temp_model: SensorSimulator::new(
                config.initial_temperature_c,
                config.temperature_measurement_noise_sigma,
                config.temperature_drift_sigma,
                0.0,
            ),
            pressure_model: SensorSimulator::new(
                config.initial_pressure_hpa,
                config.pressure_measurement_noise_sigma,
                config.pressure_drift_sigma,
                0.0,
            ),
            humidity_baseline: config.initial_humidity_pct.clamp(0.0, 100.0),
            temp_noise: Normal::new(0.0, config.temperature_measurement_noise_sigma).unwrap(),
            humidity_noise: Normal::new(0.0, config.humidity_noise_sigma).unwrap(),
            pressure_noise: Normal::new(0.0, config.pressure_measurement_noise_sigma).unwrap(),
            storm: StormModifier {
                active: false,
                intensity: 0.0,
                transition_rate: config.storm_transition_rate,
            },
            storm_temperature_drop_c: config.storm_temperature_drop_c,
            storm_pressure_drop_hpa: config.storm_pressure_drop_hpa,
            storm_humidity_rise_pct: config.storm_humidity_rise_pct,
            diurnal_temperature_amplitude_c: config.diurnal_temperature_amplitude_c,
            pressure_tide_amplitude_hpa: config.pressure_tide_amplitude_hpa,
            storm_temperature_noise_multiplier: config.storm_temperature_noise_multiplier,
            storm_humidity_noise_multiplier: config.storm_humidity_noise_multiplier,
            storm_pressure_noise_multiplier: config.storm_pressure_noise_multiplier,
            storm_diurnal_amplitude_reduction_c: config.storm_diurnal_amplitude_reduction_c,
            temperature_cycle_phase_hour: config.temperature_cycle_phase_hour,
            temperature_humidity_coupling: config.temperature_humidity_coupling,
            storm_pressure_tide_reduction_hpa: config.storm_pressure_tide_reduction_hpa,
            pressure_tides_per_day: config.pressure_tides_per_day,
        }
    }

    /// Starts or stops a storm. The weather transitions over several steps.
    pub fn set_storm(&mut self, active: bool) {
        self.storm.active = active;
    }

    pub fn storm_intensity(&self) -> f64 {
        self.storm.intensity
    }

    pub fn storm_status(&self) -> StormStatus {
        if self.storm.intensity == 0.0 {
            StormStatus::Clear
        } else if self.storm.active && self.storm.intensity == 1.0 {
            StormStatus::Peak
        } else if self.storm.active {
            StormStatus::Rising
        } else {
            StormStatus::Clearing
        }
    }

    pub fn current_time_hours(&self) -> f64 {
        self.current_time_hours
    }

    /// Advances the simulation by `dt_hours` and returns correlated readings.
    pub fn next_step(&mut self, dt_hours: f64) -> WeatherReading {
        assert!(dt_hours >= 0.0, "dt_hours must not be negative");
        let mut rng = thread_rng();
        self.current_time_hours += dt_hours;

        let transition = self.storm.transition_rate * dt_hours;
        if self.storm.active {
            self.storm.intensity = (self.storm.intensity + transition).min(1.0);
        } else {
            self.storm.intensity = (self.storm.intensity - transition).max(0.0);
        }

        let storm_factor = self.storm.intensity;
        // Model B supplies the persistent baseline drift. The weather model
        // then layers cycles, storm effects, and Model A measurement noise on
        // top of those baselines.
        let temp_baseline = self.temp_model.next_drifting_baseline(dt_hours);
        let pressure_baseline = self.pressure_model.next_drifting_baseline(dt_hours);
        let active_temp_baseline = temp_baseline - self.storm_temperature_drop_c * storm_factor;
        let active_pressure_baseline =
            pressure_baseline - self.storm_pressure_drop_hpa * storm_factor;

        let temp_noise = self.temp_noise.sample(&mut rng)
            * (1.0 + self.storm_temperature_noise_multiplier * storm_factor);
        let humidity_noise = self.humidity_noise.sample(&mut rng)
            * (1.0 + self.storm_humidity_noise_multiplier * storm_factor);
        let pressure_noise = self.pressure_noise.sample(&mut rng)
            * (1.0 + self.storm_pressure_noise_multiplier * storm_factor);

        let diurnal_amplitude = self.diurnal_temperature_amplitude_c
            - self.storm_diurnal_amplitude_reduction_c * storm_factor;
        let temp_cycle = diurnal_amplitude
            * ((2.0 * PI / 24.0) * (self.current_time_hours - self.temperature_cycle_phase_hour))
                .sin();
        let temperature_c = active_temp_baseline + temp_cycle + temp_noise;
        let humidity_pct = (self.humidity_baseline + self.storm_humidity_rise_pct * storm_factor
            - (temperature_c - active_temp_baseline) * self.temperature_humidity_coupling
            + humidity_noise)
            .clamp(0.0, 100.0);
        let pressure_tide = (self.pressure_tide_amplitude_hpa
            - self.storm_pressure_tide_reduction_hpa * storm_factor)
            * ((self.pressure_tides_per_day * 2.0 * PI / 24.0) * self.current_time_hours).cos();
        let pressure_hpa = active_pressure_baseline + pressure_tide + pressure_noise;

        WeatherReading {
            temperature_c,
            pressure_hpa,
            humidity_pct,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readings_stay_within_physical_humidity_bounds() {
        let config = crate::config::AppConfig::load(crate::config::sample_config_path()).unwrap();
        let mut station = WeatherStationSimulator::from_config(&config.models.weather);
        for _ in 0..100 {
            assert!((0.0..=100.0).contains(&station.next_step(1.0).humidity_pct));
        }
    }

    #[test]
    fn storm_rises_and_clears_smoothly() {
        let config = crate::config::AppConfig::load(crate::config::sample_config_path()).unwrap();
        let mut station = WeatherStationSimulator::from_config(&config.models.weather);
        station.set_storm(true);
        station.next_step(1.0);
        assert_eq!(station.storm_status(), StormStatus::Rising);
        for _ in 0..3 {
            station.next_step(1.0);
        }
        assert_eq!(station.storm_status(), StormStatus::Peak);
        station.set_storm(false);
        station.next_step(1.0);
        assert_eq!(station.storm_status(), StormStatus::Clearing);
    }
}
