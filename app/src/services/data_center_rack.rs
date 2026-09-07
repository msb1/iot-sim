use std::time::{Duration, Instant};

use chrono::{Datelike, Timelike};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Normal};

use crate::config::DataCenterRackConfig;
use crate::domain::ScheduledSensor;
use crate::telemetry::SensorReading;

use super::base::{SensorSchedule, SimulatorService};

const MINUTES_PER_DAY: u64 = 24 * 60;
const SAMPLES_PER_DAY: u64 = 96;
const SAMPLES_PER_WEEK: u64 = 7 * SAMPLES_PER_DAY;
const MINUTES_PER_SAMPLE: u64 = MINUTES_PER_DAY / SAMPLES_PER_DAY;

struct RackSchedule {
    schedule: SensorSchedule,
    config: DataCenterRackConfig,
    sample_index: u64,
    temperature_f: f64,
    week_started_at: Instant,
    next_due: Instant,
    rng: StdRng,
    real_calendar: bool,
}

/// Simulates correlated server-rack workload and environmental telemetry.
/// A week begins on Monday and repeats every 672 fifteen-minute samples.
pub struct DataCenterRackService {
    racks: Vec<RackSchedule>,
}

impl DataCenterRackService {
    pub fn new(sensors: Vec<ScheduledSensor>, start: Instant, real_calendar: bool) -> Self {
        let racks = sensors
            .into_iter()
            .map(|sensor| {
                let config = sensor
                    .sensor
                    .data_center_rack
                    .clone()
                    .expect("data_center_rack configuration was validated");
                let mut config = config;
                if real_calendar {
                    // Dataset mode maps one logical rack week to an actual
                    // calendar week instead of the streaming test acceleration.
                    config.simulated_week_duration_seconds = 7.0 * 24.0 * 60.0 * 60.0;
                }
                let temperature_f = config.ambient_temperature_f;
                let rng = config
                    .seed
                    .map(StdRng::seed_from_u64)
                    .unwrap_or_else(StdRng::from_entropy);
                RackSchedule {
                    schedule: SensorSchedule::new(sensor, start),
                    config,
                    sample_index: 0,
                    temperature_f,
                    week_started_at: start,
                    next_due: start,
                    rng,
                    real_calendar,
                }
            })
            .collect();
        Self { racks }
    }
}

impl SimulatorService for DataCenterRackService {
    fn name(&self) -> &'static str {
        "data_center_rack"
    }

    fn next_due(&self) -> Option<Instant> {
        self.racks.iter().map(|rack| rack.next_due).min()
    }

    fn sample_due(&mut self, now: Instant, timestamp_ms: i64) -> Vec<SensorReading> {
        let mut readings = Vec::new();
        for rack in self.racks.iter_mut().filter(|rack| rack.next_due <= now) {
            let values = rack_values(rack, timestamp_ms);
            // Report the configured cadence rather than scheduler jitter. All
            // rack metrics in a reading share this same accelerated interval.
            let interval_ms = nominal_interval_ms(&rack.config);
            let mut reading = rack.schedule.emit(now, timestamp_ms, values);
            reading.interval_ms = interval_ms;
            advance_schedule(rack);
            // Do not catch up by creating a burst with duplicate wall-clock
            // timestamps. If the scheduler is delayed, resume from the next
            // real interval instead.
            if rack.next_due <= now {
                rack.next_due = now + sample_period(&rack.config);
            }
            readings.push(reading);
        }
        readings
    }
}

fn nominal_interval_ms(config: &DataCenterRackConfig) -> i64 {
    (config.simulated_week_duration_seconds * 1_000.0 / SAMPLES_PER_WEEK as f64)
        .round()
        .max(1.0) as i64
}

fn sample_period(config: &DataCenterRackConfig) -> Duration {
    Duration::from_secs_f64(config.simulated_week_duration_seconds / SAMPLES_PER_WEEK as f64)
}

fn advance_schedule(rack: &mut RackSchedule) {
    rack.sample_index += 1;
    let week_duration = Duration::from_secs_f64(rack.config.simulated_week_duration_seconds);
    if rack.sample_index == SAMPLES_PER_WEEK {
        rack.sample_index = 0;
        rack.week_started_at += week_duration;
    }
    let elapsed = week_duration.mul_f64(rack.sample_index as f64 / SAMPLES_PER_WEEK as f64);
    rack.next_due = rack.week_started_at + elapsed;
}

fn rack_values(rack: &mut RackSchedule, timestamp_ms: i64) -> [(String, f64); 4] {
    let (day_of_week, minute_of_day) = if rack.real_calendar {
        let timestamp = chrono::DateTime::from_timestamp_millis(timestamp_ms)
            .expect("dataset timestamp is representable");
        (
            timestamp.weekday().number_from_monday() as u64,
            (timestamp.hour() as u64) * 60 + timestamp.minute() as u64,
        )
    } else {
        let minute_of_week = (rack.sample_index % SAMPLES_PER_WEEK) * MINUTES_PER_SAMPLE;
        (
            minute_of_week / MINUTES_PER_DAY + 1,
            minute_of_week % MINUTES_PER_DAY,
        )
    };
    let is_weekend = day_of_week >= 6;
    let anomaly = day_of_week == 7 && minute_of_day >= 8 * 60;

    let cpu_utilization = if anomaly {
        0.10
    } else {
        normal_cpu(is_weekend, minute_of_day, &rack.config, &mut rack.rng)
    };

    let temperature_f = if anomaly {
        rack.config.anomaly_temperature_f
    } else {
        let target =
            rack.config.ambient_temperature_f + rack.config.max_thermal_lift_f * cpu_utilization;
        let cooling_factor =
            1.0 - (-(MINUTES_PER_SAMPLE as f64) / rack.config.cooling_time_constant_minutes).exp();
        rack.temperature_f += (target - rack.temperature_f) * cooling_factor;
        rack.temperature_f + gaussian_noise(rack.config.temperature_noise_sigma_f, &mut rack.rng)
    };
    rack.temperature_f = temperature_f;

    let relative_humidity_pct = if anomaly {
        rack.config.anomaly_humidity_pct
    } else {
        relative_humidity(temperature_f, rack.config.managed_dew_point_f)
    };

    [
        ("cpu_utilization".into(), cpu_utilization),
        ("temperature_f".into(), temperature_f),
        ("relative_humidity_pct".into(), relative_humidity_pct),
        ("day_of_week".into(), day_of_week as f64),
    ]
}

fn normal_cpu(
    is_weekend: bool,
    minute_of_day: u64,
    config: &DataCenterRackConfig,
    rng: &mut StdRng,
) -> f64 {
    let base = if is_weekend {
        if rng.gen_bool(config.weekend_spike_probability) {
            rng.gen_range(0.10..=0.15)
        } else {
            0.10
        }
    } else if (8 * 60..18 * 60).contains(&minute_of_day) {
        0.825
    } else {
        0.25
    };
    (base + gaussian_noise(config.cpu_noise_sigma, rng)).clamp(0.05, 0.90)
}

fn gaussian_noise(sigma: f64, rng: &mut StdRng) -> f64 {
    if sigma == 0.0 {
        0.0
    } else {
        Normal::new(0.0, sigma)
            .expect("validated sigma")
            .sample(rng)
    }
}

/// August-Roche-Magnus saturation-vapor-pressure ratio at a fixed dew point.
fn relative_humidity(temperature_f: f64, dew_point_f: f64) -> f64 {
    let to_celsius = |f: f64| (f - 32.0) * 5.0 / 9.0;
    let saturation_pressure = |c: f64| 6.1094 * (17.625 * c / (243.04 + c)).exp();
    (100.0 * saturation_pressure(to_celsius(dew_point_f))
        / saturation_pressure(to_celsius(temperature_f)))
    .clamp(0.0, 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> DataCenterRackConfig {
        DataCenterRackConfig {
            ambient_temperature_f: 68.0,
            max_thermal_lift_f: 24.0,
            cooling_time_constant_minutes: 20.0,
            managed_dew_point_f: 48.8,
            temperature_noise_sigma_f: 0.0,
            cpu_noise_sigma: 0.0,
            weekend_spike_probability: 0.0,
            anomaly_temperature_f: 85.0,
            anomaly_humidity_pct: 22.0,
            simulated_week_duration_seconds: 70.0,
            seed: Some(1),
        }
    }

    #[test]
    fn sunday_morning_is_a_collective_anomaly() {
        let start = Instant::now();
        let sensor = ScheduledSensor {
            entity_type: "rack".into(),
            entity_id: "rack-1".into(),
            sensor: crate::domain::IoTSensor {
                sensor_id: "rack-1".into(),
                sensor_type: crate::config::SensorType::DataCenterRack,
                timestep: std::time::Duration::from_secs(60),
                min_value: 0.0,
                max_value: 100.0,
                scenario: None,
                data_center_rack: Some(config()),
            },
        };
        let mut rack = RackSchedule {
            schedule: SensorSchedule::new(sensor, start),
            config: config(),
            sample_index: 6 * SAMPLES_PER_DAY + 8 * 60 / MINUTES_PER_SAMPLE,
            temperature_f: 68.0,
            week_started_at: start,
            next_due: start,
            rng: StdRng::seed_from_u64(1),
            real_calendar: false,
        };
        let values = rack_values(&mut rack, 0);
        assert_eq!(values[0].1, 0.10);
        assert_eq!(values[1].1, 85.0);
        assert_eq!(values[2].1, 22.0);
        assert_eq!(values[3].1, 7.0);
    }

    #[test]
    fn normal_humidity_drops_as_temperature_rises() {
        assert!(relative_humidity(88.0, 48.8) < relative_humidity(68.0, 48.8));
    }

    #[test]
    fn accelerated_schedule_contains_exactly_96_samples_per_day() {
        let start = Instant::now();
        let sensor = ScheduledSensor {
            entity_type: "rack".into(),
            entity_id: "rack-1".into(),
            sensor: crate::domain::IoTSensor {
                sensor_id: "rack-1".into(),
                sensor_type: crate::config::SensorType::DataCenterRack,
                timestep: Duration::from_millis(104),
                min_value: 0.0,
                max_value: 100.0,
                scenario: None,
                data_center_rack: Some(config()),
            },
        };
        let mut rack = RackSchedule {
            schedule: SensorSchedule::new(sensor, start),
            config: config(),
            sample_index: 0,
            temperature_f: 68.0,
            week_started_at: start,
            next_due: start,
            rng: StdRng::seed_from_u64(1),
            real_calendar: false,
        };
        for _ in 0..SAMPLES_PER_DAY {
            advance_schedule(&mut rack);
        }
        assert_eq!(rack.sample_index, SAMPLES_PER_DAY);
        assert_eq!(rack_values(&mut rack, 0)[3].1, 2.0);
    }

    #[test]
    fn day_of_week_changes_every_96_samples() {
        let start = Instant::now();
        let mut rack = RackSchedule {
            schedule: SensorSchedule::new(test_sensor(), start),
            config: config(),
            sample_index: SAMPLES_PER_DAY - 1,
            temperature_f: 68.0,
            week_started_at: start,
            next_due: start,
            rng: StdRng::seed_from_u64(1),
            real_calendar: false,
        };

        assert_eq!(rack_values(&mut rack, 0)[3].1, 1.0);
        advance_schedule(&mut rack);
        assert_eq!(rack_values(&mut rack, 0)[3].1, 2.0);
    }

    fn test_sensor() -> ScheduledSensor {
        ScheduledSensor {
            entity_type: "rack".into(),
            entity_id: "rack-1".into(),
            sensor: crate::domain::IoTSensor {
                sensor_id: "rack-1".into(),
                sensor_type: crate::config::SensorType::DataCenterRack,
                timestep: Duration::from_millis(104),
                min_value: 0.0,
                max_value: 100.0,
                scenario: None,
                data_center_rack: Some(config()),
            },
        }
    }
}
