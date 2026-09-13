use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use serde::Serialize;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Normal};

use crate::config::{
    AnomaliesConfig, AnomalyConditionConfig, AnomalyProfileConfig, AnomalyStrategyConfig,
    AnomalyTargetConfig, AnomalyTriggerConfig, ComparisonOperator,
};
use crate::telemetry::SensorReading;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MetricKey {
    sensor_id: String,
    metric: String,
}

impl From<&AnomalyTargetConfig> for MetricKey {
    fn from(target: &AnomalyTargetConfig) -> Self {
        Self {
            sensor_id: target.sensor_id.clone(),
            metric: target.metric.clone(),
        }
    }
}

/// Read-only baseline values available to contextual anomaly strategies.
/// `current` includes every reading in the current scheduler batch, while
/// `previous` contains the most recent value from an earlier batch.
pub struct InjectionContext<'a> {
    current: &'a HashMap<MetricKey, f64>,
    previous: &'a HashMap<MetricKey, f64>,
}

impl InjectionContext<'_> {
    fn current_value(&self, target: &AnomalyTargetConfig) -> Option<f64> {
        self.current.get(&MetricKey::from(target)).copied()
    }

    fn previous_value(&self, target: &AnomalyTargetConfig) -> Option<f64> {
        self.previous.get(&MetricKey::from(target)).copied()
    }
}

/// Stateful behavior implemented by each anomaly type. Activation probability,
/// cooldowns, and target matching are deliberately owned by `AnomalyInjector`
/// so every strategy follows the same event lifecycle.
pub trait AnomalyStrategy: Send {
    fn duration_samples(&self) -> u64;

    fn can_start(&self, _context: &InjectionContext<'_>) -> bool {
        true
    }

    fn start(&mut self, _baseline: f64, _previous: Option<f64>) {}

    fn inject(&mut self, value: &mut f64, sample_index: u64, rng: &mut StdRng);
}

pub struct SpikeAnomaly {
    offset: f64,
    multiplier: f64,
}

/// Two-sample impulse used for water-hammer signatures: an extreme positive
/// pressure sample followed by the immediate pressure collapse.
pub struct SpikeDropAnomaly {
    spike_value: f64,
    drop_value: f64,
}

impl AnomalyStrategy for SpikeDropAnomaly {
    fn duration_samples(&self) -> u64 {
        2
    }

    fn inject(&mut self, value: &mut f64, sample_index: u64, _rng: &mut StdRng) {
        *value = if sample_index == 0 {
            self.spike_value
        } else {
            self.drop_value
        };
    }
}

impl AnomalyStrategy for SpikeAnomaly {
    fn duration_samples(&self) -> u64 {
        1
    }

    fn inject(&mut self, value: &mut f64, _sample_index: u64, _rng: &mut StdRng) {
        *value = *value * self.multiplier + self.offset;
    }
}

pub struct StuckAtAnomaly {
    duration_samples: u64,
    configured_value: Option<f64>,
    frozen_value: f64,
}

impl AnomalyStrategy for StuckAtAnomaly {
    fn duration_samples(&self) -> u64 {
        self.duration_samples
    }

    fn start(&mut self, baseline: f64, previous: Option<f64>) {
        self.frozen_value = self.configured_value.or(previous).unwrap_or(baseline);
    }

    fn inject(&mut self, value: &mut f64, _sample_index: u64, _rng: &mut StdRng) {
        *value = self.frozen_value;
    }
}

pub struct CalibrationDriftAnomaly {
    duration_samples: u64,
    total_offset: f64,
}

pub struct BatchCoolingStretchAnomaly {
    duration_samples: u64,
    low: f64,
    peak: f64,
    rise_samples: u64,
    plateau_samples: u64,
    initial_cool_samples: u64,
    final_cool_samples: u64,
    batches: u64,
}

impl AnomalyStrategy for BatchCoolingStretchAnomaly {
    fn duration_samples(&self) -> u64 {
        self.duration_samples
    }

    fn inject(&mut self, value: &mut f64, sample_index: u64, _rng: &mut StdRng) {
        let mut position = sample_index;
        let mut batch = 0;
        while batch + 1 < self.batches {
            let cycle = self.rise_samples + self.plateau_samples + self.cool_samples(batch);
            if position < cycle {
                break;
            }
            position -= cycle;
            batch += 1;
        }
        let cool_samples = self.cool_samples(batch);
        *value = if position < self.rise_samples {
            self.low + (self.peak - self.low) * position as f64 / self.rise_samples as f64
        } else if position < self.rise_samples + self.plateau_samples {
            self.peak
        } else {
            let cooling_position = position - self.rise_samples - self.plateau_samples;
            self.peak
                - (self.peak - self.low)
                    * (cooling_position.min(cool_samples) as f64 / cool_samples as f64)
        };
    }
}

impl BatchCoolingStretchAnomaly {
    fn cool_samples(&self, batch: u64) -> u64 {
        let span = self.final_cool_samples - self.initial_cool_samples;
        self.initial_cool_samples + (span * batch + (self.batches - 1) / 2) / (self.batches - 1)
    }
}

impl AnomalyStrategy for CalibrationDriftAnomaly {
    fn duration_samples(&self) -> u64 {
        self.duration_samples
    }

    fn inject(&mut self, value: &mut f64, sample_index: u64, _rng: &mut StdRng) {
        let progress = if self.duration_samples == 1 {
            1.0
        } else {
            sample_index as f64 / (self.duration_samples - 1) as f64
        };
        *value += self.total_offset * progress;
    }
}

pub struct HighFrequencyNoiseAnomaly {
    duration_samples: u64,
    distribution: Normal<f64>,
}

impl AnomalyStrategy for HighFrequencyNoiseAnomaly {
    fn duration_samples(&self) -> u64 {
        self.duration_samples
    }

    fn inject(&mut self, value: &mut f64, _sample_index: u64, rng: &mut StdRng) {
        *value += self.distribution.sample(rng);
    }
}

pub struct ContextualAnomaly {
    duration_samples: u64,
    condition: AnomalyConditionConfig,
    value: Option<f64>,
    offset: f64,
    multiplier: f64,
}

impl AnomalyStrategy for ContextualAnomaly {
    fn duration_samples(&self) -> u64 {
        self.duration_samples
    }

    fn can_start(&self, context: &InjectionContext<'_>) -> bool {
        context
            .current_value(&self.condition.source)
            .or_else(|| context.previous_value(&self.condition.source))
            .is_some_and(|actual| compare(actual, self.condition.operator, self.condition.value))
    }

    fn inject(&mut self, value: &mut f64, _sample_index: u64, _rng: &mut StdRng) {
        *value = self.value.unwrap_or(*value) * self.multiplier + self.offset;
    }
}

fn compare(actual: f64, operator: ComparisonOperator, expected: f64) -> bool {
    match operator {
        ComparisonOperator::GreaterThan => actual > expected,
        ComparisonOperator::GreaterThanOrEqual => actual >= expected,
        ComparisonOperator::LessThan => actual < expected,
        ComparisonOperator::LessThanOrEqual => actual <= expected,
        ComparisonOperator::Equal => actual == expected,
        ComparisonOperator::NotEqual => actual != expected,
    }
}

/// Read-only model-level anomaly state for one robotic-air-lock production
/// cycle. It is resolved before the physical model generates a frame.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct RoboticAirLockAnomalyState {
    pub seal_faulted: bool,
    pub maintenance_offline: bool,
    pub wet_payload_recovery_multiplier: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AnomalyEvent {
    pub timestamp_ms: i64,
    pub anomaly_type: String,
    pub profile: String,
    pub sensor_id: String,
    pub cycle: Option<u64>,
}

#[derive(Debug, Default)]
pub struct AnomalyEventCollector {
    events: Mutex<Vec<AnomalyEvent>>,
}

impl AnomalyEventCollector {
    pub fn record(&self, event: AnomalyEvent) {
        self.events.lock().expect("anomaly event collector poisoned").push(event);
    }

    pub fn snapshot(&self) -> Vec<AnomalyEvent> {
        self.events.lock().expect("anomaly event collector poisoned").clone()
    }
}

enum RoboticAirLockProfile {
    SealFailure {
        name: String,
        sensor_id: String,
        trigger: AirLockCycleTrigger,
        faulted_cycles_before_maintenance: u64,
        maintenance_cycles: u64,
        active_start_cycle: Option<u64>,
    },
    WetPayload {
        name: String,
        sensor_id: String,
        trigger: AirLockCycleTrigger,
        duration_cycles: u64,
        recovery_multiplier: f64,
        active_start_cycle: Option<u64>,
    },
}

#[derive(Clone, Copy)]
struct AirLockCycleTrigger {
    threshold: f64,
    start_after_cycles: u64,
    last_evaluated_cycle: u64,
}

/// Global model-level anomaly registry. Unlike `AnomalyInjector`, this does
/// not alter exported readings after sampling: correlated simulators query it
/// before computing their coupled physical state.
pub struct ModelAnomalyController {
    state: Mutex<ModelAnomalyState>,
    events: Arc<AnomalyEventCollector>,
}

struct ModelAnomalyState {
    robotic_air_lock_profiles: Vec<RoboticAirLockProfile>,
    rng: StdRng,
}

impl ModelAnomalyController {
    pub fn from_config(config: &AnomaliesConfig) -> Self {
        Self::from_config_with_events(config, Arc::new(AnomalyEventCollector::default()))
    }

    pub fn from_config_with_events(config: &AnomaliesConfig, events: Arc<AnomalyEventCollector>) -> Self {
        let robotic_air_lock_profiles = if config.enabled {
            config
                .profiles
                .iter()
                .filter(|profile| profile.enabled)
                .filter_map(|profile| match &profile.strategy {
                    AnomalyStrategyConfig::RoboticAirLockSealFailure {
                        faulted_cycles_before_maintenance,
                        maintenance_cycles,
                    } => Some(RoboticAirLockProfile::SealFailure {
                        name: profile.name.clone(),
                        sensor_id: profile.target.sensor_id.clone(),
                        trigger: AirLockCycleTrigger {
                            threshold: profile.trigger.threshold,
                            start_after_cycles: profile.trigger.start_after_cycles,
                            last_evaluated_cycle: 0,
                        },
                        faulted_cycles_before_maintenance: *faulted_cycles_before_maintenance,
                        maintenance_cycles: *maintenance_cycles,
                        active_start_cycle: None,
                    }),
                    AnomalyStrategyConfig::RoboticAirLockWetPayload {
                        duration_cycles,
                        recovery_multiplier,
                    } => Some(RoboticAirLockProfile::WetPayload {
                        name: profile.name.clone(),
                        sensor_id: profile.target.sensor_id.clone(),
                        trigger: AirLockCycleTrigger {
                            threshold: profile.trigger.threshold,
                            start_after_cycles: profile.trigger.start_after_cycles,
                            last_evaluated_cycle: 0,
                        },
                        duration_cycles: *duration_cycles,
                        recovery_multiplier: *recovery_multiplier,
                        active_start_cycle: None,
                    }),
                    _ => None,
                })
                .collect()
        } else {
            Vec::new()
        };
        Self {
            state: Mutex::new(ModelAnomalyState {
                robotic_air_lock_profiles,
                rng: config
                    .seed
                    .map(StdRng::seed_from_u64)
                    .unwrap_or_else(StdRng::from_entropy),
            }),
            events,
        }
    }

    pub fn active_profile_count(&self) -> usize {
        self.state
            .lock()
            .expect("model anomaly controller mutex was poisoned")
            .robotic_air_lock_profiles
            .len()
    }

    pub fn robotic_air_lock_state(
        &self,
        sensor_id: &str,
        cycle: u64,
        timestamp_ms: i64,
    ) -> RoboticAirLockAnomalyState {
        let mut state = RoboticAirLockAnomalyState::default();
        let mut controller = self
            .state
            .lock()
            .expect("model anomaly controller mutex was poisoned");
        let ModelAnomalyState {
            robotic_air_lock_profiles,
            rng,
        } = &mut *controller;
        for profile in robotic_air_lock_profiles {
            match profile {
                RoboticAirLockProfile::SealFailure {
                    name,
                    sensor_id: target_sensor,
                    trigger,
                    faulted_cycles_before_maintenance,
                    maintenance_cycles,
                    active_start_cycle,
                } if target_sensor == sensor_id => {
                    if activate_for_cycle(trigger, active_start_cycle, cycle, rng) {
                        self.events.record(AnomalyEvent { timestamp_ms, anomaly_type: "robotic_air_lock_seal_failure".into(), profile: name.clone(), sensor_id: target_sensor.clone(), cycle: Some(cycle) });
                    }
                    let Some(start_cycle) = *active_start_cycle else {
                        continue;
                    };
                    let fault_end = start_cycle.saturating_add(*faulted_cycles_before_maintenance);
                    let maintenance_end = fault_end.saturating_add(*maintenance_cycles);
                    if (start_cycle..fault_end).contains(&cycle) {
                        tracing::debug!(anomaly = %name, sensor_id, cycle, "robotic air-lock seal fault active");
                        state.seal_faulted = true;
                    } else if (fault_end..maintenance_end).contains(&cycle) {
                        tracing::debug!(anomaly = %name, sensor_id, cycle, "robotic air-lock maintenance active");
                        state.maintenance_offline = true;
                    } else if cycle >= maintenance_end {
                        *active_start_cycle = None;
                        trigger.last_evaluated_cycle = cycle;
                    }
                }
                RoboticAirLockProfile::WetPayload {
                    name,
                    sensor_id: target_sensor,
                    trigger,
                    duration_cycles,
                    recovery_multiplier,
                    active_start_cycle,
                } if target_sensor == sensor_id => {
                    if activate_for_cycle(trigger, active_start_cycle, cycle, rng) {
                        self.events.record(AnomalyEvent { timestamp_ms, anomaly_type: "robotic_air_lock_wet_payload".into(), profile: name.clone(), sensor_id: target_sensor.clone(), cycle: Some(cycle) });
                    }
                    if let Some(start_cycle) = *active_start_cycle {
                        let end_cycle = start_cycle.saturating_add(*duration_cycles);
                        if (start_cycle..end_cycle).contains(&cycle) {
                            tracing::debug!(anomaly = %name, sensor_id, cycle, "robotic air-lock wet payload active");
                            state.wet_payload_recovery_multiplier = Some(*recovery_multiplier);
                        } else if cycle >= end_cycle {
                            *active_start_cycle = None;
                            trigger.last_evaluated_cycle = cycle;
                        }
                    }
                }
                _ => {}
            }
        }
        state
    }
}

fn activate_for_cycle(
    trigger: &mut AirLockCycleTrigger,
    active_start_cycle: &mut Option<u64>,
    cycle: u64,
    rng: &mut StdRng,
) -> bool {
    if active_start_cycle.is_some()
        || cycle <= trigger.start_after_cycles
        || trigger.last_evaluated_cycle == cycle
    {
        return false;
    }
    trigger.last_evaluated_cycle = cycle;
    if rng.gen::<f64>() >= trigger.threshold {
        *active_start_cycle = Some(cycle);
        true
    } else {
        false
    }
}

fn is_model_level_strategy(strategy: &AnomalyStrategyConfig) -> bool {
    matches!(
        strategy,
        AnomalyStrategyConfig::RoboticAirLockSealFailure { .. }
            | AnomalyStrategyConfig::RoboticAirLockWetPayload { .. }
    )
}

struct AnomalyProfile {
    name: String,
    target: AnomalyTargetConfig,
    trigger: AnomalyTriggerConfig,
    strategy: Box<dyn AnomalyStrategy>,
    remaining_samples: u64,
    active_sample_index: u64,
    cooldown_remaining: u64,
    observed_samples: u64,
}

impl AnomalyProfile {
    fn from_config(config: &AnomalyProfileConfig) -> Self {
        let strategy: Box<dyn AnomalyStrategy> = match &config.strategy {
            AnomalyStrategyConfig::Spike { offset, multiplier } => Box::new(SpikeAnomaly {
                offset: *offset,
                multiplier: *multiplier,
            }),
            AnomalyStrategyConfig::SpikeDrop {
                spike_value,
                drop_value,
            } => Box::new(SpikeDropAnomaly {
                spike_value: *spike_value,
                drop_value: *drop_value,
            }),
            AnomalyStrategyConfig::StuckAt {
                duration_samples,
                value,
            } => Box::new(StuckAtAnomaly {
                duration_samples: *duration_samples,
                configured_value: *value,
                frozen_value: 0.0,
            }),
            AnomalyStrategyConfig::Drift {
                duration_samples,
                total_offset,
            } => Box::new(CalibrationDriftAnomaly {
                duration_samples: *duration_samples,
                total_offset: *total_offset,
            }),
            AnomalyStrategyConfig::BatchCoolingStretch {
                duration_samples,
                low,
                peak,
                rise_samples,
                plateau_samples,
                initial_cool_samples,
                final_cool_samples,
                batches,
            } => Box::new(BatchCoolingStretchAnomaly {
                duration_samples: *duration_samples,
                low: *low,
                peak: *peak,
                rise_samples: *rise_samples,
                plateau_samples: *plateau_samples,
                initial_cool_samples: *initial_cool_samples,
                final_cool_samples: *final_cool_samples,
                batches: *batches,
            }),
            AnomalyStrategyConfig::Noise {
                duration_samples,
                sigma,
            } => Box::new(HighFrequencyNoiseAnomaly {
                duration_samples: *duration_samples,
                distribution: Normal::new(0.0, *sigma)
                    .expect("anomaly noise sigma was validated during configuration loading"),
            }),
            AnomalyStrategyConfig::Contextual {
                duration_samples,
                condition,
                value,
                offset,
                multiplier,
            } => Box::new(ContextualAnomaly {
                duration_samples: *duration_samples,
                condition: condition.clone(),
                value: *value,
                offset: *offset,
                multiplier: *multiplier,
            }),
            AnomalyStrategyConfig::RoboticAirLockSealFailure { .. }
            | AnomalyStrategyConfig::RoboticAirLockWetPayload { .. } => {
                unreachable!("model-level anomaly profiles are not post-sample injectors")
            }
        };
        Self {
            name: config.name.clone(),
            target: config.target.clone(),
            trigger: config.trigger.clone(),
            strategy,
            remaining_samples: 0,
            active_sample_index: 0,
            cooldown_remaining: 0,
            observed_samples: 0,
        }
    }

    fn apply(
        &mut self,
        value: &mut f64,
        previous: Option<f64>,
        context: &InjectionContext<'_>,
        rng: &mut StdRng,
    ) {
        if self.observed_samples < self.trigger.start_after_samples {
            self.observed_samples += 1;
            return;
        }
        if self.remaining_samples == 0 {
            if self.cooldown_remaining > 0 {
                self.cooldown_remaining -= 1;
                return;
            }
            if !self.strategy.can_start(context) || rng.gen::<f64>() < self.trigger.threshold {
                return;
            }
            self.strategy.start(*value, previous);
            self.remaining_samples = self.strategy.duration_samples();
            self.active_sample_index = 0;
            tracing::warn!(
                anomaly = %self.name,
                sensor_id = %self.target.sensor_id,
                metric = %self.target.metric,
                duration_samples = self.remaining_samples,
                "anomaly activated"
            );
        }

        self.strategy.inject(value, self.active_sample_index, rng);
        self.active_sample_index += 1;
        self.remaining_samples -= 1;
        if self.remaining_samples == 0 {
            self.cooldown_remaining = self.trigger.cooldown_samples;
        }
    }
}

/// Applies configured anomalies to the transport-neutral telemetry batch after
/// physical models have sampled and before any exporter observes the readings.
pub struct AnomalyInjector {
    profiles: Vec<AnomalyProfile>,
    rng: StdRng,
    latest_baseline: HashMap<MetricKey, f64>,
}

impl AnomalyInjector {
    pub fn from_config(config: &AnomaliesConfig) -> Self {
        let rng = config
            .seed
            .map(StdRng::seed_from_u64)
            .unwrap_or_else(StdRng::from_entropy);
        let profiles = if config.enabled {
            config
                .profiles
                .iter()
                .filter(|profile| profile.enabled)
                .filter(|profile| !is_model_level_strategy(&profile.strategy))
                .map(AnomalyProfile::from_config)
                .collect()
        } else {
            Vec::new()
        };
        Self {
            profiles,
            rng,
            latest_baseline: HashMap::new(),
        }
    }

    pub fn active_profile_count(&self) -> usize {
        self.profiles.len()
    }

    pub fn inject(&mut self, readings: &mut [SensorReading]) {
        if self.profiles.is_empty() || readings.is_empty() {
            return;
        }

        let mut current_baseline = HashMap::new();
        for reading in readings.iter() {
            for (metric, value) in &reading.metrics {
                current_baseline.insert(
                    MetricKey {
                        sensor_id: reading.sensor_id.clone(),
                        metric: metric.clone(),
                    },
                    *value,
                );
            }
        }

        let context = InjectionContext {
            current: &current_baseline,
            previous: &self.latest_baseline,
        };
        for profile in &mut self.profiles {
            let target = profile.target.clone();
            for reading in readings.iter_mut().filter(|reading| {
                reading.sensor_id == target.sensor_id
                    && reading.metrics.contains_key(&target.metric)
            }) {
                let key = MetricKey::from(&target);
                let previous = self.latest_baseline.get(&key).copied();
                let value = reading
                    .metrics
                    .get_mut(&target.metric)
                    .expect("target metric was checked by the filter");
                profile.apply(value, previous, &context, &mut self.rng);
            }
        }
        self.latest_baseline.extend(current_baseline);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::config::{
        AnomaliesConfig, AnomalyProfileConfig, AnomalyStrategyConfig, AnomalyTargetConfig,
        AnomalyTriggerConfig, SensorType,
    };

    use super::*;

    #[test]
    fn model_level_air_lock_profiles_control_complete_cycle_states() {
        let config = AnomaliesConfig {
            enabled: true,
            seed: Some(42),
            profiles: vec![
                AnomalyProfileConfig {
                    name: "seal_failure".into(),
                    enabled: true,
                    target: AnomalyTargetConfig {
                        sensor_id: "mal-01".into(),
                        metric: "equipment_state_code".into(),
                    },
                    trigger: AnomalyTriggerConfig {
                        threshold: 0.0,
                        start_after_cycles: 1,
                        ..Default::default()
                    },
                    strategy: AnomalyStrategyConfig::RoboticAirLockSealFailure {
                        faulted_cycles_before_maintenance: 3,
                        maintenance_cycles: 1,
                    },
                },
                AnomalyProfileConfig {
                    name: "wet_payload".into(),
                    enabled: true,
                    target: AnomalyTargetConfig {
                        sensor_id: "mal-01".into(),
                        metric: "dew_point_c".into(),
                    },
                    trigger: AnomalyTriggerConfig {
                        threshold: 0.0,
                        start_after_cycles: 6,
                        ..Default::default()
                    },
                    strategy: AnomalyStrategyConfig::RoboticAirLockWetPayload {
                        duration_cycles: 1,
                        recovery_multiplier: 3.0,
                    },
                },
            ],
        };
        let controller = ModelAnomalyController::from_config(&config);

        assert_eq!(
            controller.robotic_air_lock_state("mal-01", 1, 0),
            RoboticAirLockAnomalyState::default()
        );
        assert!(controller.robotic_air_lock_state("mal-01", 2, 0).seal_faulted);
        assert!(controller.robotic_air_lock_state("mal-01", 4, 0).seal_faulted);
        assert!(
            controller
                .robotic_air_lock_state("mal-01", 5, 0)
                .maintenance_offline
        );
        assert_eq!(
            controller
                .robotic_air_lock_state("mal-01", 7, 0)
                .wet_payload_recovery_multiplier,
            Some(3.0)
        );
    }

    fn reading(
        sensor_id: &str,
        sensor_type: SensorType,
        metric: &str,
        value: f64,
    ) -> SensorReading {
        SensorReading {
            entity_type: "test".into(),
            entity_id: "entity-1".into(),
            sensor_id: sensor_id.into(),
            sensor_type,
            timestamp_ms: 1,
            interval_ms: 1_000,
            sequence: 1,
            metrics: BTreeMap::from([(metric.into(), value)]),
        }
    }

    fn config(strategy: AnomalyStrategyConfig) -> AnomaliesConfig {
        AnomaliesConfig {
            enabled: true,
            seed: Some(7),
            profiles: vec![AnomalyProfileConfig {
                name: "test".into(),
                enabled: true,
                target: AnomalyTargetConfig {
                    sensor_id: "target-01".into(),
                    metric: "value".into(),
                },
                trigger: AnomalyTriggerConfig {
                    threshold: 0.0,
                    cooldown_samples: 1,
                    start_after_samples: 0,
                    start_after_cycles: 0,
                },
                strategy,
            }],
        }
    }

    fn inject_value(injector: &mut AnomalyInjector, value: f64) -> f64 {
        let mut readings = [reading("target-01", SensorType::Ph, "value", value)];
        injector.inject(&mut readings);
        readings[0].metrics["value"]
    }

    #[test]
    fn spike_changes_exactly_one_target_sample() {
        let mut injector = AnomalyInjector::from_config(&config(AnomalyStrategyConfig::Spike {
            offset: 20.0,
            multiplier: 2.0,
        }));
        assert_eq!(inject_value(&mut injector, 10.0), 40.0);
        assert_eq!(inject_value(&mut injector, 10.0), 10.0);
    }

    #[test]
    fn spike_drop_emits_impulse_then_collapse() {
        let mut injector =
            AnomalyInjector::from_config(&config(AnomalyStrategyConfig::SpikeDrop {
                spike_value: 180.0,
                drop_value: 35.0,
            }));
        assert_eq!(inject_value(&mut injector, 60.0), 180.0);
        assert_eq!(inject_value(&mut injector, 60.0), 35.0);
        assert_eq!(inject_value(&mut injector, 60.0), 60.0);
    }

    #[test]
    fn batch_cooling_profile_reaches_sixty_samples_by_batch_fifteen() {
        let strategy = BatchCoolingStretchAnomaly {
            duration_samples: 1688,
            low: 30.0,
            peak: 85.0,
            rise_samples: 15,
            plateau_samples: 45,
            initial_cool_samples: 45,
            final_cool_samples: 60,
            batches: 15,
        };
        assert_eq!(strategy.cool_samples(0), 45);
        assert_eq!(strategy.cool_samples(14), 60);
        assert_eq!(
            (0..15)
                .map(|batch| 60 + strategy.cool_samples(batch))
                .sum::<u64>(),
            1688
        );
    }

    #[test]
    fn stuck_at_repeats_the_last_valid_value() {
        let mut injector = AnomalyInjector::from_config(&config(AnomalyStrategyConfig::StuckAt {
            duration_samples: 3,
            value: None,
        }));
        assert_eq!(inject_value(&mut injector, 10.0), 10.0);
        assert_eq!(inject_value(&mut injector, 11.0), 10.0);
        assert_eq!(inject_value(&mut injector, 12.0), 10.0);
        assert_eq!(inject_value(&mut injector, 13.0), 13.0);
    }

    #[test]
    fn drift_ramps_across_the_configured_window() {
        let mut injector = AnomalyInjector::from_config(&config(AnomalyStrategyConfig::Drift {
            duration_samples: 3,
            total_offset: 6.0,
        }));
        assert_eq!(inject_value(&mut injector, 10.0), 10.0);
        assert_eq!(inject_value(&mut injector, 10.0), 13.0);
        assert_eq!(inject_value(&mut injector, 10.0), 16.0);
        assert_eq!(inject_value(&mut injector, 10.0), 10.0);
    }

    #[test]
    fn seeded_noise_is_reproducible() {
        let anomaly = config(AnomalyStrategyConfig::Noise {
            duration_samples: 2,
            sigma: 4.0,
        });
        let mut first = AnomalyInjector::from_config(&anomaly);
        let mut second = AnomalyInjector::from_config(&anomaly);
        let first_value = inject_value(&mut first, 10.0);
        let second_value = inject_value(&mut second, 10.0);
        assert_eq!(first_value, second_value);
        assert_ne!(first_value, 10.0);
    }

    #[test]
    fn contextual_anomaly_uses_another_sensor_in_the_same_batch() {
        let mut anomaly = config(AnomalyStrategyConfig::Contextual {
            duration_samples: 1,
            condition: AnomalyConditionConfig {
                source: AnomalyTargetConfig {
                    sensor_id: "load-01".into(),
                    metric: "load_cell_weight_kg".into(),
                },
                operator: ComparisonOperator::GreaterThan,
                value: 900.0,
            },
            value: Some(2.0),
            offset: 0.0,
            multiplier: 1.0,
        });
        anomaly.profiles[0].target = AnomalyTargetConfig {
            sensor_id: "level-01".into(),
            metric: "tank_level_pct".into(),
        };
        let mut injector = AnomalyInjector::from_config(&anomaly);
        let mut readings = [
            reading("level-01", SensorType::Level, "tank_level_pct", 55.0),
            reading(
                "load-01",
                SensorType::LoadCell,
                "load_cell_weight_kg",
                1_100.0,
            ),
        ];
        injector.inject(&mut readings);
        assert_eq!(readings[0].metrics["tank_level_pct"], 2.0);
        assert_eq!(readings[1].metrics["load_cell_weight_kg"], 1_100.0);
    }
}
