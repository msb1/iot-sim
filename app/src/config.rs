use std::collections::{HashMap, HashSet};
use std::path::Path;

use config::{Config, ConfigError, Environment, File, FileFormat};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub config_name: String,
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub exporters: ExportersConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub anomalies: AnomaliesConfig,
    pub models: ModelsConfig,
    pub entities: Vec<EntityConfig>,
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigurationError> {
        let path = path.as_ref();
        let config = Config::builder()
            .add_source(File::from(path).format(FileFormat::Yaml))
            .add_source(
                Environment::with_prefix("IOT_SIM")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?
            .try_deserialize()?;
        let config: Self = config;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigurationError> {
        if self.config_name.trim().is_empty() {
            return Err(ConfigurationError::Validation(
                "config_name cannot be empty".into(),
            ));
        }
        if self.entities.is_empty() {
            return Err(ConfigurationError::Validation(
                "at least one entity is required".into(),
            ));
        }

        let mut entity_ids = HashSet::new();
        let mut sensor_ids = HashSet::new();
        let mut sensor_metrics = HashMap::new();
        for entity in &self.entities {
            if !entity_ids.insert(&entity.entity_id) {
                return Err(ConfigurationError::Validation(format!(
                    "duplicate entity_id '{}'",
                    entity.entity_id
                )));
            }
            if entity.sensors.is_empty() {
                return Err(ConfigurationError::Validation(format!(
                    "entity '{}' has no sensors",
                    entity.entity_id
                )));
            }
            for sensor in &entity.sensors {
                if sensor.quantity == 0 {
                    return Err(ConfigurationError::Validation(format!(
                        "sensor '{}' quantity must be greater than zero",
                        sensor.id_prefix
                    )));
                }
                if sensor.timestep_ms == 0 {
                    return Err(ConfigurationError::Validation(format!(
                        "sensor '{}' timestep_ms must be greater than zero",
                        sensor.id_prefix
                    )));
                }
                if sensor.min_value >= sensor.max_value {
                    return Err(ConfigurationError::Validation(format!(
                        "sensor '{}' min_value must be less than max_value",
                        sensor.id_prefix
                    )));
                }
                for index in 1..=sensor.quantity {
                    let id = sensor.expanded_id(index);
                    if !sensor_ids.insert(id.clone()) {
                        return Err(ConfigurationError::Validation(format!(
                            "duplicate expanded sensor_id '{id}'"
                        )));
                    }
                    let metrics = if sensor.sensor_type == SensorType::ScenarioSignal {
                        let signal = sensor.scenario.as_ref().ok_or_else(|| {
                            ConfigurationError::Validation(format!(
                                "scenario_signal sensor '{}' requires a scenario block",
                                sensor.id_prefix
                            ))
                        })?;
                        signal.validate(&sensor.id_prefix)?;
                        HashSet::from([signal.metric.clone()])
                    } else {
                        if sensor.scenario.is_some() {
                            return Err(ConfigurationError::Validation(format!(
                                "sensor '{}' may only use scenario with type scenario_signal",
                                sensor.id_prefix
                            )));
                        }
                        if sensor.sensor_type == SensorType::DataCenterRack {
                            sensor.data_center_rack.as_ref().ok_or_else(|| {
                                ConfigurationError::Validation(format!(
                                    "data_center_rack sensor '{}' requires a data_center_rack block",
                                    sensor.id_prefix
                                ))
                            })?.validate(&sensor.id_prefix)?;
                        } else if sensor.data_center_rack.is_some() {
                            return Err(ConfigurationError::Validation(format!(
                                "sensor '{}' may only use data_center_rack with type data_center_rack",
                                sensor.id_prefix
                            )));
                        }
                        sensor
                            .sensor_type
                            .metric_names()
                            .iter()
                            .map(|metric| (*metric).to_string())
                            .collect()
                    };
                    sensor_metrics.insert(id, metrics);
                }
            }
        }

        self.anomalies.validate(&sensor_metrics)?;

        if self.exporters.prometheus.enabled && self.exporters.prometheus.bind.trim().is_empty() {
            return Err(ConfigurationError::Validation(
                "exporters.prometheus.bind cannot be empty".into(),
            ));
        }
        if self.exporters.kafka.enabled {
            if self.exporters.kafka.brokers.trim().is_empty() {
                return Err(ConfigurationError::Validation(
                    "exporters.kafka.brokers cannot be empty when Kafka is enabled".into(),
                ));
            }
            if self.exporters.kafka.topic.trim().is_empty() {
                return Err(ConfigurationError::Validation(
                    "exporters.kafka.topic cannot be empty when Kafka is enabled".into(),
                ));
            }
        }
        if self.exporters.dataset.enabled {
            if self.exporters.kafka.enabled || self.exporters.prometheus.enabled {
                return Err(ConfigurationError::Validation(
                    "dataset exporter cannot be enabled with Kafka or Prometheus exporters".into(),
                ));
            }
            self.exporters.dataset.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ConfigurationError {
    #[error("could not load configuration: {0}")]
    Load(#[from] ConfigError),
    #[error("invalid configuration: {0}")]
    Validation(String),
}

fn default_schema_version() -> String {
    "1.0".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_shutdown_timeout")]
    pub shutdown_timeout_seconds: u64,
    pub run_duration_seconds: Option<u64>,
}

/// Controls simulator-specific console output. General log filtering remains
/// configurable through `RUST_LOG`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct LoggingConfig {
    #[serde(default)]
    pub simulator_stream: SimulatorStreamConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SimulatorStreamConfig {
    /// Emit each generated reading as JSON at `INFO` level. This is useful
    /// while verifying that scheduling and models are producing telemetry.
    #[serde(default)]
    pub enabled: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            shutdown_timeout_seconds: default_shutdown_timeout(),
            run_duration_seconds: None,
        }
    }
}

fn default_shutdown_timeout() -> u64 {
    10
}

/// Post-model anomaly injection. The baseline simulators are unchanged when
/// this section is omitted or `enabled` is false.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AnomaliesConfig {
    #[serde(default)]
    pub enabled: bool,
    /// A fixed seed makes random anomaly activation and noise reproducible.
    pub seed: Option<u64>,
    #[serde(default)]
    pub profiles: Vec<AnomalyProfileConfig>,
}

impl AnomaliesConfig {
    fn validate(
        &self,
        sensor_metrics: &HashMap<String, HashSet<String>>,
    ) -> Result<(), ConfigurationError> {
        let mut names = HashSet::new();
        for profile in &self.profiles {
            if profile.name.trim().is_empty() {
                return Err(ConfigurationError::Validation(
                    "anomaly profile name cannot be empty".into(),
                ));
            }
            if !names.insert(&profile.name) {
                return Err(ConfigurationError::Validation(format!(
                    "duplicate anomaly profile name '{}'",
                    profile.name
                )));
            }
            validate_anomaly_target(&profile.name, &profile.target, sensor_metrics)?;
            if !profile.trigger.threshold.is_finite()
                || !(0.0..=1.0).contains(&profile.trigger.threshold)
            {
                return Err(ConfigurationError::Validation(format!(
                    "anomaly profile '{}' trigger.threshold must be between 0 and 1",
                    profile.name
                )));
            }
            profile.strategy.validate(&profile.name, sensor_metrics)?;
        }
        Ok(())
    }
}

fn validate_anomaly_target(
    profile_name: &str,
    target: &AnomalyTargetConfig,
    sensor_metrics: &HashMap<String, HashSet<String>>,
) -> Result<(), ConfigurationError> {
    let Some(metrics) = sensor_metrics.get(&target.sensor_id) else {
        return Err(ConfigurationError::Validation(format!(
            "anomaly profile '{profile_name}' references unknown sensor_id '{}'",
            target.sensor_id
        )));
    };
    if !metrics.contains(&target.metric) {
        return Err(ConfigurationError::Validation(format!(
            "anomaly profile '{profile_name}' metric '{}' is not emitted by sensor '{}'",
            target.metric, target.sensor_id
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
pub struct AnomalyProfileConfig {
    pub name: String,
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
    pub target: AnomalyTargetConfig,
    #[serde(default)]
    pub trigger: AnomalyTriggerConfig,
    #[serde(flatten)]
    pub strategy: AnomalyStrategyConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub struct AnomalyTargetConfig {
    pub sensor_id: String,
    pub metric: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AnomalyTriggerConfig {
    /// An event starts when a uniform random value in [0, 1) is greater than
    /// or equal to this threshold. High values therefore make anomalies rare.
    #[serde(default = "default_anomaly_threshold")]
    pub threshold: f64,
    #[serde(default)]
    pub cooldown_samples: u64,
    /// Deterministic baseline warm-up before this profile may activate.
    #[serde(default)]
    pub start_after_samples: u64,
}

impl Default for AnomalyTriggerConfig {
    fn default() -> Self {
        Self {
            threshold: default_anomaly_threshold(),
            cooldown_samples: 0,
            start_after_samples: 0,
        }
    }
}

fn default_anomaly_threshold() -> f64 {
    0.999
}

fn default_multiplier() -> f64 {
    1.0
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnomalyStrategyConfig {
    Spike {
        #[serde(default)]
        offset: f64,
        #[serde(default = "default_multiplier")]
        multiplier: f64,
    },
    SpikeDrop {
        spike_value: f64,
        drop_value: f64,
    },
    StuckAt {
        duration_samples: u64,
        /// Omit to freeze at the last valid value observed before activation.
        value: Option<f64>,
    },
    Drift {
        duration_samples: u64,
        total_offset: f64,
    },
    BatchCoolingStretch {
        duration_samples: u64,
        low: f64,
        peak: f64,
        rise_samples: u64,
        plateau_samples: u64,
        initial_cool_samples: u64,
        final_cool_samples: u64,
        batches: u64,
    },
    Noise {
        duration_samples: u64,
        sigma: f64,
    },
    Contextual {
        duration_samples: u64,
        condition: AnomalyConditionConfig,
        value: Option<f64>,
        #[serde(default)]
        offset: f64,
        #[serde(default = "default_multiplier")]
        multiplier: f64,
    },
}

impl AnomalyStrategyConfig {
    fn validate(
        &self,
        profile_name: &str,
        sensor_metrics: &HashMap<String, HashSet<String>>,
    ) -> Result<(), ConfigurationError> {
        let invalid_duration = match self {
            Self::Spike { .. } | Self::SpikeDrop { .. } => false,
            Self::StuckAt {
                duration_samples, ..
            }
            | Self::Drift {
                duration_samples, ..
            }
            | Self::BatchCoolingStretch {
                duration_samples, ..
            }
            | Self::Noise {
                duration_samples, ..
            }
            | Self::Contextual {
                duration_samples, ..
            } => *duration_samples == 0,
        };
        if invalid_duration {
            return Err(ConfigurationError::Validation(format!(
                "anomaly profile '{profile_name}' duration_samples must be greater than zero"
            )));
        }

        match self {
            Self::Spike { offset, multiplier } => {
                validate_finite(profile_name, "offset", *offset)?;
                validate_finite(profile_name, "multiplier", *multiplier)?;
            }
            Self::SpikeDrop {
                spike_value,
                drop_value,
            } => {
                validate_finite(profile_name, "spike_value", *spike_value)?;
                validate_finite(profile_name, "drop_value", *drop_value)?;
            }
            Self::StuckAt { value, .. } => {
                if let Some(value) = value {
                    validate_finite(profile_name, "value", *value)?;
                }
            }
            Self::Drift { total_offset, .. } => {
                validate_finite(profile_name, "total_offset", *total_offset)?;
            }
            Self::BatchCoolingStretch {
                low,
                peak,
                rise_samples,
                plateau_samples,
                initial_cool_samples,
                final_cool_samples,
                batches,
                ..
            } => {
                for (name, value) in [("low", *low), ("peak", *peak)] {
                    validate_finite(profile_name, name, value)?;
                }
                if *rise_samples == 0
                    || *plateau_samples == 0
                    || *initial_cool_samples == 0
                    || final_cool_samples < initial_cool_samples
                    || *batches < 2
                {
                    return Err(ConfigurationError::Validation(format!(
                        "anomaly profile '{profile_name}' has an invalid batch cooling profile"
                    )));
                }
            }
            Self::Noise { sigma, .. } => {
                if !sigma.is_finite() || *sigma <= 0.0 {
                    return Err(ConfigurationError::Validation(format!(
                        "anomaly profile '{profile_name}' sigma must be finite and greater than zero"
                    )));
                }
            }
            Self::Contextual {
                condition,
                value,
                offset,
                multiplier,
                ..
            } => {
                validate_anomaly_target(profile_name, &condition.source, sensor_metrics)?;
                validate_finite(profile_name, "condition.value", condition.value)?;
                if let Some(value) = value {
                    validate_finite(profile_name, "value", *value)?;
                }
                validate_finite(profile_name, "offset", *offset)?;
                validate_finite(profile_name, "multiplier", *multiplier)?;
            }
        }
        Ok(())
    }
}

fn validate_finite(
    profile_name: &str,
    field_name: &str,
    value: f64,
) -> Result<(), ConfigurationError> {
    if !value.is_finite() {
        return Err(ConfigurationError::Validation(format!(
            "anomaly profile '{profile_name}' {field_name} must be finite"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
pub struct AnomalyConditionConfig {
    pub source: AnomalyTargetConfig,
    pub operator: ComparisonOperator,
    pub value: f64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOperator {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Equal,
    NotEqual,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExportersConfig {
    #[serde(default)]
    pub prometheus: PrometheusConfig,
    #[serde(default)]
    pub kafka: KafkaConfig,
    #[serde(default)]
    pub dataset: DatasetConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Inclusive RFC 3339 instant at which to begin generated history.
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default = "default_s3_endpoint_url")]
    pub s3_endpoint_url: String,
    #[serde(default = "default_s3_bucket_name")]
    pub s3_bucket_name: String,
    #[serde(default = "default_s3_access_key")]
    pub s3_access_key: String,
    #[serde(default = "default_s3_secret_key")]
    pub s3_secret_key: String,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            start_time: None,
            s3_endpoint_url: default_s3_endpoint_url(),
            s3_bucket_name: default_s3_bucket_name(),
            s3_access_key: default_s3_access_key(),
            s3_secret_key: default_s3_secret_key(),
        }
    }
}

impl DatasetConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        let start_time = self.start_time.as_deref().ok_or_else(|| {
            ConfigurationError::Validation(
                "exporters.dataset.start_time is required when dataset exporter is enabled".into(),
            )
        })?;
        let start = chrono::DateTime::parse_from_rfc3339(start_time).map_err(|error| {
            ConfigurationError::Validation(format!(
                "exporters.dataset.start_time must be RFC 3339: {error}"
            ))
        })?;
        if start.timestamp_millis() > chrono::Utc::now().timestamp_millis() {
            return Err(ConfigurationError::Validation(
                "exporters.dataset.start_time must not be in the future".into(),
            ));
        }
        for (name, value) in [
            ("s3_endpoint_url", &self.s3_endpoint_url),
            ("s3_bucket_name", &self.s3_bucket_name),
            ("s3_access_key", &self.s3_access_key),
            ("s3_secret_key", &self.s3_secret_key),
        ] {
            if value.trim().is_empty() {
                return Err(ConfigurationError::Validation(format!(
                    "exporters.dataset.{name} cannot be empty when dataset exporter is enabled"
                )));
            }
        }
        Ok(())
    }
}

fn default_s3_endpoint_url() -> String {
    "http://192.168.1.50:9000".into()
}
fn default_s3_bucket_name() -> String {
    "iotsim".into()
}
fn default_s3_access_key() -> String {
    "access".into()
}
fn default_s3_secret_key() -> String {
    "secret".into()
}
#[derive(Debug, Clone, Deserialize)]
pub struct PrometheusConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_prometheus_bind")]
    pub bind: String,
    #[serde(default = "default_prometheus_path")]
    pub path: String,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_prometheus_bind(),
            path: default_prometheus_path(),
        }
    }
}

fn default_prometheus_bind() -> String {
    "0.0.0.0:9898".into()
}

fn default_prometheus_path() -> String {
    "/metrics".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub brokers: String,
    #[serde(default = "default_kafka_topic")]
    pub topic: String,
    #[serde(default = "default_client_id")]
    pub client_id: String,
    #[serde(default)]
    pub security: KafkaSecurityConfig,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            brokers: String::new(),
            topic: default_kafka_topic(),
            client_id: default_client_id(),
            security: KafkaSecurityConfig::default(),
        }
    }
}

fn default_kafka_topic() -> String {
    "iot.telemetry.v1".into()
}

fn default_client_id() -> String {
    "iot-sim".into()
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct KafkaSecurityConfig {
    pub security_protocol: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EntityConfig {
    pub entity_type: String,
    pub entity_id: String,
    #[serde(default)]
    pub description: String,
    pub sensors: Vec<SensorConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SensorConfig {
    #[serde(rename = "type")]
    pub sensor_type: SensorType,
    pub id_prefix: String,
    #[serde(default = "default_quantity")]
    pub quantity: usize,
    pub timestep_ms: u64,
    pub min_value: f64,
    pub max_value: f64,
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
    /// A configurable single-metric physical signal used by the documented
    /// industrial anomaly scenarios. Existing fixed sensor models do not use it.
    pub scenario: Option<ScenarioSignalConfig>,
    pub data_center_rack: Option<DataCenterRackConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataCenterRackConfig {
    pub ambient_temperature_f: f64,
    pub max_thermal_lift_f: f64,
    pub cooling_time_constant_minutes: f64,
    pub managed_dew_point_f: f64,
    #[serde(default)]
    pub temperature_noise_sigma_f: f64,
    #[serde(default)]
    pub cpu_noise_sigma: f64,
    #[serde(default)]
    pub weekend_spike_probability: f64,
    pub anomaly_temperature_f: f64,
    pub anomaly_humidity_pct: f64,
    /// Wall-clock duration in which the 672-sample logical week is emitted.
    #[serde(default = "default_rack_week_duration_seconds")]
    pub simulated_week_duration_seconds: f64,
    pub seed: Option<u64>,
}

fn default_rack_week_duration_seconds() -> f64 {
    7.0 * 24.0 * 60.0 * 60.0
}

impl DataCenterRackConfig {
    fn validate(&self, sensor_id: &str) -> Result<(), ConfigurationError> {
        let values = [
            self.ambient_temperature_f,
            self.max_thermal_lift_f,
            self.cooling_time_constant_minutes,
            self.managed_dew_point_f,
            self.temperature_noise_sigma_f,
            self.cpu_noise_sigma,
            self.weekend_spike_probability,
            self.anomaly_temperature_f,
            self.anomaly_humidity_pct,
            self.simulated_week_duration_seconds,
        ];
        if !values.into_iter().all(f64::is_finite)
            || self.max_thermal_lift_f < 0.0
            || self.cooling_time_constant_minutes <= 0.0
            || self.temperature_noise_sigma_f < 0.0
            || self.cpu_noise_sigma < 0.0
            || !(0.0..=1.0).contains(&self.weekend_spike_probability)
            || !(0.0..=100.0).contains(&self.anomaly_humidity_pct)
            || self.simulated_week_duration_seconds <= 0.0
        {
            return Err(ConfigurationError::Validation(format!(
                "data_center_rack sensor '{sensor_id}' has invalid physical model settings"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioSignalConfig {
    pub metric: String,
    pub baseline: f64,
    #[serde(default)]
    pub noise_sigma: f64,
    #[serde(default)]
    pub waveform: ScenarioWaveformConfig,
    pub seed: Option<u64>,
}

impl ScenarioSignalConfig {
    fn validate(&self, sensor_id: &str) -> Result<(), ConfigurationError> {
        if self.metric.trim().is_empty() || !self.baseline.is_finite() {
            return Err(ConfigurationError::Validation(format!(
                "scenario_signal sensor '{sensor_id}' requires a non-empty metric and finite baseline"
            )));
        }
        if !self.noise_sigma.is_finite() || self.noise_sigma < 0.0 {
            return Err(ConfigurationError::Validation(format!(
                "scenario_signal sensor '{sensor_id}' noise_sigma must be finite and non-negative"
            )));
        }
        self.waveform.validate(sensor_id)
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScenarioWaveformConfig {
    #[default]
    Constant,
    Sine {
        amplitude: f64,
        period_samples: u64,
        #[serde(default)]
        phase_radians: f64,
    },
    StepCycle {
        values: Vec<f64>,
        samples_per_step: u64,
    },
    BatchTemperature {
        low: f64,
        peak: f64,
        rise_samples: u64,
        plateau_samples: u64,
        cool_samples: u64,
    },
}

impl ScenarioWaveformConfig {
    fn validate(&self, sensor_id: &str) -> Result<(), ConfigurationError> {
        let invalid = match self {
            Self::Constant => false,
            Self::Sine {
                amplitude,
                period_samples,
                phase_radians,
            } => !amplitude.is_finite() || *period_samples == 0 || !phase_radians.is_finite(),
            Self::StepCycle {
                values,
                samples_per_step,
            } => {
                values.is_empty()
                    || *samples_per_step == 0
                    || values.iter().any(|value| !value.is_finite())
            }
            Self::BatchTemperature {
                low,
                peak,
                rise_samples,
                plateau_samples,
                cool_samples,
            } => {
                !low.is_finite()
                    || !peak.is_finite()
                    || *rise_samples == 0
                    || *plateau_samples == 0
                    || *cool_samples == 0
            }
        };
        if invalid {
            return Err(ConfigurationError::Validation(format!(
                "scenario_signal sensor '{sensor_id}' has an invalid waveform"
            )));
        }
        Ok(())
    }
}

impl SensorConfig {
    pub fn expanded_id(&self, one_based_index: usize) -> String {
        if self.quantity == 1 {
            self.id_prefix.clone()
        } else {
            format!("{}-{one_based_index:02}", self.id_prefix)
        }
    }
}

fn default_quantity() -> usize {
    1
}

#[cfg(test)]
pub(crate) fn sample_config_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../config/simulation.yaml")
}

fn enabled_by_default() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SensorType {
    Weather,
    Sound,
    Electrical,
    Ph,
    Orp,
    Conductivity,
    ChemicalConcentration,
    ProcessAnalyticsFtir,
    DissolvedOxygen,
    ToxicGas,
    CombustibleGas,
    Photoionization,
    Level,
    MassFlow,
    LoadCell,
    ScenarioSignal,
    DataCenterRack,
}

impl SensorType {
    pub const ALL: [Self; 17] = [
        Self::Weather,
        Self::Sound,
        Self::Electrical,
        Self::Ph,
        Self::Orp,
        Self::Conductivity,
        Self::ChemicalConcentration,
        Self::ProcessAnalyticsFtir,
        Self::DissolvedOxygen,
        Self::ToxicGas,
        Self::CombustibleGas,
        Self::Photoionization,
        Self::Level,
        Self::MassFlow,
        Self::LoadCell,
        Self::ScenarioSignal,
        Self::DataCenterRack,
    ];

    pub fn metric_names(self) -> &'static [&'static str] {
        match self {
            Self::Weather => &["temperature_c", "humidity_pct", "pressure_hpa"],
            Self::Sound => &["decibels_db"],
            Self::Electrical => &[
                "voltage_a_v",
                "current_a_a",
                "power_a_kw",
                "apparent_power_a_kva",
                "power_factor_a",
                "voltage_thd_a_pct",
                "active_energy_a_kwh",
                "apparent_energy_a_kvah",
                "voltage_b_v",
                "current_b_a",
                "power_b_kw",
                "apparent_power_b_kva",
                "power_factor_b",
                "voltage_thd_b_pct",
                "active_energy_b_kwh",
                "apparent_energy_b_kvah",
                "voltage_c_v",
                "current_c_a",
                "power_c_kw",
                "apparent_power_c_kva",
                "power_factor_c",
                "voltage_thd_c_pct",
                "active_energy_c_kwh",
                "apparent_energy_c_kvah",
                "safety_relay_tripped",
            ],
            Self::Ph => &["ph_units"],
            Self::Orp => &["orp_mv"],
            Self::Conductivity => &["conductivity_ms_cm"],
            Self::ChemicalConcentration => &["concentration_mol_l"],
            Self::ProcessAnalyticsFtir => &[
                "absorbance_lambda_1",
                "absorbance_lambda_2",
                "absorbance_lambda_3",
                "absorbance_lambda_4",
                "optical_path_length_cm",
            ],
            Self::DissolvedOxygen => &["dissolved_oxygen_mg_l"],
            Self::ToxicGas => &["carbon_monoxide_co_ppm", "sensor_health_warning"],
            Self::CombustibleGas => &["combustible_gas_lel_pct"],
            Self::Photoionization => &["voc_pid_ppm"],
            Self::Level => &["tank_level_pct", "fluid_volume_liters"],
            Self::MassFlow => &["mass_flow_rate_l_min"],
            Self::LoadCell => &["load_cell_weight_kg", "mixer_active"],
            // Metric names are declared per sensor in SensorConfig::scenario.
            Self::ScenarioSignal => &[],
            Self::DataCenterRack => &[
                "cpu_utilization",
                "temperature_f",
                "relative_humidity_pct",
                "day_of_week",
            ],
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelsConfig {
    pub weather: WeatherModelConfig,
    pub sound: SoundModelConfig,
    pub electrical: ElectricalModelConfig,
    pub ph: DriftingModelConfig,
    pub orp: DriftingModelConfig,
    pub conductivity: DriftingModelConfig,
    pub process: ProcessModelConfig,
    pub gas: GasModelConfig,
    pub hydraulics: HydraulicsModelConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WeatherModelConfig {
    pub initial_temperature_c: f64,
    pub initial_pressure_hpa: f64,
    pub initial_humidity_pct: f64,
    pub temperature_measurement_noise_sigma: f64,
    pub temperature_drift_sigma: f64,
    pub pressure_measurement_noise_sigma: f64,
    pub pressure_drift_sigma: f64,
    pub humidity_noise_sigma: f64,
    pub storm_transition_rate: f64,
    pub storm_temperature_drop_c: f64,
    pub storm_pressure_drop_hpa: f64,
    pub storm_humidity_rise_pct: f64,
    pub diurnal_temperature_amplitude_c: f64,
    pub pressure_tide_amplitude_hpa: f64,
    pub storm_temperature_noise_multiplier: f64,
    pub storm_humidity_noise_multiplier: f64,
    pub storm_pressure_noise_multiplier: f64,
    pub storm_diurnal_amplitude_reduction_c: f64,
    pub temperature_cycle_phase_hour: f64,
    pub temperature_humidity_coupling: f64,
    pub storm_pressure_tide_reduction_hpa: f64,
    pub pressure_tides_per_day: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SoundModelConfig {
    pub ambient_floor_db: f64,
    pub noise_sigma: f64,
    pub spike_probability_per_sample: f64,
    pub decay_rate: f64,
    pub spike_min_db: f64,
    pub spike_max_db: f64,
    pub spike_cutoff_db: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ElectricalModelConfig {
    pub nominal_voltage_v: f64,
    pub grid_impedance_ohm: f64,
    pub trip_threshold_amps: f64,
    pub phase_impedance_ohm: [f64; 3],
    pub base_power_factor: [f64; 3],
    pub background_voltage_thd_pct: f64,
    pub polluted_voltage_thd_pct: f64,
    pub fault_impedance_ohm: f64,
    pub voltage_noise_sigma: f64,
    pub current_noise_sigma: f64,
    pub power_factor_noise_sigma: f64,
    pub thd_noise_sigma: f64,
    pub current_thd_divisor_amps: f64,
    pub current_thd_multiplier: f64,
    pub thd_power_factor_penalty: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DriftingModelConfig {
    pub target_mean: f64,
    pub measurement_noise_sigma: f64,
    pub drift_sigma: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProcessModelConfig {
    pub initial_concentration_mol_l: f64,
    pub target_equilibrium_mol_l: f64,
    pub reversion_rate: f64,
    pub optical_path_length_cm: f64,
    pub molar_extinction: [f64; 4],
    pub optical_noise_sigma: f64,
    pub optical_drift_sigma: f64,
    pub concentration_noise_sigma: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GasModelConfig {
    pub initial_temperature_c: f64,
    pub initial_humidity_pct: f64,
    pub baseline_co_ppm: f64,
    pub baseline_lel_pct: f64,
    pub baseline_voc_ppm: f64,
    pub hazard_co_ppm: f64,
    pub hazard_lel_pct: f64,
    pub hazard_voc_ppm: f64,
    pub safety_noise_sigma: f64,
    pub degradation_drift_sigma: f64,
    pub co_warning_ppm: f64,
    pub lel_warning_pct: f64,
    pub humidity_quenching_per_pct: f64,
    pub humidity_quenching_threshold_pct: f64,
    pub dissolved_oxygen_saturation_coefficients: [f64; 3],
    pub dissolved_oxygen_noise_multiplier: f64,
    pub co_noise_multiplier: f64,
    pub lel_noise_multiplier: f64,
    pub voc_noise_multiplier: f64,
    pub ventilation_poisoning_retention: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HydraulicsModelConfig {
    pub max_capacity_liters: f64,
    pub initial_fill_pct: f64,
    pub fluid_density_kg_l: f64,
    pub tank_tare_weight_kg: f64,
    pub base_inflow_l_min: f64,
    pub base_outflow_l_min: f64,
    pub high_discharge_l_min: f64,
    pub mixer_rpm: f64,
    pub mixer_harmonic_amplitude_kg: f64,
    pub level_noise_sigma: f64,
    pub flow_noise_sigma: f64,
    pub load_cell_noise_sigma: f64,
    pub mixer_start_after_seconds: Option<f64>,
    pub high_discharge_after_seconds: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::{AppConfig, ConfigurationError};

    #[test]
    fn sample_configuration_loads_and_defines_all_sensor_types() {
        let config = AppConfig::load(super::sample_config_path()).unwrap();
        let count: usize = config
            .entities
            .iter()
            .map(|entity| entity.sensors.len())
            .sum();
        assert_eq!(count, 1);
        assert_eq!(config.entities.len(), 1);
        assert_eq!(config.anomalies.profiles.len(), 1);
    }

    #[test]
    fn anomaly_target_metric_must_belong_to_its_sensor() {
        let mut config = AppConfig::load(super::sample_config_path()).unwrap();
        config.anomalies.profiles[0].target.metric = "not_a_weather_metric".into();

        let error = config.validate().unwrap_err();
        assert!(matches!(error, ConfigurationError::Validation(_)));
        assert!(error
            .to_string()
            .contains("not emitted by sensor 'rack-telemetry-01'"));
    }

    #[test]
    fn anomaly_threshold_must_be_a_probability_boundary() {
        let mut config = AppConfig::load(super::sample_config_path()).unwrap();
        config.anomalies.profiles[0].trigger.threshold = 1.01;

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("must be between 0 and 1"));
    }

    #[test]
    fn production_scenario_configuration_loads() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../config/simulation.yaml.prd");
        AppConfig::load(path).unwrap();
    }

    #[test]
    fn test_rack_configuration_emits_96_points_per_day_at_10_second_days() {
        let config = AppConfig::load(super::sample_config_path()).unwrap();
        let sensor = &config.entities[0].sensors[0];
        let rack = sensor.data_center_rack.as_ref().unwrap();

        assert_eq!(sensor.timestep_ms, 104);
        assert_eq!(rack.simulated_week_duration_seconds, 70.0);
    }

    #[test]
    fn dataset_exporter_is_exclusive_with_streaming_exporters() {
        let mut config = AppConfig::load(super::sample_config_path()).unwrap();
        config.exporters.dataset.enabled = true;
        config.exporters.dataset.start_time = Some("2026-01-01T00:00:00Z".into());
        config.exporters.kafka.enabled = true;

        let error = config.validate().unwrap_err();
        assert!(error
            .to_string()
            .contains("dataset exporter cannot be enabled with Kafka or Prometheus exporters"));
    }

    #[test]
    fn dataset_exporter_requires_a_historical_start_time() {
        let mut config = AppConfig::load(super::sample_config_path()).unwrap();
        config.exporters.prometheus.enabled = false;
        config.exporters.kafka.enabled = false;
        config.exporters.dataset.enabled = true;
        config.exporters.dataset.start_time = None;

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("dataset.start_time is required"));
    }
}
