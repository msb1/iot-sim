use std::collections::HashSet;
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
                }
            }
        }

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

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExportersConfig {
    #[serde(default)]
    pub prometheus: PrometheusConfig,
    #[serde(default)]
    pub kafka: KafkaConfig,
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
}

impl SensorType {
    pub const ALL: [Self; 15] = [
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
    ];
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
    use super::AppConfig;

    #[test]
    fn sample_configuration_loads_and_defines_all_sensor_types() {
        let config = AppConfig::load(super::sample_config_path()).unwrap();
        let count: usize = config
            .entities
            .iter()
            .map(|entity| entity.sensors.len())
            .sum();
        assert_eq!(count, 15);
        assert_eq!(config.entities.len(), 5);
    }
}
