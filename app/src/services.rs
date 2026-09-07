pub mod base;
mod data_center_rack;
mod electrical;
mod electrochemistry;
mod environment;
mod gas;
mod hydraulics;
mod process;
mod runtime;
mod scenario;

pub use base::SimulatorService;
pub use runtime::{RunningSimulation, SimulationError, SimulationRuntime};

use std::collections::HashMap;
use std::time::Instant;

use crate::config::{AppConfig, SensorType};
use crate::domain::{ScheduledSensor, SimulationSystem};

use data_center_rack::DataCenterRackService;
use electrical::ElectricalService;
use electrochemistry::ElectrochemistryService;
use environment::{SoundService, WeatherService};
use gas::GasSafetyService;
use hydraulics::HydraulicsService;
use process::ProcessService;
use scenario::ScenarioService;

pub fn build_services(
    config: &AppConfig,
    system: &SimulationSystem,
) -> Vec<Box<dyn SimulatorService>> {
    let start = Instant::now();
    let mut by_type = system.sensors_by_type();
    let mut take = |kind| by_type.remove(&kind).unwrap_or_default();
    let weather = take(SensorType::Weather);
    let sound = take(SensorType::Sound);
    let electrical = take(SensorType::Electrical);
    let ph = take(SensorType::Ph);
    let orp = take(SensorType::Orp);
    let conductivity = take(SensorType::Conductivity);
    let concentration = take(SensorType::ChemicalConcentration);
    let ftir = take(SensorType::ProcessAnalyticsFtir);
    let dissolved_oxygen = take(SensorType::DissolvedOxygen);
    let toxic = take(SensorType::ToxicGas);
    let combustible = take(SensorType::CombustibleGas);
    let voc = take(SensorType::Photoionization);
    let level = take(SensorType::Level);
    let flow = take(SensorType::MassFlow);
    let load = take(SensorType::LoadCell);
    let scenario = take(SensorType::ScenarioSignal);
    let data_center_rack = take(SensorType::DataCenterRack);

    let mut services: Vec<Box<dyn SimulatorService>> = Vec::new();
    if !weather.is_empty() {
        services.push(Box::new(WeatherService::new(
            &config.models.weather,
            weather,
            start,
        )));
    }
    if !sound.is_empty() {
        services.push(Box::new(SoundService::new(
            &config.models.sound,
            sound,
            start,
        )));
    }
    if !electrical.is_empty() {
        services.push(Box::new(ElectricalService::new(
            &config.models.electrical,
            electrical,
            start,
        )));
    }
    if !ph.is_empty() || !orp.is_empty() || !conductivity.is_empty() {
        services.push(Box::new(ElectrochemistryService::new(
            (&config.models.ph, ph),
            (&config.models.orp, orp),
            (&config.models.conductivity, conductivity),
            start,
        )));
    }
    if !concentration.is_empty() || !ftir.is_empty() {
        services.push(Box::new(ProcessService::new(
            &config.models.process,
            concentration,
            ftir,
            start,
        )));
    }
    if !dissolved_oxygen.is_empty()
        || !toxic.is_empty()
        || !combustible.is_empty()
        || !voc.is_empty()
    {
        services.push(Box::new(GasSafetyService::new(
            &config.models.gas,
            [dissolved_oxygen, toxic, combustible, voc],
            start,
        )));
    }
    if !level.is_empty() || !flow.is_empty() || !load.is_empty() {
        services.push(Box::new(HydraulicsService::new(
            &config.models.hydraulics,
            [level, flow, load],
            start,
        )));
    }
    if !scenario.is_empty() {
        services.push(Box::new(ScenarioService::new(scenario, start)));
    }
    if !data_center_rack.is_empty() {
        services.push(Box::new(DataCenterRackService::new(
            data_center_rack,
            start,
            config.exporters.dataset.enabled,
        )));
    }
    for service in &services {
        tracing::info!(service = service.name(), "simulator service enabled");
    }
    services
}

#[allow(dead_code)]
fn _type_check(_: HashMap<SensorType, Vec<ScheduledSensor>>) {}
