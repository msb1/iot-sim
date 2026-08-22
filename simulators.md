# Simulators and sensors

The configuration defines 15 logical sensor types. Some are individual models and some are output channels of a shared, coupled physical model. A logical sensor can be replicated with `quantity`; the sample facility currently configures one instance of each type.

## Inventory

| Service / model | Entity in sample config | Logical sensor | Sensor ID | Interval | Metrics emitted |
| --- | --- | --- | --- | ---: | --- |
| Weather service / weather station | `facility_weather_station_01` | Weather | `weather-01` | 5,000 ms | `temperature_c`, `humidity_pct`, `pressure_hpa` |
| Sound service / sound model | `facility_weather_station_01` | Sound | `sound-01` | 1,000 ms | `decibels_db` |
| Electrical service / three-phase model | `cnc_milling_machine_01` | Electrical | `elec-01` | 1,000 ms | Three-phase voltage, current, real/apparent power, power factor, THD, cumulative active/apparent energy, safety relay state |
| Electrochemistry service / drifting model | `water_quality_skid_01` | pH | `ph-01` | 2,000 ms | `ph_units` |
| Electrochemistry service / drifting model | `water_quality_skid_01` | ORP | `orp-01` | 3,000 ms | `orp_mv` |
| Electrochemistry service / drifting model | `water_quality_skid_01` | Conductivity | `cond-01` | 2,500 ms | `conductivity_ms_cm` |
| Process analytics service / reactor model | `batch_reactor_01` | Chemical concentration | `chem-01` | 1,500 ms | `concentration_mol_l` |
| Process analytics service / reactor model | `batch_reactor_01` | FTIR process analytics | `spec-01` | 4,000 ms | Four absorbance bands and `optical_path_length_cm` |
| Gas safety service / safety model | `batch_reactor_01` | Dissolved oxygen | `do-01` | 3,000 ms | `dissolved_oxygen_mg_l` |
| Gas safety service / safety model | `batch_reactor_01` | Toxic gas | `gas-toxic-01` | 1,000 ms | `carbon_monoxide_co_ppm`, `sensor_health_warning` |
| Gas safety service / safety model | `batch_reactor_01` | Combustible gas | `gas-comb-01` | 1,000 ms | `combustible_gas_lel_pct` |
| Gas safety service / safety model | `batch_reactor_01` | Photoionization detector | `voc-01` | 2,000 ms | `voc_pid_ppm` |
| Hydraulics service / tank model | `mixing_tank_01` | Level | `level-01` | 2,000 ms | `tank_level_pct`, `fluid_volume_liters` |
| Hydraulics service / tank model | `mixing_tank_01` | Mass flow | `flow-01` | 1,000 ms | `mass_flow_rate_l_min` |
| Hydraulics service / tank model | `mixing_tank_01` | Load cell | `load-01` | 2,500 ms | `load_cell_weight_kg`, `mixer_active` |

## Simulator behavior

### Weather station

The weather simulator produces correlated outdoor temperature, humidity, and pressure. Temperature and pressure have persistent drift plus measurement noise; temperature also follows a diurnal cycle. An optional storm state changes temperature, pressure, humidity, cycle amplitude, and noise levels smoothly over time. Its parameters are under `models.weather`.

### Sound

The sound simulator combines a noisy ambient floor with occasional machinery-like spikes. Spikes are sampled from the configured dB range, decay exponentially, and are combined with ambient sound in the linear-power domain. Its parameters are under `models.sound`.

### Three-phase electrical

The electrical simulator models phases A, B, and C together: phase impedance determines current; current and grid impedance determine voltage; current contributes to voltage THD; THD affects power factor; and real/apparent energy accumulate over elapsed time. An over-current relay latches and zeros all phase currents when the configured threshold is exceeded. Parameters are under `models.electrical`.

### Electrochemistry: pH, ORP, conductivity

These three sensors use independent instances of the shared drifting-sensor model. Each has a configured target mean, persistent calibration/fouling drift, and per-reading measurement noise. They are grouped in one service only for consistent scheduling and lifecycle management; they do not share physical state. Parameters are under `models.ph`, `models.orp`, and `models.conductivity`.

### Process analytics: concentration and FTIR

Concentration follows a noisy mean-reverting process. The FTIR sensor uses that same current concentration with Beer-Lambert-style absorbance across four configured extinction bands, optical path length, and per-channel optical drift/noise. Therefore concentration and FTIR are coupled outputs of one reactor model even though their reporting intervals differ. Parameters are under `models.process`.

### Gas environmental safety

The gas-safety model couples four outputs through shared ambient conditions and safety state. Dissolved oxygen derives from temperature with membrane drift; toxic gas reports CO; combustible gas reports percent LEL with catalytic-bead degradation; and the photoionization detector reports VOC with humidity quenching. The toxic-gas sensor also emits a warning flag when CO or LEL crosses the configured threshold. Parameters are under `models.gas`.

### Fluid hydraulics: level, flow, load cell

The hydraulics model maintains a shared tank volume. Inflow and outflow drive the volume and level; outflow is measured as mass flow; volume and density determine load-cell weight. Configured scenario timings can start a mixer and increase discharge flow. When the mixer is active, the load cell includes a harmonic vibration component. Parameters are under `models.hydraulics`.

## Adding or scaling sensors

Add a sensor entry under an entity in `config/simulation.yaml`, choose one of the existing `type` values, and set `id_prefix`, `quantity`, `timestep_ms`, `min_value`, and `max_value`. Sensors of an existing type use that type's configured model parameters. A completely new sensor type requires four additions: a `SensorType` value, configuration schema, a simulator service or existing-service mapping, and exporter-compatible metrics.

For reliable analytics, keep `sensor_id` stable across runs and change only scenario/model parameters when you want a different signal profile. `entity_id` is also the Kafka key, so changing it changes Kafka partition affinity and ordering scope.
