# Architecture

## Purpose and boundaries

The project separates physical simulation, scheduling, telemetry representation, and transport. A model can evolve without changing exporters; an exporter can change from JSON to Protobuf without changing sensor cadence or physical state.

```text
YAML + environment overrides
            │
            ▼
     typed AppConfig ──► SimulationSystem / IoTEntity / IoTSensor
            │                         │
            ▼                         ▼
  model parameters              scheduled sensor definitions
            │                         │
            └──────────► simulator services ◄──────────┐
                              │                         │
                       timestamped SensorReading         │
                              │                         │
                              ▼                         │
                    anomaly injector                   │
                 (optional, stateful, seeded)           │
                              │                         │
                 ┌────────────┴────────────┐            │
                 ▼                         ▼            │
       Prometheus scrape registry       Kafka JSON       │
                 │                         │            │
              /metrics              one keyed record/entity
```

## Configuration and domain model

`app/src/config.rs` deserializes `config/simulation.yaml` with the `config` crate. The YAML is the source of operating parameters: physical model coefficients, noise and drift, scenario thresholds, sensor bounds, exporter settings, and per-sensor timesteps. Environment variables prefixed `IOT_SIM__` overlay the YAML, which is appropriate for deployment-specific endpoints and credentials.

`app/src/impl/` contains the configured domain objects:

- `SimulationSystem` is the configured facility and exposes sensors grouped by type.
- `IoTEntity` represents a physical or logical asset with a stable `entity_id`.
- `IoTSensor` represents one expanded sensor instance, including its cadence and output bounds.

`quantity` is expanded during this conversion. This means one configuration template can represent a bank of otherwise identical sensors while retaining distinct IDs.

## Models and services

`app/src/models/` owns stochastic and physical state. It knows nothing about output transports, entity IDs, or wall-clock scheduling.

`app/src/services/` adapts models to configured sensors. Every service implements the shared `SimulatorService` trait:

```rust
pub trait SimulatorService: Send {
    fn name(&self) -> &'static str;
    fn next_due(&self) -> Option<Instant>;
    fn sample_due(&mut self, now: Instant, timestamp_ms: i64) -> Vec<SensorReading>;
}
```

Each `SensorSchedule` starts due immediately, then advances by that sensor's configured timestep. If a process wakes late, a schedule catches up to the next future deadline without creating a burst of historical readings. The service advances a coupled model once when any of its child sensors is due and emits only the child sensors that are due. This preserves shared state while allowing different sensor intervals.

The runtime (`services/runtime.rs`) finds the earliest service deadline, sleeps through Tokio until that deadline, samples due services, applies the configured anomaly injector, then sends the resulting batch to every configured exporter. `RunningSimulation::stop` cancels this loop and waits up to the configured shutdown timeout.

## Anomaly layer

`app/src/anomalies.rs` is a stateful, transport-neutral middleware layer between sampling and export. An `AnomalyInjector` owns the seeded random generator, profile activation/cooldown state, and a cache of unmodified baseline metrics. Profiles implement the shared behavior contract:

```rust
pub trait AnomalyStrategy: Send {
    fn duration_samples(&self) -> u64;
    fn can_start(&self, context: &InjectionContext<'_>) -> bool;
    fn start(&mut self, baseline: f64, previous: Option<f64>);
    fn inject(&mut self, value: &mut f64, sample_index: u64, rng: &mut StdRng);
}
```

The injector snapshots all baseline metrics in a scheduler batch before applying profiles. Contextual conditions can therefore inspect correlated values without depending on the order in which services or readings happen to be traversed. Its persistent baseline cache bridges sensors with different timesteps. The cache deliberately stores pre-injection values so a prior anomaly cannot become the reference for a later freeze or condition.

The five concrete strategies cover spikes, freezes, linear drift, high-frequency Gaussian noise, and cross-stream contextual overrides. Triggering and cooldown behavior live in the profile wrapper rather than individual strategies, which gives every anomaly type the same rare-event semantics. Configuration validation resolves exact expanded sensor IDs and verifies metrics against the selected sensor type.

## Telemetry contract

`app/src/telemetry.rs` contains the transport-neutral `SensorReading`:

- identity: `entity_type`, `entity_id`, `sensor_id`, `sensor_type`;
- source time: `timestamp_ms` as Unix epoch milliseconds;
- `sequence`: monotonically increasing per sensor;
- `metrics`: a named numeric map.

Kafka emits one compact message per metric. The JSON form is defined in [schemas/entity-telemetry.schema.json](schemas/entity-telemetry.schema.json): sensor identity, timestamp, metric name, and numeric value. Kafka-specific headers are not duplicated in the value.

## Exporters

`TelemetryExporter` is the asynchronous transport contract:

```rust
async fn start(&self, cancellation: CancellationToken) -> Result<(), ExportError>;
async fn export(&self, readings: &[SensorReading]) -> Result<(), ExportError>;
async fn shutdown(&self) -> Result<(), ExportError>;
```

### Prometheus

The Prometheus exporter owns a registry and serves it with Axum at the configured `bind` and `path` (the sample binds `0.0.0.0:9898/metrics`). It exposes a single labeled gauge family, `iot_sensor_value`, plus timestamp and sequence gauge families. Metric identity is expressed by labels rather than generating a new Prometheus metric name for every sensor field:

`config_name`, `entity_type`, `entity_id`, `sensor_id`, `sensor_type`, and `metric`.

This is a pull exporter; a Prometheus server must scrape it. The source timestamp is retained as a gauge because Prometheus assigns the scrape timestamp to standard exposition samples.

### Kafka

Kafka is behind Cargo's `kafka` feature to avoid making the default simulator build depend on CMake and the native `librdkafka` build. When enabled, `KafkaExporter` uses one topic by default. A record's key is `entity_id`, so Kafka chooses the same partition for each entity and maintains its record ordering. Its only headers are `entity_id`, `sensor_id`, `sensor_type`, and `metric`, enabling consumers to filter streams before deserializing values. The producer enables idempotence and waits for all acknowledgements.

## Source map

| Area | Responsibility |
| --- | --- |
| `app/src/main.rs` | Parse configuration path and own process lifecycle. |
| `app/src/config.rs` | Typed YAML/environment configuration and validation. |
| `app/src/anomalies.rs` | Stateful point, contextual, freeze, drift, and noise injection. |
| `app/src/impl/` | Facility, entity, and expanded sensor definitions. |
| `app/src/models/` | Mathematical and physical simulation state. |
| `app/src/services/` | Common scheduling trait, domain services, Tokio runtime. |
| `app/src/telemetry.rs` | Exporter-neutral readings and entity envelope. |
| `app/src/export/prometheus/` | Metrics registry and scrape HTTP endpoint. |
| `app/src/export/kafka/` | Feature-gated keyed JSON Kafka producer. |
| `schemas/` | Versioned JSON telemetry contract. |
