# iot-sim

`iot-sim` is an asynchronous Rust simulator for analytics development and integration testing. It retains its original 15 fixed sensor types and adds configurable industrial scenario signals, schedules every enabled sensor at its own interval, can inject point/contextual/collective anomalies, and exports timestamped telemetry to Prometheus, Kafka, or a historical Parquet dataset in S3-compatible storage.

Read [architecture.md](architecture.md) for the component design and data flow,
[simulators.md](simulators.md) for the original simulator inventory, and
[simulated-scenarios.md](simulated-scenarios.md) for the water, data-center,
cleanroom, refinery, and reactor anomaly workload.

## Prerequisites

- Rust stable (install with [rustup](https://rustup.rs/));
- a Prometheus server when testing the Prometheus integration;
- a Kafka broker when testing the Kafka integration;
- a system `librdkafka` installation when using the checked-in Kafka-enabled configuration.

## Run locally

The supplied configuration starts the Prometheus exporter on `0.0.0.0:9898` and enables Kafka for the broker addresses in the file. Override `IOT_SIM__EXPORTERS__KAFKA__ENABLED=false` when running without those brokers.

```bash
cargo run -- --config config/simulation.yaml
```

The process runs until `Ctrl-C`/SIGINT. Shutdown is deliberate: it stops scheduling, closes the Prometheus HTTP server gracefully, and flushes Kafka when Kafka is enabled.

For a bounded smoke test, add this to `runtime` in a copy of the YAML:

```yaml
run_duration_seconds: 30
```

Or set the equivalent environment override:

```bash
IOT_SIM__RUNTIME__RUN_DURATION_SECONDS=30 cargo run -- --config config/simulation.yaml
```

All configuration keys can be overridden with `IOT_SIM__` environment variables and `__` between nested keys. This keeps Kafka credentials out of source control; for example, use `IOT_SIM__EXPORTERS__KAFKA__SECURITY__USERNAME` and `IOT_SIM__EXPORTERS__KAFKA__SECURITY__PASSWORD` when those values are available.

To confirm the scheduler and models are generating data, enable the opt-in console stream in the configuration:

```yaml
logging:
  simulator_stream:
    enabled: true
```

Every generated sensor reading is then logged as JSON at `INFO` level. It can also be enabled without changing the file with `IOT_SIM__LOGGING__SIMULATOR_STREAM__ENABLED=true`. Lifecycle logs identify enabled services and exporters; set `RUST_LOG=iot_sim=debug` to additionally see each Prometheus update and confirmed Kafka record delivery.

## Inject anomalies

Anomaly injection is globally off in the supplied configuration. Enable it in a scenario copy with:

```yaml
anomalies:
  enabled: true
  seed: 42                 # omit for a different scenario on each start
  profiles:
    - name: "temperature_spike"
      enabled: true
      type: "spike"
      target:
        sensor_id: "weather-01"
        metric: "temperature_c"
      trigger:
        threshold: 0.999   # event starts when random [0,1) >= threshold
        cooldown_samples: 100
      offset: 70.0
      multiplier: 1.0
```

Every enabled profile evaluates its trigger only when its exact target metric is emitted. Thus `threshold: 0.999` gives approximately a 0.1% activation chance per target sample, `0` always activates when eligible, and `1` never activates. `cooldown_samples` counts later target emissions after an event. A fixed `seed` makes trigger decisions and Gaussian noise repeatable.

The available profile types are `spike`, `stuck_at`, `drift`, `noise`, and `contextual`. Multi-sample durations are measured in emissions of the target sensor, not wall-clock seconds. A `stuck_at` profile with no `value` captures the last valid baseline reading; a contextual profile evaluates another sensor/metric from the current scheduler batch or its latest cached baseline. See the fully annotated profiles in [config/simulation.yaml](config/simulation.yaml) and detailed semantics in [simulators.md](simulators.md).

Injection occurs after normal sensor-bound clamping, intentionally allowing an anomaly to exceed `min_value` or `max_value`. The telemetry schema is unchanged and readings are not labeled as anomalous, so downstream detection systems receive the same contract in baseline and anomaly scenarios. Each activation is recorded in the simulator log with its profile, sensor, metric, and duration.

## Test the Prometheus integration

Prometheus is a pull system. The simulator exposes current values at its HTTP endpoint; Prometheus scrapes that endpoint rather than receiving pushed samples.

1. Start the simulator with the sample configuration.
2. Confirm its endpoint responds:

   ```bash
   curl --fail http://127.0.0.1:9898/metrics
   ```

   The output should include `iot_sensor_value`, `iot_sensor_timestamp_milliseconds`, and `iot_sensor_sequence`.

3. Add a scrape target to the Prometheus server configuration. If Prometheus runs on the host, use:

   ```yaml
   scrape_configs:
     - job_name: iot-sim
       scrape_interval: 5s
       static_configs:
         - targets: ["127.0.0.1:9898"]
   ```

   If Prometheus runs in Docker or Kubernetes, use an address reachable from that environment instead of `127.0.0.1`; update `exporters.prometheus.bind` accordingly, for example `0.0.0.0:9898` for a containerized local test.

4. Reload Prometheus, then verify the target is `UP` on its Targets page or query:

   ```promql
   up{job="iot-sim"}
   ```

5. Explore samples with labels that identify the entity, sensor, and metric:

   ```promql
   iot_sensor_value{entity_id="batch_reactor_01"}
   iot_sensor_timestamp_milliseconds{sensor_id="gas-toxic-01"}
   ```

`iot_sensor_timestamp_milliseconds` is the simulator's source time for the latest reading. Prometheus's own sample timestamp remains scrape time, which is the normal behavior for a pull exporter.

## Test the Kafka integration

Kafka support is compiled into the default binary because the checked-in configuration enables it. Verify the build with:

```bash
cargo check
```

Configure a broker and enable Kafka using environment overrides (or a deployment-specific YAML copy):

```bash
IOT_SIM__EXPORTERS__KAFKA__ENABLED=true \
IOT_SIM__EXPORTERS__KAFKA__BROKERS=localhost:9092 \
IOT_SIM__EXPORTERS__KAFKA__TOPIC=iot.telemetry.v1 \
cargo run -- --config config/simulation.yaml
```

For authenticated clusters, also provide the security protocol, mechanism, username, and password through the matching `IOT_SIM__EXPORTERS__KAFKA__SECURITY__...` variables.

Consume the topic with your normal Kafka client. For example, with `kcat`:

```bash
kcat -b localhost:9092 -C -t iot.telemetry.v1 -o beginning -f 'key=%k headers=%h value=%s\n'
```

Each record uses:

- key: the stable `entity_id`, so all messages for an entity are partitioned and ordered together;
- headers: `entity_id`, `sensor_id`, `sensor_type`, and `metric`;
- value: one compact JSON message per sensor metric: `entity_type`, `entity_id`, `sensor_id`, `sensor_type`, `timestamp_ms`, positive `interval_ms`, `metric`, and numeric `value`. The value never contains Kafka headers.

The producer uses idempotence and `acks=all`. The default topic is deliberately multi-tenant (`iot.telemetry.v1`) rather than one topic per sensor. The message schema is [schemas/entity-telemetry.schema.json](schemas/entity-telemetry.schema.json).

## Generate a historical dataset

Enable the `dataset` exporter to generate a finite dataset from an RFC 3339 `start_time` through the real time at process start. The scheduler advances every model using its configured cadence, but each output record receives its true historical source timestamp; it does not emit accelerated test timestamps. Enabled anomaly profiles are injected through the same path as streaming mode.

Dataset mode is exclusive: both Kafka and Prometheus must be disabled. It writes all simulated data for a run to one Snappy-compressed, long-form Parquet object under `dataset/` in the configured bucket. The filename is `<entity-id>-<ISO-8601-start-time>.parquet`, using the first emitted entity ID, so each simulator dataset is identifiable.

```yaml
exporters:
  prometheus: { enabled: false }
  kafka: { enabled: false }
  dataset:
    enabled: true
    start_time: "2026-08-01T00:00:00Z"
    s3_endpoint_url: "http://192.168.1.50:9000"
    s3_bucket_name: "iotsim"
    s3_access_key: "access"
    s3_secret_key: "secret"
```

Run it normally with that configuration. Completion uploads one object, for example `dataset/data_center_rack_01-2026-08-01T00:00:00Z.parquet`, to RustFS and exits. The target bucket must already exist and the configured credentials must have `PutObject` permission. For deployments, override S3 credentials with `IOT_SIM__EXPORTERS__DATASET__S3_ACCESS_KEY` and `IOT_SIM__EXPORTERS__DATASET__S3_SECRET_KEY` instead of storing credentials in YAML.

Each Parquet row represents one sensor metric and is labeled with `entity_type`, `entity_id`, `sensor_id`, `sensor_type`, `timestamp_ms`, `interval_ms`, `sequence`, `metric`, and `value`.

## Configuration

[config/simulation.yaml](config/simulation.yaml) defines entities, sensors, model operating parameters, anomaly profiles, exporter settings, and lifecycle controls.

Each logical sensor has an ID prefix, quantity, enabled flag, output bounds, and `timestep_ms`. A quantity greater than one expands IDs as `<id_prefix>-01`, `<id_prefix>-02`, and so on. Startup validates duplicate entity IDs, duplicate expanded sensor IDs, zero timesteps, invalid bounds, exporter prerequisites, anomaly thresholds/durations, anomaly sensor/metric references, dataset start dates, and the exclusive dataset-exporter rule.

The coupled physical models are preserved even when their child sensor intervals differ: concentration and FTIR share a process state; the four gas sensors share an environmental-safety state; and level, flow, and load-cell share tank-hydraulics state.

## Development checks

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
```

To build a Prometheus-only binary, use `cargo run --no-default-features` with Kafka disabled through configuration or `IOT_SIM__EXPORTERS__KAFKA__ENABLED=false`.
