# iot-sim

`iot-sim` is an asynchronous Rust simulator for analytics development and integration testing. It produces 15 logical IoT sensors attached to five industrial entities, schedules each sensor at its own configured interval, and exports timestamped telemetry to Prometheus and Kafka.

Read [architecture.md](architecture.md) for the component design and data flow, and [simulators.md](simulators.md) for the simulator and sensor inventory.

## Prerequisites

- Rust stable (install with [rustup](https://rustup.rs/));
- a Prometheus server when testing the Prometheus integration;
- a Kafka broker when testing the Kafka integration;
- a system `librdkafka` installation when using the checked-in Kafka-enabled configuration.

## Run locally

The supplied configuration starts the Prometheus exporter on `127.0.0.1:9898` and leaves Kafka disabled.

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
- value: one compact JSON message per sensor metric: `entity_type`, `entity_id`, `sensor_id`, `sensor_type`, `timestamp_ms`, `metric`, and numeric `value`. The value never contains Kafka headers.

The producer uses idempotence and `acks=all`. The default topic is deliberately multi-tenant (`iot.telemetry.v1`) rather than one topic per sensor. The message schema is [schemas/entity-telemetry.schema.json](schemas/entity-telemetry.schema.json).

## Configuration

[config/simulation.yaml](config/simulation.yaml) defines entities, sensors, model operating parameters, exporter settings, and lifecycle controls.

Each logical sensor has an ID prefix, quantity, enabled flag, output bounds, and `timestep_ms`. A quantity greater than one expands IDs as `<id_prefix>-01`, `<id_prefix>-02`, and so on. Startup validates duplicate entity IDs, duplicate expanded sensor IDs, zero timesteps, invalid bounds, and exporter prerequisites.

The coupled physical models are preserved even when their child sensor intervals differ: concentration and FTIR share a process state; the four gas sensors share an environmental-safety state; and level, flow, and load-cell share tank-hydraulics state.

## Development checks

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
```

To build a Prometheus-only binary, use `cargo run --no-default-features` with Kafka disabled through configuration or `IOT_SIM__EXPORTERS__KAFKA__ENABLED=false`.
