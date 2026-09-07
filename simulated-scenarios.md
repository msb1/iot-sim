# Simulated industrial IoT scenarios

This workload is the source-side companion to
`../anomaly-detect/anomaly-test-scenarios.md`. The checked-in
`config/simulation.yaml` contains only the sensors needed for the scenarios
below. Existing simulator sensor implementations have not been removed.

## Execution contract

- All telemetry is published to `iot.telemetry.v1`; Kafka headers identify the
  entity, sensor, sensor type, and metric.
- `scenario_signal` is a configurable single-metric sensor supporting
  constant, sinusoidal, step-cycle, and batch-temperature baselines with
  deterministic seeded noise.
- Anomalies run after baseline generation and before every exporter.
  `trigger.start_after_samples` provides deterministic normal-data warm-up;
  the large cooldown makes each checked-in event run once.
- Cadence is real wall-clock cadence. The 14-day FCCU trend takes its stated
  time unless source cadences and matching detector intervals are accelerated
  together.
- The checked-in rack test configuration emits one complete 672-sample week
  every 70 wall-clock seconds. Kafka timestamps and `interval_ms` reflect the
  approximately 104 ms wall-clock cadence; each logical day contains 96
  fifteen-minute points and takes 10 wall-clock seconds.

## Water treatment and pumping instrumentation

`municipal_water_system_01` supplies pH, conductivity, turbidity, and free
chlorine at five-second cadence. Filter differential pressure and flow cover
loading/backwash decisions. Pump vibration, bearing temperature, active power,
wet-well level, and line pressure cover condition monitoring, energy/flow
optimization, level control, overflow prevention, and dry-run risk. These
baseline channels support broader experiments even though the checked-in
detector config selects only water-hammer pressure from this entity.

## Scenario matrix

| Class | Scenario | Source signature | Activation |
| --- | --- | --- | --- |
| Point | Pumping-station water hammer | `line_pressure_psi`, 500 ms, 60 ± roughly 2 PSI; 180 PSI then 35 PSI then baseline | After 43,210 normal samples (just over 6 hours) |
| Collective/contextual | Data-center Sunday cooling failure | One correlated 15-minute rack record: `cpu_utilization`, `temperature_f`, `relative_humidity_pct`, and `day_of_week`; healthy humidity is psychrometric at a fixed dew point | Sunday from 08:00 through the end of the weekly cycle |
| Collective | Battery cleanroom frozen transmitter | `moisture_ppm`, 5 seconds, bounded 9.5–10.5 PPM; exactly 10 PPM for 20 minutes while cart passages continue | After 4,500 normal samples, for 240 samples |
| Point | FCCU feed-pump pressure spike | `discharge_pressure_psi`, 250 ms, 320 ± 5 PSI; one 750 PSI point | After 86,410 normal samples (just over 6 hours) |
| Contextual | FCCU hot-day coolant restriction | 1-minute ambient/flow pair; normal 39 C/350 LPM and 5 C/80 LPM; hot period forced to 80 LPM | After 480 hot samples, for 120 samples |
| Collective | FCCU catalyst wall erosion | Two hourly skin thermocouples with 210–230 C daily waves; sensor 01 gains 1.4 C across 336 hours while sensor 02 remains normal | After 14 normal days, for 14 days |
| Point | Stripping-column flooding impulse | `differential_pressure_psi`, 250 ms, 12 ± 0.4 PSI; one 45 PSI point | After 86,410 normal samples (just over 6 hours) |
| Contextual | Wrong low-viscosity reactor recipe | 1-minute current and batch step; normal current is 5/15/28/28 A, then 5 A in phase 3/4 | After 480 samples, for 60 samples when phase condition is met |
| Collective | Reactor jacket fouling | 1-minute temperature array; 15-minute rise, 45-minute 85 C plateau, 45-minute cool; cooling grows to 60 minutes over 15 batches | After five normal 105-minute batches, for 1,688 samples |

## Expected anomaly shape and evaluation

### Water-hammer pressure

`spike_drop` produces a two-sample impulse rather than a generic high point:
180 PSI is followed by 35 PSI and then the baseline resumes near 60 PSI. A
point detector should flag 180 PSI immediately; the following collapse may
also score anomalously and is retained as part of the physical signature.

### Sunday rack cooling failure

The rack's 672-sample cycle starts on Monday (`day_of_week=1`). Weekday CPU
is 0.825 during 08:00–18:00 and 0.25 otherwise; weekend CPU idles near 0.10.
Exhaust temperature follows CPU with a first-order cooling lag. Relative humidity
is calculated with August-Roche-Magnus from a constant managed dew point, so it
falls as normal exhaust temperature rises.

At Sunday 08:00, CPU remains 0.10 while temperature is forced to 85 F and RH to
22%. Those values can each appear plausible alone, but their combination with an
idle CPU and a Sunday timestamp is the collective/contextual anomaly.

### Cleanroom frozen sequence

Normal readings use bounded white noise around 10 PPM. During the event every
moisture point is 10 PPM, safely inside 9.5–10.5 PPM, while the independent cart
passage channel continues. Evidence is zero rolling variance and loss of the
learned temporal relationship; no individual point is an outlier.

### FCCU scenarios

The pressure spike is an isolated physical outlier. Coolant flow is contextual:
80 LPM is normal in the paired cold period but anomalous at 39 C. Wall erosion
preserves the daily wave and 260 C ceiling, adding only +0.1 C/day to one
thermocouple relative to its companion.

### Reactor and stripper scenarios

The stripper DP spike is isolated. Agitator current is evaluated with batch
step, so 5 A is normal during filling but wrong during final polymer assembly.
The fouling injector emits 15 batch profiles whose cooling duration increases
from 45 to 60 minutes. Values remain at or below 85 C and baseline noise is
replaced by a tight, strained curve during the event.

## Reproducibility and tuning

Each scenario sensor has its own seed. Activation is deterministic because the
trigger probability becomes 1.0 after warm-up. To accelerate a long test,
change source `timestep_ms`, affected anomaly sample counts, and detector
`interval_ms` together. The detector relies on cadence for event-time
interpolation, priming, and sequence alignment.
