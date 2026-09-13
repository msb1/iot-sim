use serde::Serialize;

/// The correlated numeric portion of a robotic material-air-lock frame.
/// Exporters flatten these fields into their existing per-metric contract.
#[derive(Debug, Clone, Serialize)]
pub struct RoboticAirLockTelemetry {
    pub dew_point_c: f64,
    pub ambient_temperature_c: f64,
    pub relative_humidity_pct: f64,
    pub differential_pressure_pa: f64,
    pub purge_flow_rate_cfm: f64,
    pub outer_door_open: f64,
    pub inner_door_open: f64,
    pub inflatable_seal_pressure_bar: f64,
    pub cart_presence_detected: f64,
    pub cart_speed_m_s: f64,
    pub cycle_status_code: f64,
    pub equipment_state_code: f64,
}

impl RoboticAirLockTelemetry {
    pub fn metrics(self) -> Vec<(String, f64)> {
        vec![
            ("dew_point_c".into(), self.dew_point_c),
            ("ambient_temperature_c".into(), self.ambient_temperature_c),
            ("relative_humidity_pct".into(), self.relative_humidity_pct),
            (
                "differential_pressure_pa".into(),
                self.differential_pressure_pa,
            ),
            ("purge_flow_rate_cfm".into(), self.purge_flow_rate_cfm),
            ("outer_door_open".into(), self.outer_door_open),
            ("inner_door_open".into(), self.inner_door_open),
            (
                "inflatable_seal_pressure_bar".into(),
                self.inflatable_seal_pressure_bar,
            ),
            ("cart_presence_detected".into(), self.cart_presence_detected),
            ("cart_speed_m_s".into(), self.cart_speed_m_s),
            ("cycle_status_code".into(), self.cycle_status_code),
            ("equipment_state_code".into(), self.equipment_state_code),
        ]
    }
}
