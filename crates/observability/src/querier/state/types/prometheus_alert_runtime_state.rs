#[derive(Clone, Debug)]
pub(crate) struct PrometheusAlertRuntimeState {
    pub(crate) active_at: i64,
    pub(crate) last_active_at: i64,
    pub(crate) value: String,
}
