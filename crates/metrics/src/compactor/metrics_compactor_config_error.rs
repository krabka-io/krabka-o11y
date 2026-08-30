/// Errors raised while configuring the metrics compactor role.
#[derive(Debug, thiserror::Error)]
pub enum MetricsCompactorConfigError {
    #[error("metrics compactor config `{field}` must not be empty")]
    Empty { field: &'static str },

    #[error("metrics compactor poll_timeout must be non-zero")]
    ZeroPollTimeout,

    #[error("metrics compactor flush_max_rows must be non-zero")]
    ZeroFlushMaxRows,
}
