use super::Error;

/// Why a requested log level did not take effect.
#[derive(Debug, Error)]
pub enum LogLevelError {
    #[error(
        "this process installed its logging without a reload handle, so its level is fixed at start-up: set RUST_LOG and restart"
    )]
    Fixed,
    #[error("log filter reload failed: {0}")]
    Reload(String),
}
