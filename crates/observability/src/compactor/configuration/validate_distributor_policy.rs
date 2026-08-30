use super::*;

pub(crate) fn validate_distributor_policy(
    config: &ServiceConfig,
) -> Result<(), ServiceConfigError> {
    if config.wal_connect_attempt_timeout > config.wal_connect_startup_deadline {
        return Err(ServiceConfigError::WalConnectAttemptExceedsDeadline);
    }
    if config.wal_connect_initial_backoff > config.wal_connect_max_backoff {
        return Err(ServiceConfigError::WalConnectInitialBackoffExceedsMaximum);
    }
    Ok(())
}
