use super::*;

pub(crate) fn validate_compactor_policy(config: &ServiceConfig) -> Result<(), ServiceConfigError> {
    if config.compactor_accumulation_poll_timeout > config.compactor_accumulation_window {
        return Err(ServiceConfigError::CompactorAccumulationPollExceedsWindow);
    }
    if config.compactor_object_store_initial_backoff > config.compactor_object_store_max_backoff {
        return Err(ServiceConfigError::CompactorObjectStoreInitialBackoffExceedsMaximum);
    }
    Ok(())
}
