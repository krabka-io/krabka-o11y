use super::*;

pub(crate) fn compactor_delete_requests_for_config(
    config: &ServiceConfig,
    provided: Option<SharedLogDeleteRequests>,
) -> Result<SharedLogDeleteRequests, LogDeleteRequestStoreError> {
    match provided {
        Some(delete_requests) => Ok(delete_requests),
        None => SharedLogDeleteRequests::from_data_root(&config.data_root),
    }
}
