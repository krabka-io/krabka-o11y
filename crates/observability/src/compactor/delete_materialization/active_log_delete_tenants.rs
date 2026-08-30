use super::{ActiveLogDeleteFilterError, BTreeSet, SharedLogDeleteRequests};

pub(crate) fn active_log_delete_tenants(
    delete_requests: &SharedLogDeleteRequests,
) -> Result<BTreeSet<String>, ActiveLogDeleteFilterError> {
    delete_requests.refresh()?;
    let requests = delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    Ok(requests
        .requests
        .iter()
        .map(|request| request.tenant.clone())
        .collect())
}
