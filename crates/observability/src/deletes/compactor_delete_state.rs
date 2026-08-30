use super::SharedLogDeleteRequests;

#[derive(Clone, Default)]
pub(crate) struct CompactorDeleteState {
    pub(crate) delete_requests: SharedLogDeleteRequests,
}
