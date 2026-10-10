use super::{Arc, HeaderMap, Principal, ProfileStore, QuerierState};

/// What a querier Connect handler extracted from the request parts: the
/// shared querier state, the authenticated caller, and the request headers.
pub(crate) struct QuerierRequestParts<S: ProfileStore> {
    pub(crate) state: Arc<QuerierState<S>>,
    pub(crate) principal: Principal,
    pub(crate) headers: HeaderMap,
}
