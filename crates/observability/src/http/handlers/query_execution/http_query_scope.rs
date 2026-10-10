use super::{QuerierState, QueryKind, TimeRange};

/// The querier, tenant, window and kind one HTTP query runs with.
#[derive(Clone, Copy)]
pub(crate) struct HttpQueryScope<'a> {
    pub(crate) state: &'a QuerierState,
    pub(crate) tenant: &'a str,
    pub(crate) time_range: TimeRange,
    pub(crate) step: Option<i64>,
    pub(crate) kind: QueryKind,
}
