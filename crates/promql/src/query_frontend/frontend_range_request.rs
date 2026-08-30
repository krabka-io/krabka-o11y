use super::{QueryFrontendOptions, Time};

/// One user range query that enters the query-frontend.
#[derive(Clone, Debug, PartialEq)]
pub struct FrontendRangeRequest {
    pub tenant: String,
    pub query: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step: Time,
    pub opts: QueryFrontendOptions,
}
