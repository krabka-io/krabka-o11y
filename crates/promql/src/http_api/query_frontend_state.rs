use super::{Arc, QueryFrontendOptions, RangeQueryCache};

pub(crate) struct QueryFrontendState {
    pub(crate) opts: QueryFrontendOptions,
    pub(crate) cache: Arc<dyn RangeQueryCache>,
}
