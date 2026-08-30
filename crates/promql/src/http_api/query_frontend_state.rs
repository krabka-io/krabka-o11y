use super::{QueryFrontendOptions, Arc, RangeQueryCache};

pub(crate) struct QueryFrontendState {
    pub(crate) opts: QueryFrontendOptions,
    pub(crate) cache: Arc<dyn RangeQueryCache>,
}
