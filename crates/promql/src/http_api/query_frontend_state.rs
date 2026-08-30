use super::*;

pub(crate) struct QueryFrontendState {
    pub(crate) opts: QueryFrontendOptions,
    pub(crate) cache: Arc<dyn RangeQueryCache>,
}
