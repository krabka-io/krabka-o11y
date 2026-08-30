use super::{MetricStore, Arc, EngineOpts};

/// `PromQL` evaluator over a concrete metric store.
pub struct PromqlEngine<S: MetricStore> {
    pub(crate) store: Arc<S>,
    pub(crate) opts: EngineOpts,
}

impl<S: MetricStore> PromqlEngine<S> {
    #[must_use]
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self {
        Self { store, opts }
    }
}
