use super::{
    BackendError, MetricsJobRequest, MetricsPartial, Mutex, QuerierBackend, SearchJobRequest,
    SearchPartial, TagNamesJobRequest, TagNamesPartial, TagValuesJobRequest, TagValuesPartial,
    TraceByIdJobRequest, TracePartial, async_trait,
};

/// A programmable in-process backend for tests.
///
/// It returns the next stubbed response, FIFO, and the last stub repeats if
/// more calls arrive. It records every request for assertions.
///
/// This type is un-gated so that integration tests in `tests/` can construct
/// it. It is a fixture, not production wiring.
pub struct MockQuerier {
    pub(crate) querier_count: usize,
    pub(crate) search_stubs: Mutex<Vec<SearchPartial>>,
    pub(crate) trace_stubs: Mutex<Vec<TracePartial>>,
    pub(crate) tag_names_stubs: Mutex<Vec<TagNamesPartial>>,
    pub(crate) tag_values_stubs: Mutex<Vec<TagValuesPartial>>,
    pub(crate) metrics_stubs: Mutex<Vec<MetricsPartial>>,
    pub(crate) search_calls: Mutex<Vec<SearchJobRequest>>,
    pub(crate) trace_calls: Mutex<Vec<TraceByIdJobRequest>>,
    pub(crate) tag_names_calls: Mutex<Vec<TagNamesJobRequest>>,
    pub(crate) tag_values_calls: Mutex<Vec<TagValuesJobRequest>>,
    pub(crate) metrics_calls: Mutex<Vec<MetricsJobRequest>>,
}

impl MockQuerier {
    #[must_use]
    pub fn new() -> Self {
        Self::with_querier_count(1)
    }

    #[must_use]
    pub fn with_querier_count(querier_count: usize) -> Self {
        Self {
            querier_count: querier_count.max(1),
            search_stubs: Mutex::new(Vec::new()),
            trace_stubs: Mutex::new(Vec::new()),
            tag_names_stubs: Mutex::new(Vec::new()),
            tag_values_stubs: Mutex::new(Vec::new()),
            metrics_stubs: Mutex::new(Vec::new()),
            search_calls: Mutex::new(Vec::new()),
            trace_calls: Mutex::new(Vec::new()),
            tag_names_calls: Mutex::new(Vec::new()),
            tag_values_calls: Mutex::new(Vec::new()),
            metrics_calls: Mutex::new(Vec::new()),
        }
    }

    /// Enqueue a canned search-job response, FIFO.
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_search(&self, p: SearchPartial) {
        self.search_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned by-id-job response, FIFO.
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_trace(&self, p: TracePartial) {
        self.trace_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned tag-names-job response, FIFO.
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_tag_names(&self, p: TagNamesPartial) {
        self.tag_names_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned tag-values-job response, FIFO.
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_tag_values(&self, p: TagValuesPartial) {
        self.tag_values_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned metrics-job response, FIFO.
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_metrics(&self, p: MetricsPartial) {
        self.metrics_stubs.lock().unwrap().push(p);
    }

    /// All recorded search-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn search_calls(&self) -> Vec<SearchJobRequest> {
        self.search_calls.lock().unwrap().clone()
    }

    /// All recorded by-id-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn trace_calls(&self) -> Vec<TraceByIdJobRequest> {
        self.trace_calls.lock().unwrap().clone()
    }

    /// All recorded tag-names-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn tag_names_calls(&self) -> Vec<TagNamesJobRequest> {
        self.tag_names_calls.lock().unwrap().clone()
    }

    /// All recorded tag-values-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn tag_values_calls(&self) -> Vec<TagValuesJobRequest> {
        self.tag_values_calls.lock().unwrap().clone()
    }

    /// All recorded metrics-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn metrics_calls(&self) -> Vec<MetricsJobRequest> {
        self.metrics_calls.lock().unwrap().clone()
    }

    pub(crate) fn pop<T: Clone + Default>(stubs: &Mutex<Vec<T>>) -> T {
        let mut s = stubs.lock().unwrap();
        if s.len() > 1 {
            s.remove(0)
        } else {
            s.first().cloned().unwrap_or_default()
        }
    }
}

impl Default for MockQuerier {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QuerierBackend for MockQuerier {
    fn querier_count(&self) -> usize {
        self.querier_count
    }

    async fn search_job(&self, req: &SearchJobRequest) -> Result<SearchPartial, BackendError> {
        self.search_calls.lock().unwrap().push(req.clone());
        Ok(Self::pop(&self.search_stubs))
    }

    async fn trace_by_id_job(
        &self,
        req: &TraceByIdJobRequest,
    ) -> Result<TracePartial, BackendError> {
        self.trace_calls.lock().unwrap().push(req.clone());
        Ok(Self::pop(&self.trace_stubs))
    }

    async fn tag_names_job(
        &self,
        req: &TagNamesJobRequest,
    ) -> Result<TagNamesPartial, BackendError> {
        self.tag_names_calls.lock().unwrap().push(req.clone());
        Ok(Self::pop(&self.tag_names_stubs))
    }

    async fn tag_values_job(
        &self,
        req: &TagValuesJobRequest,
    ) -> Result<TagValuesPartial, BackendError> {
        self.tag_values_calls.lock().unwrap().push(req.clone());
        Ok(Self::pop(&self.tag_values_stubs))
    }

    async fn metrics_job(&self, req: &MetricsJobRequest) -> Result<MetricsPartial, BackendError> {
        self.metrics_calls.lock().unwrap().push(req.clone());
        Ok(Self::pop(&self.metrics_stubs))
    }
}
