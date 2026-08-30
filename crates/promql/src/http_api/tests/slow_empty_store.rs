use super::*;

pub(crate) struct SlowEmptyStore {
    pub(crate) active: Arc<AtomicUsize>,
    pub(crate) max_active: Arc<AtomicUsize>,
}

impl SlowEmptyStore {
    pub(crate) fn new(active: Arc<AtomicUsize>, max_active: Arc<AtomicUsize>) -> Self {
        Self { active, max_active }
    }

    pub(crate) async fn enter(&self) {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        let mut current = self.max_active.load(Ordering::SeqCst);
        while active > current {
            match self.max_active.compare_exchange(
                current,
                active,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl MetricStore for SlowEmptyStore {
    async fn scan(
        &self,
        _tenant: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<ScanResult, PromqlError> {
        self.enter().await;
        Ok(ScanResult {
            ctx: datafusion::prelude::SessionContext::new(),
            float_table: None,
            histogram_table: None,
        })
    }

    async fn label_names(
        &self,
        _tenant: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>, PromqlError> {
        Ok(Vec::new())
    }

    async fn label_values(
        &self,
        _tenant: &str,
        _name: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>, PromqlError> {
        Ok(Vec::new())
    }

    async fn series(
        &self,
        _tenant: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<Labels>, PromqlError> {
        Ok(Vec::new())
    }

    async fn exemplars(
        &self,
        _tenant: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<ExemplarRecord>, PromqlError> {
        Ok(Vec::new())
    }

    async fn metadata(
        &self,
        _tenant: &str,
        _metric: Option<&str>,
    ) -> Result<Vec<MetadataRecord>, PromqlError> {
        Ok(Vec::new())
    }

    async fn cardinality_label_names(
        &self,
        _tenant: &str,
    ) -> Result<Vec<LabelNameCardinality>, PromqlError> {
        Ok(Vec::new())
    }

    async fn cardinality_label_values(
        &self,
        _tenant: &str,
    ) -> Result<Vec<LabelValueCardinality>, PromqlError> {
        Ok(Vec::new())
    }

    async fn cardinality_active_series(&self, _tenant: &str) -> Result<Vec<Labels>, PromqlError> {
        Ok(Vec::new())
    }

    async fn tsdb_stats(&self, _tenant: &str) -> Result<TsdbStats, PromqlError> {
        Ok(TsdbStats {
            head_stats: TsdbHeadStats {
                num_series: 0,
                num_samples: 0,
                num_chunks: 0,
                min_time: 0,
                max_time: 0,
            },
            series_count_by_metric_name: Vec::new(),
            label_value_count_by_label_name: Vec::new(),
            memory_in_bytes_by_label_name: Vec::new(),
            series_count_by_label_value_pair: Vec::new(),
        })
    }

    async fn tsdb_blocks(&self, _tenant: &str) -> Result<Vec<TsdbBlock>, PromqlError> {
        Ok(Vec::new())
    }
}
