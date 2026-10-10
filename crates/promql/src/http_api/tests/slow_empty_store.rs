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

crate::test_support::metric_store_with_empty_lookups!(SlowEmptyStore {
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
            warnings: Vec::new(),
        })
    }
});
