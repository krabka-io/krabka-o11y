//! Metric data access abstraction.

use datafusion::prelude::SessionContext;
use krabka_blockstore::{LabelMatcher, Labels};

use crate::PromqlError;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use krabka_blockstore::Labels;

    use super::*;

    struct Empty;

    #[async_trait::async_trait]
    impl MetricStore for Empty {
        async fn scan(
            &self,
            _tenant: &str,
            _matchers: &[krabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<ScanResult, PromqlError> {
            Ok(ScanResult {
                ctx: SessionContext::new(),
                float_table: None,
                histogram_table: None,
            })
        }

        async fn label_names(
            &self,
            _tenant: &str,
            _matchers: &[krabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<String>, PromqlError> {
            Ok(vec![])
        }

        async fn label_values(
            &self,
            _tenant: &str,
            _name: &str,
            _matchers: &[krabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<String>, PromqlError> {
            Ok(vec![])
        }

        async fn series(
            &self,
            _tenant: &str,
            _matchers: &[krabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<Labels>, PromqlError> {
            Ok(vec![])
        }

        async fn exemplars(
            &self,
            _tenant: &str,
            _matchers: &[krabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<ExemplarRecord>, PromqlError> {
            Ok(vec![])
        }

        async fn metadata(
            &self,
            _tenant: &str,
            _metric: Option<&str>,
        ) -> Result<Vec<MetadataRecord>, PromqlError> {
            Ok(vec![])
        }

        async fn cardinality_label_names(
            &self,
            _tenant: &str,
        ) -> Result<Vec<LabelNameCardinality>, PromqlError> {
            Ok(vec![])
        }

        async fn cardinality_label_values(
            &self,
            _tenant: &str,
        ) -> Result<Vec<LabelValueCardinality>, PromqlError> {
            Ok(vec![])
        }

        async fn cardinality_active_series(
            &self,
            _tenant: &str,
        ) -> Result<Vec<Labels>, PromqlError> {
            Ok(vec![])
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

    #[tokio::test]
    async fn trait_is_object_safe_and_default_returns_none_tables() {
        let store: Arc<dyn MetricStore> = Arc::new(Empty);
        let result = store.scan("t", &[], 0, 1).await.unwrap();
        assert2::assert!(result.float_table.is_none());
        assert2::assert!(result.histogram_table.is_none());
    }
}

// === split-modules: generated submodules ===
mod exemplar_record;
mod label_name_cardinality;
mod label_value_cardinality;
mod metadata_record;
mod metric_store;
mod named_tsdb_stat;
mod scan_result;
mod tsdb_block;
mod tsdb_head_stats;
mod tsdb_stats;

pub use exemplar_record::ExemplarRecord;
pub use label_name_cardinality::LabelNameCardinality;
pub use label_value_cardinality::LabelValueCardinality;
pub use metadata_record::MetadataRecord;
pub use metric_store::MetricStore;
pub use named_tsdb_stat::NamedTsdbStat;
pub use scan_result::ScanResult;
pub use tsdb_block::TsdbBlock;
pub use tsdb_head_stats::TsdbHeadStats;
pub use tsdb_stats::TsdbStats;
