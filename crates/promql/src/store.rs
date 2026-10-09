//! Metric data access abstraction.

use datafusion::prelude::SessionContext;

use crate::{PromqlError, PromqlLabels as Labels, PromqlMatcher as LabelMatcher};

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use assert2::assert;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[derive(Default)]
    struct Empty {
        labels: Vec<Arc<Labels>>,
        samples: Option<Vec<krabka_metrics::FloatSampleRow>>,
        failure: Option<&'static str>,
        name: &'static str,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl MetricStore for Empty {
        async fn try_latest_float_samples(
            &self,
            _tenant: &str,
            _matchers: &[LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
            _max_samples: usize,
        ) -> Result<Option<Vec<krabka_metrics::FloatSampleRow>>, PromqlError> {
            Ok(self.samples.clone())
        }

        async fn series_shared(
            &self,
            _tenant: &str,
            _matchers: &[LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<Arc<Labels>>, PromqlError> {
            self.calls.lock().unwrap().push(self.name);
            if let Some(failure) = self.failure {
                return Err(PromqlError::Store(failure.into()));
            }
            Ok(self.labels.clone())
        }

        async fn scan(
            &self,
            _tenant: &str,
            _matchers: &[crate::PromqlMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<ScanResult, PromqlError> {
            Ok(ScanResult {
                ctx: SessionContext::new(),
                float_table: None,
                histogram_table: None,
                warnings: Vec::new(),
            })
        }

        async fn label_names(
            &self,
            _tenant: &str,
            _matchers: &[crate::PromqlMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<String>, PromqlError> {
            Ok(vec![])
        }

        async fn label_values(
            &self,
            _tenant: &str,
            _name: &str,
            _matchers: &[crate::PromqlMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<krabka_metrics::MetricString>, PromqlError> {
            Ok(vec![])
        }

        async fn series(
            &self,
            _tenant: &str,
            _matchers: &[crate::PromqlMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<Labels>, PromqlError> {
            Ok(vec![])
        }

        async fn exemplars(
            &self,
            _tenant: &str,
            _matchers: &[crate::PromqlMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<ExemplarScan, PromqlError> {
            Ok(ExemplarScan::default())
        }

        async fn metadata(
            &self,
            _tenant: &str,
            _metric: Option<&str>,
        ) -> Result<MetadataScan, PromqlError> {
            Ok(MetadataScan::default())
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
    async fn canonical_shared_map_keeps_default_last_and_merged_first_owners() {
        let first = Arc::new(Labels::from_pairs([("__name__", "up"), ("job", "api")]));
        let last = Arc::new(first.as_ref().clone());
        let mut raw = first.as_ref().clone();
        raw.insert("job", vec![0xff]);
        let raw = Arc::new(raw);
        let store = Empty {
            labels: vec![Arc::clone(&first), Arc::clone(&raw), Arc::clone(&last)],
            ..Empty::default()
        };
        let generic: &dyn MetricStore = &store;
        let map = generic
            .series_shared_by_fingerprint("t", &[], 0, 1)
            .await
            .unwrap();
        assert!(
            map == BTreeMap::from([
                (first.fingerprint(), Arc::clone(&last)),
                (raw.fingerprint(), Arc::clone(&raw)),
            ])
        );
        assert!(Arc::ptr_eq(&map[&first.fingerprint()], &last));
        assert!(Arc::ptr_eq(&map[&raw.fingerprint()], &raw));
        let hot = Arc::new(first.as_ref().clone());
        let merged = crate::MergedMetricStore::new(
            store,
            Empty {
                labels: vec![hot],
                ..Empty::default()
            },
        );
        let map = merged
            .series_shared_by_fingerprint("t", &[], 0, 1)
            .await
            .unwrap();
        assert!(
            map == BTreeMap::from([
                (first.fingerprint(), Arc::clone(&first)),
                (raw.fingerprint(), raw),
            ])
        );
        assert!(Arc::ptr_eq(&map[&first.fingerprint()], &first));
        let shared = merged.series_shared("t", &[], 0, 1).await.unwrap();
        assert!(shared.len() == map.len());
        for (actual, expected) in shared.iter().zip(map.values()) {
            assert!(Arc::ptr_eq(actual, expected));
        }
    }

    #[tokio::test]
    async fn canonical_shared_map_preserves_cold_then_hot_errors() {
        for (cold_failure, hot_failure, expected_calls, expected_error) in [
            (Some("cold"), Some("hot"), vec!["cold"], "cold"),
            (None, Some("hot"), vec!["cold", "hot"], "hot"),
        ] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let store = crate::MergedMetricStore::new(
                Empty {
                    failure: cold_failure,
                    name: "cold",
                    calls: Arc::clone(&calls),
                    ..Empty::default()
                },
                Empty {
                    failure: hot_failure,
                    name: "hot",
                    calls: Arc::clone(&calls),
                    ..Empty::default()
                },
            );
            let error = store
                .series_shared_by_fingerprint("t", &[], 0, 1)
                .await
                .unwrap_err();
            assert!(matches!(error, PromqlError::Store(message) if message == expected_error));
            assert!(*calls.lock().unwrap() == expected_calls);
        }
    }

    #[tokio::test]
    async fn latest_scan_keeps_sample_limit_before_label_errors() {
        let store = Empty {
            samples: Some(vec![(7, 0, 1.0, None), (7, 1, 2.0, None)]),
            failure: Some("labels"),
            ..Empty::default()
        };
        let scan = store
            .try_latest_float_scan("t", &[], 0, 0, 1, 1)
            .await
            .unwrap()
            .unwrap();
        assert!(
            scan.samples == vec![(7, 0, 1.0, None), (7, 1, 2.0, None)] && scan.labels.is_empty()
        );
        assert!(store.calls.lock().unwrap().is_empty());
        assert!(
            matches!(store.try_latest_float_scan("t", &[], 0, 0, 1, 2).await,
            Err(PromqlError::Store(message)) if message == "labels")
        );
        assert!(*store.calls.lock().unwrap() == vec![""]);
    }

    #[tokio::test]
    async fn trait_is_object_safe_and_default_returns_none_tables() {
        let store: Arc<dyn MetricStore> = Arc::new(Empty::default());
        let result = store.scan("t", &[], 0, 1).await.unwrap();
        assert2::assert!(result.float_table.is_none());
        assert2::assert!(result.histogram_table.is_none());
    }
}

mod exemplar_record;
mod exemplar_scan;
mod label_name_cardinality;
mod label_value_cardinality;
mod latest_float_scan;
mod metadata_record;
mod metadata_scan;
mod metric_store;
mod named_tsdb_stat;
mod scan_result;
mod tsdb_block;
mod tsdb_head_stats;
mod tsdb_stats;

pub use exemplar_record::ExemplarRecord;
pub use exemplar_scan::ExemplarScan;
pub use label_name_cardinality::LabelNameCardinality;
pub use label_value_cardinality::LabelValueCardinality;
pub use latest_float_scan::LatestFloatScan;
pub use metadata_record::MetadataRecord;
pub use metadata_scan::MetadataScan;
pub use metric_store::MetricStore;
pub use named_tsdb_stat::NamedTsdbStat;
pub use scan_result::ScanResult;
pub use tsdb_block::TsdbBlock;
pub use tsdb_head_stats::TsdbHeadStats;
pub use tsdb_stats::TsdbStats;
