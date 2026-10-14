//! Metric data access abstraction.

use std::collections::BTreeSet;

use datafusion::prelude::SessionContext;

use crate::{PromqlError, PromqlLabels as Labels, PromqlMatcher as LabelMatcher};

pub(crate) fn distinct_label_names(series: impl IntoIterator<Item = Labels>) -> Vec<String> {
    let mut names = BTreeSet::new();
    for labels in series {
        names.extend(labels.iter().map(|pair| pair.name.clone()));
    }
    names.into_iter().collect()
}

pub(crate) fn distinct_label_values(
    series: impl IntoIterator<Item = Labels>,
    name: &str,
) -> Vec<krabka_metrics::MetricString> {
    let mut values = BTreeSet::new();
    for labels in series {
        if let Some(value) = labels.get_value(name) {
            values.insert(value.clone());
        }
    }
    values.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use assert2::assert;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[derive(bon::Builder)]
    struct ErrorPropagationCase {
        cold_failure: Option<&'static str>,
        hot_failure: Option<&'static str>,
        expected_calls: Vec<&'static str>,
        expected_error: &'static str,
    }

    #[derive(Default)]
    struct Empty {
        labels: Vec<Arc<Labels>>,
        samples: Option<Vec<krabka_metrics::FloatSampleRow>>,
        failure: Option<&'static str>,
        name: &'static str,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    crate::test_support::metric_store_with_empty_lookups!(Empty {
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
    });

    #[tokio::test]
    async fn canonical_shared_map_keeps_default_last_and_merged_first_owners() {
        let first = Arc::new(Labels::from_pairs([
            krabka_metrics::MetricLabelPair::new("__name__", "up"),
            krabka_metrics::MetricLabelPair::new("job", "api"),
        ]));
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
        let expected_values = map.values().collect::<Vec<_>>();
        for index in 0..shared.len() {
            assert!(Arc::ptr_eq(&shared[index], expected_values[index]));
        }
    }

    #[tokio::test]
    async fn canonical_shared_map_preserves_cold_then_hot_errors() {
        for case in [
            ErrorPropagationCase::builder()
                .cold_failure("cold")
                .hot_failure("hot")
                .expected_calls(vec!["cold"])
                .expected_error("cold")
                .build(),
            ErrorPropagationCase::builder()
                .hot_failure("hot")
                .expected_calls(vec!["cold", "hot"])
                .expected_error("hot")
                .build(),
        ] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let store = crate::MergedMetricStore::new(
                Empty {
                    failure: case.cold_failure,
                    name: "cold",
                    calls: Arc::clone(&calls),
                    ..Empty::default()
                },
                Empty {
                    failure: case.hot_failure,
                    name: "hot",
                    calls: Arc::clone(&calls),
                    ..Empty::default()
                },
            );
            let error = store
                .series_shared_by_fingerprint("t", &[], 0, 1)
                .await
                .unwrap_err();
            assert!(matches!(error, PromqlError::Store(message) if message == case.expected_error));
            assert!(*calls.lock().unwrap() == case.expected_calls);
        }
    }

    #[tokio::test]
    async fn latest_scan_keeps_sample_limit_before_label_errors() {
        let store = Empty {
            samples: Some(vec![
                krabka_metrics::FloatSampleRow {
                    fingerprint: 7,
                    timestamp_ms: 0,
                    value: 1.0,
                    start_timestamp_ms: None,
                },
                krabka_metrics::FloatSampleRow {
                    fingerprint: 7,
                    timestamp_ms: 1,
                    value: 2.0,
                    start_timestamp_ms: None,
                },
            ]),
            failure: Some("labels"),
            ..Empty::default()
        };
        let scan = store
            .try_latest_float_scan("t", &[], 0, 0, 1, 1)
            .await
            .unwrap()
            .unwrap();
        assert!(
            scan.samples
                == vec![
                    krabka_metrics::FloatSampleRow {
                        fingerprint: 7,
                        timestamp_ms: 0,
                        value: 1.0,
                        start_timestamp_ms: None,
                    },
                    krabka_metrics::FloatSampleRow {
                        fingerprint: 7,
                        timestamp_ms: 1,
                        value: 2.0,
                        start_timestamp_ms: None,
                    },
                ]
                && scan.labels.is_empty()
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
pub(crate) use tsdb_block::sort_tsdb_blocks;
pub use tsdb_head_stats::TsdbHeadStats;
pub use tsdb_stats::TsdbStats;
