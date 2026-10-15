//! Metric data access abstraction.

use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use datafusion::prelude::SessionContext;
use krabka_blockstore::SeriesFingerprint;
use krabka_metrics::FloatSampleRow;

use crate::{PromqlError, PromqlLabels as Labels, PromqlMatcher as LabelMatcher};

/// One exemplar attached to a metric series.
#[derive(Clone, Debug, PartialEq)]
pub struct ExemplarRecord {
    pub series_labels: Labels,
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: f64,
}

/// The exemplars a store returned, and the blocks it answered without.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExemplarScan {
    pub exemplars: Vec<ExemplarRecord>,
    pub warnings: Vec<String>,
}

/// Cardinality for one label name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelNameCardinality {
    pub name: String,
    pub series_count: usize,
}

/// Cardinality for one label value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelValueCardinality {
    pub label_name: String,
    pub label_value: String,
    pub series_count: usize,
}

/// Latest instant samples and the labels resolved over their label window.
pub struct LatestFloatScan {
    pub samples: Vec<FloatSampleRow>,
    pub labels: BTreeMap<SeriesFingerprint, Arc<Labels>>,
}

/// Metric metadata served by `/api/v1/metadata`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MetadataRecord {
    pub metric_family_name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
}

/// The metric metadata a store returned, and the blocks it answered without.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataScan {
    pub metadata: Vec<MetadataRecord>,
    pub warnings: Vec<String>,
}

/// One named TSDB status statistic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedTsdbStat {
    pub name: String,
    pub value: usize,
}

/// A leaf scan result with up to two `DataFusion` tables registered.
pub struct ScanResult {
    pub ctx: SessionContext,
    pub float_table: Option<String>,
    pub histogram_table: Option<String>,
    /// One line for each block the scan answered without.
    pub warnings: Vec<String>,
}

/// One compacted TSDB block exposed by `/api/v1/status/tsdb/blocks`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TsdbBlock {
    pub id: String,
    pub min_time: i64,
    pub max_time: i64,
    pub num_samples: usize,
    pub num_series: usize,
}

pub(crate) fn sort_tsdb_blocks(blocks: &mut [TsdbBlock]) {
    blocks.sort_by(|left, right| {
        left.min_time
            .cmp(&right.min_time)
            .then_with(|| left.max_time.cmp(&right.max_time))
            .then_with(|| left.id.cmp(&right.id))
    });
}

/// Prometheus-style head block stats for `/api/v1/status/tsdb`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TsdbHeadStats {
    pub num_series: usize,
    pub num_samples: usize,
    pub num_chunks: usize,
    pub min_time: i64,
    pub max_time: i64,
}

/// Tenant-scoped TSDB status statistics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TsdbStats {
    pub head_stats: TsdbHeadStats,
    pub series_count_by_metric_name: Vec<NamedTsdbStat>,
    pub label_value_count_by_label_name: Vec<NamedTsdbStat>,
    pub memory_in_bytes_by_label_name: Vec<NamedTsdbStat>,
    pub series_count_by_label_value_pair: Vec<NamedTsdbStat>,
}

#[cfg(test)]
impl TsdbStats {
    pub(crate) fn empty() -> Self {
        Self {
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
        }
    }
}

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

mod metric_store;
pub use metric_store::MetricStore;
