use krabka_metrics::{FloatSampleRow, NativeHistogramRow};

use super::{
    Arc, BTreeMap, ExemplarRecord, ExemplarScan, InMemoryMetricStore, LabelMatcher,
    LabelNameCardinality, LabelValueCardinality, Labels, MemTable, MetadataScan, MetricStore,
    PromqlError, Result, RowChunks, ScanResult, SeriesFingerprint, SessionContext, TsdbBlock,
    TsdbHeadStats, TsdbStats, all_match, encode_float_samples, encode_native_histograms,
    float_sample_schema, label_name_cardinality, label_value_cardinality, native_histogram_schema,
    prepare_matchers, row_matches, tsdb_stats,
};
use crate::store::{distinct_label_names, distinct_label_values, sort_tsdb_blocks};

#[async_trait::async_trait]
impl MetricStore for InMemoryMetricStore {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult> {
        let ctx = SessionContext::new();
        let matchers = prepare_matchers(matchers)?;

        let mut float_rows = Vec::new();
        if let Some(rows) = self.floats.get(tenant) {
            for row in rows.iter() {
                if row_matches(row.fp, &row.labels, row.ts_ms, &matchers, start_ms, end_ms) {
                    float_rows.push(FloatSampleRow {
                        fingerprint: row.fp,
                        timestamp_ms: row.ts_ms,
                        value: row.value,
                        start_timestamp_ms: row.start_timestamp_ms,
                    });
                }
            }
        }
        // A scan has no row-order contract. The engine's FloatWindow orders
        // the merged rows once; sorting each hot scan here repeats that work.
        let float_table = if float_rows.is_empty() {
            None
        } else {
            let batch = encode_float_samples(&float_rows)
                .map_err(|error| PromqlError::Store(error.to_string()))?;
            let table = MemTable::try_new(float_sample_schema(), vec![vec![batch]])?;
            ctx.register_table("floats", Arc::new(table))?;
            Some("floats".to_string())
        };

        let mut hist_rows = Vec::new();
        if let Some(rows) = self.hists.get(tenant) {
            for row in rows.iter() {
                if row_matches(row.fp, &row.labels, row.ts_ms, &matchers, start_ms, end_ms) {
                    hist_rows.push(NativeHistogramRow {
                        fingerprint: row.fp,
                        timestamp_ms: row.ts_ms,
                        hist: row.hist.as_ref().clone(),
                    });
                }
            }
        }
        hist_rows.sort_by(|left, right| {
            left.fingerprint
                .cmp(&right.fingerprint)
                .then(left.timestamp_ms.cmp(&right.timestamp_ms))
        });
        let histogram_table = if hist_rows.is_empty() {
            None
        } else {
            let batch = encode_native_histograms(&hist_rows)
                .map_err(|error| PromqlError::Store(error.to_string()))?;
            let table = MemTable::try_new(native_histogram_schema(), vec![vec![batch]])?;
            ctx.register_table("histograms", Arc::new(table))?;
            Some("histograms".to_string())
        };

        Ok(ScanResult {
            ctx,
            float_table,
            histogram_table,
            warnings: Vec::new(),
        })
    }

    /// Answers with the row match that [`Self::scan`] makes for its histogram
    /// table, and stops at the first matching row.
    async fn may_have_histograms(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<bool> {
        let Some(rows) = self.hists.get(tenant) else {
            return Ok(false);
        };
        let matchers = prepare_matchers(matchers)?;
        Ok(rows
            .iter()
            .any(|row| row_matches(row.fp, &row.labels, row.ts_ms, &matchers, start_ms, end_ms)))
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>> {
        Ok(distinct_label_names(
            self.matched_series(tenant, matchers, start_ms, end_ms)?,
        ))
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<krabka_metrics::MetricString>> {
        Ok(distinct_label_values(
            self.matched_series(tenant, matchers, start_ms, end_ms)?,
            name,
        ))
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>> {
        self.matched_series(tenant, matchers, start_ms, end_ms)
    }

    async fn series_shared(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Arc<Labels>>> {
        self.matched_series_shared(tenant, matchers, start_ms, end_ms)
    }

    async fn exemplars(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ExemplarScan> {
        let matchers = prepare_matchers(matchers)?;
        let mut exemplars = Vec::new();
        if let Some(rows) = self.exemplars.get(tenant) {
            for row in rows.iter() {
                if row.ts_ms >= start_ms
                    && row.ts_ms <= end_ms
                    && all_match(
                        row.series_labels.fingerprint(),
                        &row.series_labels,
                        &matchers,
                    )
                {
                    exemplars.push(ExemplarRecord {
                        series_labels: row.series_labels.as_ref().clone(),
                        labels: row.labels.as_ref().clone(),
                        ts_ms: row.ts_ms,
                        value: row.value,
                    });
                }
            }
        }
        exemplars.sort_by(|left, right| {
            left.series_labels
                .fingerprint()
                .cmp(&right.series_labels.fingerprint())
                .then(left.ts_ms.cmp(&right.ts_ms))
        });
        // The hot head holds its rows in memory, so it never answers without a
        // block and never raises a warning.
        Ok(ExemplarScan {
            exemplars,
            warnings: Vec::new(),
        })
    }

    async fn metadata(&self, tenant: &str, metric: Option<&str>) -> Result<MetadataScan> {
        let mut metadata = self
            .metadata
            .get(tenant)
            .into_iter()
            .flat_map(RowChunks::iter)
            .filter(|record| {
                metric.is_none_or(|metric| metric == record.metric_family_name.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        metadata.sort();
        Ok(MetadataScan {
            metadata,
            warnings: Vec::new(),
        })
    }

    async fn cardinality_label_names(&self, tenant: &str) -> Result<Vec<LabelNameCardinality>> {
        Ok(label_name_cardinality(self.tenant_series_rows(tenant)))
    }

    async fn cardinality_label_values(&self, tenant: &str) -> Result<Vec<LabelValueCardinality>> {
        Ok(label_value_cardinality(self.tenant_series_rows(tenant)))
    }

    async fn cardinality_active_series(&self, tenant: &str) -> Result<Vec<Labels>> {
        let mut by_fp = BTreeMap::<SeriesFingerprint, Labels>::new();
        if let Some(rows) = self.floats.get(tenant) {
            for row in rows.iter() {
                by_fp
                    .entry(row.fp)
                    .or_insert_with(|| row.labels.as_ref().clone());
            }
        }
        if let Some(rows) = self.hists.get(tenant) {
            for row in rows.iter() {
                by_fp
                    .entry(row.fp)
                    .or_insert_with(|| row.labels.as_ref().clone());
            }
        }

        let mut series = by_fp.into_values().collect::<Vec<_>>();
        series.sort_by_key(|labels| {
            labels.iter().fold(String::new(), |mut out, pair| {
                out.push_str(pair.name);
                out.push('=');
                out.push_str(pair.value.as_str());
                out.push('\n');
                out
            })
        });
        Ok(series)
    }

    async fn tsdb_stats(&self, tenant: &str) -> Result<TsdbStats> {
        let mut series = BTreeMap::<SeriesFingerprint, Labels>::new();
        let mut sample_count = 0usize;
        let mut min_time = i64::MAX;
        let mut max_time = i64::MIN;

        for row in self.tenant_sample_refs(tenant) {
            sample_count += 1;
            min_time = min_time.min(row.ts_ms);
            max_time = max_time.max(row.ts_ms);
            series
                .entry(row.fp)
                .or_insert_with(|| row.labels.as_ref().clone());
        }
        if series.is_empty() {
            min_time = 0;
            max_time = 0;
        }

        Ok(tsdb_stats(
            series.values(),
            TsdbHeadStats {
                num_series: series.len(),
                num_samples: sample_count,
                num_chunks: series.len(),
                min_time,
                max_time,
            },
        ))
    }

    async fn tsdb_blocks(&self, tenant: &str) -> Result<Vec<TsdbBlock>> {
        let mut blocks = self.blocks.get(tenant).cloned().unwrap_or_default();
        sort_tsdb_blocks(&mut blocks);
        Ok(blocks)
    }
}
