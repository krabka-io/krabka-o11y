use std::{collections::HashMap, sync::Arc};

use krabka_blockstore::SeriesFingerprint;
use krabka_metrics::FloatSampleRow;

use super::{
    FloatRow,
    matcher::{PreparedMatcher, all_match},
};
use crate::PromqlLabels as Labels;

#[derive(Clone)]
pub struct FloatSeriesSummary {
    pub labels: Arc<Labels>,
    pub latest: FloatSampleRow,
    pub row_count: usize,
}

/// Latest floats and retained raw counts from the same rows as a hot snapshot.
#[derive(Clone, Default)]
pub struct FloatHeadSummary {
    series: HashMap<SeriesFingerprint, FloatSeriesSummary>,
    row_count: usize,
    ambiguous_labels: bool,
}

impl FloatHeadSummary {
    pub fn observe(&mut self, row: &FloatRow) {
        self.row_count += 1;
        let entry = self
            .series
            .entry(row.fp)
            .or_insert_with(|| FloatSeriesSummary {
                labels: Arc::clone(&row.labels),
                latest: (row.fp, row.ts_ms, row.value, row.start_timestamp_ms),
                row_count: 0,
            });
        // Equal values in distinct Arcs do not prove which Arc owns the first
        // in-window label row. Retain the ordinary traversal in that case too.
        self.ambiguous_labels |= !Arc::ptr_eq(&entry.labels, &row.labels);
        entry.row_count += 1;
        // Preserve the first arrival at equal timestamps, including stale bits
        // and the optional counter start timestamp.
        if row.ts_ms > entry.latest.1 {
            entry.latest = (row.fp, row.ts_ms, row.value, row.start_timestamp_ms);
        }
    }

    /// Matching series and a conservative count of their sample-window rows.
    ///
    /// Old retained rows may increase the bound, so callers must fall back
    /// rather than reject a query whose bound exceeds its sample limit.
    pub fn matching_series(
        &self,
        current_row_count: usize,
        matchers: &[PreparedMatcher],
        label_start_ms: i64,
        sample_start_ms: i64,
        end_ms: i64,
    ) -> Option<(Vec<&FloatSeriesSummary>, usize)> {
        if self.row_count != current_row_count || self.ambiguous_labels {
            return None;
        }
        let mut series = Vec::new();
        let mut upper_count = 0_usize;
        for entry in self.series.values() {
            if !all_match(entry.latest.0, &entry.labels, matchers) {
                continue;
            }
            // An older eligible row may precede this matching future latest.
            // Test this before narrowing the label window, never omit it.
            if entry.latest.1 > end_ms {
                return None;
            }
            if entry.latest.1 < label_start_ms {
                continue;
            }
            if entry.latest.1 >= sample_start_ms {
                upper_count = upper_count.saturating_add(entry.row_count);
            }
            series.push(entry);
        }
        Some((series, upper_count))
    }
}
