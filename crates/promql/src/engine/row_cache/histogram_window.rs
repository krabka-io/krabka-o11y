use super::HistogramRow;

struct SeriesSpan {
    start: usize,
    end: usize,
}

/// Histogram rows indexed by series and timestamp.
///
/// Each step searches the series spans instead of filtering the complete scan.
/// The returned rows borrow their bucket payloads from the cached scan.
pub(crate) struct HistogramWindow {
    rows: Vec<HistogramRow>,
    spans: Vec<SeriesSpan>,
}

impl HistogramWindow {
    pub(crate) fn new(mut rows: Vec<HistogramRow>) -> Self {
        if !rows
            .windows(2)
            .all(|pair| (pair[0].fp, pair[0].ts_ms) <= (pair[1].fp, pair[1].ts_ms))
        {
            rows.sort_by_key(|row| (row.fp, row.ts_ms));
        }
        // Stable sorting preserves scan order. Keep the last duplicate by move.
        rows.dedup_by(|later, earlier| {
            if later.fp == earlier.fp && later.ts_ms == earlier.ts_ms {
                std::mem::swap(earlier, later);
                true
            } else {
                false
            }
        });
        let mut spans = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let fp = rows[start].fp;
            let end = start + rows[start..].partition_point(|row| row.fp == fp);
            spans.push(SeriesSpan { start, end });
            start = end;
        }
        Self { rows, spans }
    }

    /// Borrows rows in the closed window `[from_ms, to_ms]` in series order.
    pub(crate) fn rows_between(
        &self,
        from_ms: i64,
        to_ms: i64,
    ) -> impl Iterator<Item = &HistogramRow> {
        self.spans.iter().flat_map(move |span| {
            let samples = &self.rows[span.start..span.end];
            let start = samples.partition_point(|row| row.ts_ms < from_ms);
            let end = start + samples[start..].partition_point(|row| row.ts_ms <= to_ms);
            samples[start..end].iter()
        })
    }

    /// Tests whether the closed window contains a histogram without copying it.
    pub(crate) fn has_rows(&self, from_ms: i64, to_ms: i64) -> bool {
        self.rows_between(from_ms, to_ms).next().is_some()
    }
}

#[cfg(test)]
mod tests {
    use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};
    use proptest::prelude::*;

    use super::*;
    use crate::test_support::native_histogram_fixture;

    fn row(fp: u64, ts_ms: i64) -> HistogramRow {
        HistogramRow {
            fp,
            ts_ms,
            hist: native_histogram_fixture(0, true, ResetHint::No, None, None),
        }
    }

    #[test]
    fn duplicates_keep_the_last_payload_without_copying_buckets() {
        let mut last = row(7, 10);
        last.hist = NativeHistogram {
            positive_spans: vec![BucketSpan {
                offset: 3,
                length: 2,
            }],
            positive_counts: vec![2.0, 9.0],
            reset_hint: ResetHint::Yes,
            start_timestamp_ms: Some(5),
            ..last.hist
        };
        let bucket_address = last.hist.positive_counts.as_ptr();
        let expected = last.hist.clone();
        let window = HistogramWindow::new(vec![row(7, 10), row(2, 30), row(7, 10), last]);
        let rows = window.rows_between(10, 10).collect::<Vec<_>>();
        assert2::assert!(rows.len() == 1);
        assert2::assert!(rows[0].hist == expected);
        assert2::assert!(rows[0].hist.positive_counts.as_ptr() == bucket_address);
    }

    #[test]
    fn closed_boundaries_empty_windows_and_extreme_timestamps() {
        let window = HistogramWindow::new(vec![
            row(1, i64::MIN),
            row(1, 0),
            row(1, i64::MAX),
            row(2, 10),
        ]);
        for (from, to, expected) in [
            (i64::MIN, i64::MIN, vec![(1, i64::MIN)]),
            (i64::MAX, i64::MAX, vec![(1, i64::MAX)]),
            (0, 10, vec![(1, 0), (2, 10)]),
            (1, 9, vec![]),
            (10, 0, vec![]),
        ] {
            let actual = window
                .rows_between(from, to)
                .map(|row| (row.fp, row.ts_ms))
                .collect::<Vec<_>>();
            assert2::assert!(actual == expected);
            assert2::assert!(window.has_rows(from, to) == !expected.is_empty());
        }
        assert2::assert!(!HistogramWindow::new(vec![]).has_rows(i64::MIN, i64::MAX));
    }

    proptest! {
        #[test]
        fn indexed_windows_match_last_write_wins_scan(
            input in prop::collection::vec((0_u64..8, -100_i64..100), 0..100),
            from in -110_i64..110,
            to in -110_i64..110,
        ) {
            let mut expected = std::collections::BTreeMap::new();
            let rows = input.into_iter().enumerate().map(|(index, (fp, ts))| {
                let mut row = row(fp, ts);
                row.hist.count = f64::from(u32::try_from(index).unwrap());
                expected.insert((fp, ts), row.hist.count);
                row
            }).collect();
            let window = HistogramWindow::new(rows);
            let expected = expected.into_iter().filter(|((_, ts), _)| *ts >= from && *ts <= to).collect::<Vec<_>>();
            let actual = window.rows_between(from, to).map(|row| ((row.fp, row.ts_ms), row.hist.count)).collect::<Vec<_>>();
            assert2::assert!(actual == expected);
        }
    }
}
