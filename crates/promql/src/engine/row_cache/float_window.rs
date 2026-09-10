use super::{FloatRow, SeriesFingerprint};

/// The float rows of one scan, grouped by series and time-ordered inside each
/// series.
///
/// A range query scans the union of every step's lookback window once and then
/// answers each step from the result. Serving a step by filtering the flat row
/// list costs one pass over the whole union, so a 60-step query walks a day of
/// samples sixty times to answer sixty five-minute windows. The rows are laid
/// out in `(fingerprint, timestamp)` order here and indexed by the span each
/// series occupies, so a step is a binary search per matched series instead.
pub(crate) struct FloatWindow {
    rows: Vec<FloatRow>,
    /// One `(fingerprint, start, end)` per series, in ascending fingerprint
    /// order. `rows[start..end]` is that series' samples, ascending by
    /// timestamp.
    spans: Vec<(SeriesFingerprint, usize, usize)>,
}

impl FloatWindow {
    /// Indexes `rows`, sorting them into `(fingerprint, timestamp)` order first
    /// if they do not already arrive that way.
    ///
    /// A block leaves the writer in the `[series_fingerprint, timestamp]` order
    /// its declaration names, recorded in the Parquet file as its
    /// `sorting_columns`, and the in-memory store sorts too — so a scan answered
    /// by one block arrives sorted and the check finds nothing to do. The sort
    /// stays for what that does not cover: a scan spanning several candidate
    /// blocks concatenates them, and two sorted files do not concatenate into a
    /// sorted one.
    pub(crate) fn new(mut rows: Vec<FloatRow>) -> Self {
        if !rows.is_sorted_by_key(|row| (row.fp, row.ts_ms)) {
            rows.sort_unstable_by_key(|row| (row.fp, row.ts_ms));
        }
        let mut spans = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let fp = rows[start].fp;
            let end = start + rows[start..].partition_point(|row| row.fp == fp);
            spans.push((fp, start, end));
            start = end;
        }
        Self { rows, spans }
    }

    /// Every series that has a sample in the closed window `[from_ms, to_ms]`,
    /// as a borrowed, time-ordered slice.
    pub(crate) fn series(
        &self,
        from_ms: i64,
        to_ms: i64,
    ) -> impl Iterator<Item = (SeriesFingerprint, &[FloatRow])> {
        self.spans.iter().filter_map(move |&(fp, start, end)| {
            let samples = window_slice(&self.rows[start..end], from_ms, to_ms);
            (!samples.is_empty()).then_some((fp, samples))
        })
    }

    /// The rows in the closed window `[from_ms, to_ms]`, in
    /// `(fingerprint, timestamp)` order.
    pub(crate) fn rows_between(&self, from_ms: i64, to_ms: i64) -> Vec<FloatRow> {
        self.series(from_ms, to_ms)
            .flat_map(|(_, samples)| samples.iter().copied())
            .collect()
    }
}

/// The sub-slice of one series' time-ordered samples inside `[from_ms, to_ms]`.
fn window_slice(samples: &[FloatRow], from_ms: i64, to_ms: i64) -> &[FloatRow] {
    let start = samples.partition_point(|row| row.ts_ms < from_ms);
    let end = start + samples[start..].partition_point(|row| row.ts_ms <= to_ms);
    &samples[start..end]
}
