use super::{Array, BooleanArray, HistogramSpanView, Int64Array, ListArray, f64_list_value, span_list_value};

/// Zero-copy view of one native-histogram range cell.
#[derive(Clone, Copy, Debug)]
pub struct HistogramView<'a> {
    pub(crate) row_start: usize,
    pub(crate) row_len: usize,
    pub(crate) schemas: &'a [i8],
    pub(crate) is_floats: &'a BooleanArray,
    pub(crate) reset_hints: &'a [i8],
    pub(crate) zero_thresholds: &'a [f64],
    pub(crate) zero_counts: &'a [f64],
    pub(crate) counts: &'a [f64],
    pub(crate) sums: &'a [f64],
    pub(crate) positive_spans: &'a ListArray,
    pub(crate) positive_counts: &'a ListArray,
    pub(crate) negative_spans: &'a ListArray,
    pub(crate) negative_counts: &'a ListArray,
    pub(crate) custom_values: &'a ListArray,
    pub(crate) start_timestamps: &'a Int64Array,
}

impl<'a> HistogramView<'a> {
    #[must_use]
    pub fn len(&self) -> usize {
        self.row_len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_len == 0
    }

    #[must_use]
    pub fn schema_slice(&self) -> &'a [i8] {
        self.schemas
    }

    #[must_use]
    pub fn reset_hint_slice(&self) -> &'a [i8] {
        self.reset_hints
    }

    #[must_use]
    pub fn zero_threshold_slice(&self) -> &'a [f64] {
        self.zero_thresholds
    }

    #[must_use]
    pub fn zero_count_slice(&self) -> &'a [f64] {
        self.zero_counts
    }

    #[must_use]
    pub fn count_slice(&self) -> &'a [f64] {
        self.counts
    }

    #[must_use]
    pub fn sum_slice(&self) -> &'a [f64] {
        self.sums
    }

    #[must_use]
    pub fn is_float(&self, sample_index: usize) -> Option<bool> {
        let row = self.absolute_row(sample_index)?;
        (!self.is_floats.is_null(row)).then(|| self.is_floats.value(row))
    }

    #[must_use]
    pub fn positive_spans(&self, sample_index: usize) -> Option<HistogramSpanView<'a>> {
        let row = self.absolute_row(sample_index)?;
        span_list_value(self.positive_spans, row)
    }

    #[must_use]
    pub fn positive_counts(&self, sample_index: usize) -> Option<&'a [f64]> {
        let row = self.absolute_row(sample_index)?;
        f64_list_value(self.positive_counts, row)
    }

    #[must_use]
    pub fn negative_spans(&self, sample_index: usize) -> Option<HistogramSpanView<'a>> {
        let row = self.absolute_row(sample_index)?;
        span_list_value(self.negative_spans, row)
    }

    #[must_use]
    pub fn negative_counts(&self, sample_index: usize) -> Option<&'a [f64]> {
        let row = self.absolute_row(sample_index)?;
        f64_list_value(self.negative_counts, row)
    }

    #[must_use]
    pub fn custom_values(&self, sample_index: usize) -> Option<&'a [f64]> {
        let row = self.absolute_row(sample_index)?;
        f64_list_value(self.custom_values, row)
    }

    #[must_use]
    pub fn start_timestamp_ms(&self, sample_index: usize) -> Option<i64> {
        let row = self.absolute_row(sample_index)?;
        (!self.start_timestamps.is_null(row)).then(|| self.start_timestamps.value(row))
    }

    pub(crate) fn absolute_row(&self, sample_index: usize) -> Option<usize> {
        if sample_index >= self.row_len {
            return None;
        }
        Some(self.row_start + sample_index)
    }
}
