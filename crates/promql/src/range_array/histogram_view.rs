use super::{Array, HistogramColumns, HistogramSpanView, f64_list_value, span_list_value};

/// Zero-copy view of one native-histogram range cell.
#[derive(Clone, Copy, Debug)]
pub struct HistogramView<'a> {
    pub(crate) row_start: usize,
    pub(crate) row_len: usize,
    pub(crate) columns: HistogramColumns<'a>,
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
        &self.columns.schemas.values()[self.row_start..self.row_start + self.row_len]
    }

    #[must_use]
    pub fn reset_hint_slice(&self) -> &'a [i8] {
        &self.columns.reset_hints.values()[self.row_start..self.row_start + self.row_len]
    }

    #[must_use]
    pub fn zero_threshold_slice(&self) -> &'a [f64] {
        &self.columns.zero_thresholds.values()[self.row_start..self.row_start + self.row_len]
    }

    #[must_use]
    pub fn zero_count_slice(&self) -> &'a [f64] {
        &self.columns.zero_counts.values()[self.row_start..self.row_start + self.row_len]
    }

    #[must_use]
    pub fn count_slice(&self) -> &'a [f64] {
        &self.columns.counts.values()[self.row_start..self.row_start + self.row_len]
    }

    #[must_use]
    pub fn sum_slice(&self) -> &'a [f64] {
        &self.columns.sums.values()[self.row_start..self.row_start + self.row_len]
    }

    #[must_use]
    pub fn is_float(&self, sample_index: usize) -> Option<bool> {
        let row = self.absolute_row(sample_index)?;
        (!self.columns.is_floats.is_null(row)).then(|| self.columns.is_floats.value(row))
    }

    #[must_use]
    pub fn positive_spans(&self, sample_index: usize) -> Option<HistogramSpanView<'a>> {
        let row = self.absolute_row(sample_index)?;
        span_list_value(self.columns.positive_spans, row)
    }

    #[must_use]
    pub fn positive_counts(&self, sample_index: usize) -> Option<&'a [f64]> {
        let row = self.absolute_row(sample_index)?;
        f64_list_value(self.columns.positive_counts, row)
    }

    #[must_use]
    pub fn negative_spans(&self, sample_index: usize) -> Option<HistogramSpanView<'a>> {
        let row = self.absolute_row(sample_index)?;
        span_list_value(self.columns.negative_spans, row)
    }

    #[must_use]
    pub fn negative_counts(&self, sample_index: usize) -> Option<&'a [f64]> {
        let row = self.absolute_row(sample_index)?;
        f64_list_value(self.columns.negative_counts, row)
    }

    #[must_use]
    pub fn custom_values(&self, sample_index: usize) -> Option<&'a [f64]> {
        let row = self.absolute_row(sample_index)?;
        f64_list_value(self.columns.custom_values, row)
    }

    #[must_use]
    pub fn start_timestamp_ms(&self, sample_index: usize) -> Option<i64> {
        let row = self.absolute_row(sample_index)?;
        (!self.columns.start_timestamps.is_null(row))
            .then(|| self.columns.start_timestamps.value(row))
    }

    pub(crate) fn absolute_row(&self, sample_index: usize) -> Option<usize> {
        if sample_index >= self.row_len {
            return None;
        }
        Some(self.row_start + sample_index)
    }
}
