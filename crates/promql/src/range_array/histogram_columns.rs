use super::{Int8Array, BooleanArray, Float64Array, ListArray, Int64Array, Array, StructArray, struct_column, COL_NH_SCHEMA, COL_NH_IS_FLOAT, COL_NH_RESET_HINT, COL_NH_ZERO_THRESHOLD, COL_NH_ZERO_COUNT, COL_NH_COUNT, COL_NH_SUM, COL_NH_POS_SPANS, COL_NH_POS_COUNTS, COL_NH_NEG_SPANS, COL_NH_NEG_COUNTS, COL_NH_CUSTOM_VALUES, COL_NH_START_TS, HistogramView};

pub(crate) struct HistogramColumns<'a> {
    pub(crate) schemas: &'a Int8Array,
    pub(crate) is_floats: &'a BooleanArray,
    pub(crate) reset_hints: &'a Int8Array,
    pub(crate) zero_thresholds: &'a Float64Array,
    pub(crate) zero_counts: &'a Float64Array,
    pub(crate) counts: &'a Float64Array,
    pub(crate) sums: &'a Float64Array,
    pub(crate) positive_spans: &'a ListArray,
    pub(crate) positive_counts: &'a ListArray,
    pub(crate) negative_spans: &'a ListArray,
    pub(crate) negative_counts: &'a ListArray,
    pub(crate) custom_values: &'a ListArray,
    pub(crate) start_timestamps: &'a Int64Array,
}

impl<'a> HistogramColumns<'a> {
    pub(crate) fn parse(values: &'a dyn Array) -> Option<Self> {
        let histograms = values.as_any().downcast_ref::<StructArray>()?;
        Some(Self {
            schemas: struct_column(histograms, COL_NH_SCHEMA)?,
            is_floats: struct_column(histograms, COL_NH_IS_FLOAT)?,
            reset_hints: struct_column(histograms, COL_NH_RESET_HINT)?,
            zero_thresholds: struct_column(histograms, COL_NH_ZERO_THRESHOLD)?,
            zero_counts: struct_column(histograms, COL_NH_ZERO_COUNT)?,
            counts: struct_column(histograms, COL_NH_COUNT)?,
            sums: struct_column(histograms, COL_NH_SUM)?,
            positive_spans: struct_column(histograms, COL_NH_POS_SPANS)?,
            positive_counts: struct_column(histograms, COL_NH_POS_COUNTS)?,
            negative_spans: struct_column(histograms, COL_NH_NEG_SPANS)?,
            negative_counts: struct_column(histograms, COL_NH_NEG_COUNTS)?,
            custom_values: struct_column(histograms, COL_NH_CUSTOM_VALUES)?,
            start_timestamps: struct_column(histograms, COL_NH_START_TS)?,
        })
    }

    pub(crate) fn cell(self, offset: u32, len: u32) -> HistogramView<'a> {
        let start = offset as usize;
        let end = start + len as usize;
        HistogramView {
            row_start: start,
            row_len: len as usize,
            schemas: &self.schemas.values()[start..end],
            is_floats: self.is_floats,
            reset_hints: &self.reset_hints.values()[start..end],
            zero_thresholds: &self.zero_thresholds.values()[start..end],
            zero_counts: &self.zero_counts.values()[start..end],
            counts: &self.counts.values()[start..end],
            sums: &self.sums.values()[start..end],
            positive_spans: self.positive_spans,
            positive_counts: self.positive_counts,
            negative_spans: self.negative_spans,
            negative_counts: self.negative_counts,
            custom_values: self.custom_values,
            start_timestamps: self.start_timestamps,
        }
    }
}
