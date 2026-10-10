use super::{
    AggregationTemporality, DeltaAccumulator, KeyValue, Metric, OtlpError, TranslationStrategy,
};

/// The OTLP metric a histogram belongs to, and how its series are translated.
pub(crate) struct HistogramScope<'a> {
    pub(crate) metric: &'a Metric,
    pub(crate) resource_attributes: &'a [KeyValue],
    pub(crate) strategy: TranslationStrategy,
}

impl HistogramScope<'_> {
    /// Returns the accumulator a point of a histogram with `temporality`
    /// folds into: `Some` for delta, `None` for cumulative or unspecified.
    ///
    /// # Errors
    /// Returns [`OtlpError::DeltaUnsupported`] for a delta histogram without
    /// an accumulator, and for any other temporality.
    pub(crate) fn point_accumulator<'b>(
        &self,
        temporality: i32,
        accumulator: &'b mut Option<&mut DeltaAccumulator>,
    ) -> Result<Option<&'b mut DeltaAccumulator>, OtlpError> {
        if temporality == AggregationTemporality::Delta as i32 {
            accumulator
                .as_deref_mut()
                .map(Some)
                .ok_or_else(|| OtlpError::DeltaUnsupported(self.metric.name.clone()))
        } else if temporality == AggregationTemporality::Cumulative as i32
            || temporality == AggregationTemporality::Unspecified as i32
        {
            Ok(None)
        } else {
            Err(OtlpError::DeltaUnsupported(self.metric.name.clone()))
        }
    }
}
