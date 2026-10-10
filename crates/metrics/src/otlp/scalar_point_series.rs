use super::{
    DecodedExemplar, DecodedMetadata, DecodedSample, DecodedSeries, KeyValue, TranslationStrategy,
    labels, nanos_to_millis,
};

/// What every data point of one metric shares when it fans out into series.
pub(crate) struct PointFamily<'a> {
    /// The translated metric name.
    pub(crate) name: &'a str,
    pub(crate) resource_attributes: &'a [KeyValue],
    pub(crate) metadata: Option<&'a DecodedMetadata>,
    pub(crate) strategy: TranslationStrategy,
}

/// What every scalar series an OTLP data point fans out to shares: the point's
/// attributes, its timestamps and its metadata.
///
/// A classic histogram becomes bucket, `_count` and `_sum` series, and a
/// summary quantile, `_count` and `_sum` series; each is one sample under the
/// same labels plus at most one of its own.
pub(crate) struct ScalarPointSeries<'a> {
    pub(crate) resource_attributes: &'a [KeyValue],
    pub(crate) attributes: &'a [KeyValue],
    pub(crate) strategy: TranslationStrategy,
    pub(crate) time_unix_nano: u64,
    pub(crate) start_time_unix_nano: u64,
    pub(crate) metadata: Option<DecodedMetadata>,
}

/// One series of a fanned-out point: its name, the label that sets it apart
/// from its siblings, and its one sample.
pub(crate) struct ScalarSeries<'a> {
    pub(crate) name: &'a str,
    pub(crate) extra_label: Option<ExtraLabel<'a>>,
    pub(crate) sample_value: f64,
    pub(crate) exemplars: Vec<DecodedExemplar>,
}

/// A `le` or `quantile` label.
pub(crate) struct ExtraLabel<'a> {
    pub(crate) label_name: &'a str,
    pub(crate) label_value: &'a str,
}

impl ScalarPointSeries<'_> {
    pub(crate) fn series(&self, series: ScalarSeries<'_>) -> DecodedSeries {
        let ScalarSeries {
            name,
            extra_label,
            sample_value,
            exemplars,
        } = series;
        DecodedSeries {
            labels: labels(
                name,
                self.resource_attributes,
                self.attributes,
                extra_label.map(|label| (label.label_name, label.label_value)),
                self.strategy,
            ),
            samples: vec![DecodedSample::with_start_timestamp(
                nanos_to_millis(self.time_unix_nano),
                sample_value,
                (self.start_time_unix_nano != 0)
                    .then_some(nanos_to_millis(self.start_time_unix_nano)),
            )],
            histograms: Vec::new(),
            exemplars,
            metadata: self.metadata.clone(),
        }
    }
}
