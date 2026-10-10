use super::{
    DecodedMetadata, Metric, TranslationStrategy, metric_metadata, translated_metric_name,
};

/// The translated name and the metadata that every series of one OTLP
/// histogram metric shares, classic and exponential alike.
pub(crate) struct HistogramFamily {
    pub(crate) name: String,
    pub(crate) metadata: DecodedMetadata,
}

impl HistogramFamily {
    pub(crate) fn of(metric: &Metric, strategy: TranslationStrategy) -> Self {
        let name = translated_metric_name(metric, strategy, false);
        let metadata = metric_metadata(metric, &name, "histogram");
        Self { name, metadata }
    }
}
