use super::{DecodedExemplar, HistogramDataPoint, TranslationStrategy, exemplars_from_otlp};

pub(crate) fn exemplars_from_histogram_point(
    point: &HistogramDataPoint,
    strategy: TranslationStrategy,
) -> Vec<DecodedExemplar> {
    exemplars_from_otlp(&point.exemplars, strategy)
}
