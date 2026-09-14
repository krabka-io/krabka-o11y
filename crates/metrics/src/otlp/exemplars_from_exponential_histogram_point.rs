use super::{
    DecodedExemplar, ExponentialHistogramDataPoint, TranslationStrategy, exemplars_from_otlp,
};

pub(crate) fn exemplars_from_exponential_histogram_point(
    point: &ExponentialHistogramDataPoint,
    strategy: TranslationStrategy,
) -> Vec<DecodedExemplar> {
    exemplars_from_otlp(&point.exemplars, strategy)
}
