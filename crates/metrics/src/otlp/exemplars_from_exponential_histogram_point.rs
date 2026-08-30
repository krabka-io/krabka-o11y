use super::{ExponentialHistogramDataPoint, DecodedExemplar, exemplars_from_otlp};

pub(crate) fn exemplars_from_exponential_histogram_point(
    point: &ExponentialHistogramDataPoint,
) -> Vec<DecodedExemplar> {
    exemplars_from_otlp(&point.exemplars)
}
