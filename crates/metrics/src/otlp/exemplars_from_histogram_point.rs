use super::{DecodedExemplar, HistogramDataPoint, exemplars_from_otlp};

pub(crate) fn exemplars_from_histogram_point(point: &HistogramDataPoint) -> Vec<DecodedExemplar> {
    exemplars_from_otlp(&point.exemplars)
}
