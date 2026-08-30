use super::*;

pub(crate) fn exemplars_from_number_point(point: &NumberDataPoint) -> Vec<DecodedExemplar> {
    exemplars_from_otlp(&point.exemplars)
}
