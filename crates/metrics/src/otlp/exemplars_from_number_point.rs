use super::{NumberDataPoint, DecodedExemplar, exemplars_from_otlp};

pub(crate) fn exemplars_from_number_point(point: &NumberDataPoint) -> Vec<DecodedExemplar> {
    exemplars_from_otlp(&point.exemplars)
}
