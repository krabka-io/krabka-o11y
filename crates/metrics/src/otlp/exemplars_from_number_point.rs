use super::{DecodedExemplar, NumberDataPoint, TranslationStrategy, exemplars_from_otlp};

pub(crate) fn exemplars_from_number_point(
    point: &NumberDataPoint,
    strategy: TranslationStrategy,
) -> Vec<DecodedExemplar> {
    exemplars_from_otlp(&point.exemplars, strategy)
}
