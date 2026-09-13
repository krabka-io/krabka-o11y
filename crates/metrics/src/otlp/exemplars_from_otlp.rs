use super::{DecodedExemplar, OtlpExemplar, TranslationStrategy, exemplar};

pub(crate) fn exemplars_from_otlp(
    exemplars: &[OtlpExemplar],
    strategy: TranslationStrategy,
) -> Vec<DecodedExemplar> {
    exemplars
        .iter()
        .filter_map(|item| exemplar(item, strategy))
        .collect()
}
