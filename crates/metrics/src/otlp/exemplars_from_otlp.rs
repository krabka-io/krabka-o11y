use super::*;

pub(crate) fn exemplars_from_otlp(exemplars: &[OtlpExemplar]) -> Vec<DecodedExemplar> {
    exemplars.iter().filter_map(exemplar).collect()
}
