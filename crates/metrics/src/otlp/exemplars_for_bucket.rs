use super::*;

pub(crate) fn exemplars_for_bucket(
    exemplars: &[DecodedExemplar],
    point: &HistogramDataPoint,
    bucket_idx: usize,
) -> Vec<DecodedExemplar> {
    exemplars
        .iter()
        .filter(|exemplar| exemplar_belongs_to_bucket(exemplar.value, point, bucket_idx))
        .cloned()
        .collect()
}
