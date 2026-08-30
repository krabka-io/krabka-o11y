use super::*;

pub(crate) fn exemplar_key(exemplar: &ExemplarRecord) -> String {
    format!(
        "{}\n{}\n{}\n{}",
        labels_key(&exemplar.series_labels),
        labels_key(&exemplar.labels),
        exemplar.ts_ms,
        exemplar.value.to_bits()
    )
}
