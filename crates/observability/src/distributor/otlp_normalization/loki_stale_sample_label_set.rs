use super::Labels;

pub(crate) fn loki_stale_sample_label_set(labels: &Labels) -> String {
    crate::distributor::format_loki_label_set(labels, ", ")
}
