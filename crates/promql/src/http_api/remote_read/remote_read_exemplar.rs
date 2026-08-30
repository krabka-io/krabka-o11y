use super::*;

pub(crate) fn remote_read_exemplar(exemplar: &ExemplarRecord) -> pb::v1::Exemplar {
    pb::v1::Exemplar {
        labels: remote_read_labels(&exemplar.labels),
        value: exemplar.value,
        timestamp: exemplar.ts_ms,
    }
}
