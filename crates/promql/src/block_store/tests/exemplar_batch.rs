use super::*;

pub(crate) fn exemplar_batch(row: ExemplarRow) -> RecordBatch {
    encode_exemplar_rows(&[row]).unwrap()
}
