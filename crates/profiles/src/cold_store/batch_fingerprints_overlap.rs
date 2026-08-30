use super::{AsArray, BTreeSet, RecordBatch, SeriesFingerprint, UInt64Type};

pub(crate) fn batch_fingerprints_overlap(
    batch: &RecordBatch,
    fps: &BTreeSet<SeriesFingerprint>,
) -> bool {
    let fingerprints = batch.column(0).as_primitive::<UInt64Type>();
    (0..batch.num_rows()).any(|row| fps.contains(&fingerprints.value(row)))
}
