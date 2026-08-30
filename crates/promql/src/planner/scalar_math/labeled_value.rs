use super::Labels;

/// One already-evaluated inner instant-vector sample.
///
/// The sample holds the full label set, the reported timestamp of the inner
/// sample, and the float value that `f` is applied to. Leaf assembly drops the
/// metadata labels, and the projection keeps the timestamp. This type carries no
/// fingerprint, because the code reads the result label set straight from the
/// projected batch.
pub struct LabeledValue {
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: f64,
}
