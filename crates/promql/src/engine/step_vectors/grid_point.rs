use super::SeriesFingerprint;

/// One series' value at one grid instant, as a grid-driven leaf produces it.
///
/// The result labels live once per series in
/// [`GridVectors::labels_by_fp`](super::GridVectors), so a point carries only
/// the fingerprint that keys them. That is what keeps a whole grid's worth of
/// leaf results to 24 bytes a point instead of a label-set clone a point.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GridPoint {
    /// The series this value belongs to.
    pub(crate) fp: SeriesFingerprint,
    /// The timestamp the assembled sample reports. A selector reports the
    /// selected sample's own timestamp; a range fold reports the eval instant.
    pub(crate) ts_ms: i64,
    /// The float value.
    pub(crate) value: f64,
}
