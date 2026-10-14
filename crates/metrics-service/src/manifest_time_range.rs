#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ManifestTimeRange {
    pub start_ms: i64,
    pub end_ms: i64,
}
