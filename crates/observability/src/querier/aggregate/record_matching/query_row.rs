use super::*;

#[derive(Clone, Copy)]
pub(crate) struct QueryRow<'a> {
    pub(crate) fingerprint: SeriesFingerprint,
    pub(crate) timestamp_ns: i64,
    pub(crate) line: &'a str,
    pub(crate) structured_metadata: &'a Labels,
}
