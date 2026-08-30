use super::ClockReadingPayload;

/// One sorted clock confidence row ready for block encoding.
///
/// The row keeps the whole [`ClockReadingPayload`] rather than a flattened copy
/// of its two dozen fields, so the block encoder and the WAL payload can never
/// drift apart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClockReadingRow {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
    pub reading: ClockReadingPayload,
}
