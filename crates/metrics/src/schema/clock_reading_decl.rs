use super::*;

/// Clock reading block declaration used by generic schema validation.
///
/// The required set holds the two mandatory blockstore columns and the three
/// columns that carry the signal itself. A row without a reading, an
/// uncertainty, and an ingest stamp answers no clock confidence question.
#[must_use]
pub fn clock_reading_decl() -> BlockSchema {
    BlockSchema {
        required: vec![
            RequiredColumn::new(COL_FINGERPRINT, DataType::UInt64, false),
            RequiredColumn::new(COL_TIMESTAMP, DataType::Int64, false),
            RequiredColumn::new(CCOL_READING_UNIX_NANOS, DataType::Int64, false),
            RequiredColumn::new(CCOL_UNCERTAINTY_NANOS, DataType::Int64, false),
            RequiredColumn::new(CCOL_INGEST_UNIX_NANOS, DataType::Int64, false),
        ],
        sort_key: vec![COL_FINGERPRINT.to_string(), COL_TIMESTAMP.to_string()],
    }
}
