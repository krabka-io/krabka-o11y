use super::{BlockSchema, DataType, RequiredColumn};

/// The logs/metrics block declaration.
#[must_use]
pub fn series_block_schema() -> BlockSchema {
    BlockSchema {
        required: vec![
            RequiredColumn::new(crate::block::COL_FINGERPRINT, DataType::UInt64, false),
            RequiredColumn::new(crate::block::COL_TIMESTAMP, DataType::Int64, false),
        ],
        sort_key: vec![
            crate::block::COL_FINGERPRINT.to_string(),
            crate::block::COL_TIMESTAMP.to_string(),
        ],
    }
}
