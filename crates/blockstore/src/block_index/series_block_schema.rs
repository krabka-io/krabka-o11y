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
        // None. Both columns are the sort key, so row-group min/max prunes an
        // equality on either exactly, and `Index` has already dropped every
        // block whose `BlockMeta.fingerprints` misses the query's series
        // before this file is opened.
        bloom_columns: Vec::new(),
    }
}
