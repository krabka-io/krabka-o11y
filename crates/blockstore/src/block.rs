//! Block column conventions and per-block metadata.

use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};

use crate::{
    compaction::BlockLevel,
    error::{BlockStoreError, Result},
    labels::SeriesFingerprint,
};

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    struct SchemaCase {
        schema: Schema,
        is_valid: bool,
    }

    #[test]
    fn validates_required_block_schema_columns() {
        for case in [
            SchemaCase {
                schema: Schema::new(vec![
                    Field::new(COL_FINGERPRINT, DataType::UInt64, false),
                    Field::new(COL_TIMESTAMP, DataType::Int64, false),
                    Field::new("line", DataType::Utf8, true),
                ]),
                is_valid: true,
            },
            SchemaCase {
                schema: Schema::new(vec![Field::new(COL_TIMESTAMP, DataType::Int64, false)]),
                is_valid: false,
            },
            SchemaCase {
                schema: Schema::new(vec![
                    Field::new(COL_FINGERPRINT, DataType::UInt64, false),
                    Field::new(COL_TIMESTAMP, DataType::Utf8, false),
                ]),
                is_valid: false,
            },
            SchemaCase {
                schema: Schema::new(vec![
                    Field::new(COL_FINGERPRINT, DataType::UInt64, true),
                    Field::new(COL_TIMESTAMP, DataType::Int64, false),
                ]),
                is_valid: false,
            },
            SchemaCase {
                schema: Schema::new(vec![
                    Field::new(COL_FINGERPRINT, DataType::UInt64, false),
                    Field::new(COL_TIMESTAMP, DataType::Int64, true),
                ]),
                is_valid: false,
            },
        ] {
            assert2::assert!(validate_block_schema(&case.schema).is_ok() == case.is_valid);
        }
    }
}

mod block_meta;
mod col_fingerprint;
mod col_timestamp;
mod validate_against;
mod validate_block_schema;

pub use block_meta::BlockMeta;
pub use col_fingerprint::COL_FINGERPRINT;
pub use col_timestamp::COL_TIMESTAMP;
pub use validate_against::validate_against;
pub use validate_block_schema::validate_block_schema;

/// Inclusive minimum and maximum timestamps represented by matching blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockTimeBounds {
    pub min_ts: i64,
    pub max_ts: i64,
}
