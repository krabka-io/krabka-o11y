//! Block column conventions and per-block metadata.

use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};

use crate::{
    error::{BlockStoreError, Result},
    labels::SeriesFingerprint,
};

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn validates_required_block_schema_columns() {
        for (_name, schema, want_valid) in [
            (
                "required columns",
                Schema::new(vec![
                    Field::new(COL_FINGERPRINT, DataType::UInt64, false),
                    Field::new(COL_TIMESTAMP, DataType::Int64, false),
                    Field::new("line", DataType::Utf8, true),
                ]),
                true,
            ),
            (
                "missing fingerprint",
                Schema::new(vec![Field::new(COL_TIMESTAMP, DataType::Int64, false)]),
                false,
            ),
            (
                "wrong timestamp type",
                Schema::new(vec![
                    Field::new(COL_FINGERPRINT, DataType::UInt64, false),
                    Field::new(COL_TIMESTAMP, DataType::Utf8, false),
                ]),
                false,
            ),
            (
                "nullable fingerprint",
                Schema::new(vec![
                    Field::new(COL_FINGERPRINT, DataType::UInt64, true),
                    Field::new(COL_TIMESTAMP, DataType::Int64, false),
                ]),
                false,
            ),
            (
                "nullable timestamp",
                Schema::new(vec![
                    Field::new(COL_FINGERPRINT, DataType::UInt64, false),
                    Field::new(COL_TIMESTAMP, DataType::Int64, true),
                ]),
                false,
            ),
        ] {
            assert2::assert!(validate_block_schema(&schema).is_ok() == want_valid);
        }
    }
}

// === split-modules: generated submodules ===
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
