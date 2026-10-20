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

/// Metadata recorded for each written block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockMeta {
    pub tenant: String,
    pub object_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub row_count: usize,
    pub fingerprints: Vec<SeriesFingerprint>,
    /// How many rounds of compaction produced the block.
    ///
    /// Writers start at [`BlockLevel::INGESTED`]; an index promotes a
    /// registered compaction output above its replaced inputs.
    pub level: BlockLevel,
}

/// Inclusive minimum and maximum timestamps represented by matching blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockTimeBounds {
    pub min_ts: i64,
    pub max_ts: i64,
}

/// Mandatory column: the series fingerprint (`UInt64`).
pub const COL_FINGERPRINT: &str = "series_fingerprint";

/// Mandatory column: the event timestamp in nanoseconds (`Int64`).
pub const COL_TIMESTAMP: &str = "timestamp";

/// Validates that an Arrow schema carries the mandatory columns with the
/// required types. Payload columns are unconstrained.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn validate_block_schema(schema: &Schema) -> Result<()> {
    validate_against(schema, &crate::block_index::series_block_schema())
}

/// Validates an Arrow schema against a declared signal block schema.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn validate_against(schema: &Schema, decl: &crate::block_index::BlockSchema) -> Result<()> {
    for col in &decl.required {
        let found = schema.column_with_name(&col.name).ok_or_else(|| {
            BlockStoreError::InvalidBlock(format!("missing `{}` column", col.name))
        })?;
        if found.1.data_type() != &col.data_type {
            return Err(BlockStoreError::InvalidBlock(format!(
                "`{}` must be {:?}, got {:?}",
                col.name,
                col.data_type,
                found.1.data_type()
            )));
        }
        if !col.nullable && found.1.is_nullable() {
            return Err(BlockStoreError::InvalidBlock(format!(
                "`{}` must be non-nullable",
                col.name
            )));
        }
    }
    Ok(())
}
