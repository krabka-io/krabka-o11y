//! Pluggable per-signal block index and schema declaration.

use arrow::datatypes::DataType;

use crate::block::BlockMeta;

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::block::validate_against;

    #[test]
    fn series_declaration_lists_mandatory_columns() {
        assert2::assert!(
            series_block_schema()
                == BlockSchema {
                    required: vec![
                        RequiredColumn::new("series_fingerprint", DataType::UInt64, false),
                        RequiredColumn::new("timestamp", DataType::Int64, false),
                    ],
                    sort_key: vec!["series_fingerprint".to_string(), "timestamp".to_string()],
                    bloom_columns: vec![],
                }
        );
    }

    #[test]
    fn validate_against_accepts_matching_schema() {
        let decl = series_block_schema();
        let schema = Schema::new(vec![
            Field::new("series_fingerprint", DataType::UInt64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]);
        assert2::assert!(validate_against(&schema, &decl).is_ok());
    }

    #[test]
    fn validate_against_rejects_wrong_type() {
        let decl = series_block_schema();
        let schema = Schema::new(vec![
            Field::new("series_fingerprint", DataType::UInt64, false),
            Field::new("timestamp", DataType::Utf8, false),
        ]);
        assert2::assert!(validate_against(&schema, &decl).is_err());
    }
}

mod block_index_type;
pub use block_index_type::BlockIndex;

/// A signal's declared block schema, physical sort key and bloom-filtered
/// columns.
///
/// The declaration is the single input to how a block is physically written:
/// `crate::block::validate_against` checks `required` against the Arrow schema,
/// `sort_key` becomes both the order the writer enforces and the Parquet
/// `sorting_columns` it records, and `bloom_columns` names the identity
/// columns whose row groups get a bloom filter so a point lookup can skip
/// them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockSchema {
    /// Columns the Arrow schema must carry, with their type and nullability.
    pub required: Vec<RequiredColumn>,
    /// Top-level columns the block's rows are ordered by, most significant
    /// first, ascending.
    pub sort_key: Vec<String>,
    /// Top-level columns to write a per-row-group bloom filter for.
    pub bloom_columns: Vec<String>,
}

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

/// One required column in a signal block schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequiredColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl RequiredColumn {
    #[must_use]
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}
