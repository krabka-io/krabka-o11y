//! Pluggable per-signal block index and schema declaration.

use arrow::datatypes::DataType;
use serde::{Serialize, de::DeserializeOwned};

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
mod block_schema;
mod required_column;
mod series_block_schema;

pub use block_index_type::BlockIndex;
pub use block_schema::BlockSchema;
pub use required_column::RequiredColumn;
pub use series_block_schema::series_block_schema;
