//! Flattened profile-samples block schema.
//!
//! Krabka stores one row per profile sample. The raw
//! `(stacktrace_partition, stacktrace_id)` slot is resolved through the block's
//! symbol DB at query time, after merge-before-symbolize has reduced the sample
//! set.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::{
    block::{COL_FINGERPRINT, COL_TIMESTAMP},
    block_index::{BlockSchema, RequiredColumn},
};

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn mandatory_columns_match_blockstore() {
        let schema = profile_samples_schema();
        assert2::assert!(
            schema
                .column_with_name(COL_FINGERPRINT)
                .unwrap()
                .1
                .data_type()
                == &DataType::UInt64
        );
        assert2::assert!(
            schema
                .column_with_name(COL_TIMESTAMP)
                .unwrap()
                .1
                .data_type()
                == &DataType::Int64
        );
    }

    #[test]
    fn profile_type_is_dictionary_encoded() {
        let schema = profile_samples_schema();
        let (_, field) = schema.column_with_name(PCOL_PROFILE_TYPE).unwrap();
        match field.data_type() {
            DataType::Dictionary(key, value) => {
                assert2::assert!(key.as_ref() == &DataType::Int32);
                assert2::assert!(value.as_ref() == &DataType::Utf8);
            }
            other => panic!("expected Dictionary<Int32,Utf8>, got {other:?}"),
        }
    }

    #[test]
    fn raw_stacktrace_slot_columns_are_unsigned() {
        let schema = profile_samples_schema();
        assert2::assert!(
            schema
                .column_with_name(PCOL_STACKTRACE_ID)
                .unwrap()
                .1
                .data_type()
                == &DataType::UInt64
        );
        assert2::assert!(
            schema
                .column_with_name(PCOL_STACKTRACE_PARTITION)
                .unwrap()
                .1
                .data_type()
                == &DataType::UInt64
        );
    }

    #[test]
    fn value_and_total_value_are_int64() {
        let schema = profile_samples_schema();
        assert2::assert!(
            schema.column_with_name(PCOL_VALUE).unwrap().1.data_type() == &DataType::Int64
        );
        assert2::assert!(
            schema
                .column_with_name(PCOL_TOTAL_VALUE)
                .unwrap()
                .1
                .data_type()
                == &DataType::Int64
        );
    }

    #[test]
    fn cross_signal_join_keys_are_nullable() {
        let schema = profile_samples_schema();
        let span = schema.column_with_name(PCOL_SPAN_ID).unwrap().1;
        let trace = schema.column_with_name(PCOL_TRACE_ID).unwrap().1;
        assert2::assert!(span.data_type() == &DataType::UInt64);
        assert2::assert!(span.is_nullable());
        assert2::assert!(trace.data_type() == &DataType::Binary);
        assert2::assert!(trace.is_nullable());
    }

    #[test]
    fn decl_requires_fp_type_ts_and_sorts_by_them() {
        assert2::assert!(
            profile_samples_decl()
                == BlockSchema {
                    required: vec![
                        RequiredColumn::new(COL_FINGERPRINT, DataType::UInt64, false),
                        RequiredColumn::new(PCOL_PROFILE_TYPE, profile_type_dict(), false),
                        RequiredColumn::new(COL_TIMESTAMP, DataType::Int64, false),
                    ],
                    sort_key: vec![
                        COL_FINGERPRINT.to_string(),
                        PCOL_PROFILE_TYPE.to_string(),
                        COL_TIMESTAMP.to_string(),
                    ],
                }
        );
    }
}

mod pcol_profile_type;
mod pcol_span_id;
mod pcol_stacktrace_id;
mod pcol_stacktrace_partition;
mod pcol_total_value;
mod pcol_trace_id;
mod pcol_value;
mod profile_samples_decl;
mod profile_samples_schema;
mod profile_type_dict;

pub use pcol_profile_type::PCOL_PROFILE_TYPE;
pub use pcol_span_id::PCOL_SPAN_ID;
pub use pcol_stacktrace_id::PCOL_STACKTRACE_ID;
pub use pcol_stacktrace_partition::PCOL_STACKTRACE_PARTITION;
pub use pcol_total_value::PCOL_TOTAL_VALUE;
pub use pcol_trace_id::PCOL_TRACE_ID;
pub use pcol_value::PCOL_VALUE;
pub use profile_samples_decl::profile_samples_decl;
pub use profile_samples_schema::profile_samples_schema;
use profile_type_dict::profile_type_dict;
