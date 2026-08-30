//! Build profile-samples `RecordBatch`es.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BinaryBuilder, Int64Builder, StringDictionaryBuilder, UInt64Builder},
    datatypes::Int32Type,
    record_batch::RecordBatch,
};

use crate::{
    error::{BlockStoreError, Result},
    profile_schema::profile_samples_schema,
};

#[cfg(test)]
mod tests {
    use arrow::array::{Array, BinaryArray, Int64Array, UInt64Array};

    use super::*;
    use crate::{
        PCOL_STACKTRACE_ID, PCOL_TRACE_ID, PCOL_VALUE, profile_samples_decl,
        profile_samples_schema, validate_against,
    };

    fn row(fp: u64, ts: i64, stack: u64, value: i64, trace: Option<Vec<u8>>) -> ProfileSampleRow {
        ProfileSampleRow {
            series_fingerprint: fp,
            timestamp: ts,
            profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string(),
            stacktrace_id: stack,
            value,
            stacktrace_partition: 0,
            total_value: 1_000,
            span_id: None,
            trace_id: trace,
        }
    }

    #[test]
    fn encode_matches_schema_and_columns() {
        let rows = vec![
            row(1, 100, 7, 50, Some(vec![0xAB; 16])),
            row(1, 100, 9, 30, None),
        ];
        let batch = encode_profile_samples(&rows).unwrap();
        validate_against(&batch.schema(), &profile_samples_decl()).unwrap();

        let stacks = batch
            .column_by_name(PCOL_STACKTRACE_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let values = batch
            .column_by_name(PCOL_VALUE)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let traces = batch
            .column_by_name(PCOL_TRACE_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert2::assert!(batch.schema() == profile_samples_schema());
        assert2::assert!(batch.num_rows() == 2);
        assert2::assert!(stacks.value(0) == 7);
        assert2::assert!(stacks.value(1) == 9);
        assert2::assert!(values.value(0) == 50);
        assert2::assert!(traces.value(0) == [0xAB; 16].as_slice());
        assert2::assert!(traces.is_null(1));
    }
}

mod encode_profile_samples;
mod profile_sample_row;

pub use encode_profile_samples::encode_profile_samples;
pub use profile_sample_row::ProfileSampleRow;
