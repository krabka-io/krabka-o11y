//! In-memory native-histogram representation and Arrow codec.

use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int8Array,
        Int8Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, ListArray, ListBuilder,
        StructArray, StructBuilder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
    },
    datatypes::{DataType, Field, Fields},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};

use crate::{
    arrow_codec::{require_non_null, schema_mismatch, typed_column},
    schema::{
        COL_FINGERPRINT, COL_NH_COUNT, COL_NH_CUSTOM_VALUES, COL_NH_IS_FLOAT, COL_NH_NEG_COUNTS,
        COL_NH_NEG_SPANS, COL_NH_POS_COUNTS, COL_NH_POS_SPANS, COL_NH_RESET_HINT, COL_NH_SCHEMA,
        COL_NH_START_TS, COL_NH_SUM, COL_NH_ZERO_COUNT, COL_NH_ZERO_THRESHOLD, COL_TIMESTAMP,
        native_histogram_schema,
    },
};

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn reset_hint_round_trips_i8() {
        for h in [
            ResetHint::Unknown,
            ResetHint::Yes,
            ResetHint::No,
            ResetHint::Gauge,
        ] {
            assert!(ResetHint::from_i8(h.as_i8()) == h);
        }
    }

    #[test]
    fn nhcb_detected_by_schema() {
        let mut h = sample_histogram();
        assert!(!h.is_nhcb());
        h.schema = -53;
        assert!(h.is_nhcb());
    }

    #[test]
    fn encode_decode_round_trips() {
        let h1 = sample_histogram();
        let mut h2 = sample_histogram();
        h2.is_float = true;
        h2.negative_spans = vec![BucketSpan {
            offset: -1,
            length: 1,
        }];
        h2.negative_counts = vec![2.0];
        h2.custom_values = Some(vec![0.5, 1.0, 2.0]);
        h2.schema = -53;
        h2.start_timestamp_ms = Some(123);
        let mut h3 = sample_histogram();
        h3.custom_values = Some(vec![]);
        h3.schema = -53;

        let rows = vec![
            (10_u64, 1000_i64, h1.clone()),
            (20_u64, 2000_i64, h2.clone()),
            (30_u64, 3000_i64, h3.clone()),
        ];
        let batch = encode_native_histograms(&rows).unwrap();
        assert!(batch.num_rows() == 3);

        let back = decode_native_histograms(&batch).unwrap();
        assert!(back == rows);
        check!(back[0].2.custom_values == None);
        check!(back[1].2.custom_values == Some(vec![0.5, 1.0, 2.0]));
        check!(back[2].2.custom_values == Some(vec![]));
    }

    #[test]
    fn encode_validates_span_count_consistency() {
        let mut bad = sample_histogram();
        bad.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 5,
        }];
        bad.positive_counts = vec![1.0, 2.0];
        let err = encode_native_histograms(&[(1, 1, bad)]);
        assert!(err.is_err());
    }

    #[test]
    fn decode_validates_positive_span_count_consistency() {
        let batch = encoded_sample_batch();
        let mut counts = new_f64_list_builder();
        append_f64_list(&mut counts, &[4.0]);
        let batch = batch_with_column(&batch, COL_NH_POS_COUNTS, Arc::new(counts.finish()), false);

        let err = decode_native_histograms(&batch).unwrap_err();

        assert!(matches!(
            err,
            HistogramCodecError::SpanCountMismatch {
                spans: 2,
                counts: 1
            }
        ));
    }

    #[test]
    fn decode_rejects_null_required_scalar() {
        let batch = encoded_sample_batch();
        let batch = batch_with_column(
            &batch,
            COL_NH_SCHEMA,
            Arc::new(Int8Array::from(vec![None::<i8>])),
            true,
        );

        let err = decode_native_histograms(&batch).unwrap_err();

        assert!(matches!(
            err,
            HistogramCodecError::SchemaMismatch(message)
                if message.contains(COL_NH_SCHEMA) && message.contains("null")
        ));
    }

    #[test]
    fn decode_rejects_null_required_list() {
        let batch = encoded_sample_batch();
        let mut spans = new_span_list_builder();
        spans.append(false);
        let batch = batch_with_column(&batch, COL_NH_POS_SPANS, Arc::new(spans.finish()), true);

        let err = decode_native_histograms(&batch).unwrap_err();

        assert!(matches!(
            err,
            HistogramCodecError::SchemaMismatch(message)
                if message.contains(COL_NH_POS_SPANS) && message.contains("null")
        ));
    }

    #[test]
    fn decode_rejects_span_struct_with_missing_child() {
        let batch = encoded_sample_batch();
        let mut spans = ListBuilder::new(StructBuilder::from_fields(
            vec![Field::new("offset", DataType::Int32, false)],
            1,
        ));
        spans
            .values()
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(0);
        spans.values().append(true);
        spans.append(true);
        let spans = spans.finish();
        let index = batch.schema().index_of(COL_NH_POS_SPANS).unwrap();
        let mut columns = batch.columns().to_vec();
        columns[index] = Arc::new(spans.clone());
        let mut fields = batch.schema().fields().to_vec();
        fields[index] = Arc::new(Field::new(
            COL_NH_POS_SPANS,
            spans.data_type().clone(),
            false,
        ));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();

        let err = decode_native_histograms(&batch).unwrap_err();

        assert!(matches!(
            err,
            HistogramCodecError::SchemaMismatch(message) if message.contains(COL_NH_POS_SPANS)
        ));
    }

    #[test]
    fn decode_tolerates_extra_column() {
        let batch = encoded_sample_batch();
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("extra", DataType::UInt64, false)));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(UInt64Array::from(vec![123_u64])));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();

        let decoded = decode_native_histograms(&batch).unwrap();

        assert!(decoded == sample_rows());
    }

    fn encoded_sample_batch() -> RecordBatch {
        encode_native_histograms(&sample_rows()).unwrap()
    }

    fn sample_rows() -> Vec<(u64, i64, NativeHistogram)> {
        vec![(7_u64, 99_i64, sample_histogram())]
    }

    fn batch_with_column(
        batch: &RecordBatch,
        name: &str,
        column: ArrayRef,
        make_field_nullable: bool,
    ) -> RecordBatch {
        let index = batch.schema().index_of(name).unwrap();
        let mut columns = batch.columns().to_vec();
        columns[index] = column;
        let mut fields = batch.schema().fields().to_vec();
        if make_field_nullable {
            fields[index] = Arc::new(fields[index].as_ref().clone().with_nullable(true));
        }
        let struct_columns = fields.iter().cloned().zip(columns).collect::<Vec<_>>();
        RecordBatch::from(StructArray::from(struct_columns))
    }

    fn sample_histogram() -> NativeHistogram {
        NativeHistogram {
            schema: 2,
            is_float: false,
            reset_hint: ResetHint::No,
            zero_threshold: 1e-128,
            zero_count: 3.0,
            count: 10.0,
            sum: 42.5,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![4.0, 3.0],
            negative_spans: vec![],
            negative_counts: vec![],
            custom_values: None,
            start_timestamp_ms: None,
        }
    }
}

// === split-modules: generated submodules ===
mod append_f64_list;
mod append_spans;
mod bucket_span;
mod decode_native_histograms;
mod encode_native_histograms;
mod f64_list_field;
mod histogram_codec_error;
mod native_histogram;
mod new_f64_list_builder;
mod new_span_list_builder;
mod read_f64_list;
mod read_spans;
mod reset_hint;
mod span_bucket_total;
mod span_list_field;
mod span_struct_fields;
mod validate_span_count_consistency;

use append_f64_list::append_f64_list;
use append_spans::append_spans;
pub use bucket_span::BucketSpan;
pub use decode_native_histograms::decode_native_histograms;
pub use encode_native_histograms::encode_native_histograms;
use f64_list_field::f64_list_field;
pub use histogram_codec_error::HistogramCodecError;
pub use native_histogram::NativeHistogram;
use new_f64_list_builder::new_f64_list_builder;
use new_span_list_builder::new_span_list_builder;
use read_f64_list::read_f64_list;
use read_spans::read_spans;
pub use reset_hint::ResetHint;
use span_bucket_total::span_bucket_total;
use span_list_field::span_list_field;
use span_struct_fields::span_struct_fields;
use validate_span_count_consistency::validate_span_count_consistency;
