//! Arrow schemas for metric block types.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use krabka_blockstore::{BlockSchema, RequiredColumn};

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use assert2::{assert, check};
    use krabka_blockstore::validate_against;

    use super::*;

    #[test]
    fn float_schema_has_mandatory_and_value() {
        let s = float_sample_schema();
        for (column, data_type) in [
            (COL_FINGERPRINT, DataType::UInt64),
            (COL_TIMESTAMP, DataType::Int64),
            ("value", DataType::Float64),
        ] {
            check!(
                s.column_with_name(column).unwrap().1.data_type() == &data_type,
                "column {column}",
            );
        }
    }

    #[test]
    fn native_histogram_span_columns_are_list_of_struct() {
        let s = native_histogram_schema();
        let (_, field) = s.column_with_name(COL_NH_POS_SPANS).unwrap();
        // List<Struct<offset:Int32, length:UInt32>>
        match field.data_type() {
            DataType::List(inner) => match inner.data_type() {
                DataType::Struct(fields) => {
                    assert!(fields.len() == 2);
                    check!(fields[0].name() == "offset");
                    check!(fields[1].name() == "length");
                }
                other => panic!("expected Struct, got {other:?}"),
            },
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn exemplar_schema_promotes_trace_and_span() {
        let s = exemplar_schema();
        assert!(s.column_with_name("trace_id").unwrap().1.data_type() == &DataType::Utf8);
        assert!(s.column_with_name("span_id").unwrap().1.data_type() == &DataType::Utf8);
    }

    #[test]
    fn metadata_schema_has_metric_metadata_columns() {
        let s = metadata_schema();
        for (column, data_type) in [
            (COL_FINGERPRINT, DataType::UInt64),
            (COL_TIMESTAMP, DataType::Int64),
            ("metric_family_name", DataType::Utf8),
            ("metric_type", DataType::Utf8),
            ("help", DataType::Utf8),
            ("unit", DataType::Utf8),
        ] {
            check!(
                s.column_with_name(column).unwrap().1.data_type() == &data_type,
                "column {column}",
            );
        }
    }

    fn clock_column_names() -> Vec<&'static str> {
        vec![
            COL_FINGERPRINT,
            COL_TIMESTAMP,
            CCOL_NODE,
            CCOL_CLOCK,
            CCOL_SOURCE_KIND,
            CCOL_READING_UNIX_NANOS,
            CCOL_UNCERTAINTY_NANOS,
            CCOL_OFFSET_NANOS,
            CCOL_SYNC_STATE,
            CCOL_REFERENCE_ID,
            CCOL_LAST_SYNC_UNIX_NANOS,
            CCOL_FREQUENCY_PPB,
            CCOL_LAST_STEP_NANOS,
            CCOL_ROOT_DELAY_NANOS,
            CCOL_ROOT_DISPERSION_NANOS,
            CCOL_STRATUM,
            CCOL_MEAN_PATH_DELAY_NANOS,
            CCOL_STEPS_REMOVED,
            CCOL_GM_CLOCK_CLASS,
            CCOL_GM_CLOCK_ACCURACY,
            CCOL_MAX_ERROR_NANOS,
            CCOL_EST_ERROR_NANOS,
            CCOL_UNSYNCHRONIZED,
            CCOL_SATELLITES_USED,
            CCOL_GNSS_FIX,
            CCOL_INGEST_UNIX_NANOS,
        ]
    }

    #[test]
    fn clock_reading_schema_validates_against_its_declaration() {
        check!(validate_against(&clock_reading_schema(), &clock_reading_decl()).is_ok());
    }

    #[test]
    fn clock_reading_schema_leads_with_the_mandatory_columns() {
        let schema = clock_reading_schema();
        let leading: Vec<Field> = schema
            .fields()
            .iter()
            .take(2)
            .map(|field| field.as_ref().clone())
            .collect();

        assert!(
            leading
                == vec![
                    Field::new(COL_FINGERPRINT, DataType::UInt64, false),
                    Field::new(COL_TIMESTAMP, DataType::Int64, false),
                ]
        );
    }

    #[test]
    fn clock_reading_column_constants_name_every_schema_field_in_order() {
        let schema = clock_reading_schema();
        let names: Vec<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();

        assert!(names == clock_column_names());
    }

    #[test]
    fn clock_reading_low_cardinality_columns_are_dictionary_encoded() {
        let schema = clock_reading_schema();
        for column in [
            CCOL_NODE,
            CCOL_CLOCK,
            CCOL_SOURCE_KIND,
            CCOL_SYNC_STATE,
            CCOL_REFERENCE_ID,
            CCOL_GNSS_FIX,
        ] {
            let (_, field) = schema.column_with_name(column).unwrap();
            check!(
                field.data_type()
                    == &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                "column {column}",
            );
        }
    }

    #[test]
    fn clock_reading_ingest_stamp_is_a_non_null_int64() {
        let schema = clock_reading_schema();
        let (_, field) = schema.column_with_name(CCOL_INGEST_UNIX_NANOS).unwrap();

        assert!(field == &Field::new(CCOL_INGEST_UNIX_NANOS, DataType::Int64, false));
    }

    #[test]
    fn clock_reading_source_specific_columns_are_nullable() {
        let schema = clock_reading_schema();
        for column in [
            CCOL_ROOT_DELAY_NANOS,
            CCOL_ROOT_DISPERSION_NANOS,
            CCOL_STRATUM,
            CCOL_MEAN_PATH_DELAY_NANOS,
            CCOL_STEPS_REMOVED,
            CCOL_GM_CLOCK_CLASS,
            CCOL_GM_CLOCK_ACCURACY,
            CCOL_MAX_ERROR_NANOS,
            CCOL_EST_ERROR_NANOS,
            CCOL_UNSYNCHRONIZED,
            CCOL_SATELLITES_USED,
            CCOL_GNSS_FIX,
        ] {
            check!(
                schema.column_with_name(column).unwrap().1.is_nullable(),
                "column {column}",
            );
        }
    }

    #[test]
    fn clock_reading_decl_requires_the_signal_columns_and_sorts_by_series_then_time() {
        assert!(
            clock_reading_decl()
                == BlockSchema {
                    required: vec![
                        RequiredColumn::new(COL_FINGERPRINT, DataType::UInt64, false),
                        RequiredColumn::new(COL_TIMESTAMP, DataType::Int64, false),
                        RequiredColumn::new(CCOL_READING_UNIX_NANOS, DataType::Int64, false),
                        RequiredColumn::new(CCOL_UNCERTAINTY_NANOS, DataType::Int64, false),
                        RequiredColumn::new(CCOL_INGEST_UNIX_NANOS, DataType::Int64, false),
                    ],
                    sort_key: vec![COL_FINGERPRINT.to_string(), COL_TIMESTAMP.to_string()],
                }
        );
    }
}

// === split-modules: generated submodules ===
mod ccol_clock;
mod ccol_est_error_nanos;
mod ccol_frequency_ppb;
mod ccol_gm_clock_accuracy;
mod ccol_gm_clock_class;
mod ccol_gnss_fix;
mod ccol_ingest_unix_nanos;
mod ccol_last_step_nanos;
mod ccol_last_sync_unix_nanos;
mod ccol_max_error_nanos;
mod ccol_mean_path_delay_nanos;
mod ccol_node;
mod ccol_offset_nanos;
mod ccol_reading_unix_nanos;
mod ccol_reference_id;
mod ccol_root_delay_nanos;
mod ccol_root_dispersion_nanos;
mod ccol_satellites_used;
mod ccol_source_kind;
mod ccol_steps_removed;
mod ccol_stratum;
mod ccol_sync_state;
mod ccol_uncertainty_nanos;
mod ccol_unsynchronized;
mod clock_label_dict;
mod clock_reading_decl;
mod clock_reading_schema;
mod col_fingerprint;
mod col_nh_count;
mod col_nh_custom_values;
mod col_nh_is_float;
mod col_nh_neg_counts;
mod col_nh_neg_spans;
mod col_nh_pos_counts;
mod col_nh_pos_spans;
mod col_nh_reset_hint;
mod col_nh_schema;
mod col_nh_start_ts;
mod col_nh_sum;
mod col_nh_zero_count;
mod col_nh_zero_threshold;
mod col_timestamp;
mod exemplar_schema;
mod f64_list_type;
mod fingerprint_field;
mod float_sample_schema;
mod metadata_schema;
mod native_histogram_schema;
mod span_list_type;
mod timestamp_field;
mod utf8_map_field;

pub use ccol_clock::CCOL_CLOCK;
pub use ccol_est_error_nanos::CCOL_EST_ERROR_NANOS;
pub use ccol_frequency_ppb::CCOL_FREQUENCY_PPB;
pub use ccol_gm_clock_accuracy::CCOL_GM_CLOCK_ACCURACY;
pub use ccol_gm_clock_class::CCOL_GM_CLOCK_CLASS;
pub use ccol_gnss_fix::CCOL_GNSS_FIX;
pub use ccol_ingest_unix_nanos::CCOL_INGEST_UNIX_NANOS;
pub use ccol_last_step_nanos::CCOL_LAST_STEP_NANOS;
pub use ccol_last_sync_unix_nanos::CCOL_LAST_SYNC_UNIX_NANOS;
pub use ccol_max_error_nanos::CCOL_MAX_ERROR_NANOS;
pub use ccol_mean_path_delay_nanos::CCOL_MEAN_PATH_DELAY_NANOS;
pub use ccol_node::CCOL_NODE;
pub use ccol_offset_nanos::CCOL_OFFSET_NANOS;
pub use ccol_reading_unix_nanos::CCOL_READING_UNIX_NANOS;
pub use ccol_reference_id::CCOL_REFERENCE_ID;
pub use ccol_root_delay_nanos::CCOL_ROOT_DELAY_NANOS;
pub use ccol_root_dispersion_nanos::CCOL_ROOT_DISPERSION_NANOS;
pub use ccol_satellites_used::CCOL_SATELLITES_USED;
pub use ccol_source_kind::CCOL_SOURCE_KIND;
pub use ccol_steps_removed::CCOL_STEPS_REMOVED;
pub use ccol_stratum::CCOL_STRATUM;
pub use ccol_sync_state::CCOL_SYNC_STATE;
pub use ccol_uncertainty_nanos::CCOL_UNCERTAINTY_NANOS;
pub use ccol_unsynchronized::CCOL_UNSYNCHRONIZED;
use clock_label_dict::clock_label_dict;
pub use clock_reading_decl::clock_reading_decl;
pub use clock_reading_schema::clock_reading_schema;
pub use col_fingerprint::COL_FINGERPRINT;
pub use col_nh_count::COL_NH_COUNT;
pub use col_nh_custom_values::COL_NH_CUSTOM_VALUES;
pub use col_nh_is_float::COL_NH_IS_FLOAT;
pub use col_nh_neg_counts::COL_NH_NEG_COUNTS;
pub use col_nh_neg_spans::COL_NH_NEG_SPANS;
pub use col_nh_pos_counts::COL_NH_POS_COUNTS;
pub use col_nh_pos_spans::COL_NH_POS_SPANS;
pub use col_nh_reset_hint::COL_NH_RESET_HINT;
pub use col_nh_schema::COL_NH_SCHEMA;
pub use col_nh_start_ts::COL_NH_START_TS;
pub use col_nh_sum::COL_NH_SUM;
pub use col_nh_zero_count::COL_NH_ZERO_COUNT;
pub use col_nh_zero_threshold::COL_NH_ZERO_THRESHOLD;
pub use col_timestamp::COL_TIMESTAMP;
pub use exemplar_schema::exemplar_schema;
use f64_list_type::f64_list_type;
use fingerprint_field::fingerprint_field;
pub use float_sample_schema::float_sample_schema;
pub use metadata_schema::metadata_schema;
pub use native_histogram_schema::native_histogram_schema;
use span_list_type::span_list_type;
use timestamp_field::timestamp_field;
use utf8_map_field::utf8_map_field;
