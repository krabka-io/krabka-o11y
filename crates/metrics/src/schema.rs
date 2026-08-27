//! Arrow schemas for metric block types.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use crabka_blockstore::{BlockSchema, RequiredColumn};

/// Mandatory blockstore column for the series fingerprint (`UInt64`).
pub const COL_FINGERPRINT: &str = "series_fingerprint";
/// Mandatory blockstore column for the sample timestamp in epoch milliseconds
/// (`Int64`).
pub const COL_TIMESTAMP: &str = "timestamp";

/// Native histogram schema column (`Int8`).
pub const COL_NH_SCHEMA: &str = "schema";
/// Native histogram float/integer flavor column (`Boolean`).
pub const COL_NH_IS_FLOAT: &str = "is_float";
/// Native histogram reset hint column (`Int8`).
pub const COL_NH_RESET_HINT: &str = "reset_hint";
/// Native histogram zero threshold column (`Float64`).
pub const COL_NH_ZERO_THRESHOLD: &str = "zero_threshold";
/// Native histogram zero bucket count column (`Float64`).
pub const COL_NH_ZERO_COUNT: &str = "zero_count";
/// Native histogram total count column (`Float64`).
pub const COL_NH_COUNT: &str = "count";
/// Native histogram sum column (`Float64`).
pub const COL_NH_SUM: &str = "sum";
/// Native histogram positive bucket spans column.
pub const COL_NH_POS_SPANS: &str = "positive_spans";
/// Native histogram positive bucket counts column.
pub const COL_NH_POS_COUNTS: &str = "positive_counts";
/// Native histogram negative bucket spans column.
pub const COL_NH_NEG_SPANS: &str = "negative_spans";
/// Native histogram negative bucket counts column.
pub const COL_NH_NEG_COUNTS: &str = "negative_counts";
/// Native histogram custom bucket boundary values column.
pub const COL_NH_CUSTOM_VALUES: &str = "custom_values";
/// Native histogram start timestamp in epoch milliseconds column.
pub const COL_NH_START_TS: &str = "start_timestamp_ms";

/// Clock reading host column (`Dictionary<Int32, Utf8>`).
pub const CCOL_NODE: &str = "node";
/// Clock reading clock-name column (`Dictionary<Int32, Utf8>`).
///
/// The value names one clock on the host, such as `CLOCK_REALTIME` or
/// `/dev/ptp0`.
pub const CCOL_CLOCK: &str = "clock";
/// Clock reading source-kind column (`Dictionary<Int32, Utf8>`).
pub const CCOL_SOURCE_KIND: &str = "source_kind";
/// Clock reading host-clock value column in epoch nanoseconds (`Int64`).
pub const CCOL_READING_UNIX_NANOS: &str = "reading_unix_nanos";
/// Clock reading uncertainty half-width column (`Int64`).
///
/// True time is in [`CCOL_READING_UNIX_NANOS`] plus or minus this value.
pub const CCOL_UNCERTAINTY_NANOS: &str = "uncertainty_nanos";
/// Clock reading signed offset from the reference column (`Int64`).
pub const CCOL_OFFSET_NANOS: &str = "offset_nanos";
/// Clock reading discipline-state column (`Dictionary<Int32, Utf8>`).
pub const CCOL_SYNC_STATE: &str = "sync_state";
/// Clock reading reference-identity column (`Dictionary<Int32, Utf8>`).
///
/// The value holds the PTP `gmIdentity`, the NTP peer, or the GNSS
/// constellation.
pub const CCOL_REFERENCE_ID: &str = "reference_id";
/// Clock reading last-valid-reference column in epoch nanoseconds (`Int64`).
///
/// A `PromQL` query computes the holdover duration from this column alone.
pub const CCOL_LAST_SYNC_UNIX_NANOS: &str = "last_sync_unix_nanos";
/// Clock reading frequency correction column in parts per billion (`Int64`).
pub const CCOL_FREQUENCY_PPB: &str = "frequency_ppb";
/// Clock reading last-applied-step magnitude column (`Int64`).
pub const CCOL_LAST_STEP_NANOS: &str = "last_step_nanos";
/// NTP root delay column (`Int64`).
pub const CCOL_ROOT_DELAY_NANOS: &str = "root_delay_nanos";
/// NTP root dispersion column (`Int64`).
///
/// RFC 5905 names the sum of half the root delay and the root dispersion the
/// synchronization distance. That sum is the real NTP uncertainty bound, and
/// neither term alone is.
pub const CCOL_ROOT_DISPERSION_NANOS: &str = "root_dispersion_nanos";
/// NTP stratum column (`UInt32`).
pub const CCOL_STRATUM: &str = "stratum";
/// PTP mean path delay column (`Int64`).
pub const CCOL_MEAN_PATH_DELAY_NANOS: &str = "mean_path_delay_nanos";
/// PTP steps-removed column (`UInt32`).
pub const CCOL_STEPS_REMOVED: &str = "steps_removed";
/// PTP grandmaster `clockClass` column (`UInt32`).
pub const CCOL_GM_CLOCK_CLASS: &str = "gm_clock_class";
/// PTP grandmaster `clockAccuracy` column (`UInt32`).
pub const CCOL_GM_CLOCK_ACCURACY: &str = "gm_clock_accuracy";
/// Kernel timex `maxerror` column (`Int64`).
///
/// `adjtimex(2)` grows `maxerror` at 500 ppm between updates and sets the
/// `STA_UNSYNC` bit at 16 s, so this column is already an uncertainty bound.
pub const CCOL_MAX_ERROR_NANOS: &str = "max_error_nanos";
/// Kernel timex `esterror` column (`Int64`).
pub const CCOL_EST_ERROR_NANOS: &str = "est_error_nanos";
/// Kernel timex `STA_UNSYNC` bit column (`Boolean`).
pub const CCOL_UNSYNCHRONIZED: &str = "unsynchronized";
/// GNSS satellite count column (`UInt32`).
pub const CCOL_SATELLITES_USED: &str = "satellites_used";
/// GNSS fix quality column (`Dictionary<Int32, Utf8>`).
pub const CCOL_GNSS_FIX: &str = "gnss_fix";
/// Ingest clock column in epoch nanoseconds (`Int64`).
///
/// The ingester stamps this column from its own clock when the row arrives.
/// The host does not supply it. The difference between this column and
/// [`CCOL_READING_UNIX_NANOS`] is a measured skew between two named hosts, and
/// no single exporter can compute that number.
pub const CCOL_INGEST_UNIX_NANOS: &str = "ingest_unix_nanos";

fn fingerprint_field() -> Field {
    Field::new(COL_FINGERPRINT, DataType::UInt64, false)
}

fn timestamp_field() -> Field {
    Field::new(COL_TIMESTAMP, DataType::Int64, false)
}

fn f64_list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Float64, false)))
}

fn span_list_type() -> DataType {
    let struct_fields = Fields::from(vec![
        Field::new("offset", DataType::Int32, false),
        Field::new("length", DataType::UInt32, false),
    ]);

    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(struct_fields),
        false,
    )))
}

fn utf8_map_field(name: &str, nullable: bool) -> Field {
    Field::new_map(
        name,
        "entries",
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        false,
        nullable,
    )
}

/// Float samples, which are counters, gauges, and classic histogram bucket
/// series.
#[must_use]
pub fn float_sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new("value", DataType::Float64, false),
    ]))
}

/// Native histogram samples with absolute bucket counts.
#[must_use]
pub fn native_histogram_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new(COL_NH_SCHEMA, DataType::Int8, false),
        Field::new(COL_NH_IS_FLOAT, DataType::Boolean, false),
        Field::new(COL_NH_RESET_HINT, DataType::Int8, false),
        Field::new(COL_NH_ZERO_THRESHOLD, DataType::Float64, false),
        Field::new(COL_NH_ZERO_COUNT, DataType::Float64, false),
        Field::new(COL_NH_COUNT, DataType::Float64, false),
        Field::new(COL_NH_SUM, DataType::Float64, false),
        Field::new(COL_NH_POS_SPANS, span_list_type(), false),
        Field::new(COL_NH_POS_COUNTS, f64_list_type(), false),
        Field::new(COL_NH_NEG_SPANS, span_list_type(), false),
        Field::new(COL_NH_NEG_COUNTS, f64_list_type(), false),
        Field::new(COL_NH_CUSTOM_VALUES, f64_list_type(), true),
        Field::new(COL_NH_START_TS, DataType::Int64, true),
    ]))
}

/// Exemplars whose trace and span identifiers are first-class columns.
#[must_use]
pub fn exemplar_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new("value", DataType::Float64, false),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        utf8_map_field("labels", false),
    ]))
}

/// Metric metadata rows used by the per-tenant metadata index.
#[must_use]
pub fn metadata_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new("metric_family_name", DataType::Utf8, false),
        Field::new("metric_type", DataType::Utf8, false),
        Field::new("help", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, false),
    ]))
}

fn clock_label_dict() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

/// Clock confidence readings, one row per clock, per host, per moment.
///
/// [`COL_TIMESTAMP`] carries the host reading in epoch milliseconds, the unit
/// that every other metric block in this crate uses. The ingest path converts
/// [`CCOL_READING_UNIX_NANOS`] to milliseconds to fill it. The nanosecond
/// reading stays in its own column, so the conversion drops no precision from
/// the block.
///
/// The source-specific columns are nullable. One exporter reads one kind of
/// clock, so an NTP row leaves the PTP columns empty and a PTP row leaves the
/// NTP columns empty.
#[must_use]
pub fn clock_reading_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        // Identity.
        Field::new(CCOL_NODE, clock_label_dict(), false),
        Field::new(CCOL_CLOCK, clock_label_dict(), false),
        Field::new(CCOL_SOURCE_KIND, clock_label_dict(), false),
        // The reading.
        Field::new(CCOL_READING_UNIX_NANOS, DataType::Int64, false),
        Field::new(CCOL_UNCERTAINTY_NANOS, DataType::Int64, false),
        Field::new(CCOL_OFFSET_NANOS, DataType::Int64, false),
        // Discipline state.
        Field::new(CCOL_SYNC_STATE, clock_label_dict(), false),
        Field::new(CCOL_REFERENCE_ID, clock_label_dict(), true),
        Field::new(CCOL_LAST_SYNC_UNIX_NANOS, DataType::Int64, true),
        Field::new(CCOL_FREQUENCY_PPB, DataType::Int64, true),
        Field::new(CCOL_LAST_STEP_NANOS, DataType::Int64, true),
        // NTP.
        Field::new(CCOL_ROOT_DELAY_NANOS, DataType::Int64, true),
        Field::new(CCOL_ROOT_DISPERSION_NANOS, DataType::Int64, true),
        Field::new(CCOL_STRATUM, DataType::UInt32, true),
        // PTP.
        Field::new(CCOL_MEAN_PATH_DELAY_NANOS, DataType::Int64, true),
        Field::new(CCOL_STEPS_REMOVED, DataType::UInt32, true),
        Field::new(CCOL_GM_CLOCK_CLASS, DataType::UInt32, true),
        Field::new(CCOL_GM_CLOCK_ACCURACY, DataType::UInt32, true),
        // Kernel timex.
        Field::new(CCOL_MAX_ERROR_NANOS, DataType::Int64, true),
        Field::new(CCOL_EST_ERROR_NANOS, DataType::Int64, true),
        Field::new(CCOL_UNSYNCHRONIZED, DataType::Boolean, true),
        // GNSS.
        Field::new(CCOL_SATELLITES_USED, DataType::UInt32, true),
        Field::new(CCOL_GNSS_FIX, clock_label_dict(), true),
        // Stamped by the ingester, not by the host.
        Field::new(CCOL_INGEST_UNIX_NANOS, DataType::Int64, false),
    ]))
}

/// Clock reading block declaration used by generic schema validation.
///
/// The required set holds the two mandatory blockstore columns and the three
/// columns that carry the signal itself. A row without a reading, an
/// uncertainty, and an ingest stamp answers no clock confidence question.
#[must_use]
pub fn clock_reading_decl() -> BlockSchema {
    BlockSchema {
        required: vec![
            RequiredColumn::new(COL_FINGERPRINT, DataType::UInt64, false),
            RequiredColumn::new(COL_TIMESTAMP, DataType::Int64, false),
            RequiredColumn::new(CCOL_READING_UNIX_NANOS, DataType::Int64, false),
            RequiredColumn::new(CCOL_UNCERTAINTY_NANOS, DataType::Int64, false),
            RequiredColumn::new(CCOL_INGEST_UNIX_NANOS, DataType::Int64, false),
        ],
        sort_key: vec![COL_FINGERPRINT.to_string(), COL_TIMESTAMP.to_string()],
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use assert2::{assert, check};
    use crabka_blockstore::validate_against;

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
