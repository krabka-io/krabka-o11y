use super::*;

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
