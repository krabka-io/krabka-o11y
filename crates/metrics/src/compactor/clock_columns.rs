use super::{
    BooleanBuilder, CCOL_CLOCK, CCOL_EST_ERROR_NANOS, CCOL_FREQUENCY_PPB, CCOL_GM_CLOCK_ACCURACY,
    CCOL_GM_CLOCK_CLASS, CCOL_GNSS_FIX, CCOL_INGEST_UNIX_NANOS, CCOL_LAST_STEP_NANOS,
    CCOL_LAST_SYNC_UNIX_NANOS, CCOL_MAX_ERROR_NANOS, CCOL_MEAN_PATH_DELAY_NANOS, CCOL_NODE,
    CCOL_OFFSET_NANOS, CCOL_READING_UNIX_NANOS, CCOL_REFERENCE_ID, CCOL_ROOT_DELAY_NANOS,
    CCOL_ROOT_DISPERSION_NANOS, CCOL_SATELLITES_USED, CCOL_SOURCE_KIND, CCOL_STEPS_REMOVED,
    CCOL_STRATUM, CCOL_SYNC_STATE, CCOL_UNCERTAINTY_NANOS, CCOL_UNSYNCHRONIZED, COL_FINGERPRINT,
    COL_TIMESTAMP, ClockReadingRow, GnssFix, Int32Type, Int64Builder, StringDictionaryBuilder,
    UInt32Builder, UInt64Builder, UnixNanos,
};

/// Column builders for one clock reading block.
///
/// The derive creates each builder and pairs each finished array with its
/// column name. The caller orders those arrays by the clock schema.
#[derive(krabka_column_macros::ColumnBuilders)]
#[columns(named)]
pub(crate) struct ClockColumns {
    #[column(name = "COL_FINGERPRINT")]
    pub(crate) fingerprints: UInt64Builder,
    #[column(name = "COL_TIMESTAMP")]
    pub(crate) timestamps: Int64Builder,
    #[column(name = "CCOL_NODE")]
    pub(crate) nodes: StringDictionaryBuilder<Int32Type>,
    #[column(name = "CCOL_CLOCK")]
    pub(crate) clocks: StringDictionaryBuilder<Int32Type>,
    #[column(name = "CCOL_SOURCE_KIND")]
    pub(crate) source_kinds: StringDictionaryBuilder<Int32Type>,
    #[column(name = "CCOL_READING_UNIX_NANOS")]
    pub(crate) reading_unix_nanos: Int64Builder,
    #[column(name = "CCOL_UNCERTAINTY_NANOS")]
    pub(crate) uncertainty_nanos: Int64Builder,
    #[column(name = "CCOL_OFFSET_NANOS")]
    pub(crate) offset_nanos: Int64Builder,
    #[column(name = "CCOL_SYNC_STATE")]
    pub(crate) sync_states: StringDictionaryBuilder<Int32Type>,
    #[column(name = "CCOL_REFERENCE_ID")]
    pub(crate) reference_ids: StringDictionaryBuilder<Int32Type>,
    #[column(name = "CCOL_LAST_SYNC_UNIX_NANOS")]
    pub(crate) last_sync_unix_nanos: Int64Builder,
    #[column(name = "CCOL_FREQUENCY_PPB")]
    pub(crate) frequency_ppb: Int64Builder,
    #[column(name = "CCOL_LAST_STEP_NANOS")]
    pub(crate) last_step_nanos: Int64Builder,
    #[column(name = "CCOL_ROOT_DELAY_NANOS")]
    pub(crate) root_delay_nanos: Int64Builder,
    #[column(name = "CCOL_ROOT_DISPERSION_NANOS")]
    pub(crate) root_dispersion_nanos: Int64Builder,
    #[column(name = "CCOL_STRATUM")]
    pub(crate) stratum: UInt32Builder,
    #[column(name = "CCOL_MEAN_PATH_DELAY_NANOS")]
    pub(crate) mean_path_delay_nanos: Int64Builder,
    #[column(name = "CCOL_STEPS_REMOVED")]
    pub(crate) steps_removed: UInt32Builder,
    #[column(name = "CCOL_GM_CLOCK_CLASS")]
    pub(crate) gm_clock_class: UInt32Builder,
    #[column(name = "CCOL_GM_CLOCK_ACCURACY")]
    pub(crate) gm_clock_accuracy: UInt32Builder,
    #[column(name = "CCOL_MAX_ERROR_NANOS")]
    pub(crate) max_error_nanos: Int64Builder,
    #[column(name = "CCOL_EST_ERROR_NANOS")]
    pub(crate) est_error_nanos: Int64Builder,
    #[column(name = "CCOL_UNSYNCHRONIZED")]
    pub(crate) unsynchronized: BooleanBuilder,
    #[column(name = "CCOL_SATELLITES_USED")]
    pub(crate) satellites_used: UInt32Builder,
    #[column(name = "CCOL_GNSS_FIX")]
    pub(crate) gnss_fixes: StringDictionaryBuilder<Int32Type>,
    #[column(name = "CCOL_INGEST_UNIX_NANOS")]
    pub(crate) ingest_unix_nanos: Int64Builder,
}

impl ClockColumns {
    pub(crate) fn append(&mut self, row: &ClockReadingRow) {
        let reading = &row.reading.reading;
        self.fingerprints.append_value(row.fingerprint);
        self.timestamps.append_value(row.timestamp_ms);
        self.nodes.append_value(&reading.node);
        self.clocks.append_value(&reading.clock);
        self.source_kinds
            .append_value(reading.source_kind.as_label());
        self.reading_unix_nanos
            .append_value(reading.reading_unix_nanos.as_i64());
        self.uncertainty_nanos
            .append_value(reading.uncertainty_nanos);
        self.offset_nanos.append_value(reading.offset_nanos);
        self.sync_states.append_value(reading.sync_state.as_label());
        self.reference_ids
            .append_option(reading.reference_id.as_deref());
        self.last_sync_unix_nanos
            .append_option(reading.last_sync_unix_nanos.map(UnixNanos::as_i64));
        self.frequency_ppb.append_option(reading.frequency_ppb);
        self.last_step_nanos.append_option(reading.last_step_nanos);

        // A source-specific column stays null when this reading came from a
        // different kind of clock. The schema declares them nullable for
        // exactly that reason.
        self.root_delay_nanos
            .append_option(reading.ntp.map(|ntp| ntp.root_delay_nanos));
        self.root_dispersion_nanos
            .append_option(reading.ntp.map(|ntp| ntp.root_dispersion_nanos));
        self.stratum
            .append_option(reading.ntp.map(|ntp| ntp.stratum));

        self.mean_path_delay_nanos
            .append_option(reading.ptp.map(|ptp| ptp.mean_path_delay_nanos));
        self.steps_removed
            .append_option(reading.ptp.map(|ptp| ptp.steps_removed));
        self.gm_clock_class
            .append_option(reading.ptp.map(|ptp| ptp.gm_clock_class));
        self.gm_clock_accuracy
            .append_option(reading.ptp.map(|ptp| ptp.gm_clock_accuracy));

        self.max_error_nanos
            .append_option(reading.timex.map(|timex| timex.max_error_nanos));
        self.est_error_nanos
            .append_option(reading.timex.map(|timex| timex.est_error_nanos));
        self.unsynchronized
            .append_option(reading.timex.map(|timex| timex.unsynchronized));

        self.satellites_used
            .append_option(reading.gnss.map(|gnss| gnss.satellites_used));
        self.gnss_fixes.append_option(
            reading
                .gnss
                .and_then(|gnss| gnss.fix)
                .map(GnssFix::as_label),
        );

        self.ingest_unix_nanos
            .append_value(row.reading.ingest_unix_nanos.as_i64());
    }
}
