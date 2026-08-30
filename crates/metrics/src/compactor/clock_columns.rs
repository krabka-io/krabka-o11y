use super::{UInt64Builder, Int64Builder, StringDictionaryBuilder, Int32Type, UInt32Builder, BooleanBuilder, ClockReadingRow, UnixNanos, GnssFix, ArrayRef, COL_FINGERPRINT, Arc, COL_TIMESTAMP, CCOL_NODE, CCOL_CLOCK, CCOL_SOURCE_KIND, CCOL_READING_UNIX_NANOS, CCOL_UNCERTAINTY_NANOS, CCOL_OFFSET_NANOS, CCOL_SYNC_STATE, CCOL_REFERENCE_ID, CCOL_LAST_SYNC_UNIX_NANOS, CCOL_FREQUENCY_PPB, CCOL_LAST_STEP_NANOS, CCOL_ROOT_DELAY_NANOS, CCOL_ROOT_DISPERSION_NANOS, CCOL_STRATUM, CCOL_MEAN_PATH_DELAY_NANOS, CCOL_STEPS_REMOVED, CCOL_GM_CLOCK_CLASS, CCOL_GM_CLOCK_ACCURACY, CCOL_MAX_ERROR_NANOS, CCOL_EST_ERROR_NANOS, CCOL_UNSYNCHRONIZED, CCOL_SATELLITES_USED, CCOL_GNSS_FIX, CCOL_INGEST_UNIX_NANOS};

/// Column builders for one clock reading block.
///
/// The clock schema has 26 columns, so the builders travel in a struct rather
/// than as 26 locals. Every method appends to exactly one column, and
/// [`ClockColumns::finish`] hands them back in schema order.
pub(crate) struct ClockColumns {
    pub(crate) fingerprints: UInt64Builder,
    pub(crate) timestamps: Int64Builder,
    pub(crate) nodes: StringDictionaryBuilder<Int32Type>,
    pub(crate) clocks: StringDictionaryBuilder<Int32Type>,
    pub(crate) source_kinds: StringDictionaryBuilder<Int32Type>,
    pub(crate) reading_unix_nanos: Int64Builder,
    pub(crate) uncertainty_nanos: Int64Builder,
    pub(crate) offset_nanos: Int64Builder,
    pub(crate) sync_states: StringDictionaryBuilder<Int32Type>,
    pub(crate) reference_ids: StringDictionaryBuilder<Int32Type>,
    pub(crate) last_sync_unix_nanos: Int64Builder,
    pub(crate) frequency_ppb: Int64Builder,
    pub(crate) last_step_nanos: Int64Builder,
    pub(crate) root_delay_nanos: Int64Builder,
    pub(crate) root_dispersion_nanos: Int64Builder,
    pub(crate) stratum: UInt32Builder,
    pub(crate) mean_path_delay_nanos: Int64Builder,
    pub(crate) steps_removed: UInt32Builder,
    pub(crate) gm_clock_class: UInt32Builder,
    pub(crate) gm_clock_accuracy: UInt32Builder,
    pub(crate) max_error_nanos: Int64Builder,
    pub(crate) est_error_nanos: Int64Builder,
    pub(crate) unsynchronized: BooleanBuilder,
    pub(crate) satellites_used: UInt32Builder,
    pub(crate) gnss_fixes: StringDictionaryBuilder<Int32Type>,
    pub(crate) ingest_unix_nanos: Int64Builder,
}

impl ClockColumns {
    pub(crate) fn new() -> Self {
        Self {
            fingerprints: UInt64Builder::new(),
            timestamps: Int64Builder::new(),
            nodes: StringDictionaryBuilder::new(),
            clocks: StringDictionaryBuilder::new(),
            source_kinds: StringDictionaryBuilder::new(),
            reading_unix_nanos: Int64Builder::new(),
            uncertainty_nanos: Int64Builder::new(),
            offset_nanos: Int64Builder::new(),
            sync_states: StringDictionaryBuilder::new(),
            reference_ids: StringDictionaryBuilder::new(),
            last_sync_unix_nanos: Int64Builder::new(),
            frequency_ppb: Int64Builder::new(),
            last_step_nanos: Int64Builder::new(),
            root_delay_nanos: Int64Builder::new(),
            root_dispersion_nanos: Int64Builder::new(),
            stratum: UInt32Builder::new(),
            mean_path_delay_nanos: Int64Builder::new(),
            steps_removed: UInt32Builder::new(),
            gm_clock_class: UInt32Builder::new(),
            gm_clock_accuracy: UInt32Builder::new(),
            max_error_nanos: Int64Builder::new(),
            est_error_nanos: Int64Builder::new(),
            unsynchronized: BooleanBuilder::new(),
            satellites_used: UInt32Builder::new(),
            gnss_fixes: StringDictionaryBuilder::new(),
            ingest_unix_nanos: Int64Builder::new(),
        }
    }

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

    /// The finished arrays, each paired with the schema column it fills.
    ///
    /// The caller orders them by the schema rather than by this list, so a
    /// reordered schema stays correct and a column this list forgets becomes a
    /// build error instead of a silently shifted block.
    pub(crate) fn finish(mut self) -> Vec<(&'static str, ArrayRef)> {
        vec![
            (COL_FINGERPRINT, Arc::new(self.fingerprints.finish())),
            (COL_TIMESTAMP, Arc::new(self.timestamps.finish())),
            (CCOL_NODE, Arc::new(self.nodes.finish())),
            (CCOL_CLOCK, Arc::new(self.clocks.finish())),
            (CCOL_SOURCE_KIND, Arc::new(self.source_kinds.finish())),
            (
                CCOL_READING_UNIX_NANOS,
                Arc::new(self.reading_unix_nanos.finish()),
            ),
            (
                CCOL_UNCERTAINTY_NANOS,
                Arc::new(self.uncertainty_nanos.finish()),
            ),
            (CCOL_OFFSET_NANOS, Arc::new(self.offset_nanos.finish())),
            (CCOL_SYNC_STATE, Arc::new(self.sync_states.finish())),
            (CCOL_REFERENCE_ID, Arc::new(self.reference_ids.finish())),
            (
                CCOL_LAST_SYNC_UNIX_NANOS,
                Arc::new(self.last_sync_unix_nanos.finish()),
            ),
            (CCOL_FREQUENCY_PPB, Arc::new(self.frequency_ppb.finish())),
            (
                CCOL_LAST_STEP_NANOS,
                Arc::new(self.last_step_nanos.finish()),
            ),
            (
                CCOL_ROOT_DELAY_NANOS,
                Arc::new(self.root_delay_nanos.finish()),
            ),
            (
                CCOL_ROOT_DISPERSION_NANOS,
                Arc::new(self.root_dispersion_nanos.finish()),
            ),
            (CCOL_STRATUM, Arc::new(self.stratum.finish())),
            (
                CCOL_MEAN_PATH_DELAY_NANOS,
                Arc::new(self.mean_path_delay_nanos.finish()),
            ),
            (CCOL_STEPS_REMOVED, Arc::new(self.steps_removed.finish())),
            (CCOL_GM_CLOCK_CLASS, Arc::new(self.gm_clock_class.finish())),
            (
                CCOL_GM_CLOCK_ACCURACY,
                Arc::new(self.gm_clock_accuracy.finish()),
            ),
            (
                CCOL_MAX_ERROR_NANOS,
                Arc::new(self.max_error_nanos.finish()),
            ),
            (
                CCOL_EST_ERROR_NANOS,
                Arc::new(self.est_error_nanos.finish()),
            ),
            (CCOL_UNSYNCHRONIZED, Arc::new(self.unsynchronized.finish())),
            (
                CCOL_SATELLITES_USED,
                Arc::new(self.satellites_used.finish()),
            ),
            (CCOL_GNSS_FIX, Arc::new(self.gnss_fixes.finish())),
            (
                CCOL_INGEST_UNIX_NANOS,
                Arc::new(self.ingest_unix_nanos.finish()),
            ),
        ]
    }
}
