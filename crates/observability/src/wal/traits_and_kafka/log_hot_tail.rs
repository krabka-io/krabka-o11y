use super::*;

pub trait LogHotTail: Send + Sync + 'static {
    fn records(&self) -> Vec<WalLogRecord>;

    /// Returns the hot-tail records whose `timestamp_ns` falls within the
    /// inclusive window `[start_ns, end_ns]`.
    ///
    /// Callers re-apply their exact per-record time bound downstream, so this
    /// may return a *superset* of the in-window records, for example records
    /// that share a coarse time bucket with the window edges. It MUST NOT drop
    /// any record whose timestamp lies in `[start_ns, end_ns]`. The default
    /// implementation filters [`LogHotTail::records`] and keeps its order.
    /// Implementations that hold a time index, see [`BufferedLogHotTail`],
    /// override this to avoid a full-buffer scan.
    fn records_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<WalLogRecord> {
        self.records()
            .into_iter()
            .filter(|record| record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns)
            .collect()
    }
}
